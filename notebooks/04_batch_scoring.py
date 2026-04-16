"""
Notebook 04: Batch Scoring
===========================
Loads the champion safety stock model from MLflow registry,
scores the gold features table, computes SHAP-based driver explanations,
and writes results to serving layer Delta tables.

Outputs:
  serving/ss_recommendations   — model outputs with drivers
  serving/approval_requests    — empty table (schema only)
"""

import os
import sys
import uuid
import numpy as np
import pandas as pd
import mlflow
import mlflow.sklearn
import shap
from deltalake import DeltaTable, write_deltalake

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import DELTA_BASE_PATH, MLFLOW_TRACKING_URI, MODEL_NAME

FEATURE_COLS = [
    "demand_mean",
    "demand_std",
    "demand_cv",
    "lead_time_mean",
    "lead_time_std",
    "service_level_z",
    "abc_class_encoded",
    "category_encoded",
]

DRIVER_LABELS = {
    "demand_mean": "avg demand",
    "demand_std": "demand variability (σ)",
    "demand_cv": "demand CV",
    "lead_time_mean": "avg lead time",
    "lead_time_std": "lead time variability",
    "service_level_z": "service level target",
    "abc_class_encoded": "ABC classification",
    "category_encoded": "material category",
}

DIRECTION_EMOJI = {True: "↑", False: "↓"}


# ---------------------------------------------------------------------------
# Load model
# ---------------------------------------------------------------------------

def load_model():
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = mlflow.tracking.MlflowClient()

    # Get latest version of the registered model
    try:
        versions = client.search_model_versions(f"name='{MODEL_NAME}'")
        if not versions:
            raise ValueError(f"No versions found for model '{MODEL_NAME}'")
        latest = sorted(versions, key=lambda v: int(v.version), reverse=True)[0]
        model_uri = f"models:/{MODEL_NAME}/{latest.version}"
        print(f"  Loading model: {model_uri}")
        return mlflow.sklearn.load_model(model_uri)
    except Exception as e:
        print(f"  Warning: Could not load from registry ({e}). Using last run artifact.")
        runs = mlflow.search_runs(
            experiment_names=["safety-stock-estimation"],
            order_by=["start_time DESC"],
        )
        run_id = runs.iloc[0]["run_id"]
        return mlflow.sklearn.load_model(f"runs:/{run_id}/model")


# ---------------------------------------------------------------------------
# Score and compute SHAP drivers
# ---------------------------------------------------------------------------

def compute_shap_drivers(model, X: pd.DataFrame, feature_cols: list) -> pd.DataFrame:
    """Compute SHAP values and return top-3 drivers per row."""
    explainer = shap.TreeExplainer(model)
    shap_values = explainer.shap_values(X)

    results = []
    for i, row_shap in enumerate(shap_values):
        # Sort features by absolute SHAP value
        abs_shap = np.abs(row_shap)
        top_idx = np.argsort(abs_shap)[::-1][:3]

        drivers = []
        for idx in top_idx:
            feat = feature_cols[idx]
            direction = DIRECTION_EMOJI[row_shap[idx] > 0]
            label = DRIVER_LABELS.get(feat, feat)
            val = X.iloc[i][feat]
            drivers.append(f"{direction} {label} ({val:.2f})")

        results.append({
            "driver_1": drivers[0] if len(drivers) > 0 else "",
            "driver_2": drivers[1] if len(drivers) > 1 else "",
            "driver_3": drivers[2] if len(drivers) > 2 else "",
        })

    return pd.DataFrame(results)


def score_materials(gold: pd.DataFrame, model) -> pd.DataFrame:
    print("Scoring materials...")
    X = gold[FEATURE_COLS].fillna(0)

    # Point predictions
    predictions = model.predict(X)
    new_ss = np.round(predictions).clip(1).astype(int)

    # Confidence score via prediction std across trees (RandomForest)
    tree_preds = np.array([tree.predict(X) for tree in model.estimators_])
    pred_std = tree_preds.std(axis=0)
    # Normalize to a 0–1 confidence score (lower std = higher confidence)
    max_std = pred_std.max() if pred_std.max() > 0 else 1
    confidence = (1 - pred_std / max_std).clip(0, 1).round(3)

    # SHAP drivers
    print("Computing SHAP explanations...")
    drivers_df = compute_shap_drivers(model, X, FEATURE_COLS)

    # Assemble recommendations
    recs = gold[["material_id", "plant", "buyer_id", "material_desc",
                  "category", "abc_class", "current_ss"]].copy()
    recs["new_ss"] = new_ss
    recs["pct_change"] = ((recs["new_ss"] - recs["current_ss"]) / recs["current_ss"].clip(lower=1) * 100).round(1)
    recs["confidence_score"] = confidence
    recs = pd.concat([recs.reset_index(drop=True), drivers_df.reset_index(drop=True)], axis=1)
    recs["scored_at"] = pd.Timestamp.now()
    recs["status"] = "pending_review"

    return recs


# ---------------------------------------------------------------------------
# Write serving tables
# ---------------------------------------------------------------------------

def write_recommendations(recs: pd.DataFrame) -> None:
    path = os.path.join(DELTA_BASE_PATH, "serving", "ss_recommendations")
    os.makedirs(path, exist_ok=True)
    write_deltalake(path, recs, mode="overwrite")
    print(f"  Wrote {len(recs):,} recommendations → {path}")


def create_approval_requests_table() -> None:
    """Create empty approval_requests Delta table with correct schema."""
    schema = pd.DataFrame(columns=[
        "request_id",
        "material_id",
        "plant",
        "buyer_id",
        "current_ss",
        "new_ss",
        "pct_change",
        "driver_1",
        "driver_2",
        "driver_3",
        "status",       # pending / approved / rejected
        "submitted_at",
        "manager_id",
        "reviewed_at",
        "manager_comment",
    ])
    schema["current_ss"] = schema["current_ss"].astype("Int64")
    schema["new_ss"] = schema["new_ss"].astype("Int64")
    schema["pct_change"] = schema["pct_change"].astype(float)
    schema["submitted_at"] = pd.to_datetime(schema["submitted_at"])
    schema["reviewed_at"] = pd.to_datetime(schema["reviewed_at"])

    path = os.path.join(DELTA_BASE_PATH, "serving", "approval_requests")
    os.makedirs(path, exist_ok=True)
    write_deltalake(path, schema, mode="overwrite")
    print(f"  Created empty approval_requests table → {path}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print(f"\n{'='*60}")
    print("  Generac Safety Stock — Batch Scoring")
    print(f"{'='*60}\n")

    # Load gold
    gold_path = os.path.join(DELTA_BASE_PATH, "gold", "safety_stock_features")
    gold = DeltaTable(gold_path).to_pandas()
    print(f"  Gold features: {len(gold)} materials")

    # Load model
    print("Loading model from MLflow...")
    model = load_model()

    # Score
    recs = score_materials(gold, model)

    # Summary
    n_increase = (recs["pct_change"] > 5).sum()
    n_decrease = (recs["pct_change"] < -5).sum()
    n_stable = len(recs) - n_increase - n_decrease
    print(f"\n  SS increase (>5%):  {n_increase} materials")
    print(f"  SS decrease (<-5%): {n_decrease} materials")
    print(f"  SS stable:          {n_stable} materials")
    print(f"\nSample recommendations:")
    print(recs[["material_id", "current_ss", "new_ss", "pct_change", "driver_1"]].head(5).to_string(index=False))

    # Write
    print("\nWriting serving tables...")
    write_recommendations(recs)
    create_approval_requests_table()

    print("\nDone.")
