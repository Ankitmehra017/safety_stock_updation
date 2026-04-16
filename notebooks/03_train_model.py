"""
Notebook 03: Train Safety Stock Model
======================================
Trains a RandomForest model on gold features to predict optimal safety stock.

Ground truth: classical safety stock formula
    optimal_ss = Z * sqrt(lead_time_mean * demand_std² + demand_mean² * lead_time_std²)

Features used:
    demand_mean, demand_std, demand_cv,
    lead_time_mean, lead_time_std,
    service_level_z, abc_class_encoded, category_encoded

Model logged + registered in local MLflow tracking.
"""

import os
import sys
import numpy as np
import pandas as pd
import mlflow
import mlflow.sklearn
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
from deltalake import DeltaTable

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import DELTA_BASE_PATH, MLFLOW_TRACKING_URI, MODEL_NAME

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
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
TARGET_COL = "optimal_ss"
TEST_SIZE = 0.2
RANDOM_STATE = 42


# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------

def load_gold_features() -> pd.DataFrame:
    path = os.path.join(DELTA_BASE_PATH, "gold", "safety_stock_features")
    dt = DeltaTable(path)
    return dt.to_pandas()


# ---------------------------------------------------------------------------
# Generate ground-truth labels using classical SS formula
# ---------------------------------------------------------------------------

def compute_optimal_ss(df: pd.DataFrame) -> pd.DataFrame:
    """
    Classical safety stock formula:
        SS = Z * sqrt(L * σ_d² + d̄² * σ_L²)

    Where:
        Z    = service level Z-score
        L    = mean lead time (in weeks; convert from days / 7)
        σ_d  = weekly demand std dev
        d̄    = mean weekly demand
        σ_L  = lead time std dev (in weeks)
    """
    df = df.copy()
    L = df["lead_time_mean"] / 7.0       # days → weeks
    sigma_L = df["lead_time_std"] / 7.0  # days → weeks
    Z = df["service_level_z"]
    sigma_d = df["demand_std"]
    d_bar = df["demand_mean"]

    variance = L * (sigma_d ** 2) + (d_bar ** 2) * (sigma_L ** 2)
    df[TARGET_COL] = (Z * np.sqrt(variance)).clip(lower=1).round().astype(int)

    # Add small noise to make it a realistic regression problem
    rng = np.random.default_rng(42)
    noise = rng.normal(0, 0.05 * df[TARGET_COL], size=len(df))
    df[TARGET_COL] = (df[TARGET_COL] + noise).clip(lower=1).round().astype(int)

    return df


# ---------------------------------------------------------------------------
# Train
# ---------------------------------------------------------------------------

def train(df: pd.DataFrame) -> tuple:
    X = df[FEATURE_COLS].fillna(0)
    y = df[TARGET_COL]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=TEST_SIZE, random_state=RANDOM_STATE
    )

    model = RandomForestRegressor(
        n_estimators=200,
        max_depth=10,
        min_samples_leaf=2,
        n_jobs=-1,
        random_state=RANDOM_STATE,
    )
    model.fit(X_train, y_train)

    y_pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)

    cv_scores = cross_val_score(model, X, y, cv=5, scoring="r2")

    print(f"\n  MAE  : {mae:.2f}")
    print(f"  RMSE : {rmse:.2f}")
    print(f"  R²   : {r2:.4f}")
    print(f"  CV R²: {cv_scores.mean():.4f} ± {cv_scores.std():.4f}")

    return model, {"mae": mae, "rmse": rmse, "r2": r2, "cv_r2_mean": cv_scores.mean()}


# ---------------------------------------------------------------------------
# Feature importance
# ---------------------------------------------------------------------------

def log_feature_importance(model: RandomForestRegressor) -> None:
    importances = pd.Series(
        model.feature_importances_, index=FEATURE_COLS
    ).sort_values(ascending=False)
    print("\n  Feature importances:")
    for feat, imp in importances.items():
        bar = "█" * int(imp * 40)
        print(f"    {feat:<25} {imp:.4f}  {bar}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print(f"\n{'='*60}")
    print("  Generac Safety Stock — Model Training")
    print(f"{'='*60}\n")

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment("safety-stock-estimation")

    print("Loading gold features...")
    gold = load_gold_features()
    print(f"  {len(gold)} materials")

    print("Computing ground truth labels (classical SS formula)...")
    gold = compute_optimal_ss(gold)
    print(f"  optimal_ss range: {gold[TARGET_COL].min()} – {gold[TARGET_COL].max()}")
    print(f"  optimal_ss mean : {gold[TARGET_COL].mean():.1f}")

    print("\nTraining RandomForest model...")
    with mlflow.start_run(run_name="rf-safety-stock") as run:
        model, metrics = train(gold)

        # Log params
        mlflow.log_params({
            "n_estimators": 200,
            "max_depth": 10,
            "min_samples_leaf": 2,
            "features": ",".join(FEATURE_COLS),
            "target": TARGET_COL,
        })

        # Log metrics
        mlflow.log_metrics(metrics)

        # Log feature importances as artifact
        log_feature_importance(model)

        # Log model
        mlflow.sklearn.log_model(
            model,
            artifact_path="model",
            registered_model_name=MODEL_NAME,
            input_example=gold[FEATURE_COLS].head(1),
        )

        run_id = run.info.run_id
        print(f"\n  MLflow run_id: {run_id}")

    print(f"\nModel registered as: {MODEL_NAME}")
    print(f"MLflow tracking: {MLFLOW_TRACKING_URI}")
    print("\nDone.")
