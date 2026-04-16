"""
Notebook 02: Medallion Pipeline — Bronze → Silver → Gold
=========================================================
Transforms raw bronze data into feature-rich gold tables.

Bronze → Silver:
  - Remove demand outliers (IQR method)
  - Fill demand gaps
  - Aggregate daily demand → weekly
  - Compute lead time statistics

Silver → Gold:
  - demand_mean, demand_std, demand_cv
  - lead_time_mean, lead_time_std
  - service_level_z (Z-score from target)
  - abc_class_encoded, category_encoded
  - current_ss (joined)
  - buyer_id (joined)

Output: gold/safety_stock_features
"""

import os
import sys
import numpy as np
import pandas as pd
from scipy import stats
from deltalake import DeltaTable, write_deltalake

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import DELTA_BASE_PATH


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def read_delta(layer: str, table: str) -> pd.DataFrame:
    path = os.path.join(DELTA_BASE_PATH, layer, table)
    dt = DeltaTable(path)
    return dt.to_pandas()


def write_delta(df: pd.DataFrame, layer: str, table: str) -> None:
    path = os.path.join(DELTA_BASE_PATH, layer, table)
    os.makedirs(path, exist_ok=True)
    write_deltalake(path, df, mode="overwrite")
    print(f"  Wrote {len(df):,} rows → {path}")


# ---------------------------------------------------------------------------
# Bronze → Silver: Demand
# ---------------------------------------------------------------------------

def build_silver_demand(demand_raw: pd.DataFrame) -> pd.DataFrame:
    print("Building silver demand (outlier removal + weekly aggregation)...")

    df = demand_raw.copy()
    df["date"] = pd.to_datetime(df["date"])

    # Remove outliers per material using IQR
    def remove_outliers(group):
        q1 = group["demand_qty"].quantile(0.25)
        q3 = group["demand_qty"].quantile(0.75)
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        return group[
            (group["demand_qty"] >= lower) & (group["demand_qty"] <= upper)
        ]

    df = df.groupby(["material_id", "plant"], group_keys=False).apply(remove_outliers)

    # Aggregate to weekly demand
    df["week"] = df["date"].dt.to_period("W").apply(lambda r: r.start_time)
    weekly = (
        df.groupby(["material_id", "plant", "week"])["demand_qty"]
        .sum()
        .reset_index()
        .rename(columns={"demand_qty": "weekly_demand", "week": "week_start"})
    )

    weekly["week_start"] = pd.to_datetime(weekly["week_start"])
    weekly["cleaned_at"] = pd.Timestamp.now()
    return weekly


# ---------------------------------------------------------------------------
# Bronze → Silver: Lead Times
# ---------------------------------------------------------------------------

def build_silver_lead_times(lt_raw: pd.DataFrame) -> pd.DataFrame:
    print("Building silver lead times...")

    df = lt_raw.copy()
    df["po_date"] = pd.to_datetime(df["po_date"])

    # Remove extreme outliers (> 3 sigma per material)
    def remove_lt_outliers(group):
        if len(group) < 4:
            return group
        z = np.abs(stats.zscore(group["lead_time_days"]))
        return group[z < 3]

    df = df.groupby(["material_id", "plant"], group_keys=False).apply(
        remove_lt_outliers
    )
    df["cleaned_at"] = pd.Timestamp.now()
    return df


# ---------------------------------------------------------------------------
# Silver → Gold: Feature Engineering
# ---------------------------------------------------------------------------

SERVICE_LEVEL_Z_MAP = {
    0.90: 1.282,
    0.95: 1.645,
    0.99: 2.326,
}

CATEGORY_ENCODING = {
    "Generators": 0,
    "Engines": 1,
    "Electrical": 2,
    "Controls": 3,
    "Accessories": 4,
}

ABC_ENCODING = {"A": 2, "B": 1, "C": 0}


def build_gold_features(
    silver_demand: pd.DataFrame,
    silver_lt: pd.DataFrame,
    materials: pd.DataFrame,
    current_ss: pd.DataFrame,
    buyers: pd.DataFrame,
) -> pd.DataFrame:
    print("Building gold features...")

    # --- Demand features ---
    demand_stats = (
        silver_demand.groupby(["material_id", "plant"])["weekly_demand"]
        .agg(
            demand_mean="mean",
            demand_std="std",
            demand_min="min",
            demand_max="max",
            n_weeks="count",
        )
        .reset_index()
    )
    demand_stats["demand_std"] = demand_stats["demand_std"].fillna(0)
    demand_stats["demand_cv"] = np.where(
        demand_stats["demand_mean"] > 0,
        demand_stats["demand_std"] / demand_stats["demand_mean"],
        0,
    )

    # --- Lead time features ---
    lt_stats = (
        silver_lt.groupby(["material_id", "plant"])["lead_time_days"]
        .agg(
            lead_time_mean="mean",
            lead_time_std="std",
            lead_time_min="min",
            lead_time_max="max",
            n_pos="count",
        )
        .reset_index()
    )
    lt_stats["lead_time_std"] = lt_stats["lead_time_std"].fillna(0)

    # --- Join everything ---
    gold = demand_stats.merge(lt_stats, on=["material_id", "plant"], how="left")
    gold = gold.merge(
        materials[["material_id", "plant", "category", "abc_class", "service_level_target", "material_desc"]],
        on=["material_id", "plant"],
        how="left",
    )
    gold = gold.merge(
        current_ss[["material_id", "plant", "current_ss"]],
        on=["material_id", "plant"],
        how="left",
    )

    # Buyer mapping (expand comma-separated material_ids)
    buyer_map_rows = []
    for _, buyer in buyers.iterrows():
        for mid in buyer["material_ids"].split(","):
            buyer_map_rows.append({"material_id": mid.strip(), "buyer_id": buyer["buyer_id"]})
    buyer_map = pd.DataFrame(buyer_map_rows)
    gold = gold.merge(buyer_map, on="material_id", how="left")

    # --- Encoded features ---
    gold["service_level_z"] = gold["service_level_target"].map(SERVICE_LEVEL_Z_MAP).fillna(1.645)
    gold["abc_class_encoded"] = gold["abc_class"].map(ABC_ENCODING).fillna(0)
    gold["category_encoded"] = gold["category"].map(CATEGORY_ENCODING).fillna(0)

    # Fill any nulls from join
    gold["lead_time_mean"] = gold["lead_time_mean"].fillna(14.0)
    gold["lead_time_std"] = gold["lead_time_std"].fillna(3.0)
    gold["current_ss"] = gold["current_ss"].fillna(10)

    gold["feature_computed_at"] = pd.Timestamp.now()

    # Reorder columns
    cols = [
        "material_id", "plant", "buyer_id", "material_desc",
        "category", "abc_class", "service_level_target",
        "demand_mean", "demand_std", "demand_cv", "demand_min", "demand_max", "n_weeks",
        "lead_time_mean", "lead_time_std", "lead_time_min", "lead_time_max", "n_pos",
        "current_ss",
        "service_level_z", "abc_class_encoded", "category_encoded",
        "feature_computed_at",
    ]
    return gold[cols]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print(f"\n{'='*60}")
    print("  Generac Safety Stock — Medallion Pipeline")
    print(f"{'='*60}\n")

    print("Reading bronze tables...")
    demand_raw = read_delta("bronze", "historical_demand")
    lt_raw = read_delta("bronze", "lead_times")
    materials = read_delta("bronze", "materials")
    current_ss = read_delta("bronze", "current_safety_stock")
    buyers = read_delta("bronze", "buyers")

    print(f"  demand rows: {len(demand_raw):,}")
    print(f"  lead time rows: {len(lt_raw):,}")

    print("\nBuilding silver layer...")
    silver_demand = build_silver_demand(demand_raw)
    write_delta(silver_demand, "silver", "demand_weekly")

    silver_lt = build_silver_lead_times(lt_raw)
    write_delta(silver_lt, "silver", "lead_times_cleaned")

    print("\nBuilding gold layer...")
    gold = build_gold_features(silver_demand, silver_lt, materials, current_ss, buyers)
    write_delta(gold, "gold", "safety_stock_features")

    print(f"\nSample gold features:")
    print(gold[["material_id", "demand_mean", "demand_cv", "lead_time_mean", "current_ss"]].head(5).to_string(index=False))

    print(f"\nDone. Gold table: {DELTA_BASE_PATH}/gold/safety_stock_features/")
