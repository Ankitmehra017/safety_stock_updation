"""
app/backend/db.py
==================
Data access layer for the Streamlit app.
Reads from local Delta tables (or Databricks when configured).
All functions return pandas DataFrames.
"""

import os
import sys
import pandas as pd
from pathlib import Path
from deltalake import DeltaTable

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config import DELTA_BASE_PATH


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _delta_path(layer: str, table: str) -> str:
    return os.path.join(DELTA_BASE_PATH, layer, table)


def _read(layer: str, table: str) -> pd.DataFrame:
    path = _delta_path(layer, table)
    return DeltaTable(path).to_pandas()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_buyers() -> pd.DataFrame:
    """Return all buyers (buyer_id, buyer_name, email, manager_id, material_ids)."""
    return _read("bronze", "buyers")


def get_materials() -> pd.DataFrame:
    """Return material master."""
    return _read("bronze", "materials")


def get_recommendations(buyer_id: str | None = None) -> pd.DataFrame:
    """
    Return SS recommendations, optionally filtered by buyer_id.

    Columns:
        material_id, plant, buyer_id, material_desc, category, abc_class,
        current_ss, new_ss, pct_change, confidence_score,
        driver_1, driver_2, driver_3, scored_at, status
    """
    df = _read("serving", "ss_recommendations")
    if buyer_id:
        df = df[df["buyer_id"] == buyer_id]
    return df.reset_index(drop=True)


def get_gold_features(material_ids: list[str] | None = None) -> pd.DataFrame:
    """Return gold feature table (used by Genie QA)."""
    df = _read("gold", "safety_stock_features")
    if material_ids:
        df = df[df["material_id"].isin(material_ids)]
    return df.reset_index(drop=True)


def get_weekly_demand(material_ids: list[str] | None = None) -> pd.DataFrame:
    """Return silver weekly demand for charting."""
    df = _read("silver", "demand_weekly")
    if material_ids:
        df = df[df["material_id"].isin(material_ids)]
    df["week_start"] = pd.to_datetime(df["week_start"])
    return df.sort_values("week_start")


def get_approval_requests(
    manager_id: str | None = None,
    status: str | None = None,
) -> pd.DataFrame:
    """Return approval requests, optionally filtered."""
    df = _read("serving", "approval_requests")
    if df.empty:
        return df
    if manager_id:
        # Filter by manager of the buyers involved
        buyers = get_buyers()
        mgr_buyer_ids = buyers[buyers["manager_id"] == manager_id]["buyer_id"].tolist()
        df = df[df["buyer_id"].isin(mgr_buyer_ids)]
    if status:
        df = df[df["status"] == status]
    return df.reset_index(drop=True)


def get_buyer_for_manager(manager_id: str) -> list[str]:
    """Return buyer_ids managed by this manager."""
    buyers = get_buyers()
    return buyers[buyers["manager_id"] == manager_id]["buyer_id"].tolist()


def get_managers() -> pd.DataFrame:
    """Return distinct manager IDs and names (derived from buyers table)."""
    buyers = get_buyers()
    managers = buyers[["manager_id"]].drop_duplicates().copy()
    name_map = {"MGR001": "Sarah Connor", "MGR002": "John Doe"}
    managers["manager_name"] = managers["manager_id"].map(name_map).fillna(managers["manager_id"])
    return managers
