"""
app/backend/db.py
==================
Data access layer for the Streamlit app.

Primary mode  : Databricks SQL connector → reads Unity Catalog tables
Fallback mode : local deltalake library  → reads Delta files from DELTA_BASE_PATH
                (used when DATABRICKS_HOST is not set)
"""

import os
import sys
import pandas as pd
from pathlib import Path
from functools import lru_cache

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config import (
    CATALOG, SCHEMA,
    DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH,
    DELTA_BASE_PATH,
)

_USE_DATABRICKS = bool(DATABRICKS_HOST and DATABRICKS_TOKEN and DATABRICKS_HTTP_PATH)


# ---------------------------------------------------------------------------
# Internal read helpers
# ---------------------------------------------------------------------------

def _read_databricks(table: str, where: str = "") -> pd.DataFrame:
    """Execute a SELECT via the Databricks SQL connector."""
    from databricks import sql as dbsql

    query = f"SELECT * FROM {CATALOG}.{SCHEMA}.{table}"
    if where:
        query += f" WHERE {where}"

    with dbsql.connect(
        server_hostname=DATABRICKS_HOST.replace("https://", ""),
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            return cursor.fetchall_arrow().to_pandas()


def _read_local(layer: str, table: str) -> pd.DataFrame:
    """Read a local Delta table using the deltalake library."""
    from deltalake import DeltaTable
    path = os.path.join(DELTA_BASE_PATH, layer, table)
    return DeltaTable(path).to_pandas()


def _read(table: str, where: str = "") -> pd.DataFrame:
    """Route to Databricks or local Delta depending on configuration."""
    if _USE_DATABRICKS:
        return _read_databricks(table, where)
    # Local fallback: infer layer from table name
    layer_map = {
        "materials":             "bronze",
        "historical_demand":     "bronze",
        "lead_times":            "bronze",
        "buyers":                "bronze",
        "current_safety_stock":  "bronze",
        "demand_weekly":         "silver",
        "lead_times_cleaned":    "silver",
        "safety_stock_features": "gold",
        "ss_recommendations":    "serving",
        "approval_requests":     "serving",
    }
    layer = layer_map.get(table, "serving")
    return _read_local(layer, table)


def _write_databricks(df: pd.DataFrame, table: str) -> None:
    """Overwrite a Unity Catalog table via spark (only available in Databricks env)."""
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.getActiveSession()
        if spark:
            sdf = spark.createDataFrame(df)
            (
                sdf.write.format("delta")
                   .mode("overwrite")
                   .option("overwriteSchema", "true")
                   .saveAsTable(f"{CATALOG}.{SCHEMA}.{table}")
            )
            return
    except Exception:
        pass
    # Fallback: use local deltalake
    _write_local(df, table)


def _write_local(df: pd.DataFrame, table: str) -> None:
    from deltalake import write_deltalake
    path = os.path.join(DELTA_BASE_PATH, "serving", table)
    os.makedirs(path, exist_ok=True)
    write_deltalake(path, df, mode="overwrite")


def _write(df: pd.DataFrame, table: str) -> None:
    if _USE_DATABRICKS:
        _write_databricks(df, table)
    else:
        _write_local(df, table)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_buyers() -> pd.DataFrame:
    return _read("buyers")


def get_materials() -> pd.DataFrame:
    return _read("materials")


def get_managers() -> pd.DataFrame:
    buyers = get_buyers()
    managers = buyers[["manager_id"]].drop_duplicates().copy()
    name_map = {"MGR001": "Sarah Connor", "MGR002": "John Doe"}
    managers["manager_name"] = managers["manager_id"].map(name_map).fillna(managers["manager_id"])
    return managers


def get_recommendations(buyer_id: str | None = None) -> pd.DataFrame:
    """
    Return SS recommendations, optionally filtered by buyer_id.
    Columns: material_id, plant, buyer_id, material_desc, category, abc_class,
             current_ss, new_ss, pct_change, confidence_score,
             driver_1, driver_2, driver_3, scored_at, status
    """
    df = _read("ss_recommendations")
    if buyer_id:
        df = df[df["buyer_id"] == buyer_id]
    return df.reset_index(drop=True)


def get_gold_features(material_ids: list[str] | None = None) -> pd.DataFrame:
    """Return gold feature table (used by Genie QA)."""
    df = _read("safety_stock_features")
    if material_ids:
        df = df[df["material_id"].isin(material_ids)]
    return df.reset_index(drop=True)


def get_weekly_demand(material_ids: list[str] | None = None) -> pd.DataFrame:
    """Return silver weekly demand for charting."""
    df = _read("demand_weekly")
    if material_ids:
        df = df[df["material_id"].isin(material_ids)]
    df["week_start"] = pd.to_datetime(df["week_start"])
    return df.sort_values("week_start")


def get_approval_requests(
    manager_id: str | None = None,
    status: str | None = None,
) -> pd.DataFrame:
    df = _read("approval_requests")
    if df.empty:
        return df
    if manager_id:
        buyers = get_buyers()
        mgr_buyer_ids = buyers[buyers["manager_id"] == manager_id]["buyer_id"].tolist()
        df = df[df["buyer_id"].isin(mgr_buyer_ids)]
    if status:
        df = df[df["status"] == status]
    return df.reset_index(drop=True)
