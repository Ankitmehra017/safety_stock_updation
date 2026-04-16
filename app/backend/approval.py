"""
app/backend/approval.py
========================
Approval workflow CRUD operations.
Reads/writes the serving/approval_requests Delta table on local disk.

Note: Approval state is managed as app-side local Delta files so it works
consistently whether the Streamlit app runs locally or on a Databricks cluster.
The Unity Catalog `approval_requests` table (created in notebook 04) holds the
initial empty schema; runtime state is written here via deltalake.
"""

import os
import sys
import uuid
import pandas as pd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config import DELTA_BASE_PATH

APPROVAL_TABLE_PATH = os.path.join(DELTA_BASE_PATH, "serving", "approval_requests")


def _read_requests() -> pd.DataFrame:
    try:
        from deltalake import DeltaTable
        dt = DeltaTable(APPROVAL_TABLE_PATH)
        df = dt.to_pandas()
        if "submitted_at" in df.columns:
            df["submitted_at"] = pd.to_datetime(df["submitted_at"], errors="coerce")
        if "reviewed_at" in df.columns:
            df["reviewed_at"] = pd.to_datetime(df["reviewed_at"], errors="coerce")
        return df
    except Exception:
        return pd.DataFrame()


def _write_requests(df: pd.DataFrame) -> None:
    from deltalake import write_deltalake
    os.makedirs(APPROVAL_TABLE_PATH, exist_ok=True)
    write_deltalake(APPROVAL_TABLE_PATH, df, mode="overwrite")


def submit_for_approval(
    material_id: str,
    plant: str,
    buyer_id: str,
    current_ss: int,
    new_ss: int,
    pct_change: float,
    driver_1: str = "",
    driver_2: str = "",
    driver_3: str = "",
) -> str:
    """
    Create a new approval request.
    Returns the generated request_id (or the existing one if already pending).
    """
    existing = _read_requests()

    # Prevent duplicate submissions
    if not existing.empty:
        dup = existing[
            (existing["material_id"] == material_id)
            & (existing["plant"] == plant)
            & (existing["status"] == "pending")
        ]
        if not dup.empty:
            return dup.iloc[0]["request_id"]

    request_id = str(uuid.uuid4())
    new_row = pd.DataFrame([{
        "request_id":      request_id,
        "material_id":     material_id,
        "plant":           plant,
        "buyer_id":        buyer_id,
        "current_ss":      int(current_ss),
        "new_ss":          int(new_ss),
        "pct_change":      float(pct_change),
        "driver_1":        driver_1,
        "driver_2":        driver_2,
        "driver_3":        driver_3,
        "status":          "pending",
        "submitted_at":    pd.Timestamp.now(),
        "manager_id":      None,
        "reviewed_at":     pd.NaT,
        "manager_comment": None,
    }])

    combined = pd.concat([existing, new_row], ignore_index=True)
    _write_requests(combined)
    return request_id


def submit_bulk(rows: list[dict]) -> list[str]:
    """Submit multiple approval requests at once."""
    return [submit_for_approval(**row) for row in rows]


def approve_request(request_id: str, manager_id: str, comment: str = "") -> bool:
    """Manager approves a request. Returns True on success."""
    df = _read_requests()
    if df.empty:
        return False
    mask = df["request_id"] == request_id
    if not mask.any():
        return False
    df.loc[mask, "status"]          = "approved"
    df.loc[mask, "manager_id"]      = manager_id
    df.loc[mask, "reviewed_at"]     = pd.Timestamp.now()
    df.loc[mask, "manager_comment"] = comment
    _write_requests(df)
    return True


def reject_request(request_id: str, manager_id: str, comment: str = "") -> bool:
    """Manager rejects a request. Returns True on success."""
    df = _read_requests()
    if df.empty:
        return False
    mask = df["request_id"] == request_id
    if not mask.any():
        return False
    df.loc[mask, "status"]          = "rejected"
    df.loc[mask, "manager_id"]      = manager_id
    df.loc[mask, "reviewed_at"]     = pd.Timestamp.now()
    df.loc[mask, "manager_comment"] = comment
    _write_requests(df)
    return True


def get_status_summary() -> dict:
    """Return count of requests by status."""
    df = _read_requests()
    if df.empty:
        return {"pending": 0, "approved": 0, "rejected": 0}
    return df["status"].value_counts().to_dict()
