"""
Notebook 01: Create Dummy Data
==============================
Seeds Bronze Delta tables with synthetic Generac material data.
Run this once to initialize the data layer.

Tables created:
  bronze/materials
  bronze/historical_demand
  bronze/lead_times
  bronze/buyers
  bronze/current_safety_stock
"""

import os
import sys
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from deltalake import DeltaTable, write_deltalake
import pyarrow as pa

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from config import DELTA_BASE_PATH

SEED = 42
rng = np.random.default_rng(SEED)

N_MATERIALS = 100
PLANTS = ["P001", "P002", "P003"]
CATEGORIES = ["Generators", "Engines", "Electrical", "Controls", "Accessories"]
ABC_CLASSES = ["A", "B", "C"]
SERVICE_LEVEL_MAP = {"A": 0.99, "B": 0.95, "C": 0.90}

N_BUYERS = 5
DEMAND_START = datetime(2023, 1, 1)
DEMAND_END = datetime(2024, 12, 31)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def delta_path(layer: str, table: str) -> str:
    path = os.path.join(DELTA_BASE_PATH, layer, table)
    os.makedirs(path, exist_ok=True)
    return path


def write_table(df: pd.DataFrame, layer: str, table: str) -> None:
    path = delta_path(layer, table)
    write_deltalake(path, df, mode="overwrite")
    print(f"  Wrote {len(df):,} rows → {path}")


# ---------------------------------------------------------------------------
# 1. Materials
# ---------------------------------------------------------------------------

def create_materials() -> pd.DataFrame:
    print("Creating materials master...")
    material_ids = [f"M{str(i).zfill(4)}" for i in range(1, N_MATERIALS + 1)]
    categories = rng.choice(CATEGORIES, size=N_MATERIALS)
    abc_classes = rng.choice(ABC_CLASSES, size=N_MATERIALS, p=[0.2, 0.3, 0.5])
    plants = rng.choice(PLANTS, size=N_MATERIALS)

    df = pd.DataFrame({
        "material_id": material_ids,
        "material_desc": [
            f"{cat} Component {i}" for i, cat in enumerate(categories, 1)
        ],
        "plant": plants,
        "category": categories,
        "abc_class": abc_classes,
        "service_level_target": [SERVICE_LEVEL_MAP[c] for c in abc_classes],
        "unit_of_measure": rng.choice(["EA", "PCE", "KG", "SET"], size=N_MATERIALS),
        "created_at": pd.Timestamp("2022-01-01"),
    })
    write_table(df, "bronze", "materials")
    return df


# ---------------------------------------------------------------------------
# 2. Historical Demand (daily, 2 years)
# ---------------------------------------------------------------------------

def create_historical_demand(materials: pd.DataFrame) -> pd.DataFrame:
    print("Creating historical demand (~73k rows)...")
    dates = pd.date_range(DEMAND_START, DEMAND_END, freq="D")

    rows = []
    for _, mat in materials.iterrows():
        mid = mat["material_id"]
        cat = mat["category"]
        abc = mat["abc_class"]

        # Base demand profile per category/ABC class
        base = {"A": 50, "B": 20, "C": 5}[abc]
        noise_factor = {"A": 0.15, "B": 0.30, "C": 0.50}[abc]

        # Seasonal multiplier (summer peak for generators)
        seasonal = 1.0 + 0.3 * np.sin(
            2 * np.pi * (np.arange(len(dates)) / 365 - 0.25)
        )
        trend = 1.0 + 0.05 * (np.arange(len(dates)) / 365)  # 5% annual growth
        demand = (
            base * seasonal * trend
            + rng.normal(0, base * noise_factor, size=len(dates))
        ).clip(0)

        # Introduce occasional zero-demand days (stock-outs / weekends)
        zero_mask = rng.random(len(dates)) < 0.10
        demand[zero_mask] = 0

        for date, qty in zip(dates, demand):
            rows.append({
                "material_id": mid,
                "plant": mat["plant"],
                "date": date,
                "demand_qty": round(float(qty), 2),
                "source_system": "SAP",
            })

    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    write_table(df, "bronze", "historical_demand")
    return df


# ---------------------------------------------------------------------------
# 3. Lead Times (PO-level, ~5-10 POs per material over 2 years)
# ---------------------------------------------------------------------------

def create_lead_times(materials: pd.DataFrame) -> pd.DataFrame:
    print("Creating lead times...")
    rows = []
    for _, mat in materials.iterrows():
        mid = mat["material_id"]
        abc = mat["abc_class"]

        # A-class: tighter lead times; C-class: more variable
        lt_mean = {"A": 7, "B": 14, "C": 21}[abc]
        lt_std = {"A": 2, "B": 5, "C": 10}[abc]
        n_pos = rng.integers(8, 20)

        po_dates = pd.to_datetime(
            DEMAND_START
            + (rng.random(n_pos) * (DEMAND_END - DEMAND_START).days).astype(int).tolist(),
            unit="D",
            origin=DEMAND_START,
        )
        lead_times = rng.normal(lt_mean, lt_std, size=n_pos).clip(1, 60).astype(int)

        for po_date, lt in zip(po_dates, lead_times):
            rows.append({
                "material_id": mid,
                "plant": mat["plant"],
                "po_date": po_date,
                "lead_time_days": int(lt),
                "vendor_id": f"V{rng.integers(100, 999)}",
            })

    df = pd.DataFrame(rows)
    df["po_date"] = pd.to_datetime(df["po_date"])
    write_table(df, "bronze", "lead_times")
    return df


# ---------------------------------------------------------------------------
# 4. Buyers
# ---------------------------------------------------------------------------

def create_buyers(materials: pd.DataFrame) -> pd.DataFrame:
    print("Creating buyers...")
    buyer_names = [
        "Alice Johnson", "Bob Smith", "Carol White", "David Lee", "Emma Davis"
    ]
    material_ids = materials["material_id"].tolist()
    rng.shuffle(material_ids)
    splits = np.array_split(material_ids, N_BUYERS)

    rows = []
    for i, (name, mat_list) in enumerate(zip(buyer_names, splits), 1):
        rows.append({
            "buyer_id": f"B{str(i).zfill(3)}",
            "buyer_name": name,
            "email": f"{name.lower().replace(' ', '.')}@generac.com",
            "manager_id": "MGR001" if i <= 3 else "MGR002",
            "material_ids": ",".join(mat_list.tolist()),  # comma-sep for Delta compat
            "active": True,
        })

    df = pd.DataFrame(rows)
    write_table(df, "bronze", "buyers")
    return df


# ---------------------------------------------------------------------------
# 5. Current Safety Stock
# ---------------------------------------------------------------------------

def create_current_safety_stock(materials: pd.DataFrame) -> pd.DataFrame:
    print("Creating current safety stock...")
    rows = []
    for _, mat in materials.iterrows():
        abc = mat["abc_class"]
        base_ss = {"A": 100, "B": 50, "C": 20}[abc]
        current_ss = int(rng.normal(base_ss, base_ss * 0.3).clip(1))
        rows.append({
            "material_id": mat["material_id"],
            "plant": mat["plant"],
            "current_ss": current_ss,
            "last_updated": pd.Timestamp("2024-01-01"),
            "last_updated_by": "SYSTEM",
        })

    df = pd.DataFrame(rows)
    write_table(df, "bronze", "current_safety_stock")
    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    print(f"\n{'='*60}")
    print("  Generac Safety Stock — Bronze Layer Seed")
    print(f"{'='*60}\n")

    materials = create_materials()
    create_historical_demand(materials)
    create_lead_times(materials)
    create_buyers(materials)
    create_current_safety_stock(materials)

    print(f"\nDone. Delta tables written to: {DELTA_BASE_PATH}/bronze/")
