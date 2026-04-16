"""
config.py — Central configuration for Safety Stock Update system.

Reads from environment variables (or .env file).
"""

import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root
load_dotenv(dotenv_path=Path(__file__).parent / ".env")

# ---------------------------------------------------------------------------
# Unity Catalog (Databricks — primary mode)
# ---------------------------------------------------------------------------
CATALOG = os.getenv("DATABRICKS_CATALOG", "dev")
SCHEMA  = os.getenv("DATABRICKS_SCHEMA",  "safety_stock_gold")

# ---------------------------------------------------------------------------
# Databricks connectivity (SQL warehouse)
# ---------------------------------------------------------------------------
DATABRICKS_HOST      = os.getenv("DATABRICKS_HOST", "")          # e.g. https://adb-xxx.azuredatabricks.net
DATABRICKS_TOKEN     = os.getenv("DATABRICKS_TOKEN", "")          # personal access token
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH", "")      # SQL warehouse HTTP path

# ---------------------------------------------------------------------------
# MLflow (set to Databricks managed URI when running on a cluster)
# ---------------------------------------------------------------------------
MLFLOW_TRACKING_URI = os.getenv(
    "MLFLOW_TRACKING_URI",
    str(Path(__file__).parent / "data" / "mlruns"),   # local fallback
)
MODEL_NAME = "safety-stock-model"

# ---------------------------------------------------------------------------
# Local Delta Lake fallback (used when DATABRICKS_HOST is not set)
# ---------------------------------------------------------------------------
DELTA_BASE_PATH = os.getenv(
    "DELTA_TABLE_PATH",
    str(Path(__file__).parent / "data" / "delta"),
)

# ---------------------------------------------------------------------------
# Anthropic (Genie QA agent)
# ---------------------------------------------------------------------------
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL      = "claude-sonnet-4-6"

# ---------------------------------------------------------------------------
# App settings
# ---------------------------------------------------------------------------
APP_TITLE = "Generac Safety Stock Update"
APP_ICON  = "🏭"
