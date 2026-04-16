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
# Delta Lake paths (local mode)
# ---------------------------------------------------------------------------
DELTA_BASE_PATH = os.getenv(
    "DELTA_TABLE_PATH",
    str(Path(__file__).parent / "data" / "delta"),
)

# ---------------------------------------------------------------------------
# MLflow
# ---------------------------------------------------------------------------
MLFLOW_TRACKING_URI = os.getenv(
    "MLFLOW_TRACKING_URI",
    str(Path(__file__).parent / "data" / "mlruns"),
)
MODEL_NAME = "safety-stock-model"

# ---------------------------------------------------------------------------
# Anthropic (Genie QA agent)
# ---------------------------------------------------------------------------
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL = "claude-sonnet-4-6"

# ---------------------------------------------------------------------------
# Databricks (optional — used when DATABRICKS_HOST is set)
# ---------------------------------------------------------------------------
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "")

# ---------------------------------------------------------------------------
# App settings
# ---------------------------------------------------------------------------
APP_TITLE = "Generac Safety Stock Update"
APP_ICON = "🏭"
