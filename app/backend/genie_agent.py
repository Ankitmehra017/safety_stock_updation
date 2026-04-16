"""
app/backend/genie_agent.py
===========================
Genie QA Agent — Claude-powered text-to-SQL.

Given a natural language question, the agent:
  1. Generates a SQL query against the gold features / weekly demand tables
  2. Executes it locally against a DuckDB in-memory view of the Delta tables
  3. Returns the SQL, raw results, and a narrative explanation

We use DuckDB for local SQL execution because it natively reads Parquet
files (the format underlying Delta Lake).
"""

import os
import sys
import textwrap
import pandas as pd
import duckdb
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config import DELTA_BASE_PATH, ANTHROPIC_API_KEY, CLAUDE_MODEL

try:
    import anthropic
    _anthropic_available = bool(ANTHROPIC_API_KEY)
except ImportError:
    _anthropic_available = False


# ---------------------------------------------------------------------------
# Schema documentation for the system prompt
# ---------------------------------------------------------------------------

GOLD_SCHEMA = """
Table: gold_features
Columns:
  material_id        TEXT       -- unique material identifier (e.g. M0001)
  plant              TEXT       -- plant code (P001, P002, P003)
  buyer_id           TEXT       -- buyer responsible for this material
  material_desc      TEXT       -- human-readable description
  category           TEXT       -- Generators | Engines | Electrical | Controls | Accessories
  abc_class          TEXT       -- A (high-value) | B | C (low-value)
  service_level_target REAL     -- target service level (0.90, 0.95, 0.99)
  demand_mean        REAL       -- mean weekly demand (units/week)
  demand_std         REAL       -- std dev of weekly demand
  demand_cv          REAL       -- coefficient of variation = std/mean (0..∞; >0.5 = highly variable)
  demand_min         REAL       -- minimum weekly demand observed
  demand_max         REAL       -- maximum weekly demand observed
  n_weeks            INTEGER    -- number of weeks of demand history
  lead_time_mean     REAL       -- mean supplier lead time (days)
  lead_time_std      REAL       -- std dev of lead time (days)
  lead_time_min      REAL       -- minimum lead time
  lead_time_max      REAL       -- maximum lead time
  n_pos              INTEGER    -- number of purchase orders observed
  current_ss         INTEGER    -- current safety stock level in SAP
  service_level_z    REAL       -- Z-score for service level target
  abc_class_encoded  INTEGER    -- A=2, B=1, C=0
  category_encoded   INTEGER    -- numeric encoding of category

Table: weekly_demand
Columns:
  material_id        TEXT
  plant              TEXT
  week_start         DATE       -- start of the ISO week
  weekly_demand      REAL       -- total demand for the week (units)
"""

SYSTEM_PROMPT = f"""You are Genie, a supply chain data analyst AI for Generac.
You answer questions about safety stock, demand patterns, and lead times
by writing SQL queries against these tables:

{GOLD_SCHEMA}

Rules:
1. Write valid DuckDB SQL (standard SQL with DuckDB extensions).
2. Always SELECT only the columns needed to answer the question.
3. Round numeric results to 2 decimal places where useful.
4. Keep queries concise — avoid unnecessary joins.
5. If the question is ambiguous, make a reasonable assumption and note it.
6. Return ONLY a JSON object with these two keys:
   - "sql": the SQL query string
   - "explanation": a 1-2 sentence plain-English answer template
     (use {{result}} as a placeholder where the query result will be shown)

Example output:
{{"sql": "SELECT material_id, demand_cv FROM gold_features WHERE demand_cv > 0.5 ORDER BY demand_cv DESC LIMIT 10", "explanation": "Materials with the highest demand variability (CV > 0.5): {{result}}"}}
"""


# ---------------------------------------------------------------------------
# SQL executor using DuckDB
# ---------------------------------------------------------------------------

def _load_duckdb_views() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with views over Delta Parquet files."""
    con = duckdb.connect(":memory:")

    gold_parquet = os.path.join(DELTA_BASE_PATH, "gold", "safety_stock_features", "*.parquet")
    demand_parquet = os.path.join(DELTA_BASE_PATH, "silver", "demand_weekly", "*.parquet")

    con.execute(f"CREATE VIEW gold_features AS SELECT * FROM read_parquet('{gold_parquet}')")
    con.execute(f"CREATE VIEW weekly_demand AS SELECT * FROM read_parquet('{demand_parquet}')")
    return con


def _execute_sql(sql: str) -> pd.DataFrame:
    con = _load_duckdb_views()
    try:
        return con.execute(sql).df()
    except Exception as e:
        return pd.DataFrame({"error": [str(e)]})
    finally:
        con.close()


# ---------------------------------------------------------------------------
# Claude-powered SQL generation
# ---------------------------------------------------------------------------

def _generate_sql_with_claude(question: str, material_context: str = "") -> dict:
    if not _anthropic_available:
        return _fallback_sql(question)

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    user_msg = question
    if material_context:
        user_msg = f"[Context: focusing on material {material_context}]\n{question}"

    try:
        response = client.messages.create(
            model=CLAUDE_MODEL,
            max_tokens=512,
            system=SYSTEM_PROMPT,
            messages=[{"role": "user", "content": user_msg}],
        )
        import json
        text = response.content[0].text.strip()
        # Strip markdown code fences if present
        if text.startswith("```"):
            text = text.split("```")[1]
            if text.startswith("json"):
                text = text[4:]
        return json.loads(text)
    except Exception as e:
        return {"sql": "", "explanation": f"Error generating SQL: {e}"}


def _fallback_sql(question: str) -> dict:
    """Simple keyword-based fallback when Claude is not available."""
    q = question.lower()

    if "variab" in q or "cv" in q:
        return {
            "sql": "SELECT material_id, material_desc, demand_cv, demand_std, demand_mean FROM gold_features ORDER BY demand_cv DESC LIMIT 10",
            "explanation": "Top 10 materials by demand variability (CV): {result}",
        }
    elif "lead time" in q:
        return {
            "sql": "SELECT material_id, material_desc, lead_time_mean, lead_time_std FROM gold_features ORDER BY lead_time_mean DESC LIMIT 10",
            "explanation": "Top 10 materials by average lead time: {result}",
        }
    elif "demand" in q and "trend" in q:
        return {
            "sql": "SELECT week_start, SUM(weekly_demand) as total_demand FROM weekly_demand GROUP BY week_start ORDER BY week_start",
            "explanation": "Weekly demand trend: {result}",
        }
    elif "abc" in q or "class" in q:
        return {
            "sql": "SELECT abc_class, COUNT(*) as n_materials, AVG(demand_mean) as avg_demand, AVG(lead_time_mean) as avg_lt FROM gold_features GROUP BY abc_class ORDER BY abc_class",
            "explanation": "Summary by ABC class: {result}",
        }
    elif "safety stock" in q or "ss" in q:
        return {
            "sql": "SELECT material_id, material_desc, current_ss, abc_class FROM gold_features ORDER BY current_ss DESC LIMIT 10",
            "explanation": "Top 10 materials by current safety stock: {result}",
        }
    else:
        return {
            "sql": "SELECT material_id, material_desc, demand_mean, demand_cv, lead_time_mean, current_ss FROM gold_features LIMIT 10",
            "explanation": "Sample of material features: {result}",
        }


# ---------------------------------------------------------------------------
# Public interface
# ---------------------------------------------------------------------------

def ask_genie(question: str, material_context: str = "") -> dict:
    """
    Main entry point for the Genie QA agent.

    Args:
        question: Natural language question from the buyer
        material_context: Optional material_id to focus the question on

    Returns dict with:
        sql         : generated SQL query
        results     : pd.DataFrame of query results
        explanation : narrative answer
        error       : error string if failed
    """
    if not question.strip():
        return {"sql": "", "results": pd.DataFrame(), "explanation": "", "error": "Empty question"}

    # Generate SQL
    generated = _generate_sql_with_claude(question, material_context)
    sql = generated.get("sql", "")
    explanation_template = generated.get("explanation", "")

    if not sql:
        return {
            "sql": sql,
            "results": pd.DataFrame(),
            "explanation": explanation_template,
            "error": generated.get("explanation", "No SQL generated"),
        }

    # Execute
    results = _execute_sql(sql)

    # Build narrative
    if "error" in results.columns:
        return {
            "sql": sql,
            "results": results,
            "explanation": "",
            "error": results["error"].iloc[0],
        }

    result_summary = results.to_string(index=False) if len(results) <= 5 else f"{len(results)} rows returned"
    explanation = explanation_template.replace("{result}", result_summary)

    return {
        "sql": sql,
        "results": results,
        "explanation": explanation,
        "error": None,
    }
