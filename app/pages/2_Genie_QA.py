"""
Page 2: Genie QA
=================
Claude-powered text-to-SQL agent for buyers to understand
why a safety stock recommendation was made.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st
import pandas as pd

from app.backend.db import get_recommendations
from app.backend.genie_agent import ask_genie

st.set_page_config(page_title="Genie QA", page_icon="🧞", layout="wide")

buyer_id = st.session_state.get("buyer_id", "B001")

st.title("🧞 Genie QA — Ask Your Data")
st.markdown(
    "Ask natural language questions about safety stock drivers, demand patterns, "
    "and lead times. Genie generates SQL, runs it, and explains the results."
)

# ---------------------------------------------------------------------------
# Material context selector
# ---------------------------------------------------------------------------
try:
    recs = get_recommendations(buyer_id=buyer_id)
    mat_options = ["All materials"] + recs["material_id"].tolist()
except Exception:
    recs = pd.DataFrame()
    mat_options = ["All materials"]

col_left, col_right = st.columns([1, 2])
with col_left:
    selected_mat = st.selectbox("Focus on material (optional)", mat_options)
    material_context = "" if selected_mat == "All materials" else selected_mat

    if material_context and not recs.empty:
        row = recs[recs["material_id"] == material_context]
        if not row.empty:
            r = row.iloc[0]
            st.info(
                f"**{r['material_id']}** — {r['material_desc']}\n\n"
                f"Current SS: **{r['current_ss']}** → New SS: **{r['new_ss']}** "
                f"({r['pct_change']:+.1f}%)\n\n"
                f"Drivers: {r['driver_1']} | {r['driver_2']}"
            )

# ---------------------------------------------------------------------------
# Example prompts
# ---------------------------------------------------------------------------
with col_right:
    st.markdown("**Example questions:**")
    examples = [
        f"Why is the safety stock high for {material_context or 'M0001'}?",
        "Which materials have the most variable demand?",
        "Show me materials with lead time > 20 days",
        "Compare A vs B vs C class by average demand variability",
        "What is the demand trend for all materials in the last 6 months?",
        "Which materials have a confidence score below 0.7?",
    ]
    for ex in examples:
        if st.button(ex, key=f"ex_{ex[:20]}", use_container_width=True):
            st.session_state.genie_question = ex

# ---------------------------------------------------------------------------
# Question input
# ---------------------------------------------------------------------------
st.markdown("---")
question = st.text_input(
    "Your question",
    value=st.session_state.get("genie_question", ""),
    placeholder="e.g. Why did the safety stock increase for M0001?",
    key="genie_input",
)
if question:
    st.session_state.genie_question = question

run_btn = st.button("Ask Genie", type="primary", disabled=not bool(question))

# ---------------------------------------------------------------------------
# Chat history
# ---------------------------------------------------------------------------
if "genie_history" not in st.session_state:
    st.session_state.genie_history = []

if run_btn and question:
    with st.spinner("Genie is thinking..."):
        result = ask_genie(question, material_context)

    st.session_state.genie_history.append({
        "question": question,
        "result": result,
    })

# Display history (newest first)
for entry in reversed(st.session_state.genie_history):
    q = entry["question"]
    r = entry["result"]

    with st.container():
        st.markdown(f"**Q:** {q}")

        if r.get("error"):
            st.error(f"Error: {r['error']}")
        else:
            col1, col2 = st.columns([1, 1])
            with col1:
                with st.expander("Generated SQL", expanded=False):
                    st.code(r["sql"], language="sql")
            with col2:
                st.markdown(f"**Answer:** {r['explanation']}")

            if r["results"] is not None and not r["results"].empty:
                st.dataframe(r["results"], use_container_width=True)

        st.markdown("---")

if st.button("Clear history"):
    st.session_state.genie_history = []
    st.rerun()
