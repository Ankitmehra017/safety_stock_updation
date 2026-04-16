"""
Page 1: Genie Q&A
==================
Natural-language interface to the Databricks Genie AI/BI space.

Buyers type questions in plain English. The page:
  1. Sends the question to the Genie REST API
  2. Shows the SQL Genie generated
  3. Executes it on the Databricks SQL warehouse
  4. Displays the results + Genie's narrative answer

Conversation context is preserved across questions within the same session.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st
import pandas as pd

from app.backend.genie_agent import ask_genie
from config import GENIE_SPACE_ID

st.set_page_config(page_title="Genie Q&A", page_icon="🧞", layout="wide")

# ---------------------------------------------------------------------------
# Session state init
# ---------------------------------------------------------------------------
if "genie_conversation_id" not in st.session_state:
    st.session_state.genie_conversation_id = None
if "genie_history" not in st.session_state:
    st.session_state.genie_history = []

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("🧞 Genie — Ask Your Safety Stock Data")
st.markdown(
    "Powered by **Databricks Genie AI/BI**. "
    "Ask any question about safety stock recommendations in plain English."
)

if not GENIE_SPACE_ID:
    st.error(
        "**Genie Space not configured.** "
        "Run notebook `05_setup_genie_space` in Databricks, create the Genie space, "
        "then add `GENIE_SPACE_ID=<your-id>` to your `.env` file and restart the app."
    )
    st.stop()

# ---------------------------------------------------------------------------
# Conversation status
# ---------------------------------------------------------------------------
col_status, col_clear = st.columns([4, 1])
with col_status:
    if st.session_state.genie_conversation_id:
        st.caption(
            f"💬 Active conversation · "
            f"ID: `{st.session_state.genie_conversation_id[:16]}…` · "
            f"{len(st.session_state.genie_history)} message(s)"
        )
    else:
        st.caption("💬 No active conversation — ask your first question below")

with col_clear:
    if st.button("🗑 New conversation", use_container_width=True):
        st.session_state.genie_conversation_id = None
        st.session_state.genie_history = []
        st.rerun()

st.markdown("---")

# ---------------------------------------------------------------------------
# Example questions
# ---------------------------------------------------------------------------
st.markdown("**Try asking:**")
examples = [
    "Which materials have the largest safety stock increase recommended?",
    "Show me all A-class materials where SS should decrease",
    "What is the average confidence score by ABC class?",
    "Which buyer has the most materials with changes greater than 20%?",
    "Show materials where demand variability is the top driver",
    "How many materials are recommended to increase vs decrease SS?",
]

cols = st.columns(3)
for i, ex in enumerate(examples):
    with cols[i % 3]:
        if st.button(ex, key=f"ex_{i}", use_container_width=True):
            st.session_state.pending_question = ex

# ---------------------------------------------------------------------------
# Question input
# ---------------------------------------------------------------------------
st.markdown("---")
default_q = st.session_state.pop("pending_question", "")
question  = st.chat_input("Ask Genie a question about safety stock...")

# Also accept via text input (for the example buttons)
if default_q and not question:
    question = default_q

# ---------------------------------------------------------------------------
# Call Genie and append to history
# ---------------------------------------------------------------------------
if question:
    with st.spinner("🧞 Genie is thinking..."):
        result = ask_genie(
            question=question,
            conversation_id=st.session_state.genie_conversation_id,
        )

    # Persist conversation_id for follow-up questions
    if result.get("conversation_id"):
        st.session_state.genie_conversation_id = result["conversation_id"]

    st.session_state.genie_history.append({
        "question": question,
        "result":   result,
    })

# ---------------------------------------------------------------------------
# Render conversation history (newest first)
# ---------------------------------------------------------------------------
for entry in reversed(st.session_state.genie_history):
    q = entry["question"]
    r = entry["result"]

    with st.container():
        # User message
        with st.chat_message("user"):
            st.markdown(q)

        # Genie response
        with st.chat_message("assistant", avatar="🧞"):
            if r.get("error"):
                st.error(f"**Error:** {r['error']}")

            else:
                # Narrative answer
                if r.get("description"):
                    st.markdown(r["description"])

                # SQL expander
                if r.get("sql"):
                    with st.expander("View generated SQL", expanded=False):
                        st.code(r["sql"], language="sql")

                # Results table
                if r.get("results") is not None and not r["results"].empty:
                    st.dataframe(r["results"], use_container_width=True, height=300)
                elif r.get("sql") and (r.get("results") is None or (r.get("results") is not None and r["results"].empty)):
                    st.info("Query returned no rows.")
