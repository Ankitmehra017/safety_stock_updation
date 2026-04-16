"""
app/streamlit_app.py
=====================
Entry point for the Generac Safety Stock Update Streamlit app.

Run:  streamlit run app/streamlit_app.py
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st

st.set_page_config(
    page_title="Generac Safety Stock Update",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Session state defaults
# ---------------------------------------------------------------------------
if "role"       not in st.session_state: st.session_state.role       = "buyer"
if "buyer_id"   not in st.session_state: st.session_state.buyer_id   = "B001"
if "manager_id" not in st.session_state: st.session_state.manager_id = "MGR001"

# ---------------------------------------------------------------------------
# Sidebar — role / persona switcher
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown("### 🏭 Generac SS Update")
    st.markdown("---")
    st.subheader("Demo Role")
    role = st.radio("Switch role", ["Buyer", "Manager"],
                    index=0 if st.session_state.role == "buyer" else 1)
    st.session_state.role = role.lower()
    st.markdown("---")

    if st.session_state.role == "buyer":
        try:
            from app.backend.db import get_buyers
            buyers = get_buyers()
            options = dict(zip(buyers["buyer_name"], buyers["buyer_id"]))
            name = st.selectbox("Buyer", list(options.keys()))
            st.session_state.buyer_id = options[name]
            st.caption(f"ID: {st.session_state.buyer_id}")
        except Exception:
            st.session_state.buyer_id = st.text_input("Buyer ID", value="B001")
    else:
        try:
            from app.backend.db import get_managers
            managers = get_managers()
            options = dict(zip(managers["manager_name"], managers["manager_id"]))
            name = st.selectbox("Manager", list(options.keys()))
            st.session_state.manager_id = options[name]
            st.caption(f"ID: {st.session_state.manager_id}")
        except Exception:
            st.session_state.manager_id = st.text_input("Manager ID", value="MGR001")

    st.markdown("---")
    st.caption("v1.0 · Safety Stock Update System")

# ---------------------------------------------------------------------------
# Landing page
# ---------------------------------------------------------------------------
st.title("🏭 Generac Safety Stock Update")
st.markdown(
    """
    Welcome. Use the **sidebar** to switch between Buyer and Manager roles,
    then navigate using the pages on the left.
    """
)

st.markdown("---")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("### 🧞 Page 1 — Genie Q&A")
    st.markdown(
        "Ask natural-language questions about safety stock recommendations. "
        "Powered by **Databricks Genie AI/BI** — Genie generates SQL, "
        "runs it, and explains the results in plain English."
    )
    st.markdown("*For: Buyers*")

with col2:
    st.markdown("### 📊 Page 2 — SS Recommendations")
    st.markdown(
        "Full `ss_recommendations` table from Unity Catalog. "
        "Filter by ABC class, category, or % change. "
        "Inspect SHAP drivers, view demand trend, "
        "and submit materials for manager approval."
    )
    st.markdown("*For: Buyers*")

with col3:
    st.markdown("### ✅ Page 3 — Manager Approval")
    st.markdown(
        "Review pending SS change requests submitted by buyers. "
        "Approve or reject each one with optional comments. "
        "Approved changes are ready to be written back to SAP."
    )
    st.markdown("*For: Managers*")

st.markdown("---")

# Quick stats
try:
    from app.backend.db import get_recommendations
    from app.backend.approval import get_status_summary

    recs    = get_recommendations()
    summary = get_status_summary()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Materials",        len(recs))
    c2.metric("SS Increase > 5%",       int((recs["pct_change"] > 5).sum()))
    c3.metric("SS Decrease > 5%",       int((recs["pct_change"] < -5).sum()))
    c4.metric("Pending Approvals",      summary.get("pending", 0))
except Exception as e:
    st.info("Run notebooks 01–04 in Databricks to populate data, then configure `.env`.")
    st.caption(str(e))
