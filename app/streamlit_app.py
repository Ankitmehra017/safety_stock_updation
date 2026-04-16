"""
app/streamlit_app.py
=====================
Entry point for the Generac Safety Stock Update Streamlit app.

Run:  streamlit run app/streamlit_app.py
"""

import sys
import os
from pathlib import Path

# Ensure project root is on path
sys.path.insert(0, str(Path(__file__).parent.parent))

import streamlit as st

st.set_page_config(
    page_title="Generac Safety Stock Update",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Session state initialization
# ---------------------------------------------------------------------------
if "role" not in st.session_state:
    st.session_state.role = "buyer"
if "buyer_id" not in st.session_state:
    st.session_state.buyer_id = "B001"
if "manager_id" not in st.session_state:
    st.session_state.manager_id = "MGR001"
if "submitted_requests" not in st.session_state:
    st.session_state.submitted_requests = []

# ---------------------------------------------------------------------------
# Sidebar: role + persona selector
# ---------------------------------------------------------------------------
with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/3/32/Generac_Power_Systems_logo.svg/320px-Generac_Power_Systems_logo.svg.png", width=180)
    st.markdown("---")
    st.subheader("Demo Role")

    role = st.radio("Switch role", ["Buyer", "Manager"], index=0 if st.session_state.role == "buyer" else 1)
    st.session_state.role = role.lower()

    st.markdown("---")

    if st.session_state.role == "buyer":
        from app.backend.db import get_buyers
        buyers = get_buyers()
        buyer_options = dict(zip(buyers["buyer_name"], buyers["buyer_id"]))
        selected_name = st.selectbox("Buyer", list(buyer_options.keys()))
        st.session_state.buyer_id = buyer_options[selected_name]
        st.caption(f"ID: {st.session_state.buyer_id}")
    else:
        from app.backend.db import get_managers
        managers = get_managers()
        mgr_options = dict(zip(managers["manager_name"], managers["manager_id"]))
        selected_mgr = st.selectbox("Manager", list(mgr_options.keys()))
        st.session_state.manager_id = mgr_options[selected_mgr]
        st.caption(f"ID: {st.session_state.manager_id}")

    st.markdown("---")
    st.caption("Safety Stock Update System v1.0")

# ---------------------------------------------------------------------------
# Landing page
# ---------------------------------------------------------------------------
st.title("🏭 Generac Safety Stock Update")
st.markdown(
    """
    Welcome to the Safety Stock Update System. Use the sidebar to navigate:

    | Page | Who | What |
    |------|-----|------|
    | **Buyer Dashboard** | Buyers | Review current vs. ML-recommended safety stock levels |
    | **Genie QA** | Buyers | Ask natural language questions to understand recommendations |
    | **Manager Approval** | Managers | Approve or reject buyer-submitted SS changes |

    **Workflow:**
    1. Buyer reviews the dashboard → submits materials for approval
    2. Buyer uses Genie to justify the recommendation
    3. Manager reviews and approves or rejects
    """,
    unsafe_allow_html=False,
)

# Quick stats
try:
    from app.backend.db import get_recommendations
    from app.backend.approval import get_status_summary

    recs = get_recommendations()
    summary = get_status_summary()

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total Materials", len(recs))
    col2.metric("SS Increase Recommended", int((recs["pct_change"] > 5).sum()))
    col3.metric("SS Decrease Recommended", int((recs["pct_change"] < -5).sum()))
    col4.metric("Pending Approvals", summary.get("pending", 0))
except Exception as e:
    st.info("Run the setup notebooks first to populate data. See README for instructions.")
    st.caption(f"({e})")
