"""
Page 3: Manager Approval
=========================
Managers see pending approval requests from their buyers
and can approve or reject each one with optional comments.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st
import pandas as pd
import plotly.express as px

from app.backend.db import get_approval_requests, get_buyers, get_managers
from app.backend.approval import approve_request, reject_request, get_status_summary

st.set_page_config(page_title="Manager Approval", page_icon="✅", layout="wide")

manager_id = st.session_state.get("manager_id", "MGR001")
managers = get_managers()
mgr_name = managers[managers["manager_id"] == manager_id]["manager_name"].iloc[0] \
    if not managers.empty else manager_id

st.title("✅ Manager Approval")
st.caption(f"Reviewing as: **{mgr_name}** ({manager_id})")

# ---------------------------------------------------------------------------
# Status summary
# ---------------------------------------------------------------------------
summary = get_status_summary()
col1, col2, col3 = st.columns(3)
col1.metric("Pending", summary.get("pending", 0), delta=None)
col2.metric("Approved", summary.get("approved", 0))
col3.metric("Rejected", summary.get("rejected", 0))

# ---------------------------------------------------------------------------
# Pending requests
# ---------------------------------------------------------------------------
st.markdown("### Pending Requests")

pending = get_approval_requests(manager_id=manager_id, status="pending")

if pending.empty:
    st.info("No pending approval requests.")
else:
    buyers = get_buyers()
    buyer_name_map = dict(zip(buyers["buyer_id"], buyers["buyer_name"]))

    for _, req in pending.iterrows():
        request_id = req["request_id"]
        buyer_name = buyer_name_map.get(req["buyer_id"], req["buyer_id"])
        pct = req["pct_change"]
        pct_color = "🔴" if pct > 20 else ("🟡" if pct > 5 else "🟢")

        with st.expander(
            f"{pct_color} **{req['material_id']}** — {req['plant']} | "
            f"SS: {req['current_ss']} → {req['new_ss']} ({pct:+.1f}%) | "
            f"Submitted by {buyer_name}",
            expanded=True,
        ):
            col1, col2 = st.columns([2, 1])

            with col1:
                st.markdown(f"**Material:** {req['material_id']} | **Plant:** {req['plant']}")
                st.markdown(f"**Buyer:** {buyer_name} ({req['buyer_id']})")
                st.markdown(f"**Submitted:** {pd.to_datetime(req['submitted_at']).strftime('%Y-%m-%d %H:%M') if pd.notna(req['submitted_at']) else 'N/A'}")

                st.markdown("**Change Summary:**")
                cols = st.columns(3)
                cols[0].metric("Current SS", req["current_ss"])
                cols[1].metric("New SS", req["new_ss"], delta=f"{pct:+.1f}%")
                cols[2].metric("Plant", req["plant"])

                st.markdown("**Top Drivers:**")
                for d_col in ["driver_1", "driver_2", "driver_3"]:
                    if req.get(d_col):
                        st.markdown(f"  - {req[d_col]}")

            with col2:
                comment = st.text_area(
                    "Comment (optional)",
                    key=f"comment_{request_id}",
                    height=80,
                )
                approve_col, reject_col = st.columns(2)
                with approve_col:
                    if st.button("✅ Approve", key=f"approve_{request_id}", type="primary", use_container_width=True):
                        success = approve_request(request_id, manager_id, comment)
                        if success:
                            st.success("Approved!")
                            st.rerun()
                        else:
                            st.error("Failed to approve.")
                with reject_col:
                    if st.button("❌ Reject", key=f"reject_{request_id}", use_container_width=True):
                        if not comment:
                            st.warning("Please add a comment when rejecting.")
                        else:
                            success = reject_request(request_id, manager_id, comment)
                            if success:
                                st.warning("Rejected.")
                                st.rerun()
                            else:
                                st.error("Failed to reject.")

# ---------------------------------------------------------------------------
# Completed requests (history)
# ---------------------------------------------------------------------------
st.markdown("---")
st.markdown("### Decision History")

tab_approved, tab_rejected = st.tabs(["Approved", "Rejected"])

with tab_approved:
    approved = get_approval_requests(manager_id=manager_id, status="approved")
    if approved.empty:
        st.info("No approved requests yet.")
    else:
        buyers = get_buyers()
        buyer_name_map = dict(zip(buyers["buyer_id"], buyers["buyer_name"]))
        approved["buyer_name"] = approved["buyer_id"].map(buyer_name_map)
        display = approved[[
            "material_id", "plant", "buyer_name", "current_ss", "new_ss",
            "pct_change", "manager_comment", "reviewed_at"
        ]].copy()
        display["pct_change"] = display["pct_change"].apply(lambda x: f"{x:+.1f}%")
        st.dataframe(display, use_container_width=True)

with tab_rejected:
    rejected = get_approval_requests(manager_id=manager_id, status="rejected")
    if rejected.empty:
        st.info("No rejected requests yet.")
    else:
        buyers = get_buyers()
        buyer_name_map = dict(zip(buyers["buyer_id"], buyers["buyer_name"]))
        rejected["buyer_name"] = rejected["buyer_id"].map(buyer_name_map)
        display = rejected[[
            "material_id", "plant", "buyer_name", "current_ss", "new_ss",
            "pct_change", "manager_comment", "reviewed_at"
        ]].copy()
        display["pct_change"] = display["pct_change"].apply(lambda x: f"{x:+.1f}%")
        st.dataframe(display, use_container_width=True)
