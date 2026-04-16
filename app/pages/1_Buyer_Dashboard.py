"""
Page 1: Buyer Dashboard
========================
Shows the ML-recommended safety stock changes per material.
Buyers can filter, inspect drivers, and submit for manager approval.
"""

import sys
import os
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px

from app.backend.db import get_recommendations, get_weekly_demand, get_buyers
from app.backend.approval import submit_bulk

st.set_page_config(page_title="Buyer Dashboard", page_icon="📊", layout="wide")

# ---------------------------------------------------------------------------
# Load buyer context from session
# ---------------------------------------------------------------------------
buyer_id = st.session_state.get("buyer_id", "B001")

st.title("📊 Buyer Dashboard")
st.caption(f"Viewing as: **{buyer_id}**")

# ---------------------------------------------------------------------------
# Load recommendations
# ---------------------------------------------------------------------------
try:
    recs = get_recommendations(buyer_id=buyer_id)
except Exception as e:
    st.error(f"Could not load recommendations: {e}")
    st.info("Run notebooks 01–04 first to populate data.")
    st.stop()

if recs.empty:
    st.warning("No recommendations found for this buyer.")
    st.stop()

# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------
with st.expander("Filters", expanded=True):
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        abc_filter = st.multiselect("ABC Class", options=sorted(recs["abc_class"].unique()), default=sorted(recs["abc_class"].unique()))
    with col2:
        cat_filter = st.multiselect("Category", options=sorted(recs["category"].unique()), default=sorted(recs["category"].unique()))
    with col3:
        change_min = st.slider("Min % change", -100, 0, -100)
    with col4:
        change_max = st.slider("Max % change", 0, 300, 300)

filtered = recs[
    (recs["abc_class"].isin(abc_filter))
    & (recs["category"].isin(cat_filter))
    & (recs["pct_change"] >= change_min)
    & (recs["pct_change"] <= change_max)
].copy()

st.markdown(f"**{len(filtered)}** materials after filters")

# ---------------------------------------------------------------------------
# Summary metrics
# ---------------------------------------------------------------------------
col1, col2, col3, col4 = st.columns(4)
col1.metric("Materials", len(filtered))
col2.metric("Avg % Change", f"{filtered['pct_change'].mean():.1f}%")
col3.metric("↑ Increase >5%", int((filtered["pct_change"] > 5).sum()))
col4.metric("↓ Decrease >5%", int((filtered["pct_change"] < -5).sum()))

# ---------------------------------------------------------------------------
# Scatter: current vs new SS
# ---------------------------------------------------------------------------
st.markdown("### Current vs. Recommended Safety Stock")

fig = px.scatter(
    filtered,
    x="current_ss",
    y="new_ss",
    color="pct_change",
    color_continuous_scale=["green", "yellow", "red"],
    hover_data=["material_id", "material_desc", "abc_class", "driver_1"],
    labels={"current_ss": "Current SS", "new_ss": "New SS (Recommended)", "pct_change": "% Change"},
    size_max=14,
)
# Reference line (no change)
max_val = max(filtered["current_ss"].max(), filtered["new_ss"].max()) * 1.1
fig.add_trace(go.Scatter(
    x=[0, max_val], y=[0, max_val],
    mode="lines",
    line=dict(dash="dash", color="gray", width=1),
    name="No Change",
    showlegend=True,
))
st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------------------
# Recommendations table with color coding + bulk select
# ---------------------------------------------------------------------------
st.markdown("### Material Recommendations")

def color_pct(val):
    if val > 20:
        return "background-color: #ffcccc"   # red
    elif val > 5:
        return "background-color: #fff3cc"   # yellow
    elif val < -5:
        return "background-color: #ccffcc"   # green
    return ""

display_cols = [
    "material_id", "material_desc", "abc_class", "category",
    "current_ss", "new_ss", "pct_change",
    "confidence_score", "driver_1", "driver_2", "driver_3",
]

styled = filtered[display_cols].style.applymap(color_pct, subset=["pct_change"]).format({
    "pct_change": "{:.1f}%",
    "confidence_score": "{:.2f}",
    "current_ss": "{:,.0f}",
    "new_ss": "{:,.0f}",
})

st.dataframe(styled, use_container_width=True, height=400)

# ---------------------------------------------------------------------------
# Demand trend for selected material
# ---------------------------------------------------------------------------
st.markdown("### Demand Trend")
selected_mat = st.selectbox("Select material to view demand trend", options=filtered["material_id"].tolist())

if selected_mat:
    demand = get_weekly_demand(material_ids=[selected_mat])
    if not demand.empty:
        fig2 = px.line(
            demand, x="week_start", y="weekly_demand",
            title=f"Weekly Demand — {selected_mat}",
            labels={"week_start": "Week", "weekly_demand": "Demand (units)"},
        )
        fig2.add_hline(
            y=filtered[filtered["material_id"] == selected_mat]["current_ss"].iloc[0],
            line_dash="dash", line_color="red",
            annotation_text="Current SS",
        )
        fig2.add_hline(
            y=filtered[filtered["material_id"] == selected_mat]["new_ss"].iloc[0],
            line_dash="dash", line_color="green",
            annotation_text="New SS",
        )
        st.plotly_chart(fig2, use_container_width=True)

# ---------------------------------------------------------------------------
# Submit for approval
# ---------------------------------------------------------------------------
st.markdown("---")
st.markdown("### Submit for Approval")
st.caption("Select materials and click Submit to send to your manager for approval.")

submit_options = filtered["material_id"].tolist()
to_submit = st.multiselect("Materials to submit", options=submit_options)

if st.button("Submit Selected for Approval", type="primary", disabled=len(to_submit) == 0):
    rows_to_submit = []
    for mat_id in to_submit:
        row = filtered[filtered["material_id"] == mat_id].iloc[0]
        rows_to_submit.append({
            "material_id": row["material_id"],
            "plant": row["plant"],
            "buyer_id": row["buyer_id"],
            "current_ss": int(row["current_ss"]),
            "new_ss": int(row["new_ss"]),
            "pct_change": float(row["pct_change"]),
            "driver_1": row.get("driver_1", ""),
            "driver_2": row.get("driver_2", ""),
            "driver_3": row.get("driver_3", ""),
        })

    with st.spinner("Submitting..."):
        request_ids = submit_bulk(rows_to_submit)

    st.success(f"Submitted {len(request_ids)} material(s) for approval!")
    st.session_state.submitted_requests = request_ids
    for rid, mat_id in zip(request_ids, to_submit):
        st.caption(f"  {mat_id} → request {rid[:8]}...")
