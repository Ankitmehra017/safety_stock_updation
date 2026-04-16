"""
Page 2: SS Recommendations
===========================
Shows the full ss_recommendations table from Unity Catalog.
Buyers can filter, inspect SHAP drivers, and submit materials
for manager approval — all in one place.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

from app.backend.db import get_recommendations, get_weekly_demand, get_buyers
from app.backend.approval import submit_bulk

st.set_page_config(page_title="SS Recommendations", page_icon="📊", layout="wide")

buyer_id = st.session_state.get("buyer_id", "B001")

st.title("📊 Safety Stock Recommendations")
st.caption(f"Viewing as buyer: **{buyer_id}**  ·  Source: `dev.safety_stock_gold.ss_recommendations`")

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
try:
    recs = get_recommendations(buyer_id=buyer_id)
except Exception as e:
    st.error(f"Could not load recommendations: {e}")
    st.info("Make sure DATABRICKS_HOST, DATABRICKS_TOKEN, and DATABRICKS_HTTP_PATH are set in `.env`.")
    st.stop()

if recs.empty:
    st.warning("No recommendations found for this buyer.")
    st.stop()

# ---------------------------------------------------------------------------
# Summary metrics
# ---------------------------------------------------------------------------
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Materials",          len(recs))
col2.metric("Avg % Change",       f"{recs['pct_change'].mean():.1f}%")
col3.metric("↑ Increase > 5%",   int((recs["pct_change"] > 5).sum()))
col4.metric("↓ Decrease > 5%",   int((recs["pct_change"] < -5).sum()))
col5.metric("Avg Confidence",     f"{recs['confidence_score'].mean():.2f}")

st.markdown("---")

# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------
with st.expander("🔍 Filters", expanded=True):
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        abc_filter = st.multiselect(
            "ABC Class",
            options=sorted(recs["abc_class"].unique()),
            default=sorted(recs["abc_class"].unique()),
        )
    with col2:
        cat_filter = st.multiselect(
            "Category",
            options=sorted(recs["category"].unique()),
            default=sorted(recs["category"].unique()),
        )
    with col3:
        change_min = st.slider("Min % change", -100, 0, -100)
    with col4:
        change_max = st.slider("Max % change", 0, 300, 300)

filtered = recs[
    recs["abc_class"].isin(abc_filter)
    & recs["category"].isin(cat_filter)
    & (recs["pct_change"] >= change_min)
    & (recs["pct_change"] <= change_max)
].copy()

st.caption(f"**{len(filtered)}** materials after filters")

# ---------------------------------------------------------------------------
# Scatter: current vs new SS
# ---------------------------------------------------------------------------
tab_scatter, tab_table, tab_trend = st.tabs(["📈 Scatter", "📋 Table", "📉 Demand Trend"])

with tab_scatter:
    fig = px.scatter(
        filtered,
        x="current_ss", y="new_ss",
        color="pct_change",
        color_continuous_scale=["green", "lightyellow", "red"],
        color_continuous_midpoint=0,
        hover_data=["material_id", "material_desc", "abc_class", "driver_1", "confidence_score"],
        labels={
            "current_ss": "Current SS (SAP)",
            "new_ss":     "New SS (ML Recommended)",
            "pct_change": "% Change",
        },
        title="Current vs. ML-Recommended Safety Stock",
    )
    max_val = max(filtered["current_ss"].max(), filtered["new_ss"].max()) * 1.1
    fig.add_trace(go.Scatter(
        x=[0, max_val], y=[0, max_val],
        mode="lines", name="No Change",
        line=dict(dash="dash", color="gray", width=1),
    ))
    st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------------------
# Recommendations table (color-coded)
# ---------------------------------------------------------------------------
with tab_table:
    display_cols = [
        "material_id", "material_desc", "abc_class", "category",
        "current_ss", "new_ss", "pct_change",
        "confidence_score", "driver_1", "driver_2", "driver_3",
    ]

    def _color_row(row):
        pct = row["pct_change"]
        if pct > 20:
            color = "background-color: #ffcccc"
        elif pct > 5:
            color = "background-color: #fff3cc"
        elif pct < -5:
            color = "background-color: #d4edda"
        else:
            color = ""
        return [color] * len(row)

    styled = (
        filtered[display_cols]
        .style
        .apply(_color_row, axis=1)
        .format({
            "pct_change":       "{:+.1f}%",
            "confidence_score": "{:.2f}",
            "current_ss":       "{:,.0f}",
            "new_ss":           "{:,.0f}",
        })
    )
    st.dataframe(styled, use_container_width=True, height=450)

# ---------------------------------------------------------------------------
# Demand trend for one material
# ---------------------------------------------------------------------------
with tab_trend:
    selected_mat = st.selectbox(
        "Select material",
        options=filtered["material_id"].tolist(),
        key="trend_material",
    )
    if selected_mat:
        try:
            demand = get_weekly_demand(material_ids=[selected_mat])
            if not demand.empty:
                row = filtered[filtered["material_id"] == selected_mat].iloc[0]
                fig2 = px.line(
                    demand, x="week_start", y="weekly_demand",
                    title=f"Weekly Demand — {selected_mat} ({row['material_desc']})",
                    labels={"week_start": "Week", "weekly_demand": "Units/week"},
                )
                fig2.add_hline(
                    y=row["current_ss"], line_dash="dash", line_color="red",
                    annotation_text=f"Current SS = {row['current_ss']}",
                )
                fig2.add_hline(
                    y=row["new_ss"], line_dash="dash", line_color="green",
                    annotation_text=f"New SS = {row['new_ss']}",
                )
                st.plotly_chart(fig2, use_container_width=True)
        except Exception as e:
            st.warning(f"Could not load demand data: {e}")

# ---------------------------------------------------------------------------
# Submit for approval
# ---------------------------------------------------------------------------
st.markdown("---")
st.subheader("📨 Submit for Manager Approval")
st.caption("Select materials and click Submit. Your manager will receive the requests for review.")

to_submit = st.multiselect(
    "Choose materials to submit",
    options=filtered["material_id"].tolist(),
    key="approval_multiselect",
)

if st.button("Submit for Approval", type="primary", disabled=not to_submit):
    rows_to_submit = []
    for mat_id in to_submit:
        row = filtered[filtered["material_id"] == mat_id].iloc[0]
        rows_to_submit.append({
            "material_id": row["material_id"],
            "plant":       row["plant"],
            "buyer_id":    row["buyer_id"],
            "current_ss":  int(row["current_ss"]),
            "new_ss":      int(row["new_ss"]),
            "pct_change":  float(row["pct_change"]),
            "driver_1":    row.get("driver_1", ""),
            "driver_2":    row.get("driver_2", ""),
            "driver_3":    row.get("driver_3", ""),
        })

    with st.spinner("Submitting..."):
        request_ids = submit_bulk(rows_to_submit)

    st.success(f"✅ Submitted **{len(request_ids)}** material(s) for approval!")
    for rid, mat_id in zip(request_ids, to_submit):
        st.caption(f"  {mat_id} → request `{rid[:8]}…`")
