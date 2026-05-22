"""Procedure Cost Analyzer — compare Medicare payments across states."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

st.set_page_config(page_title="Procedure Costs", page_icon="💊", layout="wide")

from api_client import procedure_costs, search_procedures

st.title("💊 Procedure Cost Analyzer")
st.caption("Compare what Medicare pays for the same procedure across different states.")

# ── Common procedures quick-pick ─────────────────────────────────────────
COMMON = {
    "Office Visit (99213)":         "99213",
    "Office Visit Complex (99214)": "99214",
    "ECG (93000)":                  "93000",
    "Chest X-Ray (71046)":          "71046",
    "CBC Blood Test (85025)":       "85025",
    "Colonoscopy (45378)":          "45378",
    "MRI Brain (70553)":            "70553",
    "Knee Replacement (27447)":     "27447",
}

col1, col2 = st.columns([2, 1])
with col1:
    quick = st.selectbox("Quick pick a common procedure", ["(enter manually)"] + list(COMMON.keys()))
with col2:
    manual = st.text_input("Or enter HCPCS code", placeholder="e.g. 99213")

hcpcs = manual.upper() if manual else (COMMON.get(quick, "") if quick != "(enter manually)" else "")

state_filter = st.selectbox("Filter to state (optional)", ["All States"] + [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN",
    "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV",
    "NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN",
    "TX","UT","VT","VA","WA","WV","WI","WY","DC"
])

if hcpcs:
    with st.spinner(f"Loading costs for {hcpcs}..."):
        df = procedure_costs(hcpcs, state=None if state_filter == "All States" else state_filter)

    if df.empty:
        st.warning(f"No data found for HCPCS code **{hcpcs}**.")
        st.stop()

    for col in ["avg_medicare_payment", "median_medicare_payment",
                "min_medicare_payment", "max_medicare_payment", "stddev_medicare_payment"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    desc = df["hcpcs_description"].iloc[0] if "hcpcs_description" in df.columns else hcpcs
    st.subheader(f"{hcpcs} — {desc}")

    # KPI row
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("States w/ Data",    len(df))
    k2.metric("National Avg",      f"${df['avg_medicare_payment'].mean():.2f}")
    k3.metric("Lowest State Avg",  f"${df['avg_medicare_payment'].min():.2f}")
    k4.metric("Highest State Avg", f"${df['avg_medicare_payment'].max():.2f}")

    # Bar chart — avg payment by state
    fig = px.bar(
        df.sort_values("avg_medicare_payment", ascending=False),
        x="provider_state", y="avg_medicare_payment",
        color="avg_medicare_payment",
        color_continuous_scale="RdYlGn_r",
        labels={"avg_medicare_payment": "Avg Medicare Payment ($)", "provider_state": "State"},
        title=f"Average Medicare Payment by State — {hcpcs}",
        text="avg_medicare_payment",
    )
    fig.update_traces(texttemplate="$%{text:.0f}", textposition="outside")
    fig.update_layout(coloraxis_showscale=False, height=400, xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)

    # Choropleth map
    fig_map = px.choropleth(
        df,
        locations="provider_state",
        locationmode="USA-states",
        color="avg_medicare_payment",
        scope="usa",
        color_continuous_scale="Blues",
        labels={"avg_medicare_payment": "Avg Payment ($)"},
        title=f"Geographic Cost Distribution — {hcpcs}",
    )
    fig_map.update_layout(margin=dict(l=0, r=0, t=40, b=0), height=350)
    st.plotly_chart(fig_map, use_container_width=True)

    # Detail table
    with st.expander("Full data table"):
        st.dataframe(
            df[["provider_state", "provider_count", "avg_medicare_payment",
                "median_medicare_payment", "min_medicare_payment", "max_medicare_payment"]
            ].rename(columns={
                "provider_state":          "State",
                "provider_count":          "# Providers",
                "avg_medicare_payment":    "Avg ($)",
                "median_medicare_payment": "Median ($)",
                "min_medicare_payment":    "Min ($)",
                "max_medicare_payment":    "Max ($)",
            }).sort_values("Avg ($)", ascending=False),
            use_container_width=True,
        )
else:
    st.info("Select a procedure above to compare costs across states.")
