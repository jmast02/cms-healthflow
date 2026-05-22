"""Specialty Analytics — payment patterns by medical specialty."""

import streamlit as st
import plotly.express as px
import pandas as pd

st.set_page_config(page_title="Specialty Analytics", page_icon="📊", layout="wide")

from api_client import specialties

st.title("📊 Specialty Analytics")
st.caption("Compare Medicare payment patterns across all medical specialties.")

state_filter = st.selectbox("Filter to state (optional)", ["National"] + [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN",
    "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV",
    "NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN",
    "TX","UT","VT","VA","WA","WV","WI","WY","DC"
])

with st.spinner("Loading specialty data..."):
    df = specialties(state=None if state_filter == "National" else state_filter)

if df.empty:
    st.info("No data — run the pipeline first.")
    st.stop()

for col in ["avg_medicare_payment", "total_services", "total_medicare_payment", "provider_count"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")

# KPIs
k1, k2, k3, k4 = st.columns(4)
k1.metric("Specialties",          len(df))
k2.metric("Total Providers",      f"{df['provider_count'].sum():,.0f}")
k3.metric("Total Services",       f"{df['total_services'].sum():,.0f}")
k4.metric("Total Medicare Spend", f"${df['total_medicare_payment'].sum()/1e6:.1f}M")

st.divider()

col_left, col_right = st.columns(2)

with col_left:
    top_n = st.slider("Show top N specialties", 5, len(df), min(15, len(df)))
    top_pay = df.nlargest(top_n, "avg_medicare_payment")
    fig = px.bar(
        top_pay,
        x="avg_medicare_payment", y="provider_type",
        orientation="h",
        color="avg_medicare_payment",
        color_continuous_scale="Teal",
        text="avg_medicare_payment",
        labels={"avg_medicare_payment": "Avg Medicare Payment ($)", "provider_type": ""},
        title=f"Top {top_n} by Avg Medicare Payment",
    )
    fig.update_traces(texttemplate="$%{text:.0f}", textposition="outside")
    fig.update_layout(coloraxis_showscale=False, height=450, yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig, use_container_width=True)

with col_right:
    top_vol = df.nlargest(top_n, "total_services")
    fig2 = px.bar(
        top_vol,
        x="total_services", y="provider_type",
        orientation="h",
        color="total_services",
        color_continuous_scale="Purples",
        text="total_services",
        labels={"total_services": "Total Services", "provider_type": ""},
        title=f"Top {top_n} by Total Services",
    )
    fig2.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
    fig2.update_layout(coloraxis_showscale=False, height=450, yaxis=dict(autorange="reversed"))
    st.plotly_chart(fig2, use_container_width=True)

# Scatter — payment vs volume
st.subheader("Payment vs Volume")
fig3 = px.scatter(
    df,
    x="total_services",
    y="avg_medicare_payment",
    size="provider_count",
    color="avg_medicare_payment",
    hover_name="provider_type",
    color_continuous_scale="RdYlGn",
    labels={
        "total_services":        "Total Services",
        "avg_medicare_payment":  "Avg Medicare Payment ($)",
        "provider_count":        "# Providers",
    },
    title="Specialty: Average Payment vs Total Volume (bubble size = # providers)",
)
fig3.update_layout(height=450, coloraxis_showscale=False)
st.plotly_chart(fig3, use_container_width=True)

with st.expander("Full specialty table"):
    st.dataframe(
        df.rename(columns={
            "provider_type":          "Specialty",
            "provider_count":         "# Providers",
            "avg_medicare_payment":   "Avg Payment ($)",
            "total_services":         "Total Services",
            "total_medicare_payment": "Total Medicare Spend ($)",
        }).sort_values("Avg Payment ($)", ascending=False),
        use_container_width=True,
    )
