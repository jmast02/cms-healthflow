"""CMS HealthFlow — Streamlit Dashboard."""

import streamlit as st

st.set_page_config(
    page_title="CMS HealthFlow",
    page_icon="🏥",
    layout="wide",
    initial_sidebar_state="expanded",
)

from api_client import health, state_summary, specialties
import plotly.express as px
import pandas as pd

# ── Sidebar ───────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("🏥 CMS HealthFlow")
    st.caption("Medicare Claims Analytics")
    st.divider()
    st.page_link("app.py",                        label="🏠 Overview",             icon="🏠")
    st.page_link("pages/1_provider_search.py",    label="Provider Explorer",       icon="🔍")
    st.page_link("pages/2_procedure_costs.py",    label="Procedure Costs",         icon="💊")
    st.page_link("pages/3_specialty_analytics.py",label="Specialty Analytics",     icon="📊")
    st.page_link("pages/4_geographic_analysis.py",label="Geographic Analysis",     icon="🗺️")
    st.page_link("pages/5_hospital_rankings.py",  label="Hospital Rankings",       icon="🏨")
    st.divider()
    st.caption("Data: CMS Medicare 2022\nStack: PySpark · FastAPI · PostgreSQL")

# ── Header ────────────────────────────────────────────────────────────────
st.title("🏥 CMS HealthFlow")
st.subheader("Medicare Claims Analytics Dashboard")

# ── Health / KPIs ─────────────────────────────────────────────────────────
h = health()
db_ok = h.get("database") == "connected"

col1, col2, col3, col4 = st.columns(4)
col1.metric("Pipeline Status",  "✅ Online" if db_ok else "⚠️ Degraded")
col2.metric("Total Providers",  f"{h.get('total_providers', 0):,}" if h.get('total_providers') else "—")
col3.metric("Dataset Year",     h.get("dataset_year", "—"))
col4.metric("API",              "Connected" if db_ok else "Unavailable")

st.divider()

# ── State Overview ────────────────────────────────────────────────────────
col_left, col_right = st.columns(2)

with col_left:
    st.subheader("Providers by State")
    df_states = state_summary()
    if not df_states.empty:
        df_states = df_states.rename(columns={"provider_type": "state"})
        df_states["avg_medicare_payment"] = pd.to_numeric(df_states["avg_medicare_payment"], errors="coerce")
        fig = px.choropleth(
            df_states,
            locations="state",
            locationmode="USA-states",
            color="avg_medicare_payment",
            scope="usa",
            color_continuous_scale="Blues",
            labels={"avg_medicare_payment": "Avg Payment ($)"},
            title="Average Medicare Payment by State",
        )
        fig.update_layout(margin=dict(l=0, r=0, t=40, b=0), height=350)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No state data yet — run the pipeline first.")

with col_right:
    st.subheader("Top Specialties by Avg Payment")
    df_spec = specialties()
    if not df_spec.empty:
        df_spec["avg_medicare_payment"] = pd.to_numeric(df_spec["avg_medicare_payment"], errors="coerce")
        top = df_spec.nlargest(12, "avg_medicare_payment")
        fig2 = px.bar(
            top,
            x="avg_medicare_payment",
            y="provider_type",
            orientation="h",
            color="avg_medicare_payment",
            color_continuous_scale="Teal",
            labels={"avg_medicare_payment": "Avg Medicare Payment ($)", "provider_type": ""},
            title="Top 12 Specialties",
        )
        fig2.update_layout(margin=dict(l=0, r=0, t=40, b=0), height=350, showlegend=False, coloraxis_showscale=False)
        st.plotly_chart(fig2, use_container_width=True)
    else:
        st.info("No specialty data yet — run the pipeline first.")

# ── Footer ────────────────────────────────────────────────────────────────
st.divider()
st.caption("Built with PySpark · dbt · FastAPI · Airflow · Streamlit · PostgreSQL · Docker")
