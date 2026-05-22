"""Provider Explorer — search and filter Medicare providers."""

import streamlit as st
import plotly.express as px
import pandas as pd

st.set_page_config(page_title="Provider Explorer", page_icon="🔍", layout="wide")

from api_client import search_providers, get_provider

st.title("🔍 Provider Explorer")
st.caption("Search 50,000+ Medicare providers by location, specialty, and payment range.")

# ── Filters ───────────────────────────────────────────────────────────────
with st.expander("🔧 Search Filters", expanded=True):
    col1, col2, col3 = st.columns(3)
    with col1:
        state = st.selectbox("State", [""] + [
            "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN",
            "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV",
            "NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN",
            "TX","UT","VT","VA","WA","WV","WI","WY","DC"
        ])
        zip_code = st.text_input("ZIP Code", placeholder="e.g. 33101", max_chars=5)

    with col2:
        specialty = st.text_input("Specialty", placeholder="e.g. Cardiology")
        limit = st.slider("Max Results", 10, 500, 100, step=10)

    with col3:
        min_pay, max_pay = st.slider(
            "Avg Medicare Payment Range ($)",
            min_value=0, max_value=1000, value=(0, 1000), step=10
        )

    search_btn = st.button("🔍 Search", type="primary")

# ── Results ───────────────────────────────────────────────────────────────
if search_btn or state or specialty:
    with st.spinner("Searching providers..."):
        df = search_providers(
            state=state or None,
            specialty=specialty or None,
            zip_code=zip_code or None,
            min_payment=min_pay if min_pay > 0 else None,
            max_payment=max_pay if max_pay < 1000 else None,
            limit=limit,
        )

    if df.empty:
        st.warning("No providers found. Try adjusting your filters.")
        st.stop()

    st.success(f"Found **{len(df):,}** providers")

    # Numeric coercion
    for col in ["avg_medicare_payment", "total_services"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    col_left, col_right = st.columns(2)
    with col_left:
        st.metric("Avg Payment", f"${df['avg_medicare_payment'].mean():.2f}")
    with col_right:
        st.metric("Total Services", f"{df['total_services'].sum():,.0f}")

    # Chart
    if "provider_type" in df.columns:
        fig = px.box(
            df, x="provider_type", y="avg_medicare_payment",
            color="provider_type",
            labels={"avg_medicare_payment": "Avg Medicare Payment ($)", "provider_type": ""},
            title="Payment Distribution by Specialty",
        )
        fig.update_layout(showlegend=False, xaxis_tickangle=-30, height=300)
        st.plotly_chart(fig, use_container_width=True)

    # Table
    display_cols = ["provider_npi", "provider_name", "provider_type",
                    "provider_state", "provider_city", "avg_medicare_payment",
                    "total_services", "specialty_rank", "state_rank"]
    display_cols = [c for c in display_cols if c in df.columns]

    st.dataframe(
        df[display_cols].rename(columns={
            "provider_npi":          "NPI",
            "provider_name":         "Name",
            "provider_type":         "Specialty",
            "provider_state":        "State",
            "provider_city":         "City",
            "avg_medicare_payment":  "Avg Payment ($)",
            "total_services":        "Total Services",
            "specialty_rank":        "Specialty Rank",
            "state_rank":            "State Rank",
        }).style.format({"Avg Payment ($)": "${:.2f}", "Total Services": "{:,.0f}"}),
        use_container_width=True, height=400,
    )

    # Provider detail drill-down
    st.divider()
    st.subheader("Provider Detail")
    npi_input = st.text_input("Enter NPI to view full profile", placeholder="10-digit NPI")
    if npi_input and len(npi_input) == 10:
        detail = get_provider(npi_input)
        if detail:
            d1, d2, d3, d4 = st.columns(4)
            d1.metric("Avg Medicare Payment", f"${float(detail.get('avg_medicare_payment', 0)):.2f}")
            d2.metric("Total Services", f"{detail.get('total_services', 0):,}")
            d3.metric("Specialty Rank", f"#{detail.get('specialty_rank', '—')}")
            d4.metric("State Rank", f"#{detail.get('state_rank', '—')}")
            with st.expander("Full record"):
                st.json(detail)
