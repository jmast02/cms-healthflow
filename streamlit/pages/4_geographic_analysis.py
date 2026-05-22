"""Geographic Analysis — Medicare cost heatmaps by state and ZIP."""

import streamlit as st
import plotly.express as px
import pandas as pd

st.set_page_config(page_title="Geographic Analysis", page_icon="🗺️", layout="wide")

from api_client import cost_by_geography, state_summary

st.title("🗺️ Geographic Analysis")
st.caption("Visualize how Medicare payments vary across states and ZIP codes.")

tab1, tab2 = st.tabs(["State-Level Heatmap", "ZIP Code Drill-down"])

with tab1:
    with st.spinner("Loading state data..."):
        df_states = state_summary()

    if df_states.empty:
        st.info("No data — run the pipeline first.")
    else:
        df_states = df_states.rename(columns={"provider_type": "state"})
        for col in ["avg_medicare_payment", "provider_count", "total_services"]:
            df_states[col] = pd.to_numeric(df_states[col], errors="coerce")

        metric = st.radio(
            "Color by",
            ["avg_medicare_payment", "provider_count", "total_services"],
            format_func=lambda x: {
                "avg_medicare_payment": "Avg Medicare Payment ($)",
                "provider_count":       "Number of Providers",
                "total_services":       "Total Services",
            }[x],
            horizontal=True,
        )

        label_map = {
            "avg_medicare_payment": "Avg Payment ($)",
            "provider_count":       "# Providers",
            "total_services":       "Total Services",
        }

        fig = px.choropleth(
            df_states,
            locations="state",
            locationmode="USA-states",
            color=metric,
            scope="usa",
            color_continuous_scale="Blues",
            labels={metric: label_map[metric]},
            hover_data=["state", "avg_medicare_payment", "provider_count", "total_services"],
            title=f"United States — {label_map[metric]} by State",
        )
        fig.update_layout(margin=dict(l=0, r=0, t=50, b=0), height=500)
        st.plotly_chart(fig, use_container_width=True)

        # Bar chart for ranking
        fig_bar = px.bar(
            df_states.sort_values(metric, ascending=False).head(20),
            x="state", y=metric,
            color=metric, color_continuous_scale="Blues",
            labels={"state": "State", metric: label_map[metric]},
            title=f"Top 20 States — {label_map[metric]}",
        )
        fig_bar.update_layout(coloraxis_showscale=False, height=350)
        st.plotly_chart(fig_bar, use_container_width=True)

with tab2:
    st.subheader("ZIP Code Cost Distribution")

    state_zip = st.selectbox("Select State", [
        "FL","CA","TX","NY","IL","PA","OH","GA","NC","MI",
        "NJ","VA","WA","AZ","MA","TN","IN","MD","MO","WI",
    ])
    min_prov = st.slider("Min providers per ZIP", 1, 20, 3)

    with st.spinner("Loading ZIP data..."):
        df_zip = cost_by_geography(state=state_zip, min_providers=min_prov)

    if df_zip.empty:
        st.info(f"No ZIP code data for {state_zip}.")
    else:
        df_zip["avg_medicare_payment"] = pd.to_numeric(df_zip["avg_medicare_payment"], errors="coerce")

        st.metric(f"ZIP codes in {state_zip}", len(df_zip))

        fig_zip = px.histogram(
            df_zip,
            x="avg_medicare_payment",
            nbins=30,
            labels={"avg_medicare_payment": "Avg Medicare Payment ($)"},
            title=f"Distribution of Average Medicare Payment by ZIP — {state_zip}",
            color_discrete_sequence=["#1f77b4"],
        )
        fig_zip.update_layout(height=350)
        st.plotly_chart(fig_zip, use_container_width=True)

        st.dataframe(
            df_zip[["provider_zip", "total_providers", "avg_medicare_payment", "total_services"]]
            .sort_values("avg_medicare_payment", ascending=False)
            .rename(columns={
                "provider_zip":          "ZIP",
                "total_providers":       "# Providers",
                "avg_medicare_payment":  "Avg Payment ($)",
                "total_services":        "Total Services",
            }),
            use_container_width=True, height=400,
        )
