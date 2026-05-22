"""Hospital Rankings — CMS Hospital Compare quality scores."""

import streamlit as st
import plotly.express as px
import pandas as pd

st.set_page_config(page_title="Hospital Rankings", page_icon="🏨", layout="wide")

from api_client import hospital_rankings

st.title("🏨 Hospital Rankings")
st.caption("CMS Hospital Compare quality ratings — star scores, safety, readmissions, and patient satisfaction.")

col1, col2, col3 = st.columns(3)
with col1:
    state_filter = st.selectbox("State", ["All States"] + [
        "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID","IL","IN",
        "IA","KS","KY","LA","ME","MD","MA","MI","MN","MS","MO","MT","NE","NV",
        "NH","NJ","NM","NY","NC","ND","OH","OK","OR","PA","RI","SC","SD","TN",
        "TX","UT","VT","VA","WA","WV","WI","WY","DC"
    ])
with col2:
    min_stars = st.selectbox("Minimum Star Rating", [1, 2, 3, 4, 5], index=0)
with col3:
    metric = st.selectbox("Sort by", ["rating", "state_rank", "national_rank"],
                          format_func=lambda x: {
                              "rating":        "Overall Star Rating",
                              "state_rank":    "State Rank",
                              "national_rank": "National Rank",
                          }[x])

with st.spinner("Loading hospital data..."):
    df = hospital_rankings(
        state=None if state_filter == "All States" else state_filter,
        metric=metric,
    )

if df.empty:
    st.info("No hospital data yet — download the Hospital Compare dataset and run `make spark-hospitals`.")
    st.stop()

df = df[df["overall_rating"].notna() & (pd.to_numeric(df["overall_rating"], errors="coerce") >= min_stars)]

if df.empty:
    st.warning(f"No hospitals with {min_stars}+ stars in the selected filter.")
    st.stop()

df["overall_rating"] = pd.to_numeric(df["overall_rating"], errors="coerce")

# KPIs
k1, k2, k3 = st.columns(3)
k1.metric("Hospitals Shown",      len(df))
k2.metric("Avg Star Rating",      f"{df['overall_rating'].mean():.1f} ⭐")
k3.metric("5-Star Hospitals",     len(df[df["overall_rating"] == 5]))

# Star distribution
fig_stars = px.histogram(
    df, x="overall_rating", nbins=5,
    labels={"overall_rating": "CMS Star Rating"},
    title="Star Rating Distribution",
    color_discrete_sequence=["#f5a623"],
)
fig_stars.update_layout(height=280, bargap=0.2)
st.plotly_chart(fig_stars, use_container_width=True)

# Top hospitals table
st.subheader("Hospital Rankings")
display_cols = ["facility_name", "city", "state", "hospital_type",
                "overall_rating", "emergency_services",
                "readmission_national", "mortality_national",
                "safety_national", "patient_experience",
                "state_rank", "national_rank"]
display_cols = [c for c in display_cols if c in df.columns]

rename = {
    "facility_name":        "Hospital",
    "city":                 "City",
    "state":                "State",
    "hospital_type":        "Type",
    "overall_rating":       "⭐ Stars",
    "emergency_services":   "ER",
    "readmission_national": "Readmissions",
    "mortality_national":   "Mortality",
    "safety_national":      "Safety",
    "patient_experience":   "Patient Exp.",
    "state_rank":           "State Rank",
    "national_rank":        "National Rank",
}

st.dataframe(
    df[display_cols].rename(columns=rename).reset_index(drop=True),
    use_container_width=True,
    height=500,
)
