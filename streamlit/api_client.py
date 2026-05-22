"""Thin wrapper around the CMS HealthFlow FastAPI."""

import os
import requests
import pandas as pd
import streamlit as st

BASE = os.getenv("API_BASE_URL", "http://localhost:8000")


def _get(path: str, params: dict = None) -> list | dict | None:
    try:
        r = requests.get(f"{BASE}{path}", params=params, timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"API error: {e}")
        return None


@st.cache_data(ttl=60)
def health() -> dict:
    return _get("/api/v1/health") or {}


@st.cache_data(ttl=120)
def search_providers(state=None, specialty=None, zip_code=None,
                     min_payment=None, max_payment=None, limit=100) -> pd.DataFrame:
    params = {k: v for k, v in dict(
        state=state, specialty=specialty, zip_code=zip_code,
        min_payment=min_payment, max_payment=max_payment, limit=limit
    ).items() if v is not None}
    data = _get("/api/v1/providers", params) or []
    return pd.DataFrame(data)


@st.cache_data(ttl=120)
def get_provider(npi: str) -> dict:
    return _get(f"/api/v1/providers/{npi}") or {}


@st.cache_data(ttl=120)
def procedure_costs(hcpcs_code: str, state=None) -> pd.DataFrame:
    params = {}
    if state:
        params["state"] = state
    data = _get(f"/api/v1/procedures/{hcpcs_code}/costs", params) or []
    return pd.DataFrame(data)


@st.cache_data(ttl=120)
def search_procedures(q: str = None, state: str = None, limit: int = 50) -> pd.DataFrame:
    params = {k: v for k, v in dict(q=q, state=state, limit=limit).items() if v}
    data = _get("/api/v1/procedures", params) or []
    return pd.DataFrame(data)


@st.cache_data(ttl=120)
def specialties(state=None) -> pd.DataFrame:
    params = {"state": state} if state else {}
    data = _get("/api/v1/analytics/specialties", params) or []
    return pd.DataFrame(data)


@st.cache_data(ttl=120)
def state_summary() -> pd.DataFrame:
    data = _get("/api/v1/analytics/state-summary") or []
    return pd.DataFrame(data)


@st.cache_data(ttl=120)
def cost_by_geography(state=None, min_providers=5) -> pd.DataFrame:
    params = {"min_providers": min_providers}
    if state:
        params["state"] = state
    data = _get("/api/v1/analytics/cost-by-geography", params) or []
    return pd.DataFrame(data)


@st.cache_data(ttl=120)
def hospital_rankings(state=None, metric="rating") -> pd.DataFrame:
    params = {"metric": metric}
    if state:
        params["state"] = state
    data = _get("/api/v1/hospitals/rankings", params) or []
    return pd.DataFrame(data)
