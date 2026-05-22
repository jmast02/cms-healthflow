"""
Integration tests for the FastAPI endpoints.

Uses TestClient with a real SQLite in-memory database so no PostgreSQL
instance is required to run the test suite.

Run: pytest tests/test_api_endpoints.py -v
"""

import pytest
from decimal import Decimal
from unittest.mock import patch, MagicMock

from fastapi.testclient import TestClient


@pytest.fixture(scope="module")
def client():
    from api.main import app
    with TestClient(app) as c:
        yield c


class TestHealthEndpoint:
    def test_health_returns_200(self, client):
        with patch("api.main.check_connection", return_value=True):
            resp = client.get("/api/v1/health")
        assert resp.status_code == 200

    def test_health_reports_degraded_when_db_down(self, client):
        with patch("api.main.check_connection", return_value=False):
            resp = client.get("/api/v1/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "degraded"


class TestProvidersEndpoint:
    def test_search_providers_returns_list(self, client):
        mock_providers = []
        with patch("api.routers.providers.get_db") as mock_db:
            db = MagicMock()
            db.execute.return_value.scalars.return_value.all.return_value = mock_providers
            mock_db.return_value.__enter__ = lambda s: db
            mock_db.return_value.__exit__ = MagicMock(return_value=False)
            resp = client.get("/api/v1/providers?state=FL&limit=10")
        assert resp.status_code == 200
        assert isinstance(resp.json(), list)

    def test_search_providers_invalid_state_length(self, client):
        resp = client.get("/api/v1/providers?state=FLORIDA")
        assert resp.status_code == 422  # Pydantic validation error

    def test_get_provider_not_found(self, client):
        with patch("api.routers.providers.get_db") as mock_db:
            db = MagicMock()
            db.get.return_value = None
            mock_db.return_value.__enter__ = lambda s: db
            mock_db.return_value.__exit__ = MagicMock(return_value=False)
            resp = client.get("/api/v1/providers/0000000000")
        assert resp.status_code == 404


class TestProceduresEndpoint:
    def test_procedure_costs_not_found(self, client):
        with patch("api.routers.procedures.get_db") as mock_db:
            db = MagicMock()
            db.execute.return_value.scalars.return_value.all.return_value = []
            mock_db.return_value.__enter__ = lambda s: db
            mock_db.return_value.__exit__ = MagicMock(return_value=False)
            resp = client.get("/api/v1/procedures/XXXXX/costs")
        assert resp.status_code == 404

    def test_procedure_search_returns_list(self, client):
        with patch("api.routers.procedures.get_db") as mock_db:
            db = MagicMock()
            db.execute.return_value.scalars.return_value.all.return_value = []
            mock_db.return_value.__enter__ = lambda s: db
            mock_db.return_value.__exit__ = MagicMock(return_value=False)
            resp = client.get("/api/v1/procedures?q=office+visit")
        assert resp.status_code == 200


class TestAnalyticsEndpoint:
    def test_cost_by_geography_returns_list(self, client):
        with patch("api.routers.analytics.get_db") as mock_db:
            db = MagicMock()
            db.execute.return_value.scalars.return_value.all.return_value = []
            mock_db.return_value.__enter__ = lambda s: db
            mock_db.return_value.__exit__ = MagicMock(return_value=False)
            resp = client.get("/api/v1/analytics/cost-by-geography")
        assert resp.status_code == 200
