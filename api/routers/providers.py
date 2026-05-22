"""Provider search and detail endpoints."""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from prometheus_client import Counter
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from api.db import get_db
from api.models.provider import ProviderProfile

_searches = Counter("cms_provider_searches_total", "Provider search requests")
_detail_hits = Counter("cms_provider_detail_hits_total", "Provider detail lookups")
from api.schemas.provider import ProviderDetail, ProviderSummary

router = APIRouter(prefix="/providers", tags=["providers"])


@router.get("", response_model=list[ProviderSummary], summary="Search providers")
async def search_providers(
    state:        Optional[str] = Query(None, description="State code e.g. FL", min_length=2, max_length=2),
    specialty:    Optional[str] = Query(None, description="Provider specialty (partial match)"),
    zip_code:     Optional[str] = Query(None, description="ZIP code (5 digits)", min_length=5, max_length=5),
    min_payment:  Optional[float] = Query(None, description="Minimum average Medicare payment"),
    max_payment:  Optional[float] = Query(None, description="Maximum average Medicare payment"),
    limit:        int = Query(50, ge=1, le=500),
    offset:       int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    """Search and filter providers by location, specialty, and payment range."""
    _searches.inc()
    stmt = select(ProviderProfile)

    if state:
        stmt = stmt.where(ProviderProfile.provider_state == state.upper())
    if specialty:
        stmt = stmt.where(ProviderProfile.provider_type.ilike(f"%{specialty}%"))
    if zip_code:
        stmt = stmt.where(ProviderProfile.provider_zip == zip_code)
    if min_payment is not None:
        stmt = stmt.where(ProviderProfile.avg_medicare_payment >= min_payment)
    if max_payment is not None:
        stmt = stmt.where(ProviderProfile.avg_medicare_payment <= max_payment)

    stmt = stmt.order_by(ProviderProfile.avg_medicare_payment.desc()).limit(limit).offset(offset)
    rows = db.execute(stmt).scalars().all()
    return rows


@router.get("/{npi}", response_model=ProviderDetail, summary="Get provider detail")
async def get_provider(npi: str, db: Session = Depends(get_db)):
    """Full analytics profile for a specific provider by NPI."""
    _detail_hits.inc()
    provider = db.get(ProviderProfile, npi)
    if not provider:
        raise HTTPException(status_code=404, detail=f"Provider NPI {npi} not found")
    return provider


@router.get("/state/{state}/top", response_model=list[ProviderSummary], summary="Top providers by state")
async def top_providers_by_state(
    state:     str,
    specialty: Optional[str] = None,
    limit:     int = Query(25, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """Return the top providers in a state, ranked by average Medicare payment."""
    stmt = (
        select(ProviderProfile)
        .where(ProviderProfile.provider_state == state.upper())
    )
    if specialty:
        stmt = stmt.where(ProviderProfile.provider_type.ilike(f"%{specialty}%"))

    stmt = stmt.order_by(ProviderProfile.state_rank).limit(limit)
    return db.execute(stmt).scalars().all()
