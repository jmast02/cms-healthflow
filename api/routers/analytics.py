"""Geographic and aggregate analytics endpoints."""

from decimal import Decimal
from typing import Optional

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from api.db import get_db
from api.models.provider import CostByGeography, ProviderProfile
from api.schemas.provider import GeographyCostSummary

router = APIRouter(prefix="/analytics", tags=["analytics"])


class SpecialtySummary(BaseModel):
    provider_type:        str
    provider_count:       int
    avg_medicare_payment: Optional[Decimal]
    total_services:       Optional[int]
    total_medicare_payment: Optional[Decimal]

    model_config = {"from_attributes": True}


@router.get("/cost-by-geography", response_model=list[GeographyCostSummary],
            summary="Average Medicare costs by geography")
async def cost_by_geography(
    state:         Optional[str] = Query(None, description="Filter to a specific state"),
    min_providers: int           = Query(5, description="Minimum providers per ZIP to include"),
    limit:         int           = Query(200, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    """Average Medicare costs aggregated by state and ZIP — useful for heatmap visualisation."""
    stmt = (
        select(CostByGeography)
        .where(CostByGeography.total_providers >= min_providers)
        .order_by(CostByGeography.avg_medicare_payment.desc())
        .limit(limit)
    )
    if state:
        stmt = stmt.where(CostByGeography.provider_state == state.upper())
    return db.execute(stmt).scalars().all()


@router.get("/specialties", response_model=list[SpecialtySummary],
            summary="Summary statistics by medical specialty")
async def specialties_summary(
    state: Optional[str] = Query(None, description="Filter to a specific state"),
    db: Session = Depends(get_db),
):
    """
    Aggregated Medicare payment stats per specialty.
    Shows which specialties have the highest average payments and volume.
    """
    stmt = (
        select(
            ProviderProfile.provider_type,
            func.count(ProviderProfile.provider_npi).label("provider_count"),
            func.avg(ProviderProfile.avg_medicare_payment).label("avg_medicare_payment"),
            func.sum(ProviderProfile.total_services).label("total_services"),
            func.sum(ProviderProfile.total_medicare_payment).label("total_medicare_payment"),
        )
        .where(ProviderProfile.provider_type.isnot(None))
        .group_by(ProviderProfile.provider_type)
        .order_by(func.avg(ProviderProfile.avg_medicare_payment).desc())
    )
    if state:
        stmt = stmt.where(ProviderProfile.provider_state == state.upper())

    rows = db.execute(stmt).all()
    return [
        SpecialtySummary(
            provider_type=r.provider_type,
            provider_count=r.provider_count,
            avg_medicare_payment=r.avg_medicare_payment,
            total_services=r.total_services,
            total_medicare_payment=r.total_medicare_payment,
        )
        for r in rows
    ]


@router.get("/state-summary", response_model=list[SpecialtySummary],
            summary="Provider and payment summary by state")
async def state_summary(db: Session = Depends(get_db)):
    """
    National breakdown: provider count, avg payment, and total services per state.
    """
    stmt = (
        select(
            ProviderProfile.provider_state.label("provider_type"),  # reuse schema field
            func.count(ProviderProfile.provider_npi).label("provider_count"),
            func.avg(ProviderProfile.avg_medicare_payment).label("avg_medicare_payment"),
            func.sum(ProviderProfile.total_services).label("total_services"),
            func.sum(ProviderProfile.total_medicare_payment).label("total_medicare_payment"),
        )
        .where(ProviderProfile.provider_state.isnot(None))
        .group_by(ProviderProfile.provider_state)
        .order_by(func.count(ProviderProfile.provider_npi).desc())
    )
    rows = db.execute(stmt).all()
    return [
        SpecialtySummary(
            provider_type=r.provider_type,
            provider_count=r.provider_count,
            avg_medicare_payment=r.avg_medicare_payment,
            total_services=r.total_services,
            total_medicare_payment=r.total_medicare_payment,
        )
        for r in rows
    ]
