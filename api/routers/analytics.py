"""Geographic and aggregate analytics endpoints."""

from typing import Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.db import get_db
from api.models.provider import CostByGeography
from api.schemas.provider import GeographyCostSummary

router = APIRouter(prefix="/analytics", tags=["analytics"])


@router.get("/cost-by-geography", response_model=list[GeographyCostSummary],
            summary="Average Medicare costs by geography")
async def cost_by_geography(
    state:          Optional[str] = Query(None, description="Filter to a specific state"),
    min_providers:  int = Query(5, description="Minimum providers per ZIP to include"),
    limit:          int = Query(200, ge=1, le=1000),
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
