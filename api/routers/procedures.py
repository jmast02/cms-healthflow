"""Procedure cost comparison endpoints."""

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from prometheus_client import Counter
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.db import get_db
from api.models.provider import ProcedureCost

_cost_lookups = Counter("cms_procedure_cost_lookups_total", "Procedure cost comparison requests")
from api.schemas.provider import ProcedureCostSummary

router = APIRouter(prefix="/procedures", tags=["procedures"])


@router.get("/{hcpcs_code}/costs", response_model=list[ProcedureCostSummary],
            summary="Compare procedure costs across states")
async def procedure_costs(
    hcpcs_code: str,
    state:      Optional[str] = Query(None, description="Filter to a specific state"),
    db: Session = Depends(get_db),
):
    """Compare Medicare payments for a procedure code across providers and states."""
    _cost_lookups.inc()
    stmt = select(ProcedureCost).where(ProcedureCost.hcpcs_code == hcpcs_code.upper())
    if state:
        stmt = stmt.where(ProcedureCost.provider_state == state.upper())

    stmt = stmt.order_by(ProcedureCost.avg_medicare_payment.desc())
    rows = db.execute(stmt).scalars().all()

    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No cost data found for HCPCS code {hcpcs_code}",
        )
    return rows


@router.get("", response_model=list[ProcedureCostSummary], summary="Search procedures")
async def search_procedures(
    q:     Optional[str] = Query(None, description="Search procedure description"),
    state: Optional[str] = Query(None, description="Filter by state"),
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """Search procedures by description."""
    stmt = select(ProcedureCost)
    if q:
        stmt = stmt.where(ProcedureCost.hcpcs_description.ilike(f"%{q}%"))
    if state:
        stmt = stmt.where(ProcedureCost.provider_state == state.upper())

    stmt = stmt.order_by(ProcedureCost.total_services.desc()).limit(limit)
    return db.execute(stmt).scalars().all()
