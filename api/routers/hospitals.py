"""Hospital ranking endpoints backed by gold.hospital_rankings (Hospital Compare dataset)."""

from typing import Literal, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.orm import Session

from api.db import get_db
from api.models.provider import HospitalRanking
from api.schemas.provider import HospitalSummary

router = APIRouter(prefix="/hospitals", tags=["hospitals"])


@router.get("/rankings", response_model=list[HospitalSummary], summary="Hospital quality rankings")
async def hospital_rankings(
    state:  Optional[str] = Query(None, description="Filter by state (2-letter code)"),
    rating: Optional[int] = Query(None, ge=1, le=5, description="Minimum CMS star rating (1-5)"),
    metric: Literal["rating", "state_rank", "national_rank"] = Query(
        "rating", description="Sort metric"
    ),
    limit:  int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
):
    """
    Rank hospitals by CMS overall star rating or comparison metrics.
    Data source: CMS Hospital Compare — Hospital General Information.
    """
    stmt = select(HospitalRanking)

    if state:
        stmt = stmt.where(HospitalRanking.state == state.upper())
    if rating:
        stmt = stmt.where(HospitalRanking.overall_rating >= rating)

    order_col = {
        "rating":       HospitalRanking.overall_rating,
        "state_rank":   HospitalRanking.state_rank,
        "national_rank": HospitalRanking.national_rank,
    }[metric]

    stmt = stmt.order_by(order_col.desc().nulls_last()).limit(limit)
    return db.execute(stmt).scalars().all()


@router.get("/{facility_id}", response_model=HospitalSummary, summary="Get hospital detail")
async def get_hospital(facility_id: str, db: Session = Depends(get_db)):
    """Full quality profile for a specific hospital by CMS Facility ID."""
    hospital = db.get(HospitalRanking, facility_id)
    if not hospital:
        raise HTTPException(status_code=404, detail=f"Facility ID {facility_id} not found")
    return hospital
