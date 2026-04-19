"""
Leaderboard endpoint.

GET /api/leaderboard — city rankings ordered by High Yield Index score.
"""

from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app import db

router = APIRouter(tags=["leaderboard"])

CATALOG = "workspace.default"


class LeaderboardEntry(BaseModel):
    rank: int
    city: str
    state: str
    total_permits: int
    total_active_permits: int
    total_annual_kwh: float
    total_annual_savings_usd: float
    total_co2_offset_metric_tons: float
    avg_system_size_kw: float
    high_yield_index_score: float
    last_updated: datetime


class Leaderboard(BaseModel):
    total: int
    state_filter: Optional[str]
    entries: List[LeaderboardEntry]


@router.get("/leaderboard", response_model=Leaderboard)
def get_leaderboard(
    state: Optional[str] = Query(default=None),
    limit: int = Query(default=20, ge=1, le=200),
) -> Leaderboard:
    where_parts = ["high_yield_index_score IS NOT NULL"]

    if state is not None:
        where_parts.append(f"UPPER(state) = UPPER('{state.upper().replace(chr(39), '')}')")

    where_clause = " AND ".join(where_parts)

    count_sql = f"SELECT COUNT(*) FROM {CATALOG}.city_summaries WHERE {where_clause}"
    data_sql = f"""
        SELECT
            city, state, total_permits, total_active_permits,
            total_annual_kwh, total_annual_savings_usd,
            total_co2_offset_metric_tons, avg_system_size_kw,
            high_yield_index_score, last_updated
        FROM {CATALOG}.city_summaries
        WHERE {where_clause}
        ORDER BY high_yield_index_score DESC
        LIMIT {int(limit)}
    """

    try:
        with db.get_cursor() as cursor:
            cursor.execute(count_sql)
            total = cursor.fetchone()[0] or 0
            cursor.execute(data_sql)
            rows = cursor.fetchall()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    entries = [
        LeaderboardEntry(
            rank=idx + 1,
            city=r[0], state=r[1],
            total_permits=r[2] or 0,
            total_active_permits=r[3] or 0,
            total_annual_kwh=r[4] or 0.0,
            total_annual_savings_usd=r[5] or 0.0,
            total_co2_offset_metric_tons=r[6] or 0.0,
            avg_system_size_kw=r[7] or 0.0,
            high_yield_index_score=r[8] or 0.0,
            last_updated=r[9],
        )
        for idx, r in enumerate(rows)
    ]

    return Leaderboard(total=total, state_filter=state, entries=entries)
