"""
City-scoped endpoints.

GET /api/city/{city_name}/summary   — aggregated KPIs from city_summaries
GET /api/city/{city_name}/permits   — paginated enriched permits
GET /api/city/{city_name}/forecast  — 12-month forward projection
"""

import math
from datetime import date, datetime, timezone
from typing import List, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from app import db

router = APIRouter(tags=["city"])


# ── Pydantic response models ──────────────────────────────────────────────────

class CitySummary(BaseModel):
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


class Permit(BaseModel):
    permit_id: str
    zip_code: Optional[str]
    city: str
    state: str
    system_size_kw: Optional[float]
    install_date: Optional[date]
    status: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    ac_annual_kwh: Optional[float]
    electricity_rate_per_kwh: Optional[float]
    annual_savings_usd: Optional[float]
    co2_offset_metric_tons: Optional[float]
    enriched_at: Optional[datetime]


class PermitPage(BaseModel):
    city: str
    total: int
    limit: int
    offset: int
    permits: List[Permit]


class CityForecast(BaseModel):
    city: str
    state: str
    velocity_permits_per_month: float
    avg_system_size_kw: float
    months: List[str]
    monthly_projected_kwh: List[float]
    monthly_projected_savings_usd: List[float]
    monthly_projected_co2_metric_tons: List[float]


# ── Helper ────────────────────────────────────────────────────────────────────

def _month_label(year: int, month: int) -> str:
    return datetime(year, month, 1).strftime("%b %Y")


def _advance_month(year: int, month: int, delta: int) -> tuple[int, int]:
    total = month - 1 + delta
    return year + total // 12, total % 12 + 1


# ── Endpoint 1: city summary ──────────────────────────────────────────────────

@router.get("/city/{city_name}/summary", response_model=CitySummary)
def get_city_summary(city_name: str) -> CitySummary:
    """
    Returns aggregated KPIs for a city from illuminagrid.gold.city_summaries.
    Raises 404 when the city is not present in the table.
    """
    sql = """
        SELECT
            city,
            state,
            total_permits,
            total_active_permits,
            total_annual_kwh,
            total_annual_savings_usd,
            total_co2_offset_metric_tons,
            avg_system_size_kw,
            high_yield_index_score,
            last_updated
        FROM illuminagrid.gold.city_summaries
        WHERE LOWER(city) = LOWER(%s)
        LIMIT 1
    """
    try:
        with db.get_cursor() as cursor:
            cursor.execute(sql, [city_name])
            row = cursor.fetchone()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    if row is None:
        raise HTTPException(status_code=404, detail=f"City '{city_name}' not found.")

    return CitySummary(
        city=row[0],
        state=row[1],
        total_permits=row[2] or 0,
        total_active_permits=row[3] or 0,
        total_annual_kwh=row[4] or 0.0,
        total_annual_savings_usd=row[5] or 0.0,
        total_co2_offset_metric_tons=row[6] or 0.0,
        avg_system_size_kw=row[7] or 0.0,
        high_yield_index_score=row[8] or 0.0,
        last_updated=row[9],
    )


# ── Endpoint 2: paginated permit list ────────────────────────────────────────

@router.get("/city/{city_name}/permits", response_model=PermitPage)
def get_city_permits(
    city_name: str,
    status: Optional[str] = Query(default=None, description="Filter by permit status, e.g. VALID"),
    zip_code: Optional[str] = Query(default=None, description="Filter by 5-digit ZIP code"),
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
) -> PermitPage:
    """
    Returns a paginated list of enriched permit records for a city.
    Supports optional filtering by status and zip_code.
    """
    # Build WHERE clause safely — all predicates use %s parameters.
    where_parts = ["LOWER(city) = LOWER(%s)"]
    params: list = [city_name]

    if status is not None:
        where_parts.append("status = %s")
        params.append(status.upper())

    if zip_code is not None:
        where_parts.append("zip_code = %s")
        params.append(zip_code)

    where_clause = " AND ".join(where_parts)

    count_sql = f"SELECT COUNT(*) FROM illuminagrid.gold.enriched_permits WHERE {where_clause}"

    data_sql = f"""
        SELECT
            permit_id,
            zip_code,
            city,
            state,
            system_size_kw,
            install_date,
            status,
            latitude,
            longitude,
            ac_annual_kwh,
            electricity_rate_per_kwh,
            annual_savings_usd,
            co2_offset_metric_tons,
            enriched_at
        FROM illuminagrid.gold.enriched_permits
        WHERE {where_clause}
        ORDER BY enriched_at DESC
        LIMIT %s OFFSET %s
    """

    try:
        with db.get_cursor() as cursor:
            cursor.execute(count_sql, params)
            total = cursor.fetchone()[0] or 0

            cursor.execute(data_sql, params + [limit, offset])
            rows = cursor.fetchall()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    permits = [
        Permit(
            permit_id=r[0],
            zip_code=r[1],
            city=r[2],
            state=r[3],
            system_size_kw=r[4],
            install_date=r[5],
            status=r[6],
            latitude=r[7],
            longitude=r[8],
            ac_annual_kwh=r[9],
            electricity_rate_per_kwh=r[10],
            annual_savings_usd=r[11],
            co2_offset_metric_tons=r[12],
            enriched_at=r[13],
        )
        for r in rows
    ]

    return PermitPage(city=city_name, total=total, limit=limit, offset=offset, permits=permits)


# ── Endpoint 3: 12-month forward forecast ────────────────────────────────────

@router.get("/city/{city_name}/forecast", response_model=CityForecast)
def get_city_forecast(city_name: str) -> CityForecast:
    """
    Computes a simple 12-month forward projection for a city.

    Velocity  = permits added in the past 6 months / 6
    Per-month = velocity × (avg_ac_annual_kwh / 12)
               velocity × (avg_annual_savings_usd / 12)
               velocity × (avg_co2_offset_metric_tons / 12)
    """
    sql = """
        SELECT
            COUNT(*)                        AS permit_count,
            COALESCE(AVG(ac_annual_kwh),     0) AS avg_annual_kwh,
            COALESCE(AVG(annual_savings_usd), 0) AS avg_annual_savings,
            COALESCE(AVG(co2_offset_metric_tons), 0) AS avg_co2,
            COALESCE(AVG(system_size_kw),    0) AS avg_system_size,
            DATEDIFF(
                COALESCE(MAX(install_date), CURRENT_DATE()),
                ADD_MONTHS(CURRENT_DATE(), -6)
            )                               AS date_span_days
        FROM illuminagrid.gold.enriched_permits
        WHERE LOWER(city) = LOWER(%s)
          AND install_date >= ADD_MONTHS(CURRENT_DATE(), -6)
          AND status = 'VALID'
          AND ac_annual_kwh IS NOT NULL
    """
    try:
        with db.get_cursor() as cursor:
            cursor.execute(sql, [city_name])
            row = cursor.fetchone()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    if row is None or row[0] == 0:
        raise HTTPException(
            status_code=404,
            detail=f"No active permit data found for '{city_name}' in the past 6 months.",
        )

    permit_count, avg_kwh, avg_savings, avg_co2, avg_size, date_span = row

    # Velocity: permits per month over the observed window (minimum 1 month denominator)
    months_observed = max((date_span or 180) / 30.0, 1.0)
    velocity = permit_count / months_observed

    # Monthly fractions from annual per-permit averages
    monthly_kwh_per_permit     = avg_kwh    / 12.0
    monthly_savings_per_permit = avg_savings / 12.0
    monthly_co2_per_permit     = avg_co2     / 12.0

    # Generate labels for the next 12 calendar months
    now = datetime.now(timezone.utc)
    year, month = now.year, now.month

    months_labels: list[str] = []
    proj_kwh: list[float] = []
    proj_savings: list[float] = []
    proj_co2: list[float] = []

    for i in range(1, 13):
        y, m = _advance_month(year, month, i)
        months_labels.append(_month_label(y, m))
        proj_kwh.append(round(velocity * monthly_kwh_per_permit, 2))
        proj_savings.append(round(velocity * monthly_savings_per_permit, 2))
        proj_co2.append(round(velocity * monthly_co2_per_permit, 4))

    return CityForecast(
        city=city_name,
        state="",          # state not in the query — caller can join from /summary
        velocity_permits_per_month=round(velocity, 2),
        avg_system_size_kw=round(avg_size, 2),
        months=months_labels,
        monthly_projected_kwh=proj_kwh,
        monthly_projected_savings_usd=proj_savings,
        monthly_projected_co2_metric_tons=proj_co2,
    )
