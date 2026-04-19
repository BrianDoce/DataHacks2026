"""
IlluminaGrid FastAPI application.

Mounts two routers:
  /api/city/{city_name}/…   — per-city summary, permits, forecast
  /api/leaderboard          — ranked city list by High Yield Index

Health check at GET /health verifies the Databricks SQL connection is alive.
"""

import logging
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from app import db
from app.routers import city, leaderboard

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    db.init()
    yield
    db.close()


app = FastAPI(
    title="IlluminaGrid API",
    description="Solar permit analytics powered by Databricks SQL",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

app.include_router(city.router, prefix="/api")
app.include_router(leaderboard.router, prefix="/api")


@app.get("/health", tags=["ops"])
def health_check() -> dict:
    """
    Confirms the FastAPI process is running and that a Databricks SQL query
    round-trips successfully. Returns HTTP 503 if the warehouse is unreachable.
    """
    try:
        with db.get_cursor() as cursor:
            cursor.execute("SELECT 1 AS alive")
            row = cursor.fetchone()
            alive = bool(row and row[0] == 1)
    except Exception as exc:
        logger.error("Health check failed: %s", exc)
        raise HTTPException(status_code=503, detail=f"Databricks SQL unreachable: {exc}")

    return {"status": "ok", "databricks_sql": "connected" if alive else "error"}
