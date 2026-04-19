# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # IlluminaGrid — Demo Seed Data
# MAGIC
# MAGIC Generates synthetic solar permit records and writes them directly to
# MAGIC `illuminagrid.bronze.permits` for use when the ZenPower API is unavailable
# MAGIC during the hackathon demo.
# MAGIC
# MAGIC **Synthetic records are always distinguishable from real ZenPower data** —
# MAGIC the `raw_json` payload carries `"simulation_source": "SEED_DATA"` so the
# MAGIC field survives the full bronze → silver → gold transformation chain.
# MAGIC
# MAGIC **Seed cities and volumes:**
# MAGIC
# MAGIC | City      | State | ZIPs                        | Permits |
# MAGIC |-----------|-------|-----------------------------|---------|
# MAGIC | San Diego | CA    | 92101, 92103, 92108         | 200     |
# MAGIC | Austin    | TX    | 78701, 78704, 78745         | 150     |
# MAGIC | Chicago   | IL    | 60601, 60614, 60647         | 150     |
# MAGIC
# MAGIC **Distributions:**
# MAGIC - System sizes  : 4, 6, 8, 10, 12, 15 kW (uniform random choice)
# MAGIC - Install dates : uniformly distributed over the past 24 months
# MAGIC - Status        : 70 % VALID · 20 % PENDING · 10 % INVALID
# MAGIC
# MAGIC Re-running this notebook is idempotent — records are MERGEd on `permit_id`.

# COMMAND ----------
import json
import random
import uuid
from datetime import datetime, timedelta, timezone

from pyspark.sql import Row
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType, TimestampType,
)
from delta.tables import DeltaTable

# COMMAND ----------
# ── Seed configuration ────────────────────────────────────────────────────────

RANDOM_SEED = 42          # reproducible runs
random.seed(RANDOM_SEED)

SIMULATION_SOURCE = "SEED_DATA"   # sentinel — never overwrite with this value
INGESTED_AT = datetime.now(timezone.utc)

SYSTEM_SIZES_KW = [4.0, 6.0, 8.0, 10.0, 12.0, 15.0]

# Weighted status distribution: 70 % VALID, 20 % PENDING, 10 % INVALID
STATUS_CHOICES  = ["VALID",   "PENDING", "INVALID"]
STATUS_WEIGHTS  = [0.70,      0.20,      0.10]

SEED_CITIES = [
    {
        "city":    "San Diego",
        "state":   "CA",
        "zips":    ["92101", "92103", "92108"],
        "count":   200,
        # Base lat/lon per ZIP for realistic coordinate jitter
        "zip_coords": {
            "92101": (32.7157, -117.1611),
            "92103": (32.7438, -117.1691),
            "92108": (32.7659, -117.1390),
        },
    },
    {
        "city":    "Austin",
        "state":   "TX",
        "zips":    ["78701", "78704", "78745"],
        "count":   150,
        "zip_coords": {
            "78701": (30.2672, -97.7431),
            "78704": (30.2500, -97.7590),
            "78745": (30.2080, -97.7910),
        },
    },
    {
        "city":    "Chicago",
        "state":   "IL",
        "zips":    ["60601", "60614", "60647"],
        "count":   150,
        "zip_coords": {
            "60601": (41.8858, -87.6181),
            "60614": (41.9218, -87.6510),
            "60647": (41.9213, -87.7001),
        },
    },
]

# COMMAND ----------
# ── Date helpers ──────────────────────────────────────────────────────────────

def _random_install_date(lookback_days: int = 730) -> str:
    """Return a random ISO date string within the past `lookback_days` days."""
    offset = random.randint(0, lookback_days)
    d = (INGESTED_AT - timedelta(days=offset)).date()
    return str(d)


def _jitter(base: float, spread: float = 0.02) -> float:
    """Add small random noise to a coordinate so pins don't stack."""
    return round(base + random.uniform(-spread, spread), 6)

# COMMAND ----------
# ── Row generator ─────────────────────────────────────────────────────────────

def _make_rows(city_cfg: dict) -> list[dict]:
    city  = city_cfg["city"]
    state = city_cfg["state"]
    zips  = city_cfg["zips"]
    count = city_cfg["count"]
    coords = city_cfg["zip_coords"]

    rows = []
    for i in range(count):
        zip_code        = zips[i % len(zips)]
        system_size_kw  = random.choice(SYSTEM_SIZES_KW)
        install_date    = _random_install_date()
        status          = random.choices(STATUS_CHOICES, weights=STATUS_WEIGHTS, k=1)[0]
        lat, lon        = coords[zip_code]

        # Unique, deterministic-ish permit ID: SEED-{CITY_ABBREV}-{zero-padded index}
        city_abbrev = city.replace(" ", "").upper()[:6]
        permit_id   = f"SEED-{city_abbrev}-{str(i + 1).zfill(6)}"

        raw_payload = {
            "permit_id":        permit_id,
            "city":             city,
            "state":            state,
            "zip_code":         zip_code,
            "system_size_kw":   system_size_kw,
            "install_date":     install_date,
            "status":           status,
            "latitude":         _jitter(lat),
            "longitude":        _jitter(lon),
            "simulation_source": SIMULATION_SOURCE,
        }

        rows.append({
            "permit_id":      permit_id,
            "city":           city,
            "state":          state,
            "zip_code":       zip_code,
            "system_size_kw": system_size_kw,
            "install_date":   install_date,
            # Bronze only marks INVALID when required fields are absent;
            # PENDING is not a bronze-layer status in the real pipeline, but
            # we write it verbatim here so the silver layer can handle it
            # and downstream status distributions look realistic.
            "status":         status,
            "raw_json":       json.dumps(raw_payload),
            "ingested_at":    INGESTED_AT,
        })

    return rows

# COMMAND ----------
# ── Build all rows ────────────────────────────────────────────────────────────

all_rows: list[dict] = []
for city_cfg in SEED_CITIES:
    city_rows = _make_rows(city_cfg)
    all_rows.extend(city_rows)
    print(f"IlluminaGrid seed: generated {len(city_rows)} rows for {city_cfg['city']}, {city_cfg['state']}")

print(f"\nTotal seed rows to write: {len(all_rows)}")

# COMMAND ----------
# MAGIC %md ## Write to illuminagrid.bronze.permits

# COMMAND ----------
# Reuse the table schema defined in 00_schema_setup.
BRONZE_SCHEMA = StructType([
    StructField("permit_id",      StringType(),    nullable=False),
    StructField("zip_code",       StringType(),    nullable=True),
    StructField("city",           StringType(),    nullable=True),
    StructField("state",          StringType(),    nullable=True),
    StructField("system_size_kw", DoubleType(),    nullable=True),
    StructField("install_date",   StringType(),    nullable=True),
    StructField("status",         StringType(),    nullable=True),
    StructField("raw_json",       StringType(),    nullable=True),
    StructField("ingested_at",    TimestampType(), nullable=True),
])

seed_df = spark.createDataFrame(
    [Row(**r) for r in all_rows],
    schema=BRONZE_SCHEMA,
)

bronze_table = DeltaTable.forName(spark, "illuminagrid.bronze.permits")

(
    bronze_table.alias("target")
    .merge(seed_df.alias("source"), "target.permit_id = source.permit_id")
    # Only insert rows that don't already exist; never overwrite real ZenPower data
    # with seed records — if a permit_id already exists we leave it untouched.
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------
# ── Per-city verification ─────────────────────────────────────────────────────

written_summary = (
    spark.table("illuminagrid.bronze.permits")
    .filter("permit_id LIKE 'SEED-%'")
    .groupBy("city", "state")
    .count()
    .orderBy("city")
)

display(written_summary)

# COMMAND ----------
# ── Final summary ─────────────────────────────────────────────────────────────

seed_total = (
    spark.table("illuminagrid.bronze.permits")
    .filter("permit_id LIKE 'SEED-%'")
    .count()
)

bronze_total = spark.table("illuminagrid.bronze.permits").count()

per_city_rows = written_summary.collect()
city_lines = "".join(
    f'  <span style="color:#cdd6f4">{r["city"]}, {r["state"]}</span>'
    f' : {r["count"]} seed rows<br>'
    for r in per_city_rows
)

from datetime import datetime, timezone
completed_at = datetime.now(timezone.utc).isoformat()

displayHTML(f"""
<div style="font-family:monospace; padding:16px; background:#1e1e2e; color:#cdd6f4;
            border-radius:8px; line-height:1.9">
  <b style="font-size:1.2em; color:#89b4fa">IlluminaGrid — Demo Seed Data</b><br><br>
  <span style="color:#a6e3a1">seed_rows_generated  </span>: {len(all_rows)}<br>
  <span style="color:#a6e3a1">seed_rows_in_bronze  </span>: {seed_total}<br>
  <span style="color:#cdd6f4">total_bronze_rows    </span>: {bronze_total}<br><br>
  {city_lines}
  <br>
  <span style="color:#6c7086">simulation_source = {SIMULATION_SOURCE!r}</span><br>
  <span style="color:#6c7086">All seed permit_ids match SEED-* — distinguishable from ZenPower data at any layer.</span><br>
  <span style="color:#6c7086">Re-run notebooks 03 → 06 to propagate seed data through silver and gold layers.</span><br><br>
  <span style="color:#6c7086">Completed at {completed_at}Z</span>
</div>
""")
