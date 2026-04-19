# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Download Weather — NSRDB TMY CSV via NREL API
# MAGIC
# MAGIC Downloads one NSRDB PSM3 TMY CSV per city to DBFS and registers each file
# MAGIC in `illuminagrid.bronze.weather_manifest`.
# MAGIC
# MAGIC **This is the only notebook in the pipeline that calls the NREL API.**
# MAGIC All downstream simulation (notebook 04) reads the saved CSV files locally
# MAGIC via PySAM with no further network dependency on NREL.
# MAGIC
# MAGIC **Pipeline phase:** Pre-simulation (runs once per city before any enrichment)
# MAGIC **Target table:** `illuminagrid.bronze.weather_manifest`
# MAGIC **Target path:** `dbfs:/illuminagrid/weather/{state}/{city_slug}_tmy.csv`

# COMMAND ----------
import re
import time
import requests
from datetime import datetime, timezone

from pyspark.sql import Row
from delta.tables import DeltaTable

# COMMAND ----------
# ── Secrets (never hardcoded) ─────────────────────────────────────────────────
NREL_API_KEY = dbutils.secrets.get(scope="illuminagrid", key="nrel_api_key")
NREL_EMAIL   = dbutils.secrets.get(scope="illuminagrid", key="nrel_email")

# COMMAND ----------
# ── Widgets ───────────────────────────────────────────────────────────────────
# cities format: "San Diego,CA;Los Angeles,CA;Austin,TX"
dbutils.widgets.text(
    "cities",
    "San Diego,CA",
    "Cities (City,ST;City,ST)"
)
dbutils.widgets.dropdown(
    "force_refresh",
    "false",
    ["false", "true"],
    "Force re-download even if cached"
)

cities_raw    = dbutils.widgets.get("cities")
force_refresh = dbutils.widgets.get("force_refresh").lower() == "true"

# COMMAND ----------
# ── Parse city list ───────────────────────────────────────────────────────────
def _parse_cities(raw: str) -> list[dict]:
    cities = []
    for entry in raw.split(";"):
        entry = entry.strip()
        if not entry:
            continue
        parts = [p.strip() for p in entry.split(",")]
        if len(parts) != 2:
            raise ValueError(
                f"Invalid city format '{entry}' — expected 'City Name,ST' (e.g. 'San Diego,CA')"
            )
        cities.append({"city": parts[0], "state": parts[1].upper()})
    if not cities:
        raise ValueError("No cities provided in the 'cities' widget.")
    return cities

def _city_slug(city: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", city.lower()).strip("_")

cities = _parse_cities(cities_raw)
print(f"Cities to process ({len(cities)}): {[f\"{c['city']},{c['state']}\" for c in cities]}")
print(f"force_refresh = {force_refresh}")

# COMMAND ----------
# ── Constants ─────────────────────────────────────────────────────────────────
NSRDB_URL  = "https://developer.nrel.gov/api/nsrdb/v2/solar/psm3-tmy-download.csv"
DBFS_ROOT  = "dbfs:/illuminagrid/weather"

# COMMAND ----------
# ── Global backoff state ──────────────────────────────────────────────────────
# A 429 blocks the entire key. One hit pauses ALL subsequent requests so
# each city does not independently burn through its own retry window.
_resume_at: float = 0.0

def _backoff(seconds: int, reason: str) -> None:
    global _resume_at
    new_resume = time.monotonic() + seconds
    if new_resume > _resume_at:
        _resume_at = new_resume
    print(f"  [backoff] {reason} — pausing all requests for {seconds}s")

def _wait() -> None:
    remaining = _resume_at - time.monotonic()
    if remaining > 0:
        print(f"  [wait]   {remaining:.0f}s global cooldown remaining…")
        time.sleep(remaining)
    time.sleep(1.1)  # NSRDB enforces 1 req/s

# COMMAND ----------
# ── Ensure manifest table exists ─────────────────────────────────────────────
spark.sql("""
    CREATE TABLE IF NOT EXISTS illuminagrid.bronze.weather_manifest (
        city          STRING    NOT NULL,
        state         STRING    NOT NULL,
        dbfs_path     STRING,
        downloaded_at TIMESTAMP,
        nsrdb_version STRING
    )
    USING DELTA
""")

manifest = DeltaTable.forName(spark, "illuminagrid.bronze.weather_manifest")

# COMMAND ----------
# ── Look up representative lat/lon from zip_centroids ────────────────────────
def _get_centroid(city: str, state: str) -> tuple[float, float]:
    row = spark.sql(f"""
        SELECT latitude, longitude
        FROM   illuminagrid.bronze.zip_centroids
        WHERE  UPPER(TRIM(city))  = UPPER(TRIM('{city}'))
          AND  UPPER(TRIM(state)) = UPPER(TRIM('{state}'))
        ORDER  BY zip_code
        LIMIT  1
    """).first()
    if row is None:
        raise LookupError(
            f"No ZIP centroid found for {city}, {state}. "
            "Load illuminagrid.bronze.zip_centroids before running this notebook."
        )
    return float(row.latitude), float(row.longitude)

# COMMAND ----------
# ── NSRDB TMY download with global backoff ────────────────────────────────────
def _download_tmy(lat: float, lon: float) -> str:
    params = {
        "api_key":      NREL_API_KEY,
        "wkt":          f"POINT({lon} {lat})",
        "names":        "tmy-2020",
        "attributes":   "ghi,dhi,dni,wind_speed,air_temperature,surface_albedo",
        "leap_day":     "false",
        "interval":     "60",
        "utc":          "false",
        "full_name":    "IlluminaGrid",
        "email":        NREL_EMAIL,
        "affiliation":  "DataHacks2026",
        "mailing_list": "false",
        "reason":       "research",
    }
    for attempt in range(1, 4):
        _wait()
        resp = requests.get(NSRDB_URL, params=params, timeout=60)

        if resp.status_code == 200 and resp.text.startswith("Source"):
            return resp.text

        if resp.status_code == 200:
            raise RuntimeError(f"Unexpected 200 body: {resp.text[:300]}")

        if resp.status_code == 429:
            _backoff(120 * attempt, f"429 on attempt {attempt}/3")
            continue

        if resp.status_code == 404:
            # 404 after 429 = key temporarily blocked at gateway level
            _backoff(90 * attempt, f"404 (key block?) on attempt {attempt}/3")
            continue

        raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")

    raise RuntimeError("NSRDB API failed — exhausted retries")

# COMMAND ----------
# ── Check manifest for existing entries ──────────────────────────────────────
def _is_cached(city: str, state: str) -> bool:
    return spark.sql(f"""
        SELECT 1 FROM illuminagrid.bronze.weather_manifest
        WHERE  UPPER(city)  = UPPER('{city}')
          AND  UPPER(state) = UPPER('{state}')
        LIMIT 1
    """).count() > 0

# COMMAND ----------
# ── Save CSV to DBFS and update manifest ─────────────────────────────────────
def _save_and_register(city: str, state: str, csv_text: str) -> str:
    slug      = _city_slug(city)
    dbfs_path = f"{DBFS_ROOT}/{state}/{slug}_tmy.csv"
    dbutils.fs.put(dbfs_path, csv_text, overwrite=True)

    row_df = spark.createDataFrame([Row(
        city          = city,
        state         = state,
        dbfs_path     = dbfs_path,
        downloaded_at = datetime.now(timezone.utc),
        nsrdb_version = "PSM3-tmy-2020",
    )])
    (
        manifest.alias("t")
        .merge(row_df.alias("s"), "t.city = s.city AND t.state = s.state")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    return dbfs_path

# COMMAND ----------
# ── Main loop ─────────────────────────────────────────────────────────────────
results = []

for entry in cities:
    city_name = entry["city"]
    state     = entry["state"]
    label     = f"{city_name}, {state}"

    if not force_refresh and _is_cached(city_name, state):
        print(f"  [skip] {label} — already in manifest (force_refresh=false)")
        results.append({"city": label, "status": "SKIPPED", "path": ""})
        continue

    try:
        lat, lon = _get_centroid(city_name, state)
        print(f"  [fetch] {label} — lat={lat:.4f}, lon={lon:.4f}")

        csv_text  = _download_tmy(lat, lon)
        dbfs_path = _save_and_register(city_name, state, csv_text)

        print(f"  [ok]    {label} → {dbfs_path}")
        results.append({"city": label, "status": "DOWNLOADED", "path": dbfs_path})

    except LookupError as exc:
        print(f"  [warn]  {label} — {exc}")
        results.append({"city": label, "status": "NO_CENTROID", "path": ""})

    except Exception as exc:
        print(f"  [err]   {label} — {exc}")
        results.append({"city": label, "status": "FAILED", "path": str(exc)[:120]})

# COMMAND ----------
# ── Summary ───────────────────────────────────────────────────────────────────
downloaded  = sum(1 for r in results if r["status"] == "DOWNLOADED")
skipped     = sum(1 for r in results if r["status"] == "SKIPPED")
no_centroid = sum(1 for r in results if r["status"] == "NO_CENTROID")
failed      = sum(1 for r in results if r["status"] == "FAILED")

rows_html = "".join(
    f"<tr><td style='padding:3px 8px'>{r['city']}</td>"
    f"<td style='padding:3px 8px;color:{'#a6e3a1' if r['status'] in ('DOWNLOADED','SKIPPED') else '#f38ba8'}'>{r['status']}</td>"
    f"<td style='padding:3px 8px;color:#6c7086;font-size:0.85em'>{r['path']}</td></tr>"
    for r in results
)

displayHTML(f"""
<div style="font-family:monospace; padding:16px; background:#1e1e2e; color:#cdd6f4; border-radius:8px">
  <b style="font-size:1.2em; color:#89b4fa">NSRDB TMY Download — Summary</b><br><br>
  <span style="color:#a6e3a1">downloaded  </span>: {downloaded}<br>
  <span style="color:#a6e3a1">skipped     </span>: {skipped}<br>
  <span style="color:#fab387">no_centroid </span>: {no_centroid}<br>
  <span style="color:#f38ba8">failed      </span>: {failed}<br><br>
  <table style="width:100%; border-collapse:collapse">
    <tr style="color:#6c7086; border-bottom:1px solid #313244">
      <th style="text-align:left;padding:4px 8px">City</th>
      <th style="text-align:left;padding:4px 8px">Status</th>
      <th style="text-align:left;padding:4px 8px">DBFS Path</th>
    </tr>
    {rows_html}
  </table>
  <br><span style="color:#6c7086">Completed at {datetime.now(timezone.utc).isoformat()}Z</span>
</div>
""")
