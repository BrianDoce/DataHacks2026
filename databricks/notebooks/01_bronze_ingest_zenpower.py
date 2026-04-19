# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Bronze Ingest — ZenPower Solar Permits
# MAGIC
# MAGIC Pulls raw permit JSON from the ZenPower API for a single city and upserts
# MAGIC each record into `illuminagrid.bronze.permits` using `MERGE INTO permit_id`.
# MAGIC
# MAGIC **Pipeline phase:** Observation → Bronze
# MAGIC **Target table:** `illuminagrid.bronze.permits`

# COMMAND ----------
import json
import time
import requests
from datetime import datetime, timezone

from pyspark.sql import Row
from delta.tables import DeltaTable

# COMMAND ----------
# ── Secrets (never hardcoded) ─────────────────────────────────────────────────
ZENPOWER_API_KEY  = dbutils.secrets.get(scope="illuminagrid", key="zenpower_api_key")
ZENPOWER_BASE_URL = dbutils.secrets.get(scope="illuminagrid", key="zenpower_base_url")

# COMMAND ----------
# ── Widgets — parameterised by Databricks Workflows ──────────────────────────
dbutils.widgets.text("city",  "San Diego", "City")
dbutils.widgets.text("state", "CA",        "State (2-letter)")

city  = dbutils.widgets.get("city").strip()
state = dbutils.widgets.get("state").strip().upper()

print(f"Running ingestion for: {city}, {state}")

# COMMAND ----------
# ── Resolve target table (schema owned by 00_schema_setup) ───────────────────
delta_table = DeltaTable.forName(spark, "illuminagrid.bronze.permits")

# COMMAND ----------
# ── ZenPower pagination & partial-write logic ─────────────────────────────────

PAGE_SIZE     = 100
WRITE_EVERY   = 5       # flush a MERGE every N pages so partial runs persist
MAX_RETRIES   = 3
RETRY_BACKOFF = 2       # seconds, doubled on each retry

def _fetch_page(page: int) -> dict:
    """Fetch a single page from ZenPower API with retry/backoff."""
    url = f"{ZENPOWER_BASE_URL}/permits"
    params = {
        "city":     city,
        "state":    state,
        "page":     page,
        "per_page": PAGE_SIZE,
    }
    headers = {"Authorization": f"Bearer {ZENPOWER_API_KEY}"}
    delay = RETRY_BACKOFF
    for attempt in range(1, MAX_RETRIES + 1):
        resp = requests.get(url, params=params, headers=headers, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        if resp.status_code == 429 or resp.status_code >= 500:
            print(f"  [retry {attempt}/{MAX_RETRIES}] HTTP {resp.status_code} — waiting {delay}s")
            time.sleep(delay)
            delay *= 2
            continue
        resp.raise_for_status()
    raise RuntimeError(f"ZenPower API failed after {MAX_RETRIES} retries on page {page}")


def _classify(record: dict) -> dict:
    """
    Extract scalar fields, flag INVALID if required fields are absent,
    and always preserve the full raw JSON.
    """
    missing = [f for f in ("zip_code", "system_size_kw", "install_date") if not record.get(f)]
    status = "INVALID" if missing else "VALID"

    return {
        "permit_id":      str(record.get("permit_id", "")),
        "raw_json":       json.dumps(record),
        "status":         status,
        "city":           city,
        "state":          state,
        "zip_code":       str(record.get("zip_code"))       if record.get("zip_code")       else None,
        "system_size_kw": float(record["system_size_kw"])   if record.get("system_size_kw") else None,
        "install_date":   str(record.get("install_date"))   if record.get("install_date")   else None,
        "ingested_at":    datetime.now(timezone.utc),
    }


def _merge_batch(rows: list[dict]) -> int:
    """MERGE a list of classified rows into the Delta table on permit_id."""
    if not rows:
        return 0
    df = spark.createDataFrame([Row(**r) for r in rows])
    (
        delta_table.alias("target")
        .merge(df.alias("source"), "target.permit_id = source.permit_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    return len(rows)


# COMMAND ----------
# ── Main ingest loop ──────────────────────────────────────────────────────────

total_fetched  = 0
total_written  = 0
total_skipped  = 0   # records with no permit_id (unparseable)
page_buffer: list[dict] = []

page = 1
while True:
    payload = _fetch_page(page)

    records  = payload.get("data") or payload.get("results") or payload.get("permits") or []
    has_next = payload.get("has_next") or payload.get("next_page") or (len(records) == PAGE_SIZE)

    for record in records:
        total_fetched += 1
        if not record.get("permit_id"):
            total_skipped += 1
            continue
        page_buffer.append(_classify(record))

    # Flush to Delta every WRITE_EVERY pages so partial runs are not lost
    if page % WRITE_EVERY == 0 and page_buffer:
        written = _merge_batch(page_buffer)
        total_written += written
        print(f"  page {page}: merged {written} rows (running total: {total_written})")
        page_buffer = []

    if not has_next or not records:
        break
    page += 1
    time.sleep(0.2)

# Final flush
if page_buffer:
    written = _merge_batch(page_buffer)
    total_written += written
    print(f"  final flush: merged {written} rows")

# COMMAND ----------
# ── Summary output ────────────────────────────────────────────────────────────

displayHTML(f"""
<div style="font-family:monospace; padding:16px; background:#1e1e2e; color:#cdd6f4;
            border-radius:8px; line-height:1.8">
  <b style="font-size:1.2em; color:#89b4fa">ZenPower Bronze Ingest — {city}, {state}</b><br><br>
  <span style="color:#a6e3a1">total_fetched </span>: {total_fetched}<br>
  <span style="color:#a6e3a1">total_written </span>: {total_written}<br>
  <span style="color:#f38ba8">total_skipped </span>: {total_skipped}<br><br>
  <span style="color:#6c7086">Completed at {datetime.now(timezone.utc).isoformat()}Z</span>
</div>
""")
