# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # IlluminaGrid — Demo Validation
# MAGIC
# MAGIC Runs a pre-demo checklist against the live workspace and fixture files.
# MAGIC Every check prints **DEMO READY** or **DEMO NOT READY** and a final
# MAGIC **PASS** / **FAIL** banner so you can confirm the pipeline is healthy
# MAGIC before presenting.
# MAGIC
# MAGIC **Checks performed:**
# MAGIC 1. All five required Delta tables exist in `illuminagrid.*`
# MAGIC 2. `gold.enriched_permits` has ≥ 100 rows with non-null `ac_annual_kwh`
# MAGIC 3. `gold.city_summaries` has rows for San Diego, Austin, and Chicago
# MAGIC 4. NSRDB weather files exist under `dbfs:/illuminagrid/weather/`
# MAGIC 5. Fixture JSON files match the expected FastAPI response schemas

# COMMAND ----------
import json
from pathlib import Path

# ── Result accumulator ────────────────────────────────────────────────────────
_results: list[dict] = []   # {"check": str, "passed": bool, "detail": str}

PASS_MARK = "✅  DEMO READY"
FAIL_MARK = "❌  DEMO NOT READY"

def record(check: str, passed: bool, detail: str = "") -> None:
    _results.append({"check": check, "passed": passed, "detail": detail})
    icon = PASS_MARK if passed else FAIL_MARK
    print(f"{icon}  [{check}]" + (f"  — {detail}" if detail else ""))

# COMMAND ----------
# MAGIC %md ## Check 1 · Delta tables exist

# COMMAND ----------
REQUIRED_TABLES = [
    ("bronze", "permits"),
    ("bronze", "zip_centroids"),
    ("silver", "permits"),
    ("gold",   "enriched_permits"),
    ("gold",   "city_summaries"),
]

existing_tables = {
    (row["table_schema"], row["table_name"])
    for row in (
        spark.sql("""
            SELECT table_schema, table_name
            FROM illuminagrid.information_schema.tables
            WHERE table_schema IN ('bronze', 'silver', 'gold')
        """)
        .collect()
    )
}

for schema, table in REQUIRED_TABLES:
    found = (schema, table) in existing_tables
    record(
        check=f"table_exists::{schema}.{table}",
        passed=found,
        detail="found" if found else f"illuminagrid.{schema}.{table} is missing",
    )

# COMMAND ----------
# MAGIC %md ## Check 2 · enriched_permits has ≥ 100 simulated rows

# COMMAND ----------
MIN_SIMULATED_ROWS = 100

try:
    simulated_count = (
        spark.table("illuminagrid.gold.enriched_permits")
        .filter("ac_annual_kwh IS NOT NULL")
        .count()
    )
    passed = simulated_count >= MIN_SIMULATED_ROWS
    record(
        check="enriched_permits::min_simulated_rows",
        passed=passed,
        detail=f"{simulated_count} rows with non-null ac_annual_kwh (need {MIN_SIMULATED_ROWS})",
    )
except Exception as exc:
    record(
        check="enriched_permits::min_simulated_rows",
        passed=False,
        detail=f"query failed: {exc}",
    )

# COMMAND ----------
# MAGIC %md ## Check 3 · city_summaries has rows for all three seed cities

# COMMAND ----------
SEED_CITIES = ["San Diego", "Austin", "Chicago"]

try:
    present_cities = {
        row["city"]
        for row in (
            spark.table("illuminagrid.gold.city_summaries")
            .filter("city IN ('San Diego', 'Austin', 'Chicago')")
            .select("city")
            .collect()
        )
    }
    for city in SEED_CITIES:
        found = city in present_cities
        record(
            check=f"city_summaries::city_present::{city}",
            passed=found,
            detail="present" if found else f"no row found for '{city}'",
        )
except Exception as exc:
    for city in SEED_CITIES:
        record(
            check=f"city_summaries::city_present::{city}",
            passed=False,
            detail=f"query failed: {exc}",
        )

# COMMAND ----------
# MAGIC %md ## Check 4 · NSRDB weather files on DBFS

# COMMAND ----------
WEATHER_DBFS_PREFIX = "dbfs:/illuminagrid/weather/"

# Map each seed city to the state subdirectory the download notebook uses.
EXPECTED_WEATHER_FILES = [
    "dbfs:/illuminagrid/weather/CA/san_diego_tmy.csv",
    "dbfs:/illuminagrid/weather/TX/austin_tmy.csv",
    "dbfs:/illuminagrid/weather/IL/chicago_tmy.csv",
]

# First confirm the top-level directory is reachable.
try:
    dbutils.fs.ls(WEATHER_DBFS_PREFIX)
    weather_root_ok = True
except Exception:
    weather_root_ok = False

record(
    check="dbfs::weather_root_exists",
    passed=weather_root_ok,
    detail=WEATHER_DBFS_PREFIX if weather_root_ok else f"{WEATHER_DBFS_PREFIX} not found on DBFS",
)

# Check each per-city file.
for path in EXPECTED_WEATHER_FILES:
    if not weather_root_ok:
        record(check=f"dbfs::weather_file::{path.split('/')[-1]}", passed=False,
               detail="skipped — weather root missing")
        continue
    try:
        dbutils.fs.ls(path)
        record(check=f"dbfs::weather_file::{path.split('/')[-1]}", passed=True)
    except Exception:
        record(
            check=f"dbfs::weather_file::{path.split('/')[-1]}",
            passed=False,
            detail=f"not found at {path}",
        )

# COMMAND ----------
# MAGIC %md ## Check 5 · Fixture JSON files match FastAPI response schemas

# COMMAND ----------
# Fixture files are committed to the repo and accessible via the /Workspace mount
# (Databricks Repos) or as a relative DBFS path when run locally.
# We try both locations gracefully.

FIXTURE_SEARCH_PATHS = [
    "/Workspace/Repos/brandonng2/DataHacks2026/backend/fixtures",
    "/dbfs/illuminagrid/fixtures",
]

def _load_fixture(filename: str) -> dict | None:
    for base in FIXTURE_SEARCH_PATHS:
        p = Path(base) / filename
        if p.exists():
            with open(p) as fh:
                return json.load(fh)
    return None

# ── Schema validators (required top-level keys, non-null) ────────────────────

def _check_keys(data: dict, required: list[str]) -> tuple[bool, str]:
    """Return (ok, missing_keys_message)."""
    # Strip internal _fixture / _note metadata keys before validating.
    missing = [k for k in required if k not in data]
    return (not missing), (f"missing keys: {missing}" if missing else "all required keys present")


def _check_list_of_dicts(data: list, required_item_keys: list[str]) -> tuple[bool, str]:
    if not data:
        return False, "list is empty"
    item = data[0]
    missing = [k for k in required_item_keys if k not in item]
    return (not missing), (f"item missing keys: {missing}" if missing else "item schema ok")


# ── Fixture 1: city_summary ───────────────────────────────────────────────────
SUMMARY_KEYS = [
    "city", "state", "total_permits", "total_active_permits",
    "total_annual_kwh", "total_annual_savings_usd",
    "total_co2_offset_metric_tons", "avg_system_size_kw",
    "high_yield_index_score", "last_updated",
]

fixture_summary = _load_fixture("city_summary.json")
if fixture_summary is None:
    record(check="fixture::city_summary::loadable", passed=False, detail="file not found in search paths")
else:
    record(check="fixture::city_summary::loadable", passed=True)
    ok, detail = _check_keys(fixture_summary, SUMMARY_KEYS)
    record(check="fixture::city_summary::schema", passed=ok, detail=detail)

# ── Fixture 2: city_permits ───────────────────────────────────────────────────
PERMITS_PAGE_KEYS = ["city", "total", "limit", "offset", "permits"]
PERMIT_ITEM_KEYS  = [
    "permit_id", "zip_code", "city", "state", "system_size_kw",
    "install_date", "status", "latitude", "longitude", "ac_annual_kwh",
    "electricity_rate_per_kwh", "annual_savings_usd",
    "co2_offset_metric_tons", "enriched_at",
]

fixture_permits = _load_fixture("city_permits.json")
if fixture_permits is None:
    record(check="fixture::city_permits::loadable", passed=False, detail="file not found in search paths")
else:
    record(check="fixture::city_permits::loadable", passed=True)
    ok, detail = _check_keys(fixture_permits, PERMITS_PAGE_KEYS)
    record(check="fixture::city_permits::page_schema", passed=ok, detail=detail)
    if ok:
        ok2, detail2 = _check_list_of_dicts(fixture_permits["permits"], PERMIT_ITEM_KEYS)
        record(check="fixture::city_permits::item_schema", passed=ok2, detail=detail2)

# ── Fixture 3: city_forecast ──────────────────────────────────────────────────
FORECAST_KEYS = [
    "city", "state", "velocity_permits_per_month", "avg_system_size_kw",
    "months", "monthly_projected_kwh",
    "monthly_projected_savings_usd", "monthly_projected_co2_metric_tons",
]

fixture_forecast = _load_fixture("city_forecast.json")
if fixture_forecast is None:
    record(check="fixture::city_forecast::loadable", passed=False, detail="file not found in search paths")
else:
    record(check="fixture::city_forecast::loadable", passed=True)
    ok, detail = _check_keys(fixture_forecast, FORECAST_KEYS)
    record(check="fixture::city_forecast::schema", passed=ok, detail=detail)
    if ok:
        arrays = ["months", "monthly_projected_kwh",
                  "monthly_projected_savings_usd", "monthly_projected_co2_metric_tons"]
        lengths = {k: len(fixture_forecast[k]) for k in arrays}
        all_12  = all(v == 12 for v in lengths.values())
        record(
            check="fixture::city_forecast::12_month_arrays",
            passed=all_12,
            detail=str(lengths) if not all_12 else "all four arrays have 12 elements",
        )

# ── Fixture 4: leaderboard ────────────────────────────────────────────────────
LEADERBOARD_KEYS   = ["total", "state_filter", "entries"]
LEADERBOARD_ENTRY_KEYS = [
    "rank", "city", "state", "total_permits", "total_active_permits",
    "total_annual_kwh", "total_annual_savings_usd",
    "total_co2_offset_metric_tons", "avg_system_size_kw",
    "high_yield_index_score", "last_updated",
]

fixture_leaderboard = _load_fixture("leaderboard.json")
if fixture_leaderboard is None:
    record(check="fixture::leaderboard::loadable", passed=False, detail="file not found in search paths")
else:
    record(check="fixture::leaderboard::loadable", passed=True)
    ok, detail = _check_keys(fixture_leaderboard, LEADERBOARD_KEYS)
    record(check="fixture::leaderboard::schema", passed=ok, detail=detail)
    if ok:
        ok2, detail2 = _check_list_of_dicts(fixture_leaderboard["entries"], LEADERBOARD_ENTRY_KEYS)
        record(check="fixture::leaderboard::entry_schema", passed=ok2, detail=detail2)
        # Confirm rank field is monotonically increasing from 1
        ranks = [e.get("rank") for e in fixture_leaderboard["entries"]]
        ranks_ok = ranks == list(range(1, len(ranks) + 1))
        record(
            check="fixture::leaderboard::rank_sequence",
            passed=ranks_ok,
            detail=f"ranks={ranks}" if not ranks_ok else f"ranks 1–{len(ranks)} in order",
        )

# COMMAND ----------
# MAGIC %md ## Final Summary

# COMMAND ----------
total_checks = len(_results)
passed_checks = sum(1 for r in _results if r["passed"])
failed_checks = total_checks - passed_checks
all_passed = failed_checks == 0

# ── Print per-check table ─────────────────────────────────────────────────────
rows_html = ""
for r in _results:
    icon  = "✅" if r["passed"] else "❌"
    color = "#a6e3a1" if r["passed"] else "#f38ba8"
    rows_html += (
        f'<tr>'
        f'<td style="padding:4px 12px 4px 4px">{icon}</td>'
        f'<td style="padding:4px 24px 4px 4px; color:{color}">{r["check"]}</td>'
        f'<td style="padding:4px; color:#6c7086">{r["detail"]}</td>'
        f'</tr>'
    )

# ── Overall banner ────────────────────────────────────────────────────────────
if all_passed:
    banner_color = "#a6e3a1"
    banner_text  = "✅  ALL CHECKS PASSED — DEMO READY"
else:
    banner_color = "#f38ba8"
    banner_text  = f"❌  {failed_checks} CHECK(S) FAILED — DEMO NOT READY"

from datetime import datetime, timezone
completed_at = datetime.now(timezone.utc).isoformat()

displayHTML(f"""
<div style="font-family:monospace; padding:20px; background:#1e1e2e; color:#cdd6f4;
            border-radius:8px; line-height:1.8">
  <b style="font-size:1.3em; color:#89b4fa">IlluminaGrid — Demo Validation Report</b><br><br>

  <table style="border-collapse:collapse; width:100%">
    {rows_html}
  </table>

  <hr style="border-color:#313244; margin:16px 0">

  <span style="font-size:1.15em; font-weight:bold; color:{banner_color}">{banner_text}</span><br>
  <span style="color:#6c7086">
    {passed_checks}/{total_checks} checks passed &nbsp;·&nbsp; {completed_at}Z
  </span>
</div>
""")

# Fail the notebook cell (and any downstream workflow task) if checks failed.
if not all_passed:
    failed_names = [r["check"] for r in _results if not r["passed"]]
    raise AssertionError(
        f"IlluminaGrid demo validation failed. "
        f"{failed_checks} check(s) did not pass: {failed_names}"
    )
