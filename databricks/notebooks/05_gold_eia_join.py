# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Gold EIA Join — Electricity Rates & Savings
# MAGIC
# MAGIC Fetches residential electricity retail prices from the EIA API for every state
# MAGIC present in `illuminagrid.gold.enriched_permits`, stores raw responses in
# MAGIC `illuminagrid.bronze.eia_rates`, then merges enriched financial and emissions
# MAGIC columns back into `illuminagrid.gold.enriched_permits`.
# MAGIC
# MAGIC **Pipeline phase:** Gold enrichment (EIA rates)
# MAGIC **Source table:** `illuminagrid.gold.enriched_permits`
# MAGIC **Staging table:** `illuminagrid.bronze.eia_rates`
# MAGIC **Target table:** `illuminagrid.gold.enriched_permits`
# MAGIC
# MAGIC Columns written:
# MAGIC - `electricity_rate_per_kwh` — most-recent monthly residential rate ($/kWh)
# MAGIC - `annual_savings_usd`       — ac_annual_kwh × electricity_rate_per_kwh
# MAGIC - `co2_offset_metric_tons`   — ac_annual_kwh × state_co2_factor_kg_per_kwh / 1000

# COMMAND ----------
import json
from datetime import datetime, timezone

import requests
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType, TimestampType
)
from delta.tables import DeltaTable

# COMMAND ----------
# ── Secrets ───────────────────────────────────────────────────────────────────
EIA_API_KEY = dbutils.secrets.get(scope="illuminagrid", key="eia_api_key")

# COMMAND ----------
# ── EPA eGRID state-level CO2 emission factors (kg CO2 / kWh) ─────────────────
# Source: EPA eGRID 2022 subregion factors mapped to primary state.
# National average 0.386 used where state-level factor is unavailable.
_EGRID_DEFAULT = 0.386

EGRID_STATE_FACTORS = {
    "AK": 0.295, "AL": 0.408, "AR": 0.448, "AZ": 0.341, "CA": 0.210,
    "CO": 0.510, "CT": 0.179, "DC": 0.374, "DE": 0.374, "FL": 0.390,
    "GA": 0.374, "HI": 0.650, "IA": 0.527, "ID": 0.074, "IL": 0.344,
    "IN": 0.620, "KS": 0.494, "KY": 0.654, "LA": 0.425, "MA": 0.253,
    "MD": 0.320, "ME": 0.130, "MI": 0.472, "MN": 0.430, "MO": 0.600,
    "MS": 0.425, "MT": 0.400, "NC": 0.372, "ND": 0.604, "NE": 0.511,
    "NH": 0.192, "NJ": 0.198, "NM": 0.512, "NV": 0.355, "NY": 0.181,
    "OH": 0.515, "OK": 0.437, "OR": 0.099, "PA": 0.342, "RI": 0.283,
    "SC": 0.278, "SD": 0.100, "TN": 0.367, "TX": 0.420, "UT": 0.590,
    "VA": 0.323, "VT": 0.012, "WA": 0.090, "WI": 0.491, "WV": 0.748,
    "WY": 0.728,
}

# COMMAND ----------
# ── Collect distinct states from gold table ───────────────────────────────────
states_in_gold = [
    row["state"]
    for row in (
        spark.table("illuminagrid.gold.enriched_permits")
        .filter(F.col("state").isNotNull())
        .select("state")
        .distinct()
        .collect()
    )
]

print(f"States to fetch: {sorted(states_in_gold)}")

if not states_in_gold:
    print("No states found in gold table — nothing to enrich.")
    dbutils.notebook.exit("no-states")

# COMMAND ----------
# ── Fetch EIA residential retail prices per state ─────────────────────────────
EIA_ENDPOINT = "https://api.eia.gov/v2/electricity/retail-sales/data"

fetched_at = datetime.now(timezone.utc)

raw_records = []

for state in states_in_gold:
    params = {
        "api_key":                EIA_API_KEY,
        "frequency":              "monthly",
        "data[]":                 "price",
        "facets[stateid][]":      state,
        "facets[sectorid][]":     "RES",       # residential sector
        "sort[0][column]":        "period",
        "sort[0][direction]":     "desc",
        "length":                 1,
        "offset":                 0,
    }

    try:
        resp = requests.get(EIA_ENDPOINT, params=params, timeout=15)
        resp.raise_for_status()
        payload = resp.json()
        rows = payload.get("response", {}).get("data", [])

        if rows:
            row = rows[0]
            # EIA reports price in cents/kWh — convert to $/kWh
            price_cents = row.get("price")
            rate_per_kwh = float(price_cents) / 100.0 if price_cents is not None else None
            period = row.get("period")
        else:
            print(f"[warn] No EIA data for state={state}")
            rate_per_kwh = None
            period = None

        raw_records.append({
            "state":        state,
            "rate_per_kwh": rate_per_kwh,
            "period":       period,
            "fetched_at":   fetched_at,
        })

    except Exception as exc:
        print(f"[error] EIA fetch failed for state={state}: {exc}")
        raw_records.append({
            "state":        state,
            "rate_per_kwh": None,
            "period":       None,
            "fetched_at":   fetched_at,
        })

print(f"Fetched EIA records: {len(raw_records)}")

# COMMAND ----------
# ── Write raw responses to bronze.eia_rates ───────────────────────────────────
BRONZE_SCHEMA = StructType([
    StructField("state",        StringType(),    nullable=False),
    StructField("rate_per_kwh", DoubleType(),    nullable=True),
    StructField("period",       StringType(),    nullable=True),
    StructField("fetched_at",   TimestampType(), nullable=False),
])

rates_df = spark.createDataFrame(raw_records, schema=BRONZE_SCHEMA)

(
    rates_df.write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .saveAsTable("illuminagrid.bronze.eia_rates")
)

print("Raw EIA rates written to illuminagrid.bronze.eia_rates")

# COMMAND ----------
# ── Build most-recent rate per state from bronze ──────────────────────────────
# Use a window to pick the single latest rate per state across all appended batches.
from pyspark.sql.window import Window

latest_rates = (
    spark.table("illuminagrid.bronze.eia_rates")
    .filter(F.col("rate_per_kwh").isNotNull())
    .withColumn(
        "_rank",
        F.row_number().over(
            Window.partitionBy("state").orderBy(F.col("fetched_at").desc(), F.col("period").desc())
        ),
    )
    .filter(F.col("_rank") == 1)
    .select("state", "rate_per_kwh", "period")
)

# COMMAND ----------
# ── Attach eGRID CO2 factors as a broadcast-friendly map ─────────────────────
egrid_rows = [{"state": s, "co2_kg_per_kwh": v} for s, v in EGRID_STATE_FACTORS.items()]
egrid_df = spark.createDataFrame(
    egrid_rows,
    schema=StructType([
        StructField("state",          StringType(), nullable=False),
        StructField("co2_kg_per_kwh", DoubleType(), nullable=False),
    ]),
)

# COMMAND ----------
# ── Join gold permits with rates and eGRID factors ────────────────────────────
gold_df = spark.table("illuminagrid.gold.enriched_permits")

enriched_df = (
    gold_df
    .join(latest_rates.alias("rates"),  on="state", how="left")
    .join(egrid_df.alias("egrid"),      on="state", how="left")
    .withColumn(
        "electricity_rate_per_kwh",
        F.col("rates.rate_per_kwh"),
    )
    .withColumn(
        "annual_savings_usd",
        F.when(
            F.col("ac_annual_kwh").isNotNull() & F.col("rates.rate_per_kwh").isNotNull(),
            F.col("ac_annual_kwh") * F.col("rates.rate_per_kwh"),
        ).otherwise(F.lit(None).cast("double")),
    )
    .withColumn(
        # Use state factor where available, fall back to national average
        "_co2_factor",
        F.coalesce(F.col("egrid.co2_kg_per_kwh"), F.lit(_EGRID_DEFAULT)),
    )
    .withColumn(
        "co2_offset_metric_tons",
        F.when(
            F.col("ac_annual_kwh").isNotNull(),
            F.col("ac_annual_kwh") * F.col("_co2_factor") / F.lit(1000.0),
        ).otherwise(F.lit(None).cast("double")),
    )
    .drop("_co2_factor")
    .select(
        "permit_id",
        "electricity_rate_per_kwh",
        "annual_savings_usd",
        "co2_offset_metric_tons",
    )
    .filter(F.col("electricity_rate_per_kwh").isNotNull())
)

records_to_update = enriched_df.count()
print(f"Records to merge into gold: {records_to_update}")

# COMMAND ----------
# ── MERGE INTO gold.enriched_permits on permit_id ─────────────────────────────
gold_table = DeltaTable.forName(spark, "illuminagrid.gold.enriched_permits")

(
    gold_table.alias("target")
    .merge(enriched_df.alias("source"), "target.permit_id = source.permit_id")
    .whenMatchedUpdate(set={
        "target.electricity_rate_per_kwh": "source.electricity_rate_per_kwh",
        "target.annual_savings_usd":       "source.annual_savings_usd",
        "target.co2_offset_metric_tons":   "source.co2_offset_metric_tons",
    })
    .execute()
)

# COMMAND ----------
# ── Per-state summary log ─────────────────────────────────────────────────────
state_summary = (
    spark.table("illuminagrid.gold.enriched_permits")
    .filter(F.col("electricity_rate_per_kwh").isNotNull())
    .groupBy("state")
    .agg(
        F.count("permit_id").alias("records_updated"),
        F.round(F.avg("electricity_rate_per_kwh"), 4).alias("avg_rate_per_kwh"),
        F.round(F.avg("annual_savings_usd"), 2).alias("avg_annual_savings_usd"),
        F.round(F.avg("co2_offset_metric_tons"), 4).alias("avg_co2_offset_mt"),
    )
    .orderBy("state")
)

display(state_summary)

# COMMAND ----------
# ── Final summary banner ──────────────────────────────────────────────────────
total_enriched = (
    spark.table("illuminagrid.gold.enriched_permits")
    .filter(F.col("electricity_rate_per_kwh").isNotNull())
    .count()
)

avg_rate_overall = (
    spark.table("illuminagrid.gold.enriched_permits")
    .filter(F.col("electricity_rate_per_kwh").isNotNull())
    .agg(F.round(F.avg("electricity_rate_per_kwh"), 4))
    .collect()[0][0]
)

displayHTML(f"""
<div style="font-family:monospace; padding:16px; background:#1e1e2e; color:#cdd6f4;
            border-radius:8px; line-height:1.9">
  <b style="font-size:1.2em; color:#89b4fa">Gold EIA Join — Electricity Rates &amp; Savings</b><br><br>
  <span style="color:#cdd6f4">states_fetched       </span>: {len(states_in_gold)}<br>
  <span style="color:#a6e3a1">records_merged       </span>: {records_to_update}<br>
  <span style="color:#a6e3a1">total_enriched_gold  </span>: {total_enriched}<br>
  <span style="color:#cdd6f4">avg_rate_overall     </span>: ${avg_rate_overall} / kWh<br><br>
  <span style="color:#6c7086">CO2 factors: EPA eGRID 2022 state-level; fallback = {_EGRID_DEFAULT} kg/kWh</span><br>
  <span style="color:#6c7086">Completed at {fetched_at.isoformat()}Z</span>
</div>
""")
