# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Gold City Summaries — Aggregated Metrics & High Yield Index
# MAGIC
# MAGIC Reads all active records from `illuminagrid.gold.enriched_permits`, computes
# MAGIC city-level aggregations, derives the High Yield Index (HYI) score per city,
# MAGIC and merges results into `illuminagrid.gold.city_summaries`.
# MAGIC
# MAGIC **Pipeline phase:** Gold aggregation
# MAGIC **Source table:** `illuminagrid.gold.enriched_permits`
# MAGIC **Target table:** `illuminagrid.gold.city_summaries`
# MAGIC
# MAGIC Aggregations written per (city, state):
# MAGIC - `total_permits`                — COUNT of all permits
# MAGIC - `total_active_permits`         — COUNT where status = VALID
# MAGIC - `total_annual_kwh`             — SUM of ac_annual_kwh
# MAGIC - `total_annual_savings_usd`     — SUM of annual_savings_usd
# MAGIC - `total_co2_offset_metric_tons` — SUM of co2_offset_metric_tons
# MAGIC - `avg_system_size_kw`           — AVG of system_size_kw
# MAGIC - `high_yield_index_score`       — Weighted composite 0–100 (see below)
# MAGIC
# MAGIC **High Yield Index weights:**
# MAGIC | Component | Weight | Direction |
# MAGIC |-----------|--------|-----------|
# MAGIC | electricity_rate_per_kwh (avg) | 40% | higher rate → higher score |
# MAGIC | permit approval speed (avg days to enrich) | 30% | faster → higher score |
# MAGIC | total installed kW | 30% | more kW → higher score |
# MAGIC
# MAGIC Each component is min-max normalised to 0–100 across all cities before weighting.

# COMMAND ----------
from datetime import datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------
# ── Read and cache enriched_permits ──────────────────────────────────────────
# Cache before aggregating so both passes (total + active) hit memory, not disk.
enriched = (
    spark.table("illuminagrid.gold.enriched_permits")
    .cache()
)

total_rows = enriched.count()
print(f"Cached enriched_permits: {total_rows} rows")

if total_rows == 0:
    print("No records in gold.enriched_permits — nothing to aggregate.")
    dbutils.notebook.exit("empty-source")

# COMMAND ----------
# ── Total-permit counts (all statuses) per city/state ────────────────────────
total_counts = (
    enriched
    .groupBy("city", "state")
    .agg(F.count("permit_id").alias("total_permits"))
)

# COMMAND ----------
# ── Active-only aggregations ──────────────────────────────────────────────────
# "Active" in enriched_permits means status = VALID with a completed simulation.
active = enriched.filter(
    (F.col("status") == "VALID")
    & F.col("ac_annual_kwh").isNotNull()
    & F.col("annual_savings_usd").isNotNull()
)

active_aggs = (
    active
    .groupBy("city", "state")
    .agg(
        F.count("permit_id")                          .alias("total_active_permits"),
        F.sum("ac_annual_kwh")                        .alias("total_annual_kwh"),
        F.sum("annual_savings_usd")                   .alias("total_annual_savings_usd"),
        F.sum("co2_offset_metric_tons")               .alias("total_co2_offset_metric_tons"),
        F.round(F.avg("system_size_kw"),          4)  .alias("avg_system_size_kw"),
        # HYI input 1: avg electricity rate for this city (state-level, but kept per city)
        F.avg("electricity_rate_per_kwh")             .alias("_avg_rate"),
        # HYI input 2: avg days from ingest to enrichment ≈ proxy for permit approval speed
        F.avg(
            F.datediff(F.col("enriched_at").cast("date"), F.col("ingested_at").cast("date"))
        )                                              .alias("_avg_approval_days"),
        # HYI input 3: total installed kW
        F.sum("system_size_kw")                       .alias("_total_kw"),
    )
)

# COMMAND ----------
# ── Join total + active aggregations ─────────────────────────────────────────
city_aggs = total_counts.join(active_aggs, on=["city", "state"], how="left")

# COMMAND ----------
# ── High Yield Index — min-max normalise each component across all cities ─────
#
# Pattern: compute global min/max via window functions (no collect()), then
# apply the normalisation formula in a single pass.
#
#   norm(x) = (x - min) / (max - min) * 100     (0 if max == min)
#
# For permit approval speed the direction is inverted: fewer days = higher score,
# so we use (max - x) / (max - min) * 100.

# Materialise city_aggs so window functions scan a small result set, not the
# full enriched_permits table.
city_aggs = city_aggs.cache()
city_aggs.count()  # trigger caching

w = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

hyi_df = (
    city_aggs
    # ── rate component (40%) ─────────────────────────────────────────────────
    .withColumn("_rate_min",  F.min("_avg_rate").over(w))
    .withColumn("_rate_max",  F.max("_avg_rate").over(w))
    .withColumn(
        "_rate_norm",
        F.when(
            F.col("_rate_max") > F.col("_rate_min"),
            (F.col("_avg_rate") - F.col("_rate_min"))
            / (F.col("_rate_max") - F.col("_rate_min"))
            * 100,
        ).otherwise(F.lit(50.0)),
    )
    # ── approval speed component (30%, inverted) ──────────────────────────────
    .withColumn("_days_min",  F.min("_avg_approval_days").over(w))
    .withColumn("_days_max",  F.max("_avg_approval_days").over(w))
    .withColumn(
        "_speed_norm",
        F.when(
            F.col("_days_max") > F.col("_days_min"),
            (F.col("_days_max") - F.col("_avg_approval_days"))
            / (F.col("_days_max") - F.col("_days_min"))
            * 100,
        ).otherwise(F.lit(50.0)),
    )
    # ── total kW component (30%) ──────────────────────────────────────────────
    .withColumn("_kw_min",    F.min("_total_kw").over(w))
    .withColumn("_kw_max",    F.max("_total_kw").over(w))
    .withColumn(
        "_kw_norm",
        F.when(
            F.col("_kw_max") > F.col("_kw_min"),
            (F.col("_total_kw") - F.col("_kw_min"))
            / (F.col("_kw_max") - F.col("_kw_min"))
            * 100,
        ).otherwise(F.lit(50.0)),
    )
    # ── weighted sum → HYI score ──────────────────────────────────────────────
    .withColumn(
        "high_yield_index_score",
        F.round(
            F.col("_rate_norm")  * 0.40
            + F.col("_speed_norm") * 0.30
            + F.col("_kw_norm")    * 0.30,
            2,
        ),
    )
)

# COMMAND ----------
# ── Build final output frame ──────────────────────────────────────────────────
last_updated = F.lit(datetime.now(timezone.utc)).cast("timestamp")

summaries_df = (
    hyi_df
    .withColumn("last_updated", last_updated)
    .select(
        "city",
        "state",
        "total_permits",
        "total_active_permits",
        "total_annual_kwh",
        "total_annual_savings_usd",
        "total_co2_offset_metric_tons",
        "avg_system_size_kw",
        "high_yield_index_score",
        "last_updated",
    )
)

cities_to_write = summaries_df.count()
print(f"Cities to merge: {cities_to_write}")

# COMMAND ----------
# ── MERGE INTO gold.city_summaries on (city, state) ───────────────────────────
city_table = DeltaTable.forName(spark, "illuminagrid.gold.city_summaries")

(
    city_table.alias("target")
    .merge(
        summaries_df.alias("source"),
        "target.city = source.city AND target.state = source.state",
    )
    .whenMatchedUpdate(set={
        "target.total_permits":                "source.total_permits",
        "target.total_active_permits":         "source.total_active_permits",
        "target.total_annual_kwh":             "source.total_annual_kwh",
        "target.total_annual_savings_usd":     "source.total_annual_savings_usd",
        "target.total_co2_offset_metric_tons": "source.total_co2_offset_metric_tons",
        "target.avg_system_size_kw":           "source.avg_system_size_kw",
        "target.high_yield_index_score":       "source.high_yield_index_score",
        "target.last_updated":                 "source.last_updated",
    })
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------
# ── Verification snapshot ─────────────────────────────────────────────────────
result_df = (
    spark.table("illuminagrid.gold.city_summaries")
    .orderBy(F.col("high_yield_index_score").desc())
)

display(result_df)

# COMMAND ----------
# ── Summary banner ────────────────────────────────────────────────────────────
top_city    = result_df.first()
top_label   = f"{top_city['city']}, {top_city['state']}" if top_city else "—"
top_score   = top_city["high_yield_index_score"]          if top_city else "—"

completed_at = datetime.now(timezone.utc).isoformat()

displayHTML(f"""
<div style="font-family:monospace; padding:16px; background:#1e1e2e; color:#cdd6f4;
            border-radius:8px; line-height:1.9">
  <b style="font-size:1.2em; color:#89b4fa">Gold City Summaries — Aggregated Metrics &amp; High Yield Index</b><br><br>
  <span style="color:#cdd6f4">source_rows_cached   </span>: {total_rows}<br>
  <span style="color:#a6e3a1">cities_merged        </span>: {cities_to_write}<br>
  <span style="color:#cdd6f4">top_hyi_city         </span>: {top_label}
    &nbsp;<span style="color:#a6e3a1">({top_score})</span><br><br>
  <span style="color:#cdd6f4">HYI weights</span>: electricity rate 40% · approval speed 30% · total kW 30%<br>
  <span style="color:#6c7086">Completed at {completed_at}Z</span>
</div>
""")
