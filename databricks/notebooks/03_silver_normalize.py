# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Silver Normalize — Permits
# MAGIC
# MAGIC Reads new permit records from `illuminagrid.bronze.permits` via Change Data Feed,
# MAGIC joins each to ZIP centroids and the weather manifest, and upserts clean records
# MAGIC into `illuminagrid.silver.permits`.
# MAGIC
# MAGIC **Pipeline phase:** Bronze → Silver
# MAGIC **Source table:** `illuminagrid.bronze.permits` (CDF)
# MAGIC **Lookup tables:** `illuminagrid.bronze.zip_centroids`, `illuminagrid.bronze.weather_manifest`
# MAGIC **Target table:** `illuminagrid.silver.permits`
# MAGIC
# MAGIC Every output row is guaranteed to have `latitude`, `longitude`, and a non-null
# MAGIC `status`. Rows where `weather_file_path` is NULL mean notebook 02 has not yet
# MAGIC downloaded that city's weather file — those rows are written to silver and
# MAGIC skipped by the simulation notebook until the path is populated.

# COMMAND ----------
from datetime import datetime, timezone

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------
# ── Widget ────────────────────────────────────────────────────────────────────
# -1 → read last processed version from checkpoint table
# >= 0 → explicit override (useful for backfills or debugging)
dbutils.widgets.text(
    "starting_version",
    "-1",
    "CDF Starting Version (-1 = use checkpoint)"
)

starting_version_param = int(dbutils.widgets.get("starting_version").strip())

# COMMAND ----------
# ── Checkpoint helpers ────────────────────────────────────────────────────────
SOURCE_TABLE = "illuminagrid.bronze.permits"

checkpoint_table = DeltaTable.forName(spark, "illuminagrid.bronze._cdf_checkpoints")


def _get_checkpoint() -> int | None:
    row = spark.sql(f"""
        SELECT last_version
        FROM   illuminagrid.bronze._cdf_checkpoints
        WHERE  table_name = '{SOURCE_TABLE}'
    """).first()
    return int(row.last_version) if row else None


def _update_checkpoint(version: int) -> None:
    df = spark.createDataFrame([{
        "table_name":   SOURCE_TABLE,
        "last_version": version,
        "updated_at":   datetime.now(timezone.utc),
    }])
    (
        checkpoint_table.alias("t")
        .merge(df.alias("s"), "t.table_name = s.table_name")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def _latest_version() -> int:
    return int(
        spark.sql(f"DESCRIBE HISTORY {SOURCE_TABLE} LIMIT 1")
        .select("version")
        .first()[0]
    )

# COMMAND ----------
# ── Determine starting version & read source ──────────────────────────────────
if starting_version_param >= 0:
    starting_version = starting_version_param
    print(f"Starting version from widget: {starting_version}")
else:
    checkpoint = _get_checkpoint()
    if checkpoint is None:
        starting_version = None   # first run — full table scan
        print("No checkpoint found — performing full table scan on first run.")
    else:
        starting_version = checkpoint + 1
        print(f"Resuming from checkpoint: version {starting_version}")

latest_version = _latest_version()

if starting_version is not None and starting_version > latest_version:
    print(f"No new commits since version {checkpoint}. Nothing to process.")
    dbutils.notebook.exit("up-to-date")

# First run: read full table so we don't require CDF history from version 0
# (CDF may have been enabled after initial bulk load)
if starting_version is None:
    source_df = spark.table(SOURCE_TABLE)
else:
    source_df = (
        spark.read
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", starting_version)
        .option("endingVersion",   latest_version)
        .table(SOURCE_TABLE)
        .filter(F.col("_change_type").isin("insert", "update_postimage"))
        .drop("_change_type", "_commit_version", "_commit_timestamp")
    )

total_input = source_df.count()
print(f"Records to process: {total_input}")

if total_input == 0:
    print("No new records. Updating checkpoint and exiting.")
    _update_checkpoint(latest_version)
    dbutils.notebook.exit("no-new-records")

# COMMAND ----------
# ── Register temp views for SQL joins (avoids ambiguous column names) ─────────
source_df.createOrReplaceTempView("_bronze_permits")

spark.table("illuminagrid.bronze.zip_centroids").createOrReplaceTempView("_zip_centroids")

spark.table("illuminagrid.bronze.weather_manifest").createOrReplaceTempView("_weather_manifest")

# COMMAND ----------
# ── Join: permits → zip_centroids → weather_manifest ─────────────────────────
#
# zip_centroids: LEFT join so missing ZIP becomes NULL latitude (→ unresolvable)
# weather_manifest: LEFT join so missing weather file becomes NULL path
#   (simulation notebook skips NULLs; they're populated when notebook 02 re-runs)
#
joined_df = spark.sql("""
    SELECT
        b.permit_id,
        TRIM(b.zip_code)                        AS zip_code,
        TRIM(b.city)                             AS city,
        UPPER(TRIM(b.state))                     AS state,
        b.system_size_kw,
        TRY_CAST(b.install_date AS DATE)         AS install_date,
        b.status,
        b.raw_json,
        b.ingested_at,
        z.latitude,
        z.longitude,
        w.dbfs_path                              AS weather_file_path
    FROM _bronze_permits  b
    LEFT JOIN _zip_centroids  z
           ON TRIM(b.zip_code) = TRIM(z.zip_code)
    LEFT JOIN _weather_manifest w
           ON UPPER(TRIM(b.city))  = UPPER(TRIM(w.city))
          AND UPPER(TRIM(b.state)) = UPPER(TRIM(w.state))
""")

# COMMAND ----------
# ── Split resolvable vs unresolvable ──────────────────────────────────────────
# Unresolvable: no ZIP centroid match — cannot simulate without coordinates
unresolvable_df = joined_df.filter(F.col("latitude").isNull())
resolvable_df   = joined_df.filter(F.col("latitude").isNotNull())

total_unresolvable = unresolvable_df.count()
total_matched      = resolvable_df.count()
total_processed    = total_input

print(f"total_processed    : {total_processed}")
print(f"total_matched      : {total_matched}")
print(f"total_unresolvable : {total_unresolvable}")

if total_unresolvable > 0:
    print("\nUnresolvable permit sample (no ZIP centroid):")
    unresolvable_df.select("permit_id", "zip_code", "city", "state").show(20, truncate=False)

# COMMAND ----------
# ── Prepare silver records ────────────────────────────────────────────────────
silver_df = resolvable_df.withColumn(
    "normalized_at",
    F.lit(datetime.now(timezone.utc)).cast("timestamp")
).select(
    "permit_id",
    "zip_code",
    "city",
    "state",
    "system_size_kw",
    "install_date",
    "status",
    "raw_json",
    "ingested_at",
    "latitude",
    "longitude",
    "weather_file_path",
    "normalized_at",
)

# COMMAND ----------
# ── MERGE into silver.permits on permit_id ────────────────────────────────────
silver_table = DeltaTable.forName(spark, "illuminagrid.silver.permits")

(
    silver_table.alias("target")
    .merge(silver_df.alias("source"), "target.permit_id = source.permit_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------
# ── Update checkpoint ─────────────────────────────────────────────────────────
_update_checkpoint(latest_version)
print(f"Checkpoint updated to version {latest_version}.")

# COMMAND ----------
# ── Summary ───────────────────────────────────────────────────────────────────
no_weather = resolvable_df.filter(F.col("weather_file_path").isNull()).count()

displayHTML(f"""
<div style="font-family:monospace; padding:16px; background:#1e1e2e; color:#cdd6f4;
            border-radius:8px; line-height:1.9">
  <b style="font-size:1.2em; color:#89b4fa">Silver Normalize — Complete</b><br><br>
  <span style="color:#cdd6f4">total_processed    </span>: {total_processed}<br>
  <span style="color:#a6e3a1">total_matched      </span>: {total_matched}<br>
  <span style="color:#f38ba8">total_unresolvable </span>: {total_unresolvable}
    &nbsp;<span style="color:#6c7086">(no ZIP centroid — dropped)</span><br>
  <span style="color:#fab387">no_weather_file    </span>: {no_weather}
    &nbsp;<span style="color:#6c7086">(written to silver; simulation will skip until 02 runs)</span><br><br>
  <span style="color:#6c7086">Bronze CDF version processed up to: {latest_version}</span><br>
  <span style="color:#6c7086">Completed at {datetime.now(timezone.utc).isoformat()}Z</span>
</div>
""")
