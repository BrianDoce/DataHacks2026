# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # Gold Simulate — PySAM PVWatts v8
# MAGIC
# MAGIC Runs local PySAM PVWatts v8 simulation for every unprocessed record in
# MAGIC `illuminagrid.silver.permits` and writes enriched results to
# MAGIC `illuminagrid.gold.enriched_permits`.
# MAGIC
# MAGIC **Prerequisite:** Cluster must be started with the init script
# MAGIC `databricks/init_scripts/install_pysam.sh` so that `NREL-PySAM` is available
# MAGIC on all workers before this notebook runs.
# MAGIC
# MAGIC **Pipeline phase:** Silver → Gold (simulation)
# MAGIC **Source table:** `illuminagrid.silver.permits`
# MAGIC **Target table:** `illuminagrid.gold.enriched_permits`
# MAGIC
# MAGIC EIA rate enrichment (electricity_rate_per_kwh, annual_savings_usd,
# MAGIC co2_offset_metric_tons) is applied by notebook 05 after this one.

# COMMAND ----------
from datetime import datetime, timezone

import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, DoubleType, StructField, StructType
from delta.tables import DeltaTable

# COMMAND ----------
# ── Output schema for the pandas UDF ─────────────────────────────────────────
SIM_SCHEMA = StructType([
    StructField("ac_annual_kwh",   DoubleType(),            nullable=True),
    StructField("ac_monthly_kwh",  ArrayType(DoubleType()), nullable=True),
    StructField("solrad_annual",   DoubleType(),            nullable=True),
    StructField("capacity_factor", DoubleType(),            nullable=True),
])

# COMMAND ----------
# ── PySAM simulation pandas UDF ──────────────────────────────────────────────
@pandas_udf(SIM_SCHEMA)
def simulate_udf(
    system_size_kw:    pd.Series,
    weather_file_path: pd.Series,
) -> pd.DataFrame:
    import PySAM.Pvwattsv8 as Pvwattsv8  # imported inside UDF so workers load it

    results = []
    for kw, path in zip(system_size_kw, weather_file_path):
        if not path or not kw:
            results.append({
                "ac_annual_kwh":   None,
                "ac_monthly_kwh":  None,
                "solrad_annual":   None,
                "capacity_factor": None,
            })
            continue
        try:
            # Workers access DBFS via the /dbfs local mount, not the dbfs:/ protocol
            local_path = path.replace("dbfs:/", "/dbfs/")

            pv = Pvwattsv8.new()
            pv.SolarResource.solar_resource_file = local_path
            pv.SystemDesign.system_capacity = float(kw)
            pv.SystemDesign.tilt            = 20
            pv.SystemDesign.azimuth         = 180
            pv.SystemDesign.array_type      = 1      # fixed roof-mounted
            pv.SystemDesign.module_type     = 1      # premium
            pv.SystemDesign.losses          = 14.08
            pv.execute()

            results.append({
                "ac_annual_kwh":   float(pv.Outputs.ac_annual),
                "ac_monthly_kwh":  [float(v) for v in pv.Outputs.ac_monthly],
                "solrad_annual":   float(pv.Outputs.solrad_annual),
                "capacity_factor": float(pv.Outputs.capacity_factor),
            })
        except Exception as exc:
            # Log per-row failures without crashing the whole batch
            print(f"[sim error] path={path} kw={kw}: {exc}")
            results.append({
                "ac_annual_kwh":   None,
                "ac_monthly_kwh":  None,
                "solrad_annual":   None,
                "capacity_factor": None,
            })

    return pd.DataFrame(results)

# COMMAND ----------
# ── Read unprocessed silver permits ──────────────────────────────────────────
# Unprocessed = in silver but not yet in gold. Anti-join avoids reprocessing
# on reruns without requiring CDF on silver.permits.
silver_df = (
    spark.table("illuminagrid.silver.permits")
    .filter(F.col("status")             == "VALID")
    .filter(F.col("weather_file_path").isNotNull())
    .filter(F.col("system_size_kw").isNotNull())
    .filter(F.col("system_size_kw")     >  0)
)

gold_ids = spark.table("illuminagrid.gold.enriched_permits").select("permit_id")

unprocessed_df = silver_df.join(gold_ids, on="permit_id", how="left_anti")

total_unprocessed = unprocessed_df.count()
print(f"Unprocessed silver permits: {total_unprocessed}")

if total_unprocessed == 0:
    print("Nothing to simulate.")
    dbutils.notebook.exit("up-to-date")

# COMMAND ----------
# ── Run PySAM simulation via pandas UDF ──────────────────────────────────────
enriched_at = F.lit(datetime.now(timezone.utc)).cast("timestamp")

simulated_df = (
    unprocessed_df
    .withColumn("_sim", simulate_udf(
        F.col("system_size_kw"),
        F.col("weather_file_path"),
    ))
    .withColumn("ac_annual_kwh",   F.col("_sim.ac_annual_kwh"))
    .withColumn("ac_monthly_kwh",  F.col("_sim.ac_monthly_kwh"))
    .withColumn("solrad_annual",   F.col("_sim.solrad_annual"))
    .withColumn("capacity_factor", F.col("_sim.capacity_factor"))
    .withColumn("simulation_source", F.lit("pysam_pvwattsv8_local"))
    .withColumn("enriched_at",       enriched_at)
    # EIA fields are populated by notebook 05; set NULL here
    .withColumn("electricity_rate_per_kwh", F.lit(None).cast("double"))
    .withColumn("annual_savings_usd",       F.lit(None).cast("double"))
    .withColumn("co2_offset_metric_tons",   F.lit(None).cast("double"))
    .drop("_sim")
    .select(
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
        "ac_annual_kwh",
        "ac_monthly_kwh",
        "solrad_annual",
        "capacity_factor",
        "electricity_rate_per_kwh",
        "annual_savings_usd",
        "co2_offset_metric_tons",
        "simulation_source",
        "enriched_at",
    )
)

# COMMAND ----------
# ── MERGE into gold.enriched_permits on permit_id ────────────────────────────
gold_table = DeltaTable.forName(spark, "illuminagrid.gold.enriched_permits")

(
    gold_table.alias("target")
    .merge(simulated_df.alias("source"), "target.permit_id = source.permit_id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
)

# COMMAND ----------
# ── Summary ───────────────────────────────────────────────────────────────────
total_succeeded = simulated_df.filter(F.col("ac_annual_kwh").isNotNull()).count()
total_failed    = simulated_df.filter(F.col("ac_annual_kwh").isNull()).count()

displayHTML(f"""
<div style="font-family:monospace; padding:16px; background:#1e1e2e; color:#cdd6f4;
            border-radius:8px; line-height:1.9">
  <b style="font-size:1.2em; color:#89b4fa">Gold Simulate — PySAM PVWatts v8</b><br><br>
  <span style="color:#cdd6f4">total_unprocessed </span>: {total_unprocessed}<br>
  <span style="color:#a6e3a1">total_succeeded   </span>: {total_succeeded}<br>
  <span style="color:#f38ba8">total_failed      </span>: {total_failed}
    &nbsp;<span style="color:#6c7086">(NULL ac_annual_kwh — bad weather file or system_size_kw)</span><br><br>
  <span style="color:#6c7086">simulation_source : pysam_pvwattsv8_local</span><br>
  <span style="color:#6c7086">Completed at {datetime.now(timezone.utc).isoformat()}Z</span>
</div>
""")
