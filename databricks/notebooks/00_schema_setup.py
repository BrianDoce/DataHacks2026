# Databricks notebook source

# COMMAND ----------
# MAGIC %md
# MAGIC # IlluminaGrid — Delta Lake Schema Setup
# MAGIC
# MAGIC Creates the full Unity Catalog structure for the IlluminaGrid pipeline:
# MAGIC
# MAGIC | Layer  | Schema  | Tables |
# MAGIC |--------|---------|--------|
# MAGIC | Bronze | bronze  | `permits`, `zip_centroids` |
# MAGIC | Silver | silver  | `permits` |
# MAGIC | Gold   | gold    | `enriched_permits`, `city_summaries` |
# MAGIC
# MAGIC **Idempotent** — safe to re-run; existing tables and data are never dropped.

# COMMAND ----------
# MAGIC %md ## 1 · Catalog & Schemas

# COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS illuminagrid")
spark.sql("ALTER  CATALOG illuminagrid SET OWNER TO `account users`")

for schema in ("bronze", "silver", "gold"):
    spark.sql(f"CREATE DATABASE IF NOT EXISTS illuminagrid.{schema}")

spark.sql("COMMENT ON DATABASE illuminagrid.bronze IS 'Raw ingested data exactly as received from upstream sources — no transformations.'")
spark.sql("COMMENT ON DATABASE illuminagrid.silver IS 'Normalized and joined data: permits enriched with ZIP centroids and weather file paths.'")
spark.sql("COMMENT ON DATABASE illuminagrid.gold   IS 'Fully enriched, aggregated, and business-ready data served by FastAPI endpoints.'")

print("Catalog and schemas ready.")

# COMMAND ----------
# MAGIC %md ## 2 · illuminagrid.bronze.permits

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS illuminagrid.bronze.permits (
        permit_id      STRING  NOT NULL,
        zip_code       STRING,
        city           STRING,
        state          STRING,
        system_size_kw DOUBLE,
        install_date   STRING,
        status         STRING,
        raw_json       STRING,
        ingested_at    TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (state)
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.minReaderVersion'     = '1',
        'delta.minWriterVersion'     = '4'
    )
""")

spark.sql("""
    COMMENT ON TABLE illuminagrid.bronze.permits IS
    'Raw solar permit records ingested from the ZenPower API.
     One row per API response record. No field normalisation is applied here —
     the raw_json column preserves the full original payload for reprocessing.'
""")

for col, comment in {
    "permit_id":      "Unique permit identifier assigned by ZenPower. Primary key for MERGE operations.",
    "zip_code":       "5-digit US ZIP code of the installation site. NULL triggers INVALID status in downstream silver processing.",
    "city":           "City name as returned by ZenPower. Used to partition workflow runs.",
    "state":          "2-letter US state abbreviation. Partition key.",
    "system_size_kw": "Nameplate DC capacity of the solar installation in kilowatts. NULL triggers INVALID status.",
    "install_date":   "Date the permit was issued or the system was installed. NULL triggers INVALID status.",
    "status":         "Pipeline status assigned during ingest: VALID if all required fields present, INVALID otherwise.",
    "raw_json":       "Full JSON payload from the ZenPower API response, serialised as a string. Source of truth for re-ingestion.",
    "ingested_at":    "UTC timestamp when this row was written by the bronze ingest notebook.",
}.items():
    spark.sql(f"ALTER TABLE illuminagrid.bronze.permits ALTER COLUMN {col} COMMENT '{comment}'")

print("illuminagrid.bronze.permits ready.")

# COMMAND ----------
# MAGIC %md ## 3 · illuminagrid.bronze.zip_centroids

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS illuminagrid.bronze.zip_centroids (
        zip_code  STRING NOT NULL,
        latitude  DOUBLE,
        longitude DOUBLE,
        city      STRING,
        state     STRING
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'false',
        'delta.minReaderVersion'     = '1',
        'delta.minWriterVersion'     = '2'
    )
""")

spark.sql("""
    COMMENT ON TABLE illuminagrid.bronze.zip_centroids IS
    'Static ZIP code to geographic centroid lookup derived from the US Census ZCTA dataset.
     Loaded once from the repo-committed CSV; no API calls required.
     Used by the silver layer to resolve lat/lon for PySAM weather file matching.'
""")

for col, comment in {
    "zip_code":  "5-digit US ZIP code. Primary key for joins against permits.",
    "latitude":  "Latitude of the ZIP centroid in decimal degrees (WGS-84).",
    "longitude": "Longitude of the ZIP centroid in decimal degrees (WGS-84).",
    "city":      "City name associated with this ZIP from the Census ZCTA dataset.",
    "state":     "2-letter state abbreviation.",
}.items():
    spark.sql(f"ALTER TABLE illuminagrid.bronze.zip_centroids ALTER COLUMN {col} COMMENT '{comment}'")

print("illuminagrid.bronze.zip_centroids ready.")

# COMMAND ----------
# MAGIC %md ## 4 · illuminagrid.silver.permits

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS illuminagrid.silver.permits (
        permit_id         STRING  NOT NULL,
        zip_code          STRING,
        city              STRING,
        state             STRING,
        system_size_kw    DOUBLE,
        install_date      DATE,
        status            STRING,
        raw_json          STRING,
        ingested_at       TIMESTAMP,
        latitude          DOUBLE,
        longitude         DOUBLE,
        weather_file_path STRING,
        normalized_at     TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (state)
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'false',
        'delta.minReaderVersion'     = '1',
        'delta.minWriterVersion'     = '2'
    )
""")

spark.sql("""
    COMMENT ON TABLE illuminagrid.silver.permits IS
    'Normalised permit records joined with ZIP centroid coordinates and resolved
     NSRDB TMY weather file paths. One row per valid bronze permit.
     Input to the PySAM simulation step that produces gold.enriched_permits.'
""")

for col, comment in {
    "permit_id":         "Unique permit identifier — carried forward from bronze.permits.",
    "zip_code":          "5-digit ZIP code, validated and trimmed during silver normalisation.",
    "city":              "City name, title-cased and standardised during normalisation.",
    "state":             "2-letter state abbreviation. Partition key.",
    "system_size_kw":    "DC capacity in kilowatts, cast and range-validated (> 0, < 10 000 kW).",
    "install_date":      "Parsed and validated installation date.",
    "status":            "VALID or INVALID — carried forward from bronze; may be further updated if normalisation reveals issues.",
    "raw_json":          "Original raw JSON payload preserved from bronze for audit and reprocessing.",
    "ingested_at":       "UTC timestamp of original bronze ingest — unchanged.",
    "latitude":          "Latitude of the installation site resolved from bronze.zip_centroids.",
    "longitude":         "Longitude of the installation site resolved from bronze.zip_centroids.",
    "weather_file_path": "DBFS or cloud path to the NSRDB TMY CSV file for this site, matched by city name from weather_cache/.",
    "normalized_at":     "UTC timestamp when this row was written by the silver normalisation notebook.",
}.items():
    spark.sql(f"ALTER TABLE illuminagrid.silver.permits ALTER COLUMN {col} COMMENT '{comment}'")

print("illuminagrid.silver.permits ready.")

# COMMAND ----------
# MAGIC %md ## 5 · illuminagrid.gold.enriched_permits

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS illuminagrid.gold.enriched_permits (
        permit_id                STRING  NOT NULL,
        zip_code                 STRING,
        city                     STRING,
        state                    STRING,
        system_size_kw           DOUBLE,
        install_date             DATE,
        status                   STRING,
        raw_json                 STRING,
        ingested_at              TIMESTAMP,
        latitude                 DOUBLE,
        longitude                DOUBLE,
        weather_file_path        STRING,
        normalized_at            TIMESTAMP,
        ac_annual_kwh            DOUBLE,
        ac_monthly_kwh           ARRAY<DOUBLE>,
        solrad_annual            DOUBLE,
        capacity_factor          DOUBLE,
        electricity_rate_per_kwh DOUBLE,
        annual_savings_usd       DOUBLE,
        co2_offset_metric_tons   DOUBLE,
        simulation_source        STRING,
        enriched_at              TIMESTAMP
    )
    USING DELTA
    PARTITIONED BY (state, status)
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.minReaderVersion'     = '1',
        'delta.minWriterVersion'     = '4'
    )
""")

spark.sql("""
    COMMENT ON TABLE illuminagrid.gold.enriched_permits IS
    'Fully enriched permit records combining PySAM physics simulation outputs with
     EIA electricity rate data. Each row represents one solar permit with its
     projected annual energy production, financial savings, and carbon offset.
     Change data feed enabled so city_summaries can be incrementally refreshed.'
""")

for col, comment in {
    "permit_id":                "Unique permit identifier — primary key.",
    "zip_code":                 "5-digit ZIP code — carried forward from silver.",
    "city":                     "Standardised city name — carried forward from silver.",
    "state":                    "2-letter state abbreviation. Partition key.",
    "system_size_kw":           "DC capacity in kilowatts — carried forward from silver.",
    "install_date":             "Installation date — carried forward from silver.",
    "status":                   "VALID or INVALID. Partition key. Invalid permits are stored but excluded from aggregations.",
    "raw_json":                 "Original ZenPower API payload — preserved for audit.",
    "ingested_at":              "UTC timestamp of original bronze ingest.",
    "latitude":                 "Site latitude resolved from ZIP centroid.",
    "longitude":                "Site longitude resolved from ZIP centroid.",
    "weather_file_path":        "Path to the NSRDB TMY CSV used as PySAM input.",
    "normalized_at":            "UTC timestamp of silver normalisation.",
    "ac_annual_kwh":            "Simulated annual AC energy production in kilowatt-hours from PySAM PVWatts v8.",
    "ac_monthly_kwh":           "Array of 12 monthly AC energy values (kWh) from PySAM. Index 0 = January.",
    "solrad_annual":            "Annual plane-of-array irradiance (kWh/m²) from the NSRDB TMY file via PySAM.",
    "capacity_factor":          "Ratio of actual annual output to theoretical maximum (ac_annual_kwh / (system_size_kw × 8760)).",
    "electricity_rate_per_kwh": "Retail electricity price in USD/kWh from EIA API for the installation state.",
    "annual_savings_usd":       "Projected annual bill savings: ac_annual_kwh × electricity_rate_per_kwh.",
    "co2_offset_metric_tons":   "Annual CO₂ avoided in metric tons: ac_annual_kwh × EIA regional emissions factor / 1000.",
    "simulation_source":        "PySAM model version and dataset used, e.g. PVWattsV8/NSRDB-TMY-2020.",
    "enriched_at":              "UTC timestamp when this row was written by the gold enrichment notebook.",
}.items():
    spark.sql(f"ALTER TABLE illuminagrid.gold.enriched_permits ALTER COLUMN {col} COMMENT '{comment}'")

print("illuminagrid.gold.enriched_permits ready.")

# COMMAND ----------
# MAGIC %md ## 6 · illuminagrid.gold.city_summaries

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS illuminagrid.gold.city_summaries (
        city                        STRING    NOT NULL,
        state                       STRING    NOT NULL,
        total_permits               LONG,
        total_active_permits        LONG,
        total_annual_kwh            DOUBLE,
        total_annual_savings_usd    DOUBLE,
        total_co2_offset_metric_tons DOUBLE,
        avg_system_size_kw          DOUBLE,
        high_yield_index_score      DOUBLE,
        last_updated                TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'true',
        'delta.minReaderVersion'     = '1',
        'delta.minWriterVersion'     = '4'
    )
""")

spark.sql("""
    COMMENT ON TABLE illuminagrid.gold.city_summaries IS
    'City-level aggregated metrics materialised from gold.enriched_permits.
     Primary read target for FastAPI endpoints that power the Next.js dashboard.
     Change data feed enabled for incremental downstream consumption.
     high_yield_index_score combines permit velocity, avg savings, and electricity
     price into a single 0–100 score used by Solar Sales Ops to rank target cities.'
""")

for col, comment in {
    "city":                         "Standardised city name. Composite primary key with state.",
    "state":                        "2-letter state abbreviation. Composite primary key with city.",
    "total_permits":                "Total number of solar permits ingested for this city across all statuses.",
    "total_active_permits":         "Count of permits with status = VALID that have completed enrichment.",
    "total_annual_kwh":             "Sum of ac_annual_kwh across all active permits — total community solar production.",
    "total_annual_savings_usd":     "Sum of annual_savings_usd across all active permits — total community bill savings.",
    "total_co2_offset_metric_tons": "Sum of co2_offset_metric_tons across all active permits — total carbon avoided.",
    "avg_system_size_kw":           "Mean system_size_kw across all active permits for this city.",
    "high_yield_index_score":       "Composite 0–100 score for solar sales targeting. Derived from permit velocity, avg_system_size_kw, electricity_rate_per_kwh, and annual_savings_usd. Higher = stronger market.",
    "last_updated":                 "UTC timestamp of the most recent aggregation run that updated this row.",
}.items():
    spark.sql(f"ALTER TABLE illuminagrid.gold.city_summaries ALTER COLUMN {col} COMMENT '{comment}'")

print("illuminagrid.gold.city_summaries ready.")

# COMMAND ----------
# MAGIC %md ## 7 · illuminagrid.bronze._cdf_checkpoints

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS illuminagrid.bronze._cdf_checkpoints (
        table_name   STRING    NOT NULL,
        last_version LONG,
        updated_at   TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'false',
        'delta.minReaderVersion'     = '1',
        'delta.minWriterVersion'     = '2'
    )
""")

spark.sql("""
    COMMENT ON TABLE illuminagrid.bronze._cdf_checkpoints IS
    'Tracks the last successfully processed Delta commit version per source table.
     Read by silver and gold notebooks to resume CDF reads from where they left off
     rather than reprocessing the full history on every run.'
""")

for col, comment in {
    "table_name":   "Fully-qualified Delta table name, e.g. illuminagrid.bronze.permits.",
    "last_version": "Last Delta commit version that was fully processed by a downstream notebook.",
    "updated_at":   "UTC timestamp when this checkpoint was last written.",
}.items():
    spark.sql(f"ALTER TABLE illuminagrid.bronze._cdf_checkpoints ALTER COLUMN {col} COMMENT '{comment}'")

print("illuminagrid.bronze._cdf_checkpoints ready.")

# COMMAND ----------
# MAGIC %md ## 9 · illuminagrid.bronze.weather_manifest

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS illuminagrid.bronze.weather_manifest (
        city          STRING    NOT NULL,
        state         STRING    NOT NULL,
        dbfs_path     STRING,
        downloaded_at TIMESTAMP,
        nsrdb_version STRING
    )
    USING DELTA
    TBLPROPERTIES (
        'delta.enableChangeDataFeed' = 'false',
        'delta.minReaderVersion'     = '1',
        'delta.minWriterVersion'     = '2'
    )
""")

spark.sql("""
    COMMENT ON TABLE illuminagrid.bronze.weather_manifest IS
    'Registry of NSRDB TMY weather files downloaded to DBFS by notebook 02.
     Checked before each download so cities are not re-fetched unnecessarily.
     This is the only table written by a notebook that calls the NREL API.'
""")

for col, comment in {
    "city":          "City name matching bronze.zip_centroids. Composite primary key with state.",
    "state":         "2-letter state abbreviation. Composite primary key with city.",
    "dbfs_path":     "DBFS path of the downloaded TMY CSV: dbfs:/illuminagrid/weather/{state}/{city_slug}_tmy.csv",
    "downloaded_at": "UTC timestamp when the file was written to DBFS.",
    "nsrdb_version": "NSRDB dataset version used, e.g. PSM3-tmy-2020. Parsed from the names parameter of the API request.",
}.items():
    spark.sql(f"ALTER TABLE illuminagrid.bronze.weather_manifest ALTER COLUMN {col} COMMENT '{comment}'")

print("illuminagrid.bronze.weather_manifest ready.")

# COMMAND ----------
# MAGIC %md ## 8 · Verification

# COMMAND ----------

tables = spark.sql("""
    SELECT table_catalog, table_schema, table_name, table_type
    FROM   illuminagrid.information_schema.tables
    WHERE  table_schema IN ('bronze', 'silver', 'gold')
    ORDER  BY table_schema, table_name
""")

print("\nCreated tables:")
tables.show(truncate=False)

displayHTML("""
<div style="font-family:monospace; padding:16px; background:#1e1e2e; color:#cdd6f4;
            border-radius:8px; line-height:2">
  <b style="font-size:1.2em; color:#89b4fa">IlluminaGrid Schema Setup Complete</b><br><br>
  <span style="color:#a6e3a1">&#10003;</span> illuminagrid.bronze.permits<br>
  <span style="color:#a6e3a1">&#10003;</span> illuminagrid.bronze.zip_centroids<br>
  <span style="color:#a6e3a1">&#10003;</span> illuminagrid.bronze.weather_manifest<br>
  <span style="color:#a6e3a1">&#10003;</span> illuminagrid.silver.permits<br>
  <span style="color:#a6e3a1">&#10003;</span> illuminagrid.gold.enriched_permits &nbsp;<span style="color:#6c7086">(CDF enabled)</span><br>
  <span style="color:#a6e3a1">&#10003;</span> illuminagrid.gold.city_summaries &nbsp;&nbsp;<span style="color:#6c7086">(CDF enabled)</span><br>
</div>
""")
