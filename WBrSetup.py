# Databricks notebook source
# DBTITLE 1,Imports


# COMMAND ----------

# DBTITLE 1,Config
#JA: MANUAL RESET ONLY - Uncomment line below and run with Shift+Enter to clear persisted widget values
# dbutils.widgets.removeAll()

#JA: config defaults
source_catalog = "samples"
source_schema = "wanderbricks"
target_catalog = "workspace"

# COMMAND ----------

# DBTITLE 1,Drop Create Schemas & Volume
#JA: drop schemas/volume
#JA: drop tables and volume in bronze schema before dropping schema
bronze_schema = f"bronze_{source_schema}"
if bronze_schema in [db.name for db in spark.catalog.listDatabases()]:
    #JA: Drop all tables in bronze schema
    for table in spark.catalog.listTables(f"{target_catalog}.{bronze_schema}"):
        spark.sql(f"DROP TABLE IF EXISTS {target_catalog}.{bronze_schema}.{table.name}")
    #JA: Drop volumes
    spark.sql(f"DROP VOLUME IF EXISTS {target_catalog}.{bronze_schema}.landing_{source_schema}")
    spark.sql(f"DROP VOLUME IF EXISTS {target_catalog}.{bronze_schema}.checkpoints_bronze")

#JA: drop tables in silver schema before dropping schema
silver_schema = f"silver_{source_schema}"
if silver_schema in [db.name for db in spark.catalog.listDatabases()]:
    for table in spark.catalog.listTables(f"{target_catalog}.{silver_schema}"):
        spark.sql(f"DROP TABLE IF EXISTS {target_catalog}.{silver_schema}.{table.name}")
    #JA: Drop volume
    spark.sql(f"DROP VOLUME IF EXISTS {target_catalog}.{silver_schema}.checkpoints_silver")

#JA: drop tables in gold schema before dropping schema
gold_schema = f"gold_{source_schema}"
if gold_schema in [db.name for db in spark.catalog.listDatabases()]:
    for table in spark.catalog.listTables(f"{target_catalog}.{gold_schema}"):
        spark.sql(f"DROP TABLE IF EXISTS {target_catalog}.{gold_schema}.{table.name}")

#JA: now drop schemas
spark.sql(f"DROP SCHEMA IF EXISTS {target_catalog}.bronze_{source_schema}")
spark.sql(f"DROP SCHEMA IF EXISTS {target_catalog}.silver_{source_schema}")
spark.sql(f"DROP SCHEMA IF EXISTS {target_catalog}.gold_{source_schema}")

#JA: create schemas
#JA: Bronze
spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {target_catalog}.bronze_{source_schema}
    COMMENT 'Bronze layer - raw incremental ingest from Wanderbricks source system.'
    WITH DBPROPERTIES (
        'layer' = 'bronze'
        ,'source' = 'source wanderbricks'
        ,'ingestion_pattern' = 'incremental'
        ,'retention' = '30_days'
        ,'cdf_enabled' = 'true'
    )
""")

#JA: Silver
spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {target_catalog}.silver_{source_schema}
    COMMENT 'Silver layer - cleaned, typed, deduplicated and joined Wanderbricks data'
    WITH DBPROPERTIES (
        'layer' = 'silver'
        ,'source' = 'bronze wanderbricks'
        ,'transform_pattern' = 'clean_join_dedupe (incremental)'
        ,'retention' = '90_days'
        ,'cdf_enabled' = 'true'
    )
""")

#JA: Gold
spark.sql(f"""
    CREATE SCHEMA IF NOT EXISTS {target_catalog}.gold_{source_schema}
    COMMENT 'Gold layer - aggregated Wanderbricks business metrics'
    WITH DBPROPERTIES (
        'layer' = 'gold',
        'source' = 'silver wanderbricks',
        'transform_pattern' = 'incremental aggregate',
        'retention' = '365_days',
        'cdf_enabled' = 'true'
    )
""")

#JA: create volumes
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {target_catalog}.bronze_{source_schema}.landing_{source_schema}
    COMMENT 'Landing zone - simulates Lakeflow Connect dropping incremental files from external source'
""")
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {target_catalog}.bronze_{source_schema}.checkpoints_bronze
    COMMENT 'Bronze checkpoints'
""")
spark.sql(f"""
    CREATE VOLUME IF NOT EXISTS {target_catalog}.silver_{source_schema}.checkpoints_silver
    COMMENT 'Silver checkpoints'
""")

#JA: verify schemas and volumes
schema_names = [db.name for db in spark.catalog.listDatabases() if db.name in [
    f"bronze_{source_schema}", f"silver_{source_schema}", f"gold_{source_schema}"
]]
print("Schemas found:", schema_names)

bronze_volumes = [v.volume_name for v in spark.sql(f"SHOW VOLUMES IN {target_catalog}.bronze_{source_schema}").collect()]
silver_volumes = [v.volume_name for v in spark.sql(f"SHOW VOLUMES IN {target_catalog}.silver_{source_schema}").collect()]
gold_volumes = [v.volume_name for v in spark.sql(f"SHOW VOLUMES IN {target_catalog}.gold_{source_schema}").collect()]

print("Bronze Volumes:", bronze_volumes)
print("Silver Volumes:", silver_volumes)
print("Gold Volumes:", gold_volumes)

# COMMAND ----------

# DBTITLE 1,Simulate Lakeflow Connect Landing Files
#JA: Simulate Lakeflow Connect incremental file drop
#JA: In production this would be managed by Lakeflow Connect (Probably Fivetran)
#JA: landing parquet files from wanderbricks

#JA: drop existing files
dbutils.fs.rm(f"/Volumes/{target_catalog}/bronze_{source_schema}/landing_{source_schema}", recurse=True)

#JA: get source tables to ingest
source_tables = [
    "bookings",
    "booking_updates", 
    "payments",
    "properties",
    "reviews",
    "users",
    "destinations",
    "hosts"
]

#JA: for each table, ingest simulated batch 00 as json
for table in source_tables:
    output_dir = f"/Volumes/{target_catalog}/bronze_{source_schema}/landing_{source_schema}/{table}"
    output_file = f"{output_dir}/batch_00.json"
    (
        spark.table(f"{source_catalog}.{source_schema}.{table}")
            .write
            .mode("overwrite")
            .json(output_file)
    )
    print(f"{table} batch_00.json landed")

#JA: show directories after creation
souce_tables_landed = [f.name.rstrip('/') for f in dbutils.fs.ls(f"/Volumes/{target_catalog}/bronze_{source_schema}/landing_{source_schema}")]
print("Tables Landed:", souce_tables_landed)