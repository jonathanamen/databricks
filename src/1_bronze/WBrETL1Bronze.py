# Databricks notebook source
# MAGIC %md
# MAGIC # 📘 Technical Overview
# MAGIC
# MAGIC BRONZE - Incremental ingestion pipeline moves raw data from Landing Zone to Bronze Layer using Auto Loader, schema inference, and audit metadata. Parameterized for flexible source/target configs and supports multiple formats. Optimized for distributed processing, incremental loads, and schema validation.

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import input_file_name, current_timestamp, col

# COMMAND ----------

# DBTITLE 1,Widgets
#JA: MANUAL RESET ONLY - Uncomment line below and run with Shift+Enter to clear persisted widget values
# dbutils.widgets.removeAll()

#JA: defaults
dbutils.widgets.text("source_catalog", "workspace")
dbutils.widgets.text("source_schema", "bronze_wanderbricks")
dbutils.widgets.text("source_volume", "landing_wanderbricks")
dbutils.widgets.text("source_file", "bookings")
dbutils.widgets.text("data_format", "json")
dbutils.widgets.text("target_catalog", "workspace") 
dbutils.widgets.text("target_schema", "bronze_wanderbricks")
dbutils.widgets.text("target_checkpoint_volume", "checkpoints_bronze")
dbutils.widgets.text("target_table", "bookings")

#JA: get from widgets
source_catalog = dbutils.widgets.get("source_catalog")
source_schema = dbutils.widgets.get("source_schema")
source_volume = dbutils.widgets.get("source_volume")
source_file = dbutils.widgets.get("source_file")
data_format = dbutils.widgets.get("data_format")
target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
target_checkpoints_volume = dbutils.widgets.get("target_checkpoint_volume")
target_table = dbutils.widgets.get("target_table")

#JA: fully qualified paths
source_data_path = f"/Volumes/{source_catalog}/{source_schema}/{source_volume}/{source_file}"
checkpoint_path = f"/Volumes/{target_catalog}/{target_schema}/{target_checkpoints_volume}/{target_table}"
target_table_full = f"{target_catalog}.{target_schema}.{target_table}"

# COMMAND ----------

# DBTITLE 1,Ingest
#JA: ingest file to bronze, to streaming table with autoloader
(
    spark
        .readStream
            .format("cloudFiles")
            .option("cloudFiles.format", data_format)
            .option("cloudFiles.schemaLocation", f"{source_data_path}/_schema")
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(source_data_path)
            .select( #JA: select is more performant than withColumn
                "*"
                ,col("_metadata.file_path") #JA: add file path
                ,col("_metadata.file_modification_time") #JA: add file mod date
                ,current_timestamp().alias("ingested_at") #JA: add ingest time
            )
        .writeStream
            .option("checkpointLocation", checkpoint_path)
            .trigger(availableNow=True)
            .toTable(target_table_full)
)