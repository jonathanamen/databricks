# Databricks notebook source
# MAGIC %md
# MAGIC # 📘 Technical Overview
# MAGIC
# MAGIC SILVER - Incremental processing pipeline refines data from Bronze Layer to Silver Layer using structured transformations, data quality checks, and enrichment logic. Parameterized for flexible source/target configs and supports multiple formats. Optimized for distributed processing, incremental updates, and schema validation.

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.functions import col, from_unixtime, to_date, current_timestamp
from pyspark.sql.types import TimestampType
import re

# COMMAND ----------

# DBTITLE 1,Widgets
#JA: MANUAL RESET ONLY - Uncomment line below and run with Shift+Enter to clear persisted widget values
# dbutils.widgets.removeAll()

#JA: defaults
dbutils.widgets.text("source_catalog", "workspace")
dbutils.widgets.text("source_schema", "bronze_wanderbricks")
dbutils.widgets.text("source_table", "bookings")
dbutils.widgets.text("target_catalog", "workspace")
dbutils.widgets.text("target_schema", "silver_wanderbricks")
dbutils.widgets.text("target_checkpoint_volume", "checkpoints_silver")
dbutils.widgets.text("target_table", "bookings")

#JA: get from widgets
source_catalog = dbutils.widgets.get("source_catalog")
source_schema = dbutils.widgets.get("source_schema")
source_table = dbutils.widgets.get("source_table")
target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
target_checkpoints_volume = dbutils.widgets.get("target_checkpoint_volume")
target_table = dbutils.widgets.get("target_table")

#JA: fully qualified paths
source_table_full = f"{source_catalog}.{source_schema}.{source_table}"
target_table_full = f"{target_catalog}.{target_schema}.{target_table}"


# COMMAND ----------

# DBTITLE 1,Transform
#JA: create the silver table if it does not exist, enabling CDF
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {target_table_full}
    USING DELTA
    TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    AS SELECT * FROM {source_table_full} WHERE 1=0
""")

#JA: define the transformation logic as a standalone function
def apply_silver_transforms(df):
    #JA: loop through all columns to standardize temporal types
    for field in df.schema.fields:
        #JA: convert numeric unix timestamps to actual timestamp format
        if (field.name.endswith("_at") or field.name.endswith("_time")) \
            and field.dataType.typeName() in ["long", "double", "integer"]:
            df = df.withColumn(field.name, from_unixtime(col(field.name)).cast(TimestampType()))
        
        #JA: convert specific booking date strings to date type
        elif field.name in ["check_in", "check_out"] and field.dataType.typeName() == "string":
            df = df.withColumn(field.name, to_date(col(field.name)))
    return df

#JA: define the governance and quality check logic
def apply_quality_checks(df):
    #JA: dynamically identify all columns ending in _id
    id_fields = [f.name for f in df.schema.fields if f.name.endswith("_id")]
    
    #JA: create a boolean flag: true if any ID column is null
    is_null_check = None
    for f in id_fields:
        check = col(f).isNull()
        is_null_check = (is_null_check | check) if is_null_check is not None else check
    
    #JA: add the invalid flag and a processing timestamp for auditing
    return df.withColumn("is_invalid", is_null_check) \
             .withColumn("_silver_processed_at", current_timestamp())

#JA: initialize the read stream from the bronze delta table
bronze_stream = spark.readStream.format("delta").table(source_table_full)

#JA: cascade the dataframe through the transformation then the quality check
transformed_df = apply_silver_transforms(bronze_stream)
governed_df = apply_quality_checks(transformed_df)

#JA: write the final governed data to the silver table
(
    governed_df.writeStream
        .option("checkpointLocation", f"/Volumes/{target_catalog}/{target_schema}/{target_checkpoints_volume}/{target_table}") #JA: enables incremental loads by landing checkpoint data
        .option("mergeSchema", "true") #JA: schema evolution, changes to schema definiton will merge
        .trigger(availableNow=True)
        .toTable(target_table_full)
)