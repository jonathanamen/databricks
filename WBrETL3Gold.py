# Databricks notebook source
# MAGIC %md
# MAGIC # 📘 Technical Overview
# MAGIC
# MAGIC SILVER - Incremental processing pipeline refines data from Bronze Layer to Silver Layer using structured transformations, data quality checks, and enrichment logic. Parameterized for flexible source/target configs and supports multiple formats. Optimized for distributed processing, incremental updates, and schema validation.

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# DBTITLE 1,Widgets
#JA: MANUAL RESET ONLY - Uncomment line below and run with Shift+Enter to clear persisted widget values
# dbutils.widgets.removeAll()

#JA: defaults
dbutils.widgets.text("source_catalog", "workspace")
dbutils.widgets.text("source_schema", "silver_wanderbricks")
dbutils.widgets.text("source_table", "bookings")
dbutils.widgets.text("target_catalog", "workspace")
dbutils.widgets.text("target_schema", "gold_wanderbricks")
dbutils.widgets.text("target_table", "bookings_monthly")

#JA: get from widgets
source_catalog = dbutils.widgets.get("source_catalog")
source_schema = dbutils.widgets.get("source_schema")
source_table = dbutils.widgets.get("source_table")
target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
target_table = dbutils.widgets.get("target_table")

#JA: fully qualified paths
source_table_full = f"{source_catalog}.{source_schema}.{source_table}"
target_table_full = f"{target_catalog}.{target_schema}.{target_table}"



# COMMAND ----------

# DBTITLE 1,Transform
#JA: 1. Create empty target table if not exists, with CDF enabled
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {target_table_full}
(
  year INT,
  month INT,
  total_count BIGINT,
  total_revenue DOUBLE
)
USING DELTA
TBLPROPERTIES (delta.enableChangeDataFeed = true)
""")

#JA: 2. Get the last CDF version
silver_delta = DeltaTable.forName(spark, source_table_full)
last_processed_version = silver_delta.history(1).select("version").collect()[0][0]

#JA: 3. Identify which month-years changed in Silver since that version
changed_periods = (spark.read.format("delta")
    .option("readChangeFeed", "true")
    .option("startingVersion", last_processed_version)
    .table(source_table_full)
    .select(
        F.year("created_at").alias("year"),
        F.month("created_at").alias("month")
    )
    .distinct()
)

#JA: 4. Pull full records for affected months to ensure accurate aggregation
silver_full = (spark.read.table(source_table_full)
    .withColumn("year", F.year("created_at"))
    .withColumn("month", F.month("created_at"))
)
affected_data = silver_full.join(
    F.broadcast(changed_periods), 
    ["year", "month"], 
    "inner"
)

#JA: 5. Re-aggregate only the affected periods
new_gold_aggregates = (affected_data
    .groupBy("year", "month")
    .agg(
        F.count("booking_id").alias("total_count"),
        F.sum("total_amount").alias("total_revenue")
    )
)

#JA: Merge into Gold
gold_table = DeltaTable.forName(spark, target_table_full)
(gold_table.alias("gold")
    .merge(
        new_gold_aggregates.alias("updates"),
        "gold.year = updates.year AND gold.month = updates.month"
    )
    .whenMatchedUpdate(set={
        "total_count": "updates.total_count",
        "total_revenue": "updates.total_revenue"
    })
    .whenNotMatchedInsert(values={
        "year": "updates.year",
        "month": "updates.month",
        "total_count": "updates.total_count",
        "total_revenue": "updates.total_revenue"
    })
    .execute()
)