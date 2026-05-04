# Databricks notebook source
# DBTITLE 1,Imports


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

# DBTITLE 1,Check if CDF is Enabled
#JA: check if CDF is enabled on silver table
dfrm = spark.sql(f"SHOW TBLPROPERTIES {target_table_full}")
dfrm.show(truncate=False)

#JA: drop silver table to rerun the silver notebook
spark.sql(f"DROP TABLE IF EXISTS {target_catalog}.{target_schema}.{target_table}")


# COMMAND ----------

# DBTITLE 1,Reset Widgets
#JA: Reset widgets
#JA: remove existing widgets to reset to defaults
dbutils.widgets.removeAll()