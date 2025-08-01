# Databricks notebook source
# MAGIC %md 
# MAGIC # Objective
# MAGIC - Explore the transformation of abfm data in a modular manner
# MAGIC - Demonstrate how we can modify clinical narratives to be reusuable in a more software oriented manner
# MAGIC
# MAGIC # General Practices
# MAGIC - Filter your dataframes (columns and rows) as soon as possible to reduce overhead
# MAGIC - Leverage joins, as they perform better over the cluster than filters
# MAGIC - Use PySpark functions over UDFs or Pandas UDFs whenever possible (requires serialization)
# MAGIC - Be mindful of cluster utilization
# MAGIC
# MAGIC # Demo

# COMMAND ----------

# DBTITLE 1,Set Notebook Parameters
# mandatory parameters and names. The orchestrator will always pass these
dbutils.widgets.text("source_table", defaultValue="note_bronze")
dbutils.widgets.text("source_schema", defaultValue="cdh_abfm_phi")
dbutils.widgets.text("schema", defaultValue="cdh_abfm_phi_exploratory")

# COMMAND ----------

# MAGIC %md
# MAGIC This is a temporary workaround util git folder workspaces fix

# COMMAND ----------

# DBTITLE 1,Import Helpers
# MAGIC %run ./utils

# COMMAND ----------

# DBTITLE 1,Imports
import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 1,Set Catalog Name from Cluster Config
# DBTITLE 1,Set Catalog Name from Cluster Config
# Fetch the value of the parameter spark.databricks.sql.initial.catalog.name from Spark config, exit if the value is not set in cluster configuration
catalog_name = spark.conf.get("spark.databricks.sql.initial.catalog.name")

if not catalog_name:
    dbutils.notebook.exit("Initial catalog name is empty in cluster")

# COMMAND ----------

# DBTITLE 1,Get Notebook Parameters
# DBTITLE 1,Get Notebook Parameters
args = dbutils.widgets.getAll()
source_table = f"{catalog_name}.{args['source_schema']}.{args['source_table']}"
cohort_table = f"{catalog_name}.{args['schema']}.{args['cohort_table']}"
sink_table = f"{catalog_name}.{args['schema']}.aix_feedback_{args['source_table']}"

print(f"Writing to {sink_table}.")

# COMMAND ----------

source_table

# COMMAND ----------

# DBTITLE 1,ETL
# Extract
source_df = (
    spark
    .table(source_table)
    # .selectExpr([
    #     "patientuid as person_id",
    #     "practiceid as provider_id",
    #     "encounterdate as note_datetime",
    #     "note as note_text",
    #     "concat_ws(' ', trim(group1), trim(group2), trim(group3), trim(group4)) as group"
    # ])
)

transformed_df = (
    source_df
    .transform(encoding_decider, "note_text", markup_decider)
    .transform(extract_note_text, "note_text", "encoding")
    .transform(apply_func, "clean_text", F.regexp_replace, pattern=r"[\|]", replacement=" ")
    .transform(apply_func, "clean_text", F.regexp_replace, pattern=r"[\=\_]{1,}", replacement=" ")
    .transform(apply_func, "clean_text", F.regexp_replace, pattern=r"[\u202F\u00A0]", replacement=" ")
    .transform(apply_func, "clean_text", F.regexp_replace, pattern=r"\,{2,}", replacement=",")
    .transform(apply_func, "clean_text", F.regexp_replace, pattern=r"\s{2,}", replacement=" ")
)

# Load Data
(
    transformed_df
    .write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(sink_table)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC | table | encoding | ours | other |
# MAGIC | ----- | ------- | ------ | ------ |
# MAGIC | patientnote | rtf | 7066 | 7066 |
# MAGIC | patientnote | xml | 2524 | 2524 |
# MAGIC | patientnote | utf-8 | 2,479,795 | 2,479,795|
# MAGIC | patientnote | html | 79 | 79 |
# MAGIC | | | | |
# MAGIC | patientnoteproblem | utf-8 | 677 | 677 |
# MAGIC | | | | |
# MAGIC | patientnoteresultobservation | utf | 10,070 | 10,070 |
# MAGIC | patientnoteresultobservation | xml | 700 | 700 |
# MAGIC | patientnoteresultobservation | rtf | 6780 | 6780 |
# MAGIC | patientnoteresultobservation | htm | 45 | 45 |
# MAGIC
# MAGIC ## Timing
# MAGIC | table | time (s) |
# MAGIC |-------|----------|
# MAGIC | patientnote | 39 |
# MAGIC | patientnoteproblem | 9 |
# MAGIC | patientnoteresultobservation | 22 |
# MAGIC
# MAGIC 1 hour 31 mins - 2 hour 42 mins
# MAGIC
