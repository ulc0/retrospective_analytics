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
dbutils.widgets.text("source_table", defaultValue="note")
dbutils.widgets.text("source_schema", defaultValue="cdh_ml")
dbutils.widgets.text("sink_schema", defaultValue="cdh_ml")

# COMMAND ----------

import cdh_featurization.cleaning.markup_cleaner as muc
from cdh_featurization.cleaning.utils import extract_note_text
from cdh_featurization.functional.utils import apply_func

# COMMAND ----------

# DBTITLE 1,Imports
import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 1,Set Catalog Name from Cluster Config
# Fetch the value of the parameter spark.databricks.sql.initial.catalog.name from Spark config, exit if the value is not set in cluster configuration
catalog_name = spark.conf.get("spark.databricks.sql.initial.catalog.name")

if not catalog_name:
    dbutils.notebook.exit("Initial catalog name is empty in cluster")

# COMMAND ----------

# DBTITLE 1,Get Notebook Parameters
args = dbutils.widgets.getAll()
source_table = f"{catalog_name}.{args['source_schema']}.{args['source_table']}"
sink_table = f"{catalog_name}.{args['sink_schema']}.{args['source_table']}_silver"

# COMMAND ----------

# DBTITLE 1,ETL
# Extract
source_df = (
    spark
    .table(source_table)
)

transformed_df = (
    source_df
    .transform(muc.encoding_decider, "note_text", muc.markup_decider)
    .transform(extract_note_text, "note_text", "encoding")
    .transform(apply_func, "clean_text", F.regexp_replace, pattern=r"[\|]", replacement=" ")
    .transform(apply_func, "clean_text", F.regexp_replace, pattern=r"[\=\_]{1,}", replacement=" ")
    .transform(apply_func, "clean_text", F.regexp_replace, pattern=r"[\u202F\u00A0]", replacement=" ")
    .transform(apply_func, "clean_text", F.regexp_replace, pattern=r"\&\#{1}?160\;", replacement=" ")
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
