# Databricks notebook source
# mandatory parameters and names. The orchestrator will always pass these
dbutils.widgets.text("source_table", defaultValue="note")
dbutils.widgets.text("source_schema", defaultValue="cdh_mimic_ra")
dbutils.widgets.text("sink_schema", defaultValue="cdh_mimic_exploratory")

# COMMAND ----------

import cdh_featurization.cleaning.markup_cleaner as muc
from cdh_featurization.cleaning.utils import extract_note_text
from cdh_featurization.functional.utils import apply_func
import shared.text_process as text_process


# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# Fetch the value of the parameter spark.databricks.sql.initial.catalog.name from Spark config, exit if the value is not set in cluster configuration
catalog_name = spark.conf.get("spark.databricks.sql.initial.catalog.name")

if not catalog_name:
    dbutils.notebook.exit("Initial catalog name is empty in cluster")
catalog_name='edav_dev_cdh'

# COMMAND ----------

args = dbutils.widgets.getAll()
source_table = f"{catalog_name}.{args['source_schema']}.{args['source_table']}"
sink_table = f"{catalog_name}.{args['sink_schema']}.{args['source_table']}_clean"

# COMMAND ----------

# Extract
source_df = (
    spark.table(source_table)
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
    .transform(text_process.count_words, "clean_text")).select("note_id","clean_text","encoding","word_count")


# Load Data
(
    transformed_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("mergeSchema","true")
    .saveAsTable(sink_table)
)
