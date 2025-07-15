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

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import StringType

# COMMAND ----------

# Fetch the value of the parameter spark.databricks.sql.initial.catalog.name from Spark config, exit if the value is not set in cluster configuration
catalog_name = spark.conf.get("spark.databricks.sql.initial.catalog.name")

if not catalog_name:
    dbutils.notebook.exit("Initial catalog name is empty in cluster")

# COMMAND ----------

# mandatory parameters and names. The orchestrator will always pass these
table_options = [
    "patientnote",
    "patientnoteproblem",
    "patientnoteresultobservation",
]

dbutils.widgets.text("source_table", "patientnote")#, table_options)
dbutils.widgets.text("source_schema", defaultValue="cdh_abfm_phi")
dbutils.widgets.text("schema", defaultValue="cdh_abfm_phi_exploratory",)
dbutils.widgets.text("cohort_table", defaultValue=f"mpox_data_set_presentation_v2_run9")

# COMMAND ----------

def original(text):
    decider = (
        F.when(text.contains("rtf1"), "rtf") 
        .when(text.like("<?xm%"), "xml") 
        .when(text.like("<htm%"), "htm") 
        .when(text.contains('<html>'), "htm") 
        .when(text.like("<SOA%"), "sop") 
        .when(text.like("<CHRT%"), "xml")
        .when(text.like("<Care%"), "xml")
        .otherwise("utf")
    )
    return decider

def proposed(text: Column) -> Column:
    decider = (
        F.when(text.isNull(), "unknown")
        .when(text.startswith("{\\rtf"), "rtf")
        .when(text.startswith("<?xml"), "xml")
        .when(text.contains("<html>"), "html")
        .otherwise("utf")
    )
    return decider

def encoding_decider(dataframe: DataFrame, text: "ColumnOrName", func=proposed) -> DataFrame:
    """
    Returns a dataframe with an 'encoding" column, that contains the label of from a decider
    provided by func.

    Args:
        dataframe: DataFrame
        text: Column or name of column that contains text
        func: A Callable that decides membership of a text column
    """
    # If given name, cast to Column
    if isinstance(text, str):
        text = F.col(text)
    
    decider = func(text)

    return dataframe.withColumn("encoding", decider)

# COMMAND ----------

def extract_note_text(dataframe: DataFrame, text: "ColumnOrName", encoding: "ColumnOrName") -> DataFrame:
    """
    Returns a dataframe with a 'clean_text' column, that removes tags and formatting for rtf, html, and
    xml text while leaving remaining text as is.

    Args:
        dataframe: DataFrame
        text: Column or name of column that contains text to be cleaned
        encoding: Column or name of column that states the encoding of the text
    """
    # Cast strings are column objects
    if isinstance(text, str):
        text = F.col(text)
    
    if isinstance(encoding, str):
        encoding = F.col(encoding)
    
    # We remove all the tags for html and xml
    html_xml_result = F.regexp_replace(
        string=text, 
        pattern=r"</?[^>]+>|<[^>]* /$|</[^>]*$",
        replacement=" "
    )
    
    # For rtf, we remove the control word and then braces
    rtf_result = F.regexp_replace(
        string=text, 
        pattern=r"\{\*?\\[^{}]+}|[{}]|\\\n?[A-Za-z]+\n?(?:-?\d+)?[ ]?", 
        replacement=" "
    )

    return dataframe.withColumn(
        "clean_text",
        F.when(encoding == "html", html_xml_result)
        .when(encoding == "xml", html_xml_result)
        .when(encoding == "rtf", rtf_result)
        .otherwise(text)
    )

def replace_multi_space(dataframe: DataFrame, text: "ColumnOrName") -> DataFrame:
    if isinstance(text, str):
        column, name = F.col(text), text
    elif isinstance(text, Column):
        column, name = text, text.__str__()
    else:
        raise ValueError("text parameter needs to be Column or string type")

    return dataframe.withColumn(
        name,
        F.regexp_replace(column, r"\s{2,}", " ")
    )

# COMMAND ----------

# Get Widget Parameters
args = dbutils.widgets.getAll()
source_table = f"{catalog_name}.{args['source_schema']}.{args['source_table']}"
cohort_table = f"{args['cohort_table']}"
sink_table = f"{catalog_name}.{args['schema']}.aix_demo_kb_{args['source_table']}"
"""

"""

# Extract
source_df = (
    spark
    .table(source_table)
    .selectExpr([
        "patientuid as person_id",
        "practiceid as provider_id",
        "encounterdate as note_datetime",
        "trim(note) as note_text",
        "concat_ws(' ', trim(group1), trim(group2), trim(group3), trim(group4)) as group"
    ])
)
""" 
remove cohort 
cohort_df = (
    spark
    .table(cohort_table)
    .selectExpr("patientuid as person_id")
    .distinct()
)

# Transform
### Obtain the cohort samples from the source
select_cohort = source_df.join(cohort_df, on="person_id", how="inner")
"""
transformed_df = (
    source_df
    .transform(encoding_decider, "note_text", proposed)
    .transform(extract_note_text, "note_text", "encoding")
    .transform(replace_multi_space, "clean_text")
)

# Load
(
    #transformed_df
    #.write
    #.format("delta")
    #.mode("overwrite")
    #.saveAsTable(sink_table)
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
