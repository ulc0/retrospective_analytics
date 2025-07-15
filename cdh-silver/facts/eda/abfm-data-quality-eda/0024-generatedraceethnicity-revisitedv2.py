# Databricks notebook source
# MAGIC %md
# MAGIC # Patient Ethnicity

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2021-01-14

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Configuration

# COMMAND ----------

sc.version

# COMMAND ----------

# Maximize Pandas output text width.
import pandas as pd
pd.set_option('display.max_colwidth', 200)

#ready for advanced plotting
import matplotlib.pyplot as plt
import numpy as np

#import pyspark sql functions with alias
import pyspark.sql.functions as F
import pyspark.sql.window as W

#import specific functions from pyspark sql types
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

# COMMAND ----------

# MAGIC %run ./0000-utils

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Define the Goal
# MAGIC *What problem am I solving?*
# MAGIC
# MAGIC - Create cleaning functions 
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Collect and Manage the Data
# MAGIC *What information do I need?*

# COMMAND ----------

spark.sql("USE cdh_abfm_phi")
spark.sql("SHOW TABLES").toPandas()

# COMMAND ----------

df = spark.table("cdh_abfm_phi.generatedraceethnicity")
df.printSchema()

# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### How many unique patientethnicitycode are there?

# COMMAND ----------

display(
    df
    .groupBy(F.lower(df.patientracetext))
    .agg(
        F.count('patientracetext'),
        F.collect_set('Race')
    )
)

# COMMAND ----------

display(
    df
    .groupBy(F.lower(df.Race))
    .agg(
        F.count('Race'),
        F.collect_set('patientracetext')
    )
)

# COMMAND ----------

display(
  df
  .groupBy('Race')
  .agg(
    F.countDistinct('patientuid').alias('count')
  )
  .orderBy(F.col('count').desc())
)

# COMMAND ----------

display(
    percent_full(df)
)

# COMMAND ----------

display(
    df
    .groupBy(F.lower(df.Hispanic))
    .agg(
        F.count('Hispanic'),
        F.collect_set('patientethnicitytext')
    )
)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Build the Model
# MAGIC *Find patterns in the data that lead to solutions.*

# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC ## Evaluate and Critique
# MAGIC *Does the model solve my problem?*

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Present Results and Document

# COMMAND ----------


