# Databricks notebook source
# MAGIC %md
# MAGIC # Visit (Table) Cleaned

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2021-01-13

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

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Collect and Manage the Data
# MAGIC *What information do I need?*

# COMMAND ----------

df = spark.table("cdh_abfm_phi.visit")
df.printSchema()

# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

display(
    percent_full(df)
)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Build the Model
# MAGIC *Find patterns in the data that lead to solutions.*

# COMMAND ----------

display(
    df
    .select('patientuid','encountertypecode','encounterstartdate')
    .withColumn('encountertypecode',_clean_input(F.col('encountertypecode')))
    .transform(clean_date('encounterstartdate'))
)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Evaluate and Critique
# MAGIC *Does the model solve my problem?*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Present Results and Document

# COMMAND ----------

display(
    df
    .withColumn('encountertypecode',_clean_input(F.col('encountertypecode')))
    .filter(F.col('encountertypecode').isin('u07','u070'))
)

# COMMAND ----------


