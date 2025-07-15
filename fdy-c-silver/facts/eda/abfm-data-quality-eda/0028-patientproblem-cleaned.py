# Databricks notebook source
# MAGIC %md
# MAGIC # Patientproblem (Table) Cleaned

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2021-01-11

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

# MAGIC %run /Users/sve5@cdc.gov/0000-utils

# COMMAND ----------

# MAGIC %run /Users/sve5@cdc.gov/0000-covid-definitions

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

df = spark.table("cdh_abfm_phi.patientproblem")
df.printSchema()

# COMMAND ----------

df.withColumn('problemcode',_clean_input(F.col('problemcode')))

# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Build the Model
# MAGIC *Find patterns in the data that lead to solutions.*

# COMMAND ----------

display(
    df
    .transform(clean_category_code())
    .transform(clean_date('documentationdate'))
    .transform(clean_date('problemonsetdate'))
    .transform(clean_date('problemresolutiondate'))
)

# COMMAND ----------

_clean_input(caseA_codes[0])

# COMMAND ----------

display(
    df
    .filter(F.col('documentationdate').cast('date')<=F.to_date(F.lit("2021-03-27")))
    .filter(_clean_input(F.col('problemcode')).isin(list(map(_clean_input,caseA_codes))))
    .groupBy('patientuid')
    .agg(
        F.collect_set('problemcode'),
        F.count('problemcode')
    )
)

# COMMAND ----------

list(map(_clean_input,caseA_codes))

# COMMAND ----------

display(
    df.filter(df.patientuid=="208677a6-c15a-420b-ae49-de080e81f51e")
)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Evaluate and Critique
# MAGIC *Does the model solve my problem?*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Present Results and Document

# COMMAND ----------


