# Databricks notebook source
# MAGIC %md
# MAGIC # Notebook Title

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date

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

# MAGIC %md 
# MAGIC ## Define the Goal
# MAGIC *What problem am I solving?*
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Collect and Manage the Data
# MAGIC *What information do I need?*

# COMMAND ----------

patient_df = spark.table("cdh_abfm_phi.patient")
patient_df.printSchema()

# COMMAND ----------

display(patient_df.limit(5))

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Build the Model
# MAGIC *Find patterns in the data that lead to solutions.*

# COMMAND ----------

display(
    patient_df.describe()
)

# COMMAND ----------

v = 'patientuid'
x = patient_df.agg({v:'max',v:'min'})

# COMMAND ----------

patient_df.dtypes

# COMMAND ----------

display(
    patient_df
    .select(F.max(F.col('*')))
    .limit(5)
)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Evaluate and Critique
# MAGIC *Does the model solve my problem?*

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Present Results and Document

# COMMAND ----------


