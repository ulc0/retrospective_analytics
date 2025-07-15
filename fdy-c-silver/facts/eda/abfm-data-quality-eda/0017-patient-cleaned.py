# Databricks notebook source
# MAGIC %md
# MAGIC # Patient (Table) Cleaned

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2021-01-10

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

patient_df = spark.table("cdh_abfm_phi.patient")
patient_df.printSchema()

# COMMAND ----------

display(patient_df.limit(5))

# COMMAND ----------

display(
    percent_full(patient_df)
)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Build the Model
# MAGIC *Find patterns in the data that lead to solutions.*

# COMMAND ----------

# MAGIC %run ./0000-utils

# COMMAND ----------



# COMMAND ----------

display(
    patient_df
    .transform(clean_date('dob'))
    .transform(clean_gender())
    .transform(clean_zipcode())
    .transform(clean_state('statecode'))
    .transform(create_age('dob'))
    .groupBy('patientuid')
    .agg(
        F.min('dob').alias('dob'),
        F.max('gender').alias('gender'),
        F.max('isdeceased').alias('isdeceased'),
        F.collect_set('zipcode').alias('zipcode'),
        F.collect_set('statecode').alias('statecode'),
        F.max('age').alias('age')
    )
)

# COMMAND ----------

cleaned_patient_df = (
    patient_df
    .transform(clean_date('dob'))
    .transform(clean_gender())
    .transform(clean_zipcode())
    .transform(clean_state('statecode'))
    .transform(create_age('dob'))
    .groupBy('patientuid')
    .agg(
        F.min('dob').alias('dob'),
        F.max('gender').alias('gender'),
        F.max('isdeceased').alias('isdeceased'),
        F.collect_set('zipcode').alias('zipcode'),
        F.collect_set('statecode').alias('statecode'),
        F.max('age').alias('age')
    )
)

# COMMAND ----------

cleaned_patient_df.count()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Evaluate and Critique
# MAGIC *Does the model solve my problem?*

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Present Results and Document

# COMMAND ----------


