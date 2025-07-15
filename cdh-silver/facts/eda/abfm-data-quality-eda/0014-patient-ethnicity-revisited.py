# Databricks notebook source
# MAGIC %md
# MAGIC # Patient Ethnicity

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2021-01-06

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
# MAGIC - Determine what cleaning functions are needed to clean the Patient Ethnicity table
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Collect and Manage the Data
# MAGIC *What information do I need?*

# COMMAND ----------

df = spark.table("cdh_abfm_phi.patientethnicity")
df.printSchema()

# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

display(
    percent_full(df)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### How many unique values are in `patientethnicitycode`?

# COMMAND ----------

display(
    df
    .groupBy('patientethnicitycode')
    .agg(
        F.collect_set('patientethnicitytext'),
        F.count('patientethnicitytext')
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Flag Potential Problem
# MAGIC
# MAGIC Most of the patient ethnicity codes in the Ethnicity table is formatted using [HL7 standards](https://www.hl7.org/fhir/v3/Ethnicity/cs.html#:~:text=4.4.2.50.1%20Code%20System%20Content%20%20%20Level%20,%20%20Asturian%20%2039%20more%20rows%20). However some codes are not formatted according to this standard, and instead look like patientuids.

# COMMAND ----------

display(
    df.filter(df.patientethnicitycode=="2f329902-b34e-4dc3-9566-71ad60de73d4")
)

# COMMAND ----------

display(
    df
    .groupBy('patientethnicitycode','patientethnicitytext')
    .count()
    .withColumn('count_', F.array('count','patientethnicitytext'))
    .groupBy('patientethnicitycode')
    .agg(
        F.max('count_').getItem(1).alias('term'),
        F.max('count_').getItem(0).cast('int').alias('appearances'),
    )
)



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### ethnicitycodes look like FHIR/HL7 codes

# COMMAND ----------

display(
    df
    .filter(df.patientethnicitycode.rlike('^\d{4}\-{1}'))
    .groupBy('patientethnicitycode')
    .agg(
        F.collect_set('patientethnicitytext'),
        F.count('patientethnicitytext')
    )
)

# COMMAND ----------

display(
    df
    .filter(~df.patientethnicitycode.rlike('^\d{4}\-{1}'))
    .groupBy('patientethnicitycode')
    .agg(
        F.collect_set('patientethnicitytext'),
        F.count('patientethnicitytext')
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


