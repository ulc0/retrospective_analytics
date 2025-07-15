# Databricks notebook source
# MAGIC %md
# MAGIC # Practice ID EDA

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2021-12-14

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Configuration

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

sc.version

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Define the Goal
# MAGIC *What problem am I solving?*
# MAGIC
# MAGIC Objective: Identify where practice IDs in the ABFM dataset are located.

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Collect and Manage the Data
# MAGIC *What information do I need?*
# MAGIC
# MAGIC Based on Ian Rand's analysis, the visit table contains city, state, and postal codes for service location.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load the `visit` table

# COMMAND ----------

visit_df = spark.table("cdh_abfm_phi.visit")
visit_df.printSchema()

# COMMAND ----------

display(visit_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Practice ID data profile

# COMMAND ----------

_v = ['service_location_city','service_location_state','service_location_postalcode']

display(
    visit_df
    .groupBy('practiceid')
    .agg(
        F.collect_set('service_location_city'),
        F.collect_set('service_location_state'),
        F.collect_set('service_location_postalcode'),
        F.count('practiceid')
    )
)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### What values are present for null practice ids?

# COMMAND ----------

ldf = (
visit_df
.filter(visit_df.practiceid.isNull())
)

# COMMAND ----------

display(ldf)
ldf.count()

# COMMAND ----------

ldf = (
visit_df
.filter(visit_df.practiceid.isNull() & visit_df.service_location_state.isNotNull())
)

# COMMAND ----------

ldf.count()
ldf.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Confirm error is not due to query language

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   practiceid, service_location_city
# MAGIC from
# MAGIC   cdh_abfm_phi.visit
# MAGIC where
# MAGIC   practiceid is null AND service_location_city is not null
# MAGIC limit
# MAGIC   1000;

# COMMAND ----------

# MAGIC %md
# MAGIC #### TODO: Report data issue.
# MAGIC
# MAGIC There is an issue with data in the database. Either:
# MAGIC * Data in the database was loaded incorrectly.
# MAGIC * Data was loaded correctly, but delivered with errors. 
# MAGIC
# MAGIC For the example below, although it appears that city and state are missing, they are simply shifted over a column.

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from cdh_abfm_phi.visit where patientuid == '7631D665-A6CB-4207-8F2A-4E7EC5D7D173'
# MAGIC
