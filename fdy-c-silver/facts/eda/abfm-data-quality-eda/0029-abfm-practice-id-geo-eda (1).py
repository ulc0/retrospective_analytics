# Databricks notebook source
# MAGIC %md
# MAGIC # Practice ID EDA

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2022-01-20

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

import math

# COMMAND ----------

# MAGIC %run /Users/sve5@cdc.gov/0000-utils

# COMMAND ----------

# MAGIC %run /Users/sve5@cdc.gov/0000-geo-plotting

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
total_rows = visit_df.count()
visit_df.printSchema()

# COMMAND ----------

display(visit_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### What are the unique state values?

# COMMAND ----------

display(
visit_df
    .groupBy(F.lower(F.col('service_location_state')))
    .count()
    .orderBy(F.col('count').desc())
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Null states

# COMMAND ----------

display(
visit_df
    .filter((F.col('service_location_state') == 'null') | (F.col('service_location_state').isNull()))
    .groupBy(F.lower(F.col('service_location_state')))
    .count()
    .orderBy(F.col('count').desc())
)

# COMMAND ----------

null_rows = (
    visit_df
    .filter((F.col('service_location_state') == 'null') | (F.col('service_location_state').isNull()))
    .count()
)

print(null_rows, f"{(null_rows/total_rows)*100}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean Data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### How many unique practice ids are there?

# COMMAND ----------

(
visit_df
    .select('practiceid')
    .filter(visit_df.practiceid.cast('int').isNotNull())
    .distinct()
    .count()
)

# COMMAND ----------

display(
visit_df
    .select('practiceid')
    .filter((F.col('practiceid') == 'null') | (F.col('practiceid').isNull()))
)

# COMMAND ----------

pid_count = (
    visit_df
    .groupBy('practiceid')
    .count()
).toPandas()
pid_count['perc'] = (pid_count['count']/total_rows)*100

# COMMAND ----------

pd.set_option('display.max_rows',10)
pid_count.sort_values('count',ascending=False)

# COMMAND ----------

display(
    visit_df
    .filter(visit_df.practiceid.cast('int').isNotNull())
    .transform(clean_state())
    .groupBy('practiceid')
    .agg(
#         F.collect_set('service_location_city'),
#         F.collect_set('service_location_state'),
#         F.collect_set('service_location_postalcode'),
        F.collect_set('service_location_state'),
        F.count('patientuid')
       )
    .sort('count(patientuid)',ascending=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Spot check practice ids that have no state data
# MAGIC

# COMMAND ----------

display(
    visit_df
    .filter(visit_df.practiceid==1829)
)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC #### Flag possible issue

# COMMAND ----------

display(
    visit_df
    .filter(visit_df.practiceid.isNotNull())
    .filter(visit_df.service_location_state.isNull())
)

# COMMAND ----------

display(
    visit_df
    .filter(visit_df.note.rlike('^[\w]{8}\-{1}[\w]{4,}\-[\w]{4}\-'))
)

# COMMAND ----------

(
    visit_df
    .filter(visit_df.note.rlike('^[\w]{8}\-{1}[\w]{4,}\-[\w]{4}\-'))
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### State Practice Counts

# COMMAND ----------

state_col = 'service_location_state'
uid_col = 'patientuid'


# COMMAND ----------

state_practice_counts = (
visit_df
.transform(clean_state(state_col))
.groupBy(state_col)
.agg(
        F.countDistinct('practiceid').alias('unique_practices'),
        F.countDistinct(uid_col).alias('unique_patients'),
        F.count(uid_col).alias('visit_rowcount')
       )
).toPandas()

# COMMAND ----------

state_practice_counts['patients_per_practice'] = state_practice_counts['unique_patients'] / state_practice_counts['unique_practices']
state_practice_counts['percent_total_rowcount'] = (state_practice_counts['visit_rowcount'] / total_rows)*100
state_practice_counts['visits_per_practice'] = state_practice_counts['visit_rowcount'] / state_practice_counts['unique_practices']

pd.set_option('display.max_rows',60)

display(
state_practice_counts.sort_values('visit_rowcount',ascending=False)
)

# COMMAND ----------

state_practice_counts = state_practice_counts.dropna()

# COMMAND ----------

state_practice_counts.describe()

# COMMAND ----------

summary = state_practice_counts.describe()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Build the Model
# MAGIC *Find patterns in the data that lead to solutions.*

# COMMAND ----------

import plotly.express as px

simple_geoplot(state_practice_counts,state_col,factors=['unique_patients','unique_practices'])

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Evaluate and Critique
# MAGIC *Does the model solve my problem?*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Present Results and Document

# COMMAND ----------

sc.version
