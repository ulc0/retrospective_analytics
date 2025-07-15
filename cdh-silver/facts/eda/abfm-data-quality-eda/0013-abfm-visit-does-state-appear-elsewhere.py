# Databricks notebook source
# MAGIC %md
# MAGIC # Visit Table Schema Issue (service_location_state)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2021-01-06

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

# MAGIC %run ./0000-utils

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Define the Goal
# MAGIC *What problem am I solving?*
# MAGIC
# MAGIC Objective: 
# MAGIC - Identify where practice IDs in the ABFM dataset are located.
# MAGIC - Based on preliminary analysis, "state" data is appearing in columns that don't make sense. The goal of this notebook is to Quantify the extent to which the above is an issue.

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

total_rows = visit_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create state abbreviation table

# COMMAND ----------

display(
states_df
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## What values exist for service_location_state

# COMMAND ----------

display(
    visit_df
    .groupBy(F.lower('service_location_state').alias('state'))
    .count()
    .join(states_df, F.col('state') == states_df.state_name,'leftanti')
)

# COMMAND ----------

odd_states = (
    visit_df
    .groupBy(F.lower('service_location_state').alias('state'))
    .count()
    .join(states_df, F.col('state') == states_df.state_name,'leftanti')
    .join(states_df, F.col('state') == F.lower(states_df.state_abbr),'leftanti')
    .filter(F.col('state').isNotNull())
).toPandas()

# COMMAND ----------

display(odd_states)

# COMMAND ----------

# MAGIC %md
# MAGIC ## How often do "States" appear in other columns?

# COMMAND ----------

display(
    visit_df
    .join(states_df, F.lower(visit_df.encounter_status) == states_df.state_name,'inner')
    .groupBy('practiceid')
    .agg(
#         F.collect_set('service_location_city'),
#         F.collect_set('service_location_state'),
#         F.collect_set('service_location_postalcode'),
        F.collect_set('state_abbr'),
        F.countDistinct('patientuid'),
        F.count('practiceid')
       )
)

# COMMAND ----------

_l = visit_df.columns
_l

# COMMAND ----------

[(_col,visit_df.join(states_df, F.lower(F.col(_col)) == states_df.state_name, 'inner').count()) for _col in _l[:3]]

# COMMAND ----------

[(_col,visit_df.join(states_df, F.lower(F.col(_col)) == states_df.state_name, 'inner').count()) for _col in _l[3:]]

# COMMAND ----------

(2+127246+37993922+349+1771+2)-37993922

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### States found in service_location_city
# MAGIC
# MAGIC All instances of state names found in service_location_city appear justifed. For example, California is in fact a city in Maryland, and New York is both a city and state.

# COMMAND ----------

display (
visit_df
.join(states_df, F.lower(visit_df.service_location_city) == states_df.state_name,'inner')
.groupBy('service_location_city')
.agg(
#         F.collect_set('service_location_city'),
        F.collect_set('state_abbr'),
        F.collect_set('service_location_state')
#         F.collect_set('service_location_postalcode'),
#         F.collect_set('state_abbr')
#         F.countDistinct('practiceid'),
#         F.countDistinct('patientuid')
       )
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### States found in serviceproviderrolecode
# MAGIC
# MAGIC - All instances of states found in postal code seem to be caused by the encountertypetext spilling into the next row.
# MAGIC - Errors are occuring after quotation marks ("), which could indicate an issue with the raw data or an issue when data was inserted into the database.

# COMMAND ----------

display (
visit_df
.join(states_df, F.lower(visit_df.serviceproviderrolecode) == states_df.state_name,'inner')
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### States found in reasonforvisit, note

# COMMAND ----------

display (
visit_df
.join(states_df, F.lower(visit_df.reasonforvisit) == states_df.state_name,'inner')
)

# COMMAND ----------

display (
visit_df
.join(states_df, F.lower(visit_df.note) == states_df.state_name,'inner')
)

# COMMAND ----------

display (
visit_df
.join(states_df, F.lower(visit_df.note) == states_df.state_name,'inner')
.groupBy('note')
.agg(
        F.collect_set('state_abbr'),
        F.collect_set('note')
       )
)

# COMMAND ----------

display(
    visit_df.filter(visit_df.note=="Georgia")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### States found in encountertypecode

# COMMAND ----------

state_practice_counts = (
visit_df
.join(states_df, F.lower(visit_df.encountertypecode) == states_df.state_name,'inner')
# .groupBy('state_abbr')
# .agg(
# #         F.collect_set('service_location_city'),
# #         F.collect_set('service_location_state'),
# #         F.collect_set('service_location_postalcode'),
# #         F.collect_set('state_abbr')
#         F.countDistinct('practiceid'),
#         F.countDistinct('patientuid')
#        )
)

# COMMAND ----------

state_practice_counts.count()

# COMMAND ----------

display(
    state_practice_counts.limit(2000)
)

# COMMAND ----------

display (
visit_df
.join(states_df, F.lower(visit_df.encountertypecode) == states_df.state_name,'inner')
.groupBy('encountertypecode')
.agg(
        F.collect_set('state_abbr'),
        F.collect_set('encountertypetext')
       )
)

# COMMAND ----------

pd.set_option('display.max_rows', None)
state_practice_counts.toPandas()

# COMMAND ----------

display(visit_df.filter(visit_df.encountertypecode=="DE"))

# COMMAND ----------


