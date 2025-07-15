# Databricks notebook source
# MAGIC %md
# MAGIC # Practice ID EDA

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2021-12-14
# MAGIC ## 2021-12-16

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



# COMMAND ----------

# MAGIC %md
# MAGIC ### Create State Abbreviation Table

# COMMAND ----------

states_abbr = {
    'alabama':'AL',
    'alaska':'AK',
    'arizona':'AZ',
    'arkansas':'AR',
    'california':'CA',
    'colorado':'CO',
    'connecticut':'CT',
    'delaware':'DE',
    'florida':'FL',
    'georgia':'GA',
    'hawaii':'HI',
    'idaho':'ID',
    'illinois':'IL',
    'indiana':'IN',
    'iowa':'IA',
    'kansas':'KS',
    'kentucky':'KY',
    'louisiana':'LA',
    'maine':'ME',
    'maryland':'MD',
    'massachusetts':'MA',
    'michigan':'MI',
    'minnesota':'MN',
    'mississippi':'MS',
    'missouri':'MO',
    'montanta':'MT',
    'nebraska':'NE',
    'nevada':'NV',
    'new hampshire':'NH',
    'new jersey':'NJ',
    'new mexico':'NM',
    'new york':'NY',
    'north carolina':'NC',
    'north dakota':'ND',
    'ohio':'OH',
    'oklahoma':'OK',
    'oregon':'OR',
    'pennsylvania':'PA',
    'rhode island':'RI',
    'south carolina':'SC',
    'south dakota':'SD',
    'tennessee':'TN',
    'texas':'TX',
    'utah':'UT',
    'vermont':'VT',
    'virginia':'VA',
    'washington':'WA',
    'west virginia':'WV',
    'wisconsin':'WI',
    'wyoming':'WY'
}

# COMMAND ----------

_pdf = pd.DataFrame.from_dict(states_abbr, orient='index',columns=['state_abbr']).reset_index().rename(columns={'index':'state_name'})

states_df = spark.createDataFrame(_pdf)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Quick Data Profile

# COMMAND ----------

display(
    visit_df
    .join(states_df, F.lower(visit_df.service_location_state) == states_df.state_name,'leftouter')
    .groupBy('practiceid')
    .agg(
        F.collect_set('state_abbr')
#         F.collect_set('service_location_city'),
#         F.collect_set('service_location_state'),
#         F.collect_set('service_location_postalcode'),
#         F.count('practiceid')
    )
)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### What values are present for null practice ids?

# COMMAND ----------

(
visit_df
    .select(visit_df.practiceid)
    .filter(visit_df.practiceid.isNotNull())
    .distinct()
    .count()
)

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

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean Data

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
    .filter(visit_df.practiceid.cast('int').isNotNull())
    .join(states_df, F.lower(visit_df.service_location_state) == states_df.state_name,'leftouter')
    .groupBy('practiceid')
    .agg(
#         F.collect_set('service_location_city'),
#         F.collect_set('service_location_state'),
#         F.collect_set('service_location_postalcode'),
        F.collect_set('state_abbr')
#         F.countDistinct('patientuid')
       )
)

# COMMAND ----------

state_practice_counts = (
visit_df
.filter(visit_df.practiceid.cast('int').isNotNull())
.join(states_df, F.lower(visit_df.service_location_state) == states_df.state_name,'leftouter')
.groupBy('state_abbr')
.agg(
#         F.collect_set('service_location_city'),
#         F.collect_set('service_location_state'),
#         F.collect_set('service_location_postalcode'),
#         F.collect_set('state_abbr')
        F.countDistinct('practiceid'),
        F.countDistinct('patientuid')
       )
).toPandas()

# COMMAND ----------

type(state_practice_counts)

# COMMAND ----------

display(
    state_practice_counts.sort_values('state_abbr')
)

# COMMAND ----------

display(
    state_practice_counts.sort_values('state_abbr')
)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Build the Model
# MAGIC *Find patterns in the data that lead to solutions.*

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Evaluate and Critique
# MAGIC *Does the model solve my problem?*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Present Results and Document

# COMMAND ----------

sc.version

# COMMAND ----------


