# Databricks notebook source
# MAGIC %md
# MAGIC # Practice ID EDA

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2021-12-29

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
    'montana':'MT',
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
    'wyoming':'WY',
    'district of columbia':'DC'
}

labbr_abbr = {str.lower(v): v for k, v in states_abbr.items()}

states_abbr.update(labbr_abbr)

# COMMAND ----------

_pdf = pd.DataFrame.from_dict(states_abbr, orient='index',columns=['state_abbr']).reset_index().rename(columns={'index':'state_name'})

states_df = spark.createDataFrame(_pdf)

# COMMAND ----------

display(
    states_df
)

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

(
    visit_df
    .filter(visit_df.practiceid.cast('int').isNotNull())
    .join(states_df, F.lower(visit_df.service_location_state) == states_df.state_name,'leftouter')
    .groupBy('practiceid')
    .agg(
#         F.collect_set('service_location_city'),
#         F.collect_set('service_location_state'),
#         F.collect_set('service_location_postalcode'),
        F.collect_set('state_abbr'),
        F.count('patientuid')
       )
    .sort('count(patientuid)',ascending=False)
).show(1180,truncate=False)

# COMMAND ----------

display(
    visit_df
    .filter(visit_df.practiceid.cast('int').isNotNull())
    .join(states_df, F.lower(visit_df.service_location_state) == states_df.state_name,'leftouter')
    .groupBy('state_abbr')
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Why does practice 314 have two states
# MAGIC

# COMMAND ----------

display(
    visit_df
    .filter(visit_df.practiceid=='314')
    .groupby(F.lower(visit_df.service_location_state))
    .agg(
        F.collect_set('service_location_city'),
#         F.collect_set('encountertypetext'),
        F.collect_set('serviceprovidernpi'),
        F.count('patientuid')
       )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### State Practice Counts

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
        F.countDistinct('practiceid').alias('unique_practices'),
        F.countDistinct('patientuid').alias('unique_patients'),
        F.count('practiceid').alias('visit_rowcount')
       )
).toPandas()

# COMMAND ----------

state_practice_counts['patients_per_practice'] = state_practice_counts['unique_patients'] / state_practice_counts['unique_practices']
state_practice_counts['percent_total'] = (state_practice_counts['visit_rowcount'] / total_rows)*100

state_practice_counts.sort_values('visit_rowcount',ascending=False)

# COMMAND ----------

state_practice_counts.sort_values('unique_practices',ascending=False).dropna()

# COMMAND ----------

state_practice_counts = state_practice_counts.dropna()

# COMMAND ----------

state_practice_counts.describe()

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Build the Model
# MAGIC *Find patterns in the data that lead to solutions.*

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Geo Maps
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Practices by State

# COMMAND ----------

import plotly.express as px

_factor = 'unique_practices'

def plot_geo(factor):
    vmax = state_practice_counts[factor].max()
    # vmax = 100
    p25 = state_practice_counts[factor].quantile(0.25)/vmax
    p50 = state_practice_counts[factor].quantile(0.5)/vmax
    p75 = state_practice_counts[factor].quantile(0.75)/vmax

    fig = px.choropleth(
        state_practice_counts,
        width=800,
        height=400,
        locations='state_abbr',
        locationmode='USA-states',
        color=factor,
        scope='usa',
        range_color=[0,vmax],
        color_continuous_scale=[
          [0,'rgb(255,0,0)'],[p25,'rgb(255,0,0)'],
          [p25,'rgb(0,255,0)'],[p50,'rgb(0,255,0)'],
          [p50,'rgb(0,0,255)'],[p75,'rgb(0,0,255)'],
          [p75,'purple'], [1,'purple']
      ]
    )
    fig.show()

# COMMAND ----------

plot_geo('unique_practices')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Patients by State

# COMMAND ----------

plot_geo('unique_patients')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Patients per Practice by State

# COMMAND ----------

plot_geo('patients_per_practice')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Percent rows (table) by State

# COMMAND ----------

plot_geo('visit_rowcount')

# COMMAND ----------

plot_geo('percent_total')

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


