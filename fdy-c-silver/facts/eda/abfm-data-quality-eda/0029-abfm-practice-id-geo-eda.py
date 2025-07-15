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

state_practice_counts = (
visit_df
.transform(clean_state('service_location_state'))
.groupBy('service_location_state')
.agg(
#         F.collect_set('service_location_city'),
#         F.collect_set('service_location_state'),
#         F.collect_set('service_location_postalcode'),
#         F.collect_set('state_abbr')
        F.countDistinct('practiceid').alias('unique_practices'),
        F.countDistinct('patientuid').alias('unique_patients'),
        F.count('patientuid').alias('visit_rowcount')
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



# COMMAND ----------

state_practice_counts = state_practice_counts.dropna()

# COMMAND ----------

state_practice_counts.describe()

# COMMAND ----------

summary = state_practice_counts.describe()

# COMMAND ----------

summary.loc['25%','unique_practices']

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
        locations='service_location_state',
        locationmode='USA-states',
        color=factor,
        scope='usa',
        range_color=[0,vmax],
        color_continuous_scale=[
          [0,'blue'],[p25,'blue'],
          [p25,'cyan'],[p50,'cyan'],
          [p50,'purple'],[p75,'purple'],
          [p75,'magenta'], [1,'magenta']
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

plot_geo('percent_total_rowcount')

# COMMAND ----------

total_rows*.25

# COMMAND ----------

cut_labels = ['1-10', '11-20', '21-30','31-40','41-50','51-60','61-70','>70']
cut_labels = ['1-7', '8-15', '16-36','36-135']

cut_bins = [0, 10, 20, 30, 40, 50, 60, 70, 150]
cut_bins = [0, 7, 15, 36, 135]
state_practice_counts['Practices'] = pd.cut(state_practice_counts['unique_practices'], bins=cut_bins, labels=cut_labels)

# COMMAND ----------

cut_labels2 = ['31,857-255,000', '255,001-795,500', '795,501-1.68M', '>1.68M']

cut_bins2 = [0, 255000, 795500, 1680000, 7600000]
state_practice_counts['Visits'] = pd.cut(state_practice_counts['visit_rowcount'], bins=cut_bins2, labels=cut_labels2)

# COMMAND ----------

cut_labels3 = ['31,857-255,000', '255,001-795,500', '795,501-1.68M', '>1.68M']

cut_bins2 = [0, 255000, 795500, 1680000, 7600000]
state_practice_counts['Visits'] = pd.cut(state_practice_counts['visit_rowcount'], bins=cut_bins2, labels=cut_labels2)

# COMMAND ----------

all_cbs = []
all_cls = []
all_cmaps = []

factors = ['unique_practices','visit_rowcount','visits_per_practice']

for factor in factors:

    a = math.ceil(summary.loc['min',factor])
    b = math.ceil(summary.loc['25%',factor])
    c = math.ceil(summary.loc['50%',factor])
    d = math.ceil(summary.loc['75%',factor])
    e = math.ceil(summary.loc['max',factor])

    cl = [f"{a:,} - {b:,}",f"{(b+1):,} - {c:,}",f"{(c+1):,} - {d:,}",f"{(d+1):,} - {e:,}",]
    cb = [a,b,c,d,e]
    
    c = [
    px.colors.sequential.Blues[8], 
    px.colors.sequential.Blues[4],
    px.colors.sequential.Reds[8], 
    px.colors.sequential.Reds[4]
    ]
    cmap = dict(zip(cl,c))
    
    all_cbs.append(cb)
    all_cls.append(cl)
    all_cmaps.append(cmap)

# COMMAND ----------

state_practice_counts['Practices'] = pd.cut(state_practice_counts['unique_practices'], bins=all_cbs[0], labels=all_cls[0],include_lowest=True)

state_practice_counts['Visits'] = pd.cut(state_practice_counts['visit_rowcount'], bins=all_cbs[1], labels=all_cls[1],include_lowest=True)

state_practice_counts['Visits per Practice'] = pd.cut(state_practice_counts['visits_per_practice'], bins=all_cbs[2], labels=all_cls[2],include_lowest=True)

# COMMAND ----------

# px.colors.sequential.swatches()

# COMMAND ----------

# cmap = dict(zip(cut_labels,px.colors.diverging.RdBu))

# COMMAND ----------

# c = [
#     px.colors.sequential.Blues[8], 
#     px.colors.sequential.Blues[4],
#     px.colors.sequential.Reds[8], 
#     px.colors.sequential.Reds[4]
# ]
# cmap = dict(zip(cut_labels,c))
# cmap2 = dict(zip(cut_labels2,c))

# COMMAND ----------

all_cls

# COMMAND ----------

factor = 'unique_practices'
factor_label = 'Practices'
cl_label = all_cls[0]
cmap = all_cmaps[0]

fig = px.choropleth(
    state_practice_counts,
    width=800,
    height=400,
    locations='service_location_state',
    locationmode='USA-states',
    color=factor_label,
    scope='usa',
    category_orders={factor_label:cl_label},
    color_discrete_map=cmap
)
fig.show()

# COMMAND ----------

factor = 'visit_rowcount'
factor_label = 'Visits'
cl_label = all_cls[1]
cmap = all_cmaps[1]

fig = px.choropleth(
    state_practice_counts,
    width=800,
    height=400,
    locations='service_location_state',
    locationmode='USA-states',
    color=factor_label,
    scope='usa',
    category_orders={factor_label:cl_label},
    color_discrete_map=cmap
)
fig.show()

# COMMAND ----------

factor = 'visits_per_practice'
factor_label = 'Visits per Practice'
cl_label = all_cls[2]
cmap = all_cmaps[2]

fig = px.choropleth(
    state_practice_counts,
    width=800,
    height=400,
    locations='service_location_state',
    locationmode='USA-states',
    color=factor_label,
    scope='usa',
    category_orders={factor_label:cl_label},
    color_discrete_map=cmap
)
fig.show()

# COMMAND ----------

state_practice_counts

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

visit_df.
