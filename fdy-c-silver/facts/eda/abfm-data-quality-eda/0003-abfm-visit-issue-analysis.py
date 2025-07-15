# Databricks notebook source
# MAGIC %md
# MAGIC # Visit Table Schema Issue (service_location_state)

# COMMAND ----------

# MAGIC %md
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

display(visit_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create state abbreviation table

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



# COMMAND ----------

_pdf = pd.DataFrame.from_dict(states_abbr, orient='index',columns=['state_abbr']).reset_index().rename(columns={'index':'state_name'})

states_df = spark.createDataFrame(_pdf)

# COMMAND ----------

# MAGIC %md 
# MAGIC ### How many unique practices are there?

# COMMAND ----------

(
visit_df
    .join(states_df, F.lower(visit_df.service_location_state) == states_df.state_name,'leftouter')
    .select(F.countDistinct('practiceid'))
    .show()
)

# COMMAND ----------

practiceid_count = (
visit_df
    .groupBy('practiceid')
    .count()
).toPandas()

# COMMAND ----------

display(practiceid_count)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Investigate nonsense practice ids

# COMMAND ----------

pd.set_option('max_rows',None)
practiceid_count.sort_values('count',ascending=False).head(10)

# COMMAND ----------

practiceid_df = (
    visit_df
    .groupBy('practiceid')
    .count()
).toPandas()

practiceidnumeric_df = (
    visit_df
    .filter(visit_df.practiceid.cast('int').isNotNull())
    .groupBy('practiceid')
    .count()
).toPandas()

practiceidstring_df = (
    visit_df
    .filter(visit_df.practiceid.cast('int').isNull())
    .groupBy('practiceid')
    .count()
).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC practices with numeric ids

# COMMAND ----------

(
practiceidnumeric_df.sort_values('count',ascending=False).reset_index()['count'].count(),
practiceidnumeric_df.sort_values('count',ascending=False).reset_index()['count'].sum()
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC practices with non-numeric ids

# COMMAND ----------

(
practiceidstring_df.sort_values('count',ascending=False).reset_index()['count'].count(),
practiceidstring_df.sort_values('count',ascending=False).reset_index()['count'].sum()
)

# COMMAND ----------

practiceidstring_df.sort_values('count',ascending=False).head(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ### TODO: Report data issue.
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

odd_states['count'].sum()

# COMMAND ----------

odd_states['count'].sum()/total_rows*100

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
# MAGIC ### States found in service_location_postalcode
# MAGIC
# MAGIC - All instances of states found in postal code seem to be caused by the encountertypetext spilling into the next row.
# MAGIC - Errors are occuring after quotation marks ("), which could indicate an issue with the raw data or an issue when data was inserted into the database.

# COMMAND ----------

display (
visit_df
.join(states_df, F.lower(visit_df.service_location_postalcode) == states_df.state_name,'inner')
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

# MAGIC %md
# MAGIC
# MAGIC ### States found in encounter_status

# COMMAND ----------

state_practice_counts = (
visit_df
.join(states_df, F.lower(visit_df.encounter_status) == states_df.state_name,'inner')
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

pd.set_option('display.max_rows', None)
state_practice_counts.toPandas()

# COMMAND ----------


