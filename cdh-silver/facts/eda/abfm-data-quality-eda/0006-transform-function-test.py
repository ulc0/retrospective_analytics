# Databricks notebook source
# MAGIC %md
# MAGIC # Practice ID EDA

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2021-01-07
# MAGIC ## 2021-12-23

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

# COMMAND ----------

def abbreviate_state(col):
    return F.lower(col)

# COMMAND ----------

display(
    visit_df
    .select(F.transform('service_location_state', abbreviate_state))
)

# COMMAND ----------

display(
    visit_df
    .select(F.transform('service_location_state', lambda x: len(x)))
)
