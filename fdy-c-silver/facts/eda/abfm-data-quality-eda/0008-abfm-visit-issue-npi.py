# Databricks notebook source
# MAGIC %md
# MAGIC # Visit Table Schema Issue (NPI)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2022-01-01

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
# MAGIC - Quantify the extent to which the above is an issue.

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

def is_npi(df):
    x = df.filter(~df.serviceprovidernpi.rlike('\\d{10}'))
    return x

# COMMAND ----------

display(
    visit_df
    .transform(is_npi)
)

# COMMAND ----------

(
visit_df
.filter(~visit_df.serviceprovidernpi.rlike('\\d{10}'))
.count()
)

# COMMAND ----------

display(
visit_df
.filter(~visit_df.serviceprovidernpi.rlike('\\d{10}'))
)

# COMMAND ----------

display(
    visit_df
    .groupBy('encountertypecode')
    .agg(
        F.collect_set('encountertypetext'),
        F.count('encountertypecode').alias('count')
    )
    .orderBy(F.col('count').desc())
)

# COMMAND ----------

display(visit_df.select('encountertypecode').distinct().count())

# COMMAND ----------

display(
    visit_df
    .groupBy('encountertypecode')
    .agg(
        F.collect_set('encountertypetext').alias('et'),
    )
    .select('encountertypecode',F.explode('et').alias('et2'))
    .agg(
        F.count('encountertypecode','et2').alias('count')
    )
    .orderBy(F.col('count').desc())
)

# COMMAND ----------

display(
    visit_df
    .groupBy(['encountertypecode','encountertypetext'])
    .agg(
        F.count('encountertypecode').alias('et1')
    )
    .orderBy('encountertypecode')
)

# COMMAND ----------

w = W.Window.partitionBy('encountertypecode')
w2 = W.Window.partitionBy([F.col('encountertypecode'),F.col('encountertypetext')])
display(
    visit_df
    .select(
        'encountertypecode',
        'encountertypetext'
    )
#     .distinct()
    .select(
        'encountertypecode',
        'encountertypetext',
        F.count('encountertypecode').over(w).alias('n_grp'),
        F.count('encountertypetext').over(w2).alias('n'),
    )
    .distinct()
    .sort(F.desc('n_grp'),F.desc('n'))
)

# COMMAND ----------

w = W.Window.partitionBy('encountertypecode')
w2 = W.Window.partitionBy([F.col('encountertypecode'),F.col('encountertypetext')])
display(
    visit_df
    .select(
        'encountertypecode',
        'encountertypetext'
    )
#     .distinct()
    .select(
        'encountertypecode',
        'encountertypetext',
        F.count('encountertypecode').over(w).alias('n_grp'),
        F.count('encountertypetext').over(w2).alias('n'),
    )
    .distinct()
    .sort(F.desc('n_grp'),F.desc('n'))
    .drop_duplicates(['encountertypecode'])
    .sort(F.desc('n_grp'),F.desc('n'))
)

# COMMAND ----------


