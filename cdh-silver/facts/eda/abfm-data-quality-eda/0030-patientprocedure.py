# Databricks notebook source
# MAGIC %md
# MAGIC # patientprocedure (Table) EDA and Cleaning

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2021-01-25

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

# MAGIC %run /Users/sve5@cdc.gov/0000-utils

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Define the Goal
# MAGIC *What problem am I solving?*
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Collect and Manage the Data
# MAGIC *What information do I need?*

# COMMAND ----------

df = spark.table("cdh_abfm_phi.patientprocedure")
df.printSchema()

# COMMAND ----------

total_rows = df.count()

# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

display(
    percent_not_empty(df)
)

# COMMAND ----------

display(
    column_profile(df)
)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select MIN(procedurecode), MAX(procedurecode) from cdh_abfm_phi.patientprocedure

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select MAX(proceduredescription) FROM cdh_abfm_phi.patientprocedure

# COMMAND ----------

df.select(F.max('proceduredescription')).collect()

# COMMAND ----------

df.select(F.min('modifier1')).collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inspect min/max values

# COMMAND ----------

display(
    df.filter(F.col('procedurecode') == '"fragments')
)

# COMMAND ----------

display(
    df.filter(F.col('procedurecode') == '~2008')
)

# COMMAND ----------

(
    display(
        df.filter(~df.serviceprovidernpi.rlike('\\d{10}'))
    ),
    c:=df.filter(~df.serviceprovidernpi.rlike('\\d{10}')).count(),
    f"{(c/total_rows)*100}%"
)

# COMMAND ----------

display(
    df.groupBy('codesystem').count()
)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Build the Model
# MAGIC *Find patterns in the data that lead to solutions.*

# COMMAND ----------

display(
    df
    .withColumn(a:='procedurecode', F.upper(_clean_input(F.col(a))))
    .withColumn(b:='proceduredescription', F.lower(F.col(b)))
    .withColumn(c:='serviceprovidernpi', F.when(F.col(c).rlike("\\d{10}"), F.col(c)))
    .transform(clean_date('effectivedate'))

)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Evaluate and Critique
# MAGIC *Does the model solve my problem?*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Present Results and Document

# COMMAND ----------


