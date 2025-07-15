# Databricks notebook source
# MAGIC %md
# MAGIC # visitdiagnosis (Table) Cleaned

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## 2021-01-13

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

# MAGIC %run ./0000-utils

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

df = spark.table("cdh_abfm_phi.visitdiagnosis")
df.printSchema()

# COMMAND ----------

display(df.limit(5))

# COMMAND ----------

display(
    percent_full(df)
)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Build the Model
# MAGIC *Find patterns in the data that lead to solutions.*

# COMMAND ----------

display(
    df
    .select('patientuid','encounterdate','serviceprovidernpi','encounterdiagnosiscode','documentationdate')
    .withColumn('encounterdiagnosiscode',_clean_input(F.col('encounterdiagnosiscode')))
    .transform(clean_date('encounterdate'))
    .transform(clean_date('documentationdate'))

)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2022-01-18

# COMMAND ----------



# COMMAND ----------

display(
    df
    .filter(F.col('encounterdiagnosiscode').isin('U071') | F.col('encounterdiagnosistext').isin('Acute bronchitis due to 2019-nCoV'))
)

# COMMAND ----------

display(
    df
    .filter(F.col('encounterdiagnosistext').isin('History of 2019 novel coronavirus disease (COVID-19'))
)

# COMMAND ----------

display(
    df
    .filter(F.col('encounterdiagnosistext').isin('History of 2019 novel coronavirus disease (COVID-19)'))
)

# COMMAND ----------

# MAGIC %run /Users/sve5@cdc.gov/0000-utils 

# COMMAND ----------

# MAGIC %run /Users/sve5@cdc.gov/0000-covid-definitions

# COMMAND ----------

display(
    df
    .filter(F.col('encounterdiagnosiscode').isin(caseA_codes) | starts_with_from_list('encounterdiagnosistext',caseA_text))
) 

# COMMAND ----------

(
    df
.filter(F.col('encounterdiagnosiscode').isin(caseA_codes) | starts_with_from_list('encounterdiagnosistext',caseA_text))
.select('patientuid')
.distinct()
.count()
)

# COMMAND ----------

(
    df
.filter(F.col('encounterdiagnosiscode').isin(caseA_codes) | starts_with_from_list('encounterdiagnosistext',caseA_text))
    .filter(F.col('encounterdate').cast('date')<=F.to_date(F.lit("2021-03-27")))
.select('patientuid')
.distinct()
.count()
)

# COMMAND ----------

(
    df
.filter(F.col('encounterdiagnosiscode').isin(caseB_codes) | starts_with_from_list('encounterdiagnosistext',caseB_text))
.select('patientuid')
.distinct()
.count()
)

# COMMAND ----------

(
    df
.filter(F.col('encounterdiagnosiscode').isin(caseB_codes) | starts_with_from_list('encounterdiagnosistext',caseB_text))
    .filter(F.col('encounterdate').cast('date')<=F.to_date(F.lit("2021-03-27")))
.select('patientuid')
.distinct()
.count()
)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC ## Evaluate and Critique
# MAGIC *Does the model solve my problem?*

# COMMAND ----------

# MAGIC %md
# MAGIC ## Present Results and Document

# COMMAND ----------


