# Databricks notebook source
# MAGIC %md
# MAGIC https://learn.microsoft.com/en-us/azure/databricks/workflows/jobs/parameter-value-references

# COMMAND ----------

# MAGIC %pip install databricks-feature-engineering
# MAGIC
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

#job/run level parameters
dbutils.widgets.text("DBCATALOG",defaultValue="edav_prd_cdh")
dbutils.widgets.text("SRC_SCHEMA",defaultValue="cdh_abfm_phi")
dbutils.widgets.text("DEST_SCHEMA",defaultValue="cdh_abfm_phi_ra")
dbutils.widgets.text("EXPERIMENT_ID",defaultValue="2170087916424204")


# COMMAND ----------

#task level parameters
#prefix for dictionary, name for file
dbutils.widgets.text("datagroup",defaultValue="abfm_phi")
#key to the dictionary
##dbutils.widgets.text("conceptset",defaultValue="medication")
#dbutils.widgets.text("taskname",defaultValue="this_conceptset")

# COMMAND ----------

DBCATALOG=dbutils.widgets.get("DBCATALOG")
SCHEMA=dbutils.widgets.get("SRC_SCHEMA")
DEST_SCHEMA=dbutils.widgets.get("DEST_SCHEMA")

# COMMAND ----------

DATAGROUP=dbutils.widgets.get("datagroup")
EXPERIMENT_ID=dbutils.widgets.get("EXPERIMENT_ID")

# COMMAND ----------

from shared import etl_utils as etl_utils

"""
This notebook contains high-level functions for loading cleaned ABFM datasets.
Contact sve5@cdc.gov for questions.
"""

# COMMAND ----------
import pyspark.sql.functions as F
import pyspark.sql.window as W
#import shared as etl_utils
#define custom filters and windows
datedeceased = (
F.when(F.col('isdeceased')==True, F.col('modifieddate'))
)

patient_win = W.Window.partitionBy('patientuid')
patient_win_date = patient_win.orderBy('modifieddate')

#load data
df = spark.table("cdh_abfm_phi.patient")

#transform data
df = (
    df
    .drop('countrycode','country','maritalstatustext','serviceprovidernpi','religiousaffiliationtext','state')
    .transform(etl_utils.clean_date('dob'))
    .transform(etl_utils.clean_date('modifieddate'))
    .transform(etl_utils.clean_date('createddate'))
    .transform(etl_utils.clean_gender())
    .transform(etl_utils.clean_zipcode())
    .transform(etl_utils.clean_state('statecode'))
    .transform(etl_utils.create_age('dob','modifieddate'))
    .withColumn('city', F.lower('city'))
    .withColumn('datedeceased', datedeceased)
    .withColumn('entries', F.row_number().over(patient_win_date))
    .withColumn('max_entries', F.count('patientuid').over(patient_win))
    .filter(F.col('entries')==F.col('max_entries'))
    .drop('max_entries')
)
display(df)
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable(f"{DBCATALOG}.{DEST_SCHEMA}.demographics")
