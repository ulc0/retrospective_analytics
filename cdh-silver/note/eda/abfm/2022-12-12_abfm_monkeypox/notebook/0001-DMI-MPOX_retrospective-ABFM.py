# Databricks notebook source
# MAGIC %md
# MAGIC # DMI m-pox project HV

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12/12/2022

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Define Goal
# MAGIC *What problem do I need to solve? How do I intially plan to solve it?*
# MAGIC

# COMMAND ----------

# MAGIC %md Collect retrospective information on m-pox cases

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Contact Information
# MAGIC *Who to contact for questions about this notebook?*
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC - Author: Oscar Rincon 
# MAGIC - Author: Mukesh Hamal
# MAGIC - Email: [run9@cdc.gov](run9@cdc.gov) <!--- enter your email address in both the square brackets and the parens -->
# MAGIC - Email: [rvi5@cdc.gov](rvi5@cdc.gov) <!--- enter your email address in both the square brackets and the parens -->

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Configuration

# COMMAND ----------

# MAGIC %pip install black==22.3.0 tokenize-rt==4.2.1

# COMMAND ----------

pip install wheel setuptools

# COMMAND ----------

spark.version

# COMMAND ----------

!pip install transformers

# COMMAND ----------

# MAGIC %pip install torch 

# COMMAND ----------

# MAGIC %pip install torchvision

# COMMAND ----------

# MAGIC %pip install transformers datasets

# COMMAND ----------

# Maximize Pandas output text width.
# import pandas as pd
# pd.set_option('display.max_colwidth', 200)

#ready for advanced plotting
# import matplotlib.pyplot as plt
# import numpy as np
import pyspark.sql.functions as F
import pyspark.sql.window as W

#import specific functions from pyspark sql types
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
spark.sql("SHOW DATABASES").show(truncate=False)
from pyspark.sql.window import Window
from datetime import date
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import isnan, when, count, col, desc, asc, row_number, concat_ws
import pyspark.sql.functions as F
spark.conf.set('spark.sql.shuffle.partitions',7200*4)

#from pyspark.sql import 

# spark configuration
spark.conf.set('spark.sql.shuffle.partitions', 1600)
spark.conf.set('spark.databricks.adaptive.autoOptimizeShuffle.enabled', True)
spark.conf.set('spark.databricks.queryWatchdog.enabled', True)

# COMMAND ----------

# MAGIC %run ../includes/0000-utils-high

# COMMAND ----------

from transformers import AutoTokenizer, AutoModel
tokenizer = AutoTokenizer.from_pretrained("publichealthsurveillance/PHS-BERT")
model = AutoModel.from_pretrained("publichealthsurveillance/PHS-BERT")


# COMMAND ----------

type(model)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Collect Data
# MAGIC *What data do I need? From where?*

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Data

# COMMAND ----------

# Select the data you need to work with

patient_df = load_patient_table()
immun_df = load_patientimmunization_table()
lab_df = load_patientlaborder_table()
language_df = load_patientlanguage_table()
problem_df = load_patientproblem_table()
procedure_df = load_patientprocedure_table()
raceeth_df = load_raceeth_table()
visit_df = load_visit_table()
visitdiag_df = load_visitdiagnosis_table()

# COMMAND ----------

# Visually inspect that the data loaded correctly

print('patient')
display(patient_df.limit(5))

print('patientimmunization')
display(immun_df.limit(5))

print('patientlaborder')
display(lab_df.limit(5))

# print('patientlanguage')
# display(language_df.limit(5))

print('patientproblem')
display(problem_df.limit(5))

print('patientprocedure')
display(procedure_df.limit(5))

# print('patientgeneratedraceethnicity')
# display(raceeth_df.limit(5))

print('visit')
display(visit_df.limit(5))

print('visitdiagnosis')
display(visitdiag_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean Data

# COMMAND ----------

mkpox_pox = [ # for visit and vist diagnosis table
    "87593",
    "90611 ",
    "B04",
    "B0809",
    "05900",
    "0590",
    "05909",
    "05901",
    "359811007",
    "359814004",
    "59774002",
]

mkpox_loinc = ["1003839", "1004340"] # for lab order table

# COMMAND ----------


ICDlabels = spark.read.table("edav_prd_cdh.cdh_hv_mother_baby_covid_exploratory.ICD9_ICD10_diagnosisCodes").select("diagnosis","description")
HCPC_codes = spark.read.table("edav_prd_cdh.cdh_hv_mother_baby_covid_exploratory.HCPC_codes_run9").withColumnRenamed("long_description","description").withColumnRenamed("HCPC_code","diagnosis")
CPT_codes = spark.read.table("edav_prd_cdh.cdh_hv_mother_baby_covid_exploratory.CPT_codes_run9").withColumnRenamed("code","diagnosis")

diag_codes_desc = ICDlabels.unionByName(HCPC_codes).unionByName(CPT_codes)
diag_codes_desc.count()

# COMMAND ----------

matched_visit_df = visit_df.join(diag_codes_desc, visit_df.encountertypecode == diag_codes_desc.diagnosis).drop("diagnosis").where(F.col("encountertypecode").isin(mkpox_pox))
display(matched_visit_df)
matched_visit_df.count()




# COMMAND ----------

matched_visit_diag_df = visitdiag_df.join(diag_codes_desc, visitdiag_df.encounterdiagnosiscode == diag_codes_desc.diagnosis).drop("diagnosis").where(F.col("encounterdiagnosiscode").isin(mkpox_pox))
display(matched_visit_diag_df)
matched_visit_diag_df.count()


# COMMAND ----------

ind_visit_diag_df = (
    matched_visit_diag_df
    .sort("patientuid","encounterdate")
    .groupBy("patientuid")
    .agg(
        F.collect_list("encounterdate").alias("dx_dates"),
        F.min("encounterdate").alias("index_diagnosis")
    ).drop("dx_dates")
)

display(ind_visit_diag_df)

# COMMAND ----------

# MAGIC %md ## Lab order table

# COMMAND ----------

lab_string_df = lab_df.where("description like '%monkey%' or description like 'mpox'")

display(lab_string_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final Prepared Data

# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Model
# MAGIC *What analyses could solve my problem?*

# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC ## Evaluate
# MAGIC *Did I solve my problem? If not, why? (e.g. found a new problem that I'm stuck on, invalid assumptions, etc.)*

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ## Results
# MAGIC *What do my stakeholders need to know? How can I visually represent that info?*

# COMMAND ----------


