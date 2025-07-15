# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC
# MAGIC 1. Identify all patients 0 - 64 with prior encounters between 3/1/2020 and 11/30/2021. 
# MAGIC     - Patients with missing or unknown gender/sex or US census region information (e.g. state) will be excluded.
# MAGIC
# MAGIC 2. Designate all patients as either COVID ""exposed"" or ""unexposed"" based on whether they had a COVID diagnosis or 
# MAGIC     - positive lab between 3/1/2020 and 11/30/2021. 
# MAGIC     - (B97.29: 3/1/2020 - 4/30/2020; U07.1: 4/1/2020 - 11/30/2021)
# MAGIC
# MAGIC 3. Exclude ""unexposed"" patients that have any indicator of COVID or an exclusion diagnosis from 3/2020 thru 5/2022. *see exclusion codes ------->
# MAGIC
# MAGIC 4. For each patient, identify an ""index date"" between 3/1/2020 and 11/30/2021 (start and end dates inclusive). 
# MAGIC     - For an exposed patient, the index date is the date of the first COVID claim in the time period. 
# MAGIC     - For an unexposed patient, the index date is the date of a randomly selected medical claim that occured during the time period.
# MAGIC
# MAGIC 5. Keep patients who have continuous enrollment 1 year prior to their index date and 6 months after the index date (45 day gap allowed).
# MAGIC
# MAGIC 6. For each patient, find all diagnoses 1 year prior to index date and 6 months after. 
# MAGIC     - Assign each diagnosis code a CCSR, but do not assign a category for codes that appear in the exclusion ICD list.
# MAGIC
# MAGIC 7. For every individual and each CSSR category, calculate time from ""index_date"" + 31 days to CCSR medical claim date based on the scenarios to the left.
# MAGIC
# MAGIC 8 Match every 1 indivudal in the exposed group with 2 unique individuals in the unexposed group based on age, sex, inpatient status, and month&year of index date.
# MAGIC     - For unexposed, inpatient status indicates whether the individual had an inpatient claim during Period 2. 
# MAGIC     - For exposed, inpatient status indicates whether the individual had an inpatient claim AND COVID diagnosis on the same medical claim during Period 2.
# MAGIC
# MAGIC
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			
# MAGIC 			

# COMMAND ----------

# libraries
import pyspark.sql.functions as F
import pyspark.sql.window as W
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import scipy.stats as ana

from pyspark.sql.functions import col, concat, expr, lit

# COMMAND ----------

icd_desc=spark.read.table("cdh_premier.icdcode")
icd_code=spark.read.table("cdh_premier.paticd_diag")

patcpt=spark.read.table("cdh_premier.patcpt")
cptcode = spark.table("cdh_premier.cptcode")

patlabres=spark.read.table("cdh_premier.lab_res")
genlab=spark.read.table("cdh_premier.genlab")

disstat=spark.read.table("cdh_premier.disstat")
pattype=spark.read.table("cdh_premier.pattype")

patbill=spark.read.table("cdh_premier.patbill")
chgmstr=spark.read.table("cdh_premier.chgmstr")

readmit=spark.read.table("cdh_premier.readmit")
patdemo=spark.read.table("cdh_premier.patdemo")

zip3=spark.read.table("cdh_premier.phd_providers_zip3")
providers=spark.read.table("cdh_premier.providers")
prov=providers.join(zip3,"prov_id","inner")

ccsr_lookup = spark.table("cdh_reference_data.icd10cm_diagnosis_codes_to_ccsr_categories_map")

# COMMAND ----------

icd_code=(icd_code.withColumnRenamed('icd_code', 'icd_code_original')
    .select("pat_key", 
        "ICD_PRI_SEC", 
        "year", 
        "quarter", 
        "icd_code_original", 
        F.translate(F.col("icd_code_original"), ".", "").alias("icd_code")
    )
)

patbill = (
    patbill
    .join(enc_date, on ="pat_key", how="left")
    .join(chgmstr.select("std_chg_code", "std_chg_desc")
    , on = "std_chg_code", how = "left")
)

patcpt = (
    patcpt
    .join(enc_date, on = "pat_key", how="left")
    .join(
        cptcode,
        on = "cpt_code",
        how = "left"
    )
)

pathospchg = (
    patbill
    .join(enc_date, on="pat_key", how="left")
    .join(hospchg, on="hosp_chg_id", how="left")
)

# COMMAND ----------

# select encounters 2019103 - 2022103

def get_enc_range(pat):
    pat = (
        pat
        .filter(
            F.col("")
        )
    )
