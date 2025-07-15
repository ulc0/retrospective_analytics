# Databricks notebook source
# MAGIC %md 
# MAGIC # Misa Outcome Notebook for the premier dataset link

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Requirements
# MAGIC - Primary or secondary covid-19 ICD-10 code
# MAGIC   - Feb 1, 2020 - April 30, 2021
# MAGIC   - First covid 19 inpatient visit = target
# MAGIC   - Admission 
# MAGIC     - start: Feb 1, 2020 - April 1, 2020
# MAGIC     - end: March 1, 2020 - April 1, 2020
# MAGIC       - icd-10 B97.2 "Coronavirus as the cause of diseases classified to other chapters"
# MAGIC     - start: April 1, 2020 
# MAGIC     - end: April 30, 2021
# MAGIC       - U07.1 "COVID-19, virus identified"
# MAGIC
# MAGIC - Outcomes:
# MAGIC   - ICU admission
# MAGIC     - billing codes for "Room & Board ICU" or "Room & Board Stepdown"
# MAGIC   - Death 
# MAGIC     - death from any cause ("expiration") check notebook 02 for the tracking term
# MAGIC   - MISA
# MAGIC     - Meets criteria 1,2, and 3
# MAGIC       1. Hospitalization (inpat) and > 18y
# MAGIC         - adm: Feb-Apr 2020 
# MAGIC         - disch: March-Apr 2020
# MAGIC           - Primary of Secondary ICD-10 code for B97.29 
# MAGIC         - disch: after April 2020 
# MAGIC           - primary or secondary ICD-10 cod U07.1 "COVID-19 identified"
# MAGIC       2. Marker of inflamation
# MAGIC         - IL-6 > 46.5 pg/ml
# MAGIC         - C-reactive protein >= 30mg/L
# MAGIC         - Ferritin >= 1500 ng/ml
# MAGIC         - Erythrocyte sedimentation rate >= 45 mm/hour
# MAGIC         - Procalcitonin >= 0.3 ng/ml
# MAGIC       3. greater than or equal to 2 categoris of clinical complications
# MAGIC           - Cardiac, defined by at least one of the following ICD-10 codes
# MAGIC             1. I30: Acute pericarditis
# MAGIC             2. I40: Infective myocarditis
# MAGIC             3. I25.41: Coronary artery aneurysm
# MAGIC           - Shock or hypotension, defined by at least one of the following
# MAGIC             1. Systolic blood pressure <90 mmHg on at least 2 days
# MAGIC             2. At least one of the following ICD-10 codes:
# MAGIC                 1. R57, Shock, not elsewhere classified (includes all subcodes)
# MAGIC                 2. R65.21, Severe sepsis with septic shock
# MAGIC                 3. I95, Hypotension shock (includes all subcodes)
# MAGIC             3. Vasopressor use on at least 2 days
# MAGIC           - Gastrointestinal, defined by at least one of the following ICD-10 codes
# MAGIC             1. R10.0, Acute abdomen
# MAGIC             2. R19.7, Diarrhea, unspecified
# MAGIC             3. A09, Infectious gastroenteritis and colitis, unspecified
# MAGIC             4. A08.39, Other viral enteritis
# MAGIC             5. A08.4, Viral intestinal infection, unspecified
# MAGIC             6. R11.2, Nausea with vomiting, unspecified
# MAGIC             7. R11.10, Vomiting, unspecified
# MAGIC           - Thrombocytopenia or elevated D-dimer
# MAGIC             1. Thrombocytopenia, defined by one of the following ICD-10 codes
# MAGIC                 1. D69.6, Thrombocytopenia, unspecified
# MAGIC                 2. D69.59, Other secondary thrombocytopenia
# MAGIC             2. D-dimer ≥ 1200 FEU
# MAGIC     

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set-up

# COMMAND ----------

# MAGIC %md
# MAGIC spark.conf.set("fs.azure.account.auth.type.davsynapseanalyticsdev.dfs.core.windows.net", "OAuth")
# MAGIC spark.conf.set("fs.azure.account.oauth.provider.type.davsynapseanalyticsdev.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.id.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-client-id"))
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-client-secret"))
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-tenant-id-endpoint"))

# COMMAND ----------

dbschema="twj8_premier"
dbschema="edav_prd_cdh.cdh_premier_exploratory"

#icd_lookup = spark.table("edav_prd_cdh.cdh_premier_exploratory.icdcode")
icd_enc = spark.table("edav_prd_cdh.cdh_premier_exploratory.paticd_diag")
#proc_enc = spark.table("edav_prd_cdh.cdh_premier_exploratory.paticd_proc")
hosp_chg = spark.table("edav_prd_cdh.cdh_premier_exploratory.hospchg") 
patbill = spark.table("edav_prd_cdh.cdh_premier_exploratory.patbill")
vitals = spark.table("edav_prd_cdh.cdh_premier_exploratory.vitals")
genlab = spark.table("edav_prd_cdh.cdh_premier_exploratory.genlab")
labres = spark.table("edav_prd_cdh.cdh_premier_exploratory.lab_res")
chgmstr = spark.table("edav_prd_cdh.cdh_premier_exploratory.chgmstr")


# demo = spark.table("edav_prd_cdh.cdh_premier_exploratory.patdemo")
# readmit = spark.table("edav_prd_cdh.cdh_premier_exploratory.readmit")

# Use the target encounters to prefilter
#target_enc = spark.table(f"{dbschema}.01_target_enc")

# COMMAND ----------

# Libs
import os
import numpy as np
import pandas as pd

import pyspark.sql.functions as F
from pyspark.sql.window import Window as W
import pyspark.sql.types as T
from pyspark.sql.functions import PandasUDFType

# COMMAND ----------

# MAGIC %md 
# MAGIC # Prefilter on target encounters

# COMMAND ----------

# MAGIC %md
# MAGIC ### Utilities

# COMMAND ----------

#NEW

# Remove noise
# - Remove null lab_test_results
# - Remove null result_unit
# - Remove null numeric_value
def remove_nulls(sdf, 
                 unit_var = "result_unit", 
                 result_var = "lab_test_result", 
                 value_var = "numeric_value"):
    out = (sdf
           .filter(F.col(unit_var).isNotNull() & 
                   F.col(result_var).isNotNull() & 
                   F.col(value_var).isNotNull())
          )
    return out



def keep_codes(sdf, 
               code_var = "lab_test_loinc_code", 
               codes = []):
    out = (sdf
           .filter(F.col(code_var).isin(*codes))
          )
    return(out)

# Convert result units and numeric_value
# keep units to mg/l
def convert_units(sdf, 
                  value_var = "numeric_value", 
                  unit_var = "result_unit", 
                  f=None):
    if f is None:
        f = F.col(value_var).alias("value")
    out = (sdf
           .withColumn(unit_var, F.lower(F.col(unit_var)))
           .select("*",
                   f
                  )
          )
    return out

def rename_units(sdf, 
                 unit_var = "result_unit", 
                 rename=[], 
                 new=""):
    out = (sdf
           .withColumn("unit", F.when(F.col(unit_var).isin(*rename), new).otherwise(unit_var))
          )
    return out

def drop_units(sdf, 
               unit_var = "result_unit", 
               units = []):
    out = (sdf
           .filter(~F.col(unit_var).isin(*units))
          )
    return(out)

def drop_bad_results(sdf, 
                     test_result_var="lab_test_result"):
    out = (sdf
           .filter(~F.col(test_result_var).rlike("[A-Za-z]"))
          )
    return out

def test_perc(sdf, 
              code_var = "lab_test_loinc_code", 
              group = ["result_unit"], 
              value = "numeric_value"):
    p = [0.0,0.25,0.5,0.75,0.9,0.95,1.0]
    groups = [F.col(g) for g in group]
    out = (sdf
           .select(code_var,
                   *groups,
                   F.percentile_approx(F.col(value), p).over(W.partitionBy(*groups)).alias("perc"),
                   F.count(F.col(value)).over(W.partitionBy(*groups)).alias("n")
                  )
           .distinct()
          )
    out.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Inflamation

# COMMAND ----------

# MAGIC %md
# MAGIC ### C Reactive Protein
# MAGIC C-reactive protein >= 30mg/L
# MAGIC codes: 1988-5

# COMMAND ----------

# MAGIC %md
# MAGIC labs = (genlab.filter(
# MAGIC     (F.lower(F.col("lab_test_loinc_desc")).contains("reactive")))
# MAGIC        )
# MAGIC # labs.cache()
# MAGIC labs.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC --create temp view creative_codes as
# MAGIC select distinct 
# MAGIC --result_unit,
# MAGIC LAB_TEST_LOINC_CODE,LAB_TEST_LOINC_DESC
# MAGIC from edav_prd_cdh.cdh_premier_exploratory.genlab
# MAGIC where lower(LAB_TEST_LOINC_DESC) like  "%reactive%"
# MAGIC order by LAB_TEST_LOINC_CODE,LAB_TEST_LOINC_DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC select result_unit,LAB_TEST_LOINC_DESC, count(*) as numlabs
# MAGIC from edav_prd_cdh.cdh_premier_exploratory.genlab
# MAGIC where LAB_TEST_LOINC_CODE='1988-5'
# MAGIC group by RESULT_UNIT,LAB_TEST_LOINC_DESC
# MAGIC order by numlabs desc

# COMMAND ----------

# MAGIC %sql
# MAGIC --select LAB_TEST_LOINC_CODE,LAB_TEST_LOINC_DESC,LAB_TEST_RESULT,FLAGS,REFERENCE_INTERVAL,NUMERIC_VALUE,NUMERIC_VALUE_OPERATOR
# MAGIC --select LAB_TEST_LOINC_CODE,LAB_TEST_LOINC_DESC,NUMERIC_VALUE,LAB_TEST_RESULT,REFERENCE_INTERVAL --,LAB_TEST_RESULT_Status
# MAGIC select distinct *
# MAGIC from edav_prd_cdh.cdh_premier_exploratory.genlab
# MAGIC where LAB_TEST_LOINC_CODE='1988-5' --and RESULT_UNIT is null and REFERENCE_INTERVAL IS NULL
# MAGIC --group by RESULT_UNIT,LAB_TEST_LOINC_DESC
# MAGIC --order by numlabs desc

# COMMAND ----------

# MAGIC %sql
# MAGIC --select LAB_TEST_LOINC_CODE,LAB_TEST_LOINC_DESC,LAB_TEST_RESULT,FLAGS,REFERENCE_INTERVAL,NUMERIC_VALUE,NUMERIC_VALUE_OPERATOR
# MAGIC --select LAB_TEST_LOINC_CODE,LAB_TEST_LOINC_DESC,NUMERIC_VALUE,LAB_TEST_RESULT,REFERENCE_INTERVAL --,LAB_TEST_RESULT_Status
# MAGIC select distinct LAB_TEST_RESULT,RESULT_UNIT,NUMERIC_VALUE,NUMERIC_VALUE_OPERATOR,FLAGS, REFERENCE_INTERVAL
# MAGIC from edav_prd_cdh.cdh_premier_exploratory.genlab
# MAGIC where LAB_TEST_LOINC_CODE='1988-5' --and RESULT_UNIT is null and REFERENCE_INTERVAL IS NULL
# MAGIC --group by RESULT_UNIT,LAB_TEST_LOINC_DESC
# MAGIC --order by numlabs desc

# COMMAND ----------

# MAGIC %sql
# MAGIC --select LAB_TEST_LOINC_CODE,LAB_TEST_LOINC_DESC,LAB_TEST_RESULT,FLAGS,REFERENCE_INTERVAL,NUMERIC_VALUE,NUMERIC_VALUE_OPERATOR
# MAGIC select * --LAB_TEST_LOINC_CODE,LAB_TEST_LOINC_DESC,NUMERIC_VALUE,LAB_TEST_RESULT
# MAGIC from edav_prd_cdh.cdh_premier_exploratory.genlab
# MAGIC where LAB_TEST_LOINC_CODE='1988-5' and LAB_TEST_RESULT <>NUMERIC_VALUE
# MAGIC --group by RESULT_UNIT,LAB_TEST_LOINC_DESC
# MAGIC --order by numlabs desc

# COMMAND ----------

lab_1 = remove_nulls(labs)
codes = ["1988-5"]
lab_2 = keep_codes(lab_1, codes=codes)

lab_3 = drop_units(lab_2, units = ["mg/Lmilligram per literUCUM", "ng/ml", "ng/mL", "******"])

# coversion function
fc = (F.when((F.col("result_unit") == "mg/dl"), 
             F.round(F.col("numeric_value") * 10, 2))
      .otherwise(F.col("numeric_value"))
      .alias("value"))
lab_4 = convert_units(lab_3, f = fc)

lab_5 = rename_units(lab_4, rename = ["mg/dl","mg/liter", "mg/l feu"], new="mg/l")
lab_6 = drop_bad_results(lab_5)

# print("starting n:", labs.count())
# print("removed nulls:", lab_1.count())
# print(f"keep only codes {codes}: {lab_2.count()}")
# print("dropped unmanageble units", lab_3.count())
# print("dropped bad test results (non-numeric)", lab_6.count())

# test_perc(lab_2, "result_unit", "numeric_value")
# test_perc(lab_3, "result_unit", "numeric_value")
# test_perc(lab_6, "result_unit", "numeric_value")
# test_perc(lab_6, "result_unit", "value")
# test_perc(lab_6, "unit", "numeric_value")
# test_perc(lab_6, "unit", "value")


# COMMAND ----------

# identify those with value >= 30 (use >= because the default cutoff for some tests is >30 and valued at 30)
def get_crp_30(sdf):
    out = (sdf
           .withColumn("crp_ind", F.when((F.col("value") > 30), 1).otherwise(0))
           .filter(F.col("crp_ind") == 1)
           .select("pat_key", "crp_ind", "value", "unit")
          )
    return out

crp = get_crp_30(lab_6)
# crp.count()
# this will be joined back into the encounters list

# COMMAND ----------

# labs.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Erythrocyte Sedimentation Rate
# MAGIC  Erythrocyte sedimentation rate >= 45 mm/hour
# MAGIC  
# MAGIC  codes: 30341-2, 4537-7

# COMMAND ----------

# Ferriting
labs = (genlab.filter(
    (F.lower(F.col("oinc_desc"))
     .contains("sedimentation rate")) 
    ))
# labs.cache()
# labs.count()

# COMMAND ----------


lab_1 = remove_nulls(labs)

codes = ["30341-2", "4537-7"]
lab_2 = keep_codes(lab_1, codes = codes)
lab_3 = drop_units(lab_2, units = ["%", "ZZ", "mm/hmillimeter per hourUCUM"])
lab_4 = convert_units(lab_3)
lab_5 = rename_units(lab_4, rename = ["mm/h", "mm/1hr", "mm"], new="mm/hr")
lab_6 = drop_bad_results(lab_5)


# print("starting n:", labs.count())
# print("removed nulls:", lab_1.count())
# print(f"keep only codes {codes}: {lab_2.count()}")
# print("dropped unmanageble units", lab_3.count())
# print("dropped bad test results (non-numeric)", lab_6.count())

# test_perc(lab_2, "result_unit", "numeric_value")
# test_perc(lab_3, "result_unit", "numeric_value")
# test_perc(lab_6, "result_unit", "numeric_value")
# test_perc(lab_6, "result_unit", "value")
# test_perc(lab_6, "unit", "numeric_value")
# test_perc(lab_6, "unit", "value")


# COMMAND ----------

# get esr indicator
def get_esr_45_ind(sdf):
    out = (sdf
           .withColumn("esr_ind", F.when((F.col("value") >= 45), 1).otherwise(0))
           .filter(F.col("esr_ind") == 1)
           .select("pat_key", "esr_ind", "value", "unit")
          )
    return out
    
esr = get_esr_45_ind(lab_5)
# esr.count()
# esr.select("*", F.percentile_approx(F.col("value"),[0.0,0.25,0.5,0.75,1]).over(W.partitionBy("unit")).alias("perc")).drop("pat_key", "value").distinct().display()

# COMMAND ----------

# labs.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ferritin
# MAGIC
# MAGIC - value > 1500 ng/ml
# MAGIC - codes: 2276-4

# COMMAND ----------

labs = (genlab.filter(
    (F.lower(F.col("lab_test_loinc_desc")).contains("ferritin")) 
    ))
# labs.cache()
# print(labs.count())

# COMMAND ----------

lab_1 = remove_nulls(labs)
codes = ["2276-4"]
lab_2 = keep_codes(lab_1, codes = codes)
lab_3 = drop_units(lab_2, units = ["ng/mLnanogram per milliliterUCUM", "UG/L"])
lab_4 = convert_units(lab_3)
lab_5 = rename_units(lab_4, rename = ["mcg/l","mg/ml","ng/dl", "ng/ml", "ug/l"], new="ng/ml")
lab_6 = drop_bad_results(lab_5)

# print("starting n:", labs.count())
# print("removed nulls:", lab_1.count())
# print(f"keep only codes {codes}: {lab_2.count()}")
# print("dropped unmanageble units", lab_3.count())
# print("dropped bad test results (non-numeric)", lab_6.count())

# test_perc(lab_2, "result_unit", "numeric_value")
# test_perc(lab_3, "result_unit", "numeric_value")
# test_perc(lab_6, "result_unit", "numeric_value")
# test_perc(lab_6, "result_unit", "value")
# test_perc(lab_6, "unit", "numeric_value")
# test_perc(lab_6, "unit", "value")

# COMMAND ----------

# get ferriting
def get_fer_1500_ind(sdf):
    out = (sdf
           .withColumn("fer_ind", F.when((F.col("value") >= 1500), 1).otherwise(0))
           .filter(F.col("fer_ind") == 1)
           .select("pat_key", "fer_ind", "value", "unit")
          )
    return out

fer = get_fer_1500_ind(lab_6)
# fer.count()

# COMMAND ----------

# labs.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Procalcitonin
# MAGIC procalcitonin > 0.3 ng/ml
# MAGIC
# MAGIC code: 33959-8

# COMMAND ----------

labs = (genlab.filter(
    (F.lower(F.col("lab_test_loinc_desc")).contains("procalcitonin")) 
    ))
# labs.cache()
# print(labs.count())

# COMMAND ----------

lab_1 = remove_nulls(labs)
codes = ["33959-8"]
lab_2 = keep_codes(lab_1, codes = codes)
lab_3 = drop_units(lab_2, units = ["ng/mLnanogram per milliliterUCUM", "UG/L", "ng/dL", "mg/mL", "ZZ"])
lab_4 = convert_units(lab_3)
lab_5 = rename_units(lab_4, rename = ["mcg/l","mg/ml","ng/dl", "ng/ml", "ug/l"], new="ng/ml")
lab_6 = drop_bad_results(lab_5)

# print("starting n:", labs.count())
# print("removed nulls:", lab_1.count())
# print(f"keep only codes {codes}: {lab_2.count()}")
# print("dropped unmanageble units", lab_3.count())
# print("dropped bad test results (non-numeric)", lab_6.count())

# test_perc(lab_2, "result_unit", "numeric_value")
# test_perc(lab_3, "result_unit", "numeric_value")
# test_perc(lab_6, "result_unit", "numeric_value")
# test_perc(lab_6, "result_unit", "value")
# test_perc(lab_6, "unit", "numeric_value")
# test_perc(lab_6, "unit", "value")

# COMMAND ----------

# get procalcitonin
def get_procal_30_ind(sdf):
    out = (sdf
           .withColumn("procal_ind", F.when((F.col("value") >= 0.3), 1).otherwise(0))
           .filter(F.col("procal_ind") == 1)
           .select("pat_key", "procal_ind", "value", "unit")
          )
    return out

procal = get_procal_30_ind(lab_6)
# procal.count()

# COMMAND ----------

# labs.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### IL-6
# MAGIC
# MAGIC IL-6 > 46.5 pg/ml
# MAGIC
# MAGIC codes: 26881-3, 4655-7

# COMMAND ----------

labs = (genlab.filter(
    (F.lower(F.col("lab_test_loinc_desc")).contains("interleukin 6")) 
    ))
# labs.cache()
# print(labs.count())
# labs.groupBy("lab_test_loinc_desc","lab_test_loinc_code").count().display()

# COMMAND ----------

lab_1 = remove_nulls(labs)
codes = ["26881-3", "4655-7"]
lab_2 = keep_codes(lab_1, codes = codes)
lab_3 = drop_units(lab_2, units = ["pg/mLpicogram per milliliterUCUM"])
lab_4 = convert_units(lab_3)
lab_5 = rename_units(lab_4, rename = ["pg/ml"], new="pg/ml")
lab_6 = drop_bad_results(lab_5)

# print("starting n:", labs.count())
# print("removed nulls:", lab_1.count())
# print(f"keep only codes {codes}: {lab_2.count()}")
# print("dropped unmanageble units", lab_3.count())
# print("dropped bad test results (non-numeric)", lab_6.count())

# test_perc(lab_2, "result_unit", "numeric_value")
# test_perc(lab_3, "result_unit", "numeric_value")
# test_perc(lab_6, "result_unit", "numeric_value")
# test_perc(lab_6, "result_unit", "value")
# test_perc(lab_6, "unit", "numeric_value")
# test_perc(lab_6, "unit", "value")

# COMMAND ----------

# get IL-6
def get_il6_46_ind(sdf):
    out = (sdf
           .withColumn("il6_ind", F.when((F.col("value") >= 46.5), 1).otherwise(0))
           .filter(F.col("il6_ind") == 1)
           .select("pat_key", "il6_ind", "value", "unit")
          )
    return out

il6 = get_il6_46_ind(lab_6)
# il6.count()

# COMMAND ----------

# labs.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Combine Inflammation
# MAGIC
# MAGIC atleast 1

# COMMAND ----------

# crp.display()
# esr.display()
# fer.display()
# procal.display()
# il6.display()

inflammation = (crp.select("pat_key")
                .union(esr.select("pat_key"))
                .union(fer.select("pat_key"))
                .union(procal.select("pat_key"))
                .union(il6.select("pat_key"))
                .distinct()
                .withColumn("infl_ind", F.lit(1))
               )

# inflammation.count()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Clinical Complications

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cardiac
# MAGIC 1. I30: Acute pericarditis
# MAGIC 2. I40: Infective myocarditis
# MAGIC 3. I25.41: Coronary artery aneurysm

# COMMAND ----------

# identify cc_cardiac
def get_cc_cardiac_ind(sdf):
    out = (sdf
           .select("*", 
                   F.when(F.lower(F.col("icd_code")).contains("i30"), 1)
                   .when(F.lower(F.col("icd_code")).contains("i40"), 1)
                   .when(F.lower(F.col("icd_code")).contains("i25.41"), 1)
                   .otherwise(0)
                   .alias("cardiac_ind")
                  )
           .filter(F.col("cardiac_ind") == 1)
           .select("pat_key", "cardiac_ind")
          )
    return out

cardiac = get_cc_cardiac_ind(icd_enc)

# print(icd_enc.count())
print(cardiac.count())
#cardiac.groupBy("icd_code").count().display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Shock Hypotension
# MAGIC 1. Systolic blood pressure <90 mmHg on at least 2 days
# MAGIC 2. At least one of the following ICD-10 codes:
# MAGIC     1. R57, Shock, not elsewhere classified (includes all subcodes)
# MAGIC     2. R65.21, Severe sepsis with septic shock
# MAGIC     3. I95, Hypotension shock (includes all subcodes)
# MAGIC 3. Vasopressor use on at least 2 days

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Shock ICD 10

# COMMAND ----------

# shock icd
def get_cc_shock_icd_ind(sdf):
    out = (sdf
           .select("*", 
                   F.when(F.lower(F.col("icd_code")).contains("r57"), 1)
                   .when(F.lower(F.col("icd_code")).contains("r65.21"), 1)
                   .when(F.lower(F.col("icd_code")).contains("i95"), 1)
                   .otherwise(0)
                   .alias("shock_icd")
                  )
           .filter(F.col("shock_icd") == 1)
           .select("pat_key", "shock_icd")
           .distinct()
          )
    return out

cc_shock_icd = get_cc_shock_icd_ind(icd_enc)

# cc_shock_icd.display()
# cc_shock_icd.count()

# COMMAND ----------

# MAGIC %md 
# MAGIC #### Systolic BP

# COMMAND ----------

# systolic bp
# adjust lab values
unit_var = "lab_test_result_unit"
result_var = "lab_test_result"
value_var = "test_result_numeric_value"



lab_1 = remove_nulls(vitals, unit_var=unit_var, result_var=result_var, value_var=value_var)
codes = ["8480-6"]
lab_2 = keep_codes(lab_1, codes=codes)
lab_3 = drop_units(lab_2, unit_var = unit_var, units = ["F", "D", "C"])
lab_4 = convert_units(lab_3, value_var=value_var, unit_var=unit_var)
lab_5 = rename_units(lab_4, unit_var=unit_var, rename = ["mmhg", "mm/hg", "mm hg", "mm[hg]"], new="mmhg")
lab_6 = drop_bad_results(lab_5, test_result_var = result_var)



# print("starting n:", vitals.count())
# print("removed nulls:", lab_1.count())
# print(f"keep only codes {codes}: {lab_2.count()}")
# print("dropped unmanageble units", lab_3.count())
# print("dropped bad test results (non-numeric)", lab_6.count())

# test_perc(lab_2, "lab_test_result_unit", "test_result_numeric_value")
# test_perc(lab_3, "result_unit", "numeric_value")
# test_perc(lab_6, "result_unit", "numeric_value")
# test_perc(lab_6, "result_unit", "value")
# test_perc(lab_6, "unit", "numeric_value")
# test_perc(lab_6, "unit", "value")


# COMMAND ----------

# lab_6.groupBy("episode_day_number").count().display()
def get_bp_90_2(sdf):
    out = (sdf
           .withColumn("value", F.col("value").cast("integer"))
           .filter((F.col("value") >= 90))
           # get row numbers for each obs in a single day set
           .withColumn("row", F.row_number()
                       .over(W.partitionBy("pat_key","episode_day_number").orderBy("episode_day_number")))
           # select the first obs for each day set
           .filter(F.col("row") == 1)
           # id encounters with >1 obs from different days
           .select("*",
                   F.when(F.count(F.col("pat_key")).over(W.partitionBy("pat_key")) > 1, 1)
                   .otherwise(0)
                   .alias("bp_90_2"))
           # filter the encounters with > 1 obs from different days
           .filter(F.col("bp_90_2") == 1)
           .select("pat_key", "bp_90_2")
           .distinct()
          )
    return(out)

bp_90_2 = get_bp_90_2(lab_6)
# bp_90_2.count()
# bp_90_2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Vasopressor
# MAGIC
# MAGIC - Vasopressin (Pitressin, Vasostrict)
# MAGIC - Phenylephrine (Biorphen, Vaszculep)
# MAGIC - Epinephrine
# MAGIC - Norepinephrine
# MAGIC - Dopamine
# MAGIC - Angiotensin-II
# MAGIC - Terlipressin

# COMMAND ----------

# vasopressers
# patbill.display()
def get_chg_code(sdf, var, collect = False):
    from pyspark.sql.functions import upper
    out = (sdf
           .filter(F.col("std_dept_code").contains("250"))
           .filter(F.col("std_chg_desc").contains(var))
          )
    if collect:
        out = list(out
               .select("std_chg_code")
               .toPandas()["std_chg_code"]
              )
    return out


mnems=["phenylephrine","biorphen","vazculep","terlipressin","vasopressin","epinephrine","dopamine","angiotensin ii"]
for m in mnems:
  print(m.upper())
  get_chg_code(chgmstr, m.upper()).display()


# these did not return any results
#get_chg_code(chgmstr, "phenylephr").display()
#get_chg_code(chgmstr, "biorphen").display()
#get_chg_code(chgmstr, "vazculep").display()
get_chg_code(chgmstr, "terlipressin").display()

vp = get_chg_code(chgmstr, "vasopressin", collect=True)
vp
epi_nepi = get_chg_code(chgmstr, "epinephrine", collect=True)
dop = get_chg_code(chgmstr, "dopamine", collect=True)
angii = get_chg_code(chgmstr, "angiotensin ii", collect=True)

vasopres = vp + epi_nepi + dop + angii

# COMMAND ----------


mnems=["phenylephr","biorphen","vazculep","terlipressin","vasopressin","epinephrine","dopamine","angiotensin ii"]
mnems=["phenyl","biorphen","vazculep","terlipressin","vasopressin","epinephrine","dopamine","angiotensin ii"]
for m in mnems:
  print(m)
  get_chg_code(chgmstr, m.upper()).display()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from edav_prd_cdh.cdh_premier_exploratory.chgmstr 
# MAGIC where like(lower(prod_name_desc), "dopamine")

# COMMAND ----------

# this is the list of vasopressor codes
vasopres

# COMMAND ----------

# patbill.display()
def get_vp_bill(sdf, codes):
    out = (sdf
           .filter(F.col("std_chg_code").isin(*codes))
          )
    return out

def get_vp_2day(sdf):
    out = (sdf
           .withColumn("vp_2d", 
                       F.when(F.count(F.col("pat_key"))
                                       .over(W.partitionBy(F.col("pat_key"))) > 1, 1)
                       .otherwise(0))
           .filter(F.col("vp_2d") == 1) 
           .select("pat_key", "vp_2d")
           .distinct()
          )
    return out


vp_bil = get_vp_bill(patbill, vasopres)
#vp2d_bill = get_vp_2day(vp_bil)
print(vp_bil)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Combine

# COMMAND ----------

# cc_shock_icd.display()
# bp_90_2.display()
# vp2d_bill.display()

shock = (cc_shock_icd.select("pat_key")
      .union(bp_90_2.select("pat_key"))
      .union(vp2d_bill.select("pat_key"))
      .withColumn("shock_ind", F.lit(1))
      .select("pat_key", "shock_ind")
      .distinct()
     )



# COMMAND ----------

# MAGIC %md
# MAGIC ### GI
# MAGIC 1. R10.0, Acute abdomen
# MAGIC 2. R19.7, Diarrhea, unspecified
# MAGIC 3. A09, Infectious gastroenteritis and colitis, unspecified
# MAGIC 4. A08.39, Other viral enteritis
# MAGIC 5. A08.4, Viral intestinal infection, unspecified
# MAGIC 6. R11.2, Nausea with vomiting, unspecified
# MAGIC 7. R11.10, Vomiting, unspecified

# COMMAND ----------

# GI
def get_gi_icd_ind(sdf):
    out = (sdf
           .select("*", 
                   F.when(F.lower(F.col("icd_code")).contains("r10.0"), 1)
                   .when(F.lower(F.col("icd_code")).contains("r19.7"), 1)
                   # Need this to exclude cod g40.a09
                   .when(F.lower(F.col("icd_code")).contains("g40"), 0)
                   .when(F.lower(F.col("icd_code")).contains("a09"), 1)
                   .when(F.lower(F.col("icd_code")).contains("a08.39"), 1)
                   .when(F.lower(F.col("icd_code")).contains("a08.4"), 1)
                   .when(F.lower(F.col("icd_code")).contains("r11.2"), 1)
                   .when(F.lower(F.col("icd_code")).contains("r11.10"), 1)
                   .otherwise(0)
                   .alias("gi_ind")
                  )
           .filter(F.col("gi_ind") == 1)
           .select("pat_key", "gi_ind")
           .distinct()
          )
    return out

gi = get_gi_icd_ind(icd_enc)
gi.count()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Thrombocytopenia D-dimer
# MAGIC 1. Thrombocytopenia, defined by one of the following ICD-10 codes
# MAGIC     1. D69.6, Thrombocytopenia, unspecified
# MAGIC     2. D69.59, Other secondary thrombocytopenia
# MAGIC 2. D-dimer ≥ 1200 FEU

# COMMAND ----------

# MAGIC %md
# MAGIC #### Thrombocytopenia

# COMMAND ----------

def get_thromb_icd_ind(sdf):
    out = (sdf
           .select("*", 
                   F.when(F.lower(F.col("icd_code")).contains("d69.9"), 1)
                   .when(F.lower(F.col("icd_code")).contains("d69.59"), 1)
                   .otherwise(0)
                   .alias("thrmb_icd")
                  )
           .filter(F.col("thrmb_icd") == 1)
#            .select("pat_key", "icd_code")
           .select("pat_key", "thrmb_icd")
           .distinct()
          )
    return out

thromb_icd = get_thromb_icd_ind(icd_enc)
# thromb_icd.groupBy("icd_code").count().display()
# thromb_icd.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### D-dimer
# MAGIC
# MAGIC D-dimer > 1200 FEU
# MAGIC
# MAGIC codes: 
# MAGIC - 48065-7 
# MAGIC - 71427-9
# MAGIC - 48067-3
# MAGIC - 7799-0
# MAGIC - 30240-6
# MAGIC - 15179-5

# COMMAND ----------

labs = (genlab.filter(
    (F.lower(F.col("lab_test_loinc_desc")).contains("d-dimer")) 
    ))

# labs.cache()
# labs.count()
# labs.groupBy("lab_test_loinc_desc", "lab_test_loinc_code").count().display()

# COMMAND ----------

lab_1 = remove_nulls(labs)
codes = ["48065-7", "71427-9", "48067-3", "7799-0", "30240-6","15179-5"]
lab_2 = keep_codes(lab_1, codes = codes)
units = ["ug/mLmicrogram per milliliterUCUM", "CD:******", "M", "ng/mL (DDU)", "ng/mLDDU", "ng/mLDDU", "******", "ng/mL DDU", "ZZ", "ng/mL D-DU"]
lab_3 = drop_units(lab_2, units=units)

# list of units
cv_unit = [x.lower() for x in ["ug/mL FEU", "mcg FEUnits/mL", "mg/L FEU", "ug/ml FEU", "ug/mL", "mg/L", "mcg/mL FEU", "MG/L FEU", "ug FEU/mL", "FEUug/mL", "mcg/mL", "*mcg/mL", "ug/mLFEU", "ug/mL(FEU)", "UG/ML", "mg/LFEU", "ug/ml", "mg/Liter", "ugFEU/mL", "FEUug/ml", "mg/dL", "ug/ml (feu)"]]
# adjust function
fc = (F.when(F.col("result_unit").
             isin(*cv_unit), 
             1000*F.col("numeric_value"))
      .otherwise(F.col("numeric_value"))
      .alias("value"))
# apply
lab_4 = convert_units(lab_3, f = fc)

# get units (all need to be changed)
chng_units = list(lab_4.select("result_unit").distinct().toPandas()["result_unit"])
# apply
lab_5 = rename_units(lab_4, rename = chng_units, new="ng/ml")
lab_6 = drop_bad_results(lab_5)

# print("starting n:", labs.count())
# print("removed nulls:", lab_1.count())
# print(f"keep only codes {codes}: {lab_2.count()}")
# print("dropped unmanageble units", lab_3.count())
# print("dropped bad test results (non-numeric)", lab_6.count())



# test_perc(lab_1, "lab_test_loinc_code", [F.col("lab_test_loinc_desc"), F.col("result_unit")], "numeric_value")
# test_perc(lab_3, "lab_test_loinc_code", ["result_unit"], "numeric_value")
# test_perc(lab_6, "lab_test_loinc_code", ["unit"], "value")

# test_perc(lab_2, "result_unit", "numeric_value")
# test_perc(lab_3, "result_unit", "numeric_value")
# test_perc(lab_6, "result_unit", "numeric_value")
# test_perc(lab_6, "result_unit", "value")
# test_perc(lab_6, "unit", "numeric_value")
# test_perc(lab_6, "unit", "value")

# COMMAND ----------

def get_dd_1200_ind(sdf):
    out = (sdf
           .withColumn("dd_ind", F.when((F.col("value") >= 1200), 1).otherwise(0))
           .filter(F.col("dd_ind") == 1)
           .select("pat_key", "dd_ind")
          )
    return out

dd_ind = get_dd_1200_ind(lab_6)
# dd_ind.count()

# COMMAND ----------

# labs.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Combine

# COMMAND ----------

thrombocytopenia = (thromb_icd.select("pat_key")
                    .join(dd_ind.select("pat_key"), on="pat_key", how="full")
                    .distinct()
                    .withColumn('throm_ind', F.lit(1))
                   )
# thrombocytopenia.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine Clinical Complicaitons

# COMMAND ----------

print(cardiac.columns)
print(shock.columns)
print(gi.columns)
print(thrombocytopenia.columns)

# COMMAND ----------

clin_comp = (cardiac
             .join(shock, on = "pat_key", how = "full")
             .join(gi, on = "pat_key", how = "full")
             .join(thrombocytopenia, on = "pat_key", how="full")
             .na.fill(0)
             .withColumn("sum_cc", F.col("cardiac_ind")+F.col("shock_ind")+F.col("gi_ind")+F.col("throm_ind"))
             .filter(F.col("sum_cc") >= 2)
            )

# clin_comp.display()
clin_comp.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Get MISA Encounters
# MAGIC
# MAGIC Combine covid encounter, inflamation and clinical complications
# MAGIC Becase the data is preselected on target encounters just need to join inflammation and clin_comp

# COMMAND ----------

print(target_enc.columns)
print(inflammation.columns)
print(clin_comp.columns)

# COMMAND ----------

misa_enc = (target_enc
            .join(inflammation, on="pat_key", how="inner")
            .join(clin_comp, on="pat_key", how = "inner")
            .select("pat_key", "medrec_key")
            .withColumn("misa_ind", F.lit(1))
           )

misa_enc.cache()
misa_enc.count()

# COMMAND ----------

misa_enc.display()

# COMMAND ----------

misa_enc.write.mode("overwrite").format("delta").saveAsTable(f"{dbschema}.01_misa_enc")

# COMMAND ----------

# misa = spark.table("twj8_premier.01_misa_enc")
# misa.count()
