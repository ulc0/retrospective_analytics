# Databricks notebook source
# MAGIC %md
# MAGIC # Set-up

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Libraries

# COMMAND ----------

import os
import numpy as np
import pandas as pd

import pyspark.sql.functions as F
from pyspark.sql.window import Window as W
import pyspark.sql.types as T
from pyspark.sql.functions import PandasUDFType

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.davsynapseanalyticsdev.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.davsynapseanalyticsdev.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-client-id"))
spark.conf.set("fs.azure.account.oauth2.client.secret.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-client-secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-tenant-id-endpoint"))

# COMMAND ----------

# get the cohort
target_enc = spark.table("twj8_premier.01_target_enc")

# COMMAND ----------

# get just the medrec_keys
coh_mk = target_enc.select("medrec_key", "days_from_index").withColumn("days_from_index", F.col("days_from_index").cast("integer"))

# COMMAND ----------

# verify that there are distinct number of patients:days_from_index combos
print(coh_mk.distinct().count())
print(coh_mk.select("medrec_key").distinct().count())

# COMMAND ----------

def join_readmit(sdf, readmit):
    out = (sdf
           .join(readmit, on = "pat_key", how = "left")
          )
    return out

# COMMAND ----------

readmit = (spark.table("cdh_premier.readmit")
           .select("medrec_key", "pat_key", "days_from_index", "calc_los")
           .withColumn("dfi", F.col("days_from_index").cast("integer"))
           .withColumn("los", F.col("calc_los").cast("integer"))
           .drop("days_from_index"))

# subset cols
genlab_cols = [
            'pat_key', 'concept_set_day_number', 'concept_set_time_of_day',
            'lab_test_loinc_desc', 'lab_test_result', 'numeric_value'
        ]
vital_cols = [
            'pat_key', 'observation_day_number', 'observation_time_of_day',
            'lab_test', 'test_result_numeric_value'
        ]

bill_cols = ['pat_key', 'std_chg_desc', 'serv_day']
lab_res_cols = [
            'pat_key', 'spec_day_number', 'spec_time_of_day', 'test',
            'observation'
        ]
icd_lookup1 = spark.table("cdh_premier.icdcode")
chgmstr1 = spark.table("cdh_premier.chgmstr")
hosp_chg1 = spark.table("cdh_premier.hospchg") 

icd_enc1 = spark.table("cdh_premier.paticd_diag")
proc_enc1 = spark.table("cdh_premier.paticd_proc")
patbill1 = (spark.table("cdh_premier.patbill")
            .join(chgmstr1.select("std_chg_code","std_chg_desc"), on = "std_chg_code", how = "left")
            .select(*bill_cols))
vitals1 = spark.table("cdh_premier.vitals").select(*vital_cols)
genlab1 = spark.table("cdh_premier.genlab").select(*genlab_cols)
labres1 = spark.table("cdh_premier.lab_res").select(*lab_res_cols)




icd_enc = join_readmit(icd_enc1, readmit)
proc_enc = join_readmit(proc_enc1, readmit)
patbill = join_readmit(patbill1, readmit)
vitals = join_readmit(vitals1, readmit)
genlab = join_readmit(genlab1, readmit)
labres = join_readmit(labres1, readmit)


# COMMAND ----------

def get_enc_lte_target(sdf, coh):
    out = (coh
           .join(sdf, on = "medrec_key", how = "left")
           .filter(F.col("dfi") <= F.col("days_from_index"))
          )
    return out

# COMMAND ----------

icd_enc2 = get_enc_lte_target(icd_enc, coh_mk)
proc_enc2 = get_enc_lte_target(proc_enc, coh_mk)
patbill2 = get_enc_lte_target(patbill, coh_mk)
vitals2 = get_enc_lte_target(vitals, coh_mk)
genlab2 = get_enc_lte_target(genlab, coh_mk)
labres2 = get_enc_lte_target(labres, coh_mk)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers
# MAGIC - These helper functions are used throughout the analyses

# COMMAND ----------

# helpers
def check_exclusion(orig, new):
    orig_enc = orig.count()
    orig_pat = orig.select("medrec_key").distinct().count()
    new_enc = new.count()
    new_pat = new.select("medrec_key").distinct().count()
    print(f"Orginal encounters: {orig_enc}, Original Patient: {orig_pat}, New encounters: {new_enc}, New Patients: {new_pat}")
    
# @rdd_unpersist()
# takes a list of rdds to uncache
# even if single RDD wrap in a list
# RDD ids can be found in the storage UI
def rdd_unpersist(rdds = None, check = False):
    for (id, rdd) in sc._jsc.getPersistentRDDs().items():
        if check:
            newlin = "\n"
            print(f"{newlin}RDD: {id}")
            print(rdd)
        else:
            if id in rdds:
                print(f"uncache {id}")
                rdd.unpersist()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Globals Vars

# COMMAND ----------

# Globals
REMOVE_VITALS = False
SAMPLE = True
SENSITIVITY = False
VERBOSE = False

# COMMAND ----------

# MAGIC %md 
# MAGIC # Get Cohort
# MAGIC
# MAGIC 1. Read in the covid_id and covid_pat_all datasets
# MAGIC   - These are preprocessed, and it is unknown how they are fully linked to the raw Premier Data
# MAGIC   - At the bottom of the doc the raw Premier tables have been identified, but the exact preprocessing steps would take additionally work to identify.
# MAGIC 2. Read in the icu_targets
# MAGIC   - This is also preprocessed and the exact process completed is unknown.

# COMMAND ----------

# Default directory
dir = '/home/tnk6/premier/'

# Read in the visit tables 
# p_id =  spark.read.parquet(dir + 'vw_covid_id/')
pat_all = spark.read.parquet(dir + "vw_covid_pat_all/")

# Read in MISA data
targets_dir = "/home/tnk6/premier_output/targets/"
misa_data = spark.read.csv(targets_dir + 'icu_targets.csv', header=True)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Exclusions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Target Identification and Primary Exclusion 
# MAGIC
# MAGIC On the patient datasets, most of the inclusion/exclusion criteria can be applied prior to connecting to specific encounters features.
# MAGIC
# MAGIC **Criteria** (taken from the original paper):
# MAGIC - **Include**:
# MAGIC   - Patients with an ICD code for covid Feb 1, 2020 - April 30, 2021
# MAGIC       - Predifined in the preprocessed datasets.
# MAGIC         - This would need to be created if taken from the raw data
# MAGIC   - Identified Target Encounter being the first covid encounter in the above time frame
# MAGIC   - Historical encounters in lookback window to 2018101 (Jan 1 2018)
# MAGIC     - This is further reduced in the below criteria.
# MAGIC   - Historical encounters in the range 225 prior Target Encounter to the end of the Target Encounter's first day (24hr) 
# MAGIC   - Has the outcome information associated with Death, ICU, MISA status at the Target Encounter. 
# MAGIC
# MAGIC - **Exlcude**:
# MAGIC   - age < 18 at Target Encounter
# MAGIC   - Terms ["ICU", "TCU", "STEP DOWN"] in the features associated with the Target Encounter
# MAGIC   - Target Encounter NOT inpatient
# MAGIC   - Target Encounter NOT > 1 day 
# MAGIC     - (since we are looking for the outcome in the multiple days after the initiation of encounter)
# MAGIC
# MAGIC **Additional Sensitivity**
# MAGIC - Primary Cohort: include all patient history (-225 days + target_visit)
# MAGIC - Secondary Cohort: include only the target_visit day
# MAGIC
# MAGIC **Feature code exclusion**
# MAGIC - Codes that appear in < 5 sepearte patients coded features

# COMMAND ----------

# @get_inclu_enc()
# - indicates which encounters are possible inclusion encounter
def get_inclu_enc(sdf):
                                                                                         # 1.
    cond = (((F.col("adm_mon")>= 2020102) & (F.col("adm_mon") <= 2021204)) & 
            (F.col("covid_visit") == 1)) 
                                                                                        # 2.
    out = (sdf 
           .select("*", F.when(cond, 1).otherwise(0).alias("incl_enc"))
          )
    return(out)


# @get_target
# @target
# - target encounter, being the first inpatient covid visit in the indicated time frame
# @target_dt
# - date of target encounter
# @target_dfi
# - target days_from_index
# @target_age
# - age at target encounter
# @target_inpat
# - inpatient status at target encounter
# @target_los
# - target encounter length of stay
def get_target(sdf):
    out = (sdf
                                                                                          # 3.
           .withColumn("row",
                       F.row_number()
                       .over(W.partitionBy(F.col("medrec_key"), F.col("incl_enc"))
                             .orderBy(F.col("days_from_index")))
                  )
           .withColumn("target",
                       F.when((F.col("row") == 1) & (F.col("incl_enc") == 1), 1)
                       .otherwise(0)
                      )
           .withColumn("target_dt",
                       F.max(F.when(F.col("target") == 1, F.col("adm_mon")))
                       .over(W.partitionBy(F.col("medrec_key")))
                      )
           .withColumn("target_dfi",
                       F.max(F.when(F.col("target") == 1, F.col("days_from_index")))
                       .over(W.partitionBy(F.col("medrec_key")))
                      )
           .withColumn("target_age",
                       F.max(F.when(F.col("target") == 1, F.col("age")))
                       .over(W.partitionBy(F.col("medrec_key")))                       
                      )
           .withColumn("target_inpat",
                       F.max(F.when(F.col("target") == 1, F.col("i_o_ind")))
                       .over(W.partitionBy(F.col("medrec_key")))                       
                      )
           .withColumn("target_los",
                       F.max(F.when(F.col("target") == 1, F.col("los")))
                       .over(W.partitionBy(F.col("medrec_key")))                       
                      )
          .withColumn("target_pat_key",
                       F.max(F.when(F.col("target") == 1, F.col("pat_key")))
                       .over(W.partitionBy(F.col("medrec_key")))                       
                      )
          )
    return(out)

# @get_encounters
# - sets indicator to 1 for all encounters with adm_mon <= target_dt and days_from_idex >= target_df-255
# @keep_enc
# - indicator for encounters that meet time range requirement
def get_encounters(sdf):
    out = (sdf
           .withColumn("keep_enc",
                       F.when(
                           ((F.col("adm_mon") <= F.col("target_dt")) & 
                            (F.col("days_from_index") >= (F.col("target_dfi")-255))),
                           1).otherwise(0))
          )
    return(out)

# @get_incl_enc_final
# - returns the final set of encounters that meeting inclusion exclusion criteria
# - include keep_enc
# - exclude age < 18
# - exclude inpatient status O (outpatient)
# - exclude length of stay <= 1
def get_inclu_enc_final(sdf):
    out = (sdf
                                                                                                # 4.
           .filter(
               (F.col("keep_enc") == 1) &
               (F.col("target_age") >= 18) &
               (F.col("target_inpat") == "I")
           )
           .filter(F.col("target_los") > 1)
           .drop("row", "pat_type", "prov_id")
          )
    return(out)

# COMMAND ----------

# Apply
# Identify the patients with a Covid encounter in the time frame
pat_incl1 = get_inclu_enc(pat_all)
# Identify the target encounter
pat_incl2 = get_target(pat_incl1)
# Identify encounters that need to be kept by the time frame requirement
pat_incl3 = get_encounters(pat_incl2)
# apply inclusion criteria filtering
pat_incl4 = get_inclu_enc_final(pat_incl3)

# COMMAND ----------

# CHECKS
# Check that no patient has more than one target == 1
# display(pat_incl2
#         .filter(F.col("target") == 1)
#         .groupBy("medrec_key")
#         .agg(F.sum(F.col("target")).alias("n"))
#         .filter(F.col("n") > 1)
#        )

# Check that the keep_enc appears correct
# display(pat_incl3.filter(F.col("keep_enc") == 1))

# Check min target age
# print(pat_incl4.agg(F.min(F.col("target_age"))).show())

# Check that the counts post inclusion
# check_exclusion(pat_all, pat_incl4)

# COMMAND ----------

# MAGIC %md
# MAGIC ## MISA Exclusion
# MAGIC
# MAGIC With the primary exclusion dataset an additional round of filtering can be applied by filtering patients who do not have the ICU, MISA or Death data

# COMMAND ----------

# @get_misa_targets()
# - adds selectors for misa targets and death (dis_status_desc)
# - left join misa_df to pat_df keeping all encounters in the pat_df and adding misa var indicators to matched and Null to unmatched
# - create the death indicator
# - expand the misa indicator of the matched target encounter to all encounters of the patient. If the target encounters misa vars are Null all of the patients misa indicators will be Null.
def get_misa_targets(sdf, misa_in):
    # recode to aviod confusion of NA in outcome variables
    misa_data = (misa_in
                 .select("pat_key", 
                         F.when(F.col("misa_filled") == "1", 1).otherwise(0).alias("misa_filled"),
                         F.when(F.col("icu_visit") == "1", 1).otherwise(0).alias("icu_visit")
                        )
                )
    
    # join mias and sdf
    pat_misa = (sdf
            .join(misa_data, on="pat_key", how = "left")
           )
    
    # recode death variable
    death_recode = (pat_misa
                   .withColumn("death", 
                               F.when(F.col("disc_status_desc").contains("EXPIRED"), 1)
                               .otherwise(0))
                  )
    
    # create new variables with list comprehension
    misa_death = ["death", "misa_filled", "icu_visit"]
    out = (death_recode
                                                                                                           # 1.
           .select("*",
                  *[F.max(F.when(F.col("target") == 1, F.col(x)))
                    .over(W.partitionBy(F.col("medrec_key")))
                    .alias(f"target_{x}") 
                    for x in misa_death]
                  )
          )
    return(out)

# @filter_non_misa()
# - remove patients whose misa indicators are Null for all encounters
# - the remaining patients and encounters are those from patients whose target encounter had a matched misa data encounter
def filter_non_misa(sdf):
    out = (sdf
           .filter(F.col("target_misa_filled").isNotNull() & F.col("target_icu_visit").isNotNull())
          )
    return(out)

# COMMAND ----------

# Apply
# get misa variables for targets
pat_misa = get_misa_targets(pat_incl4, misa_data)
# apply filtering of patients without misa data
pat_misa2 = filter_non_misa(pat_misa)

# COMMAND ----------

# check
# check_exclusion(pat_all, pat_misa2)

# COMMAND ----------

# MAGIC %md
# MAGIC The only exlusion left is based on having one of the exclusion FTR codes during the target visit
# MAGIC Otherwise the New patient count of 102102 is close to the originally computed primary exclusion cohort 119712 (which does not have the Age, Target length and Inpatient status exclusions applied)
# MAGIC
# MAGIC If the ftr code exclusions leave around 60,000 patients I think that is acceptable considering the significant reformat of approach completed

# COMMAND ----------

# MAGIC %md 
# MAGIC # Get Sample (optional)
# MAGIC - Provides option for getting a sample of the patients
# MAGIC - This is useful when developing the analysis and a smaller dataset can be used for testing purposes

# COMMAND ----------

# @get_sample()
# - get sample of patients and returns all associated encounters
# - 
# - caches sample
def get_sample(sdf, fraction = 0.01, seed=222, cache = True):
    temp = sdf
                                                                            # 1. 
    out = (sdf
              .select("medrec_key")
              .distinct()
              .sample(fraction = fraction, seed = seed)
              .join(sdf, on = "medrec_key", how = "inner" )
           )
                                                                            # 2.
    if cache:
        out.cache()
    
    print(f"number of encounters returned {out.count()}, numer of patients sampled {out.select('medrec_key').distinct().count()}")
    return(out)

# COMMAND ----------

# Apply
drop_list = ["i_o_ind", "covid_visit", "disc_status_desc", "adm_type_desc", "point_of_origin", "std_payor_desc", "adm_mon", "disc_mon", "disc_mon_seq", "days_from_prior", "incl_enc", "keep_enc", "misa_filled", "icu_visit", "death", "target_inpat", "target_age"]

if SAMPLE:
    pat_s = (get_sample(pat_misa2)
             .drop(*drop_list)
            )
else:
    pat_s = (pat_misa2
             .drop(*drop_list)
            )

# COMMAND ----------

# check
# display((pat_f)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Get Demographics
# MAGIC - Demographics from the target encounter are collected, coded and placed in a array variable
# MAGIC
# MAGIC dem list = [gender, hispanic_in, age, race]
# MAGIC
# MAGIC Procedure
# MAGIC 1. bin ages by 10 year increments
# MAGIC 2. adjust hispanic so if Y/N then default Y
# MAGIC 3. Race can be multiple as no ranking can be applied
# MAGIC 2. create features list of demographics in form (demg type : value)
# MAGIC
# MAGIC
# MAGIC The output `pat_d` is not used again until the Feature Count Vectorization

# COMMAND ----------

# select the demographic variables
# This might be adjusted after selecting the visits we care about

# dem list = [gender, hispanic_in, age, race]
# 1. bin ages by 10 year increments
# 2. adjust hispanic so if Y/N then default Y
# 3. Race can be multiple as no ranking can be applied
# 2. create features list of demographics in form (demg type : value)

# @get_pat_targets()
def get_pat_targets(sdf):
    out = (sdf
           .filter(F.col("target") == 1)
          )
    return(out)

# @get_age_bin()
# - bins age by 10 years
def get_age_bin(sdf):
    age_buc = (F.when((F.col("age") >=0) & (F.col("age") < 10), "0-9")
               .when((F.col("age") >=10) & (F.col("age") < 20), "10-19")
               .when((F.col("age") >= 20) & (F.col("age") < 30), "20-29")
               .when((F.col("age") >= 30) & (F.col("age") < 40), "30-39")
               .when((F.col("age") >= 40) & (F.col("age") < 50), "40-49")
               .when((F.col("age") >= 50) & (F.col("age") < 60), "50-59")
               .when((F.col("age") >= 60) & (F.col("age") < 70), "60-69")
               .when((F.col("age") >= 70) & (F.col("age") < 80), "70-79")
               .when((F.col("age") >= 80) & (F.col("age") < 90), "80-89")
               .when((F.col("age") >= 90) & (F.col("age") < 100), "90-99")
               .otherwise(">=100")
              )
    out = (sdf
           .withColumn("age_bin", age_buc)
          )
    return(out)


# @get_hisp
# - changes hispanic_ind to true if any for set patient encounters is true
# - the application of "any()" is no longer needed as only the target encounter is being used, but I have kept it in as an example
def get_hisp(sdf):
    hisp = "hispanic_ind"
    out = (sdf
                                                                                   # 1.
           .withColumn("hispanic_ind", 
                       F.when(F.expr(f"any({hisp} = 'Y')")
                              .over(W.partitionBy(F.col("medrec_key"))), "Y")
                       .when(F.expr(f"any({hisp} = 'N')")
                            .over(W.partitionBy(F.col("medrec_key"))), "N")
                       .otherwise("U"))
          )
    return(out)

# @get_dem_ftr()
# - returns the demographics for each patient as a coded ftr as a array
# - use the list comprehension to apply the concat and collect set to all columns required
def get_dem_ftr(sdf):
    dem_lst = ["gender", "hispanic_ind", "age_bin", "race"]
    term = "_ftr"
    dem_lst2 = [t + term for t in dem_lst]
    
    w = W.partitionBy(F.col("medrec_key"))
    out = (sdf
                                                                                         # 2.
           .select("*",
                   *[F.collect_set(F.concat(F.lit(f"{x} "), F.col(x)))
                     .over(w)
                     .alias(f"{x}{term}") 
                     for x in dem_lst]
                  )
           .select("medrec_key",
                   F.flatten(F.array(*dem_lst2)).alias("dem_ftr")
                  )
           .distinct()
          )
    return(out)

# @rejoin_dem()
# - rejoins the demographic features to pat_f
def rejoin_dem(dem, sdf):
    dem_lst = ["age", "gender", "hispanic_ind", "age_bin", "race"]
    out = (sdf
           .join(dem, on = "medrec_key", how = "left")
           .drop(*dem_lst)
          )
    return(out)

# COMMAND ----------

# Apply
# get the target encounters
pat_target = get_pat_targets(pat_s)
# get the binned ages
age = get_age_bin(pat_target)
# get the hispanic indicator
hisp = get_hisp(age)
# apply the coding and array concept_set
pat_dem = get_dem_ftr(hisp)
# rejoin to pat_f
pat_f = rejoin_dem(pat_dem, pat_s)

# COMMAND ----------

# test
# display(pat_f)
# pat_f.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Encounter Features Cleaning

# COMMAND ----------

# MAGIC %md
# MAGIC ## Get Features
# MAGIC - Read in the individual feature datasets
# MAGIC - If Patient data is sampled, then apply join to the sample df otherwise join to the full (exclusions applied) df

# COMMAND ----------

# Read in data
genlab_cols = [
            'pat_key', 'concept_set_day_number', 'concept_set_time_of_day',
            'lab_test_loinc_desc', 'lab_test_result', 'numeric_value'
        ]
vital_cols = [
            'pat_key', 'observation_day_number', 'observation_time_of_day',
            'lab_test', 'test_result_numeric_value'
        ]

bill_cols = ['pat_key', 'std_chg_desc', 'serv_day']
lab_res_cols = [
            'pat_key', 'spec_day_number', 'spec_time_of_day', 'test',
            'observation'
        ]


# Pulling the lab and vitals
genlab = spark.read.parquet(dir + 'vw_covid_genlab/', columns=genlab_cols)
hx_genlab = spark.read.parquet(dir + 'vw_covid_hx_genlab/', columns=genlab_cols)

lab_res = spark.read.parquet(dir + 'vw_covid_lab_res/', columns=lab_res_cols)
hx_lab_res = spark.read.parquet(dir + 'vw_covid_hx_lab_res/', columns=lab_res_cols)

vitals = spark.read.parquet(dir + 'vw_covid_vitals/', columns=vital_cols)
hx_vitals = spark.read.parquet(dir +'vw_covid_hx_vitals/', columns=vital_cols)


# Pulling in the billing tables
bill_lab = spark.read.parquet(dir + 'vw_covid_bill_lab/', columns=bill_cols)
bill_pharm = spark.read.parquet(dir + 'vw_covid_bill_pharm/', columns=bill_cols)
bill_oth = spark.read.parquet(dir + 'vw_covid_bill_oth/', columns=bill_cols)
hx_bill = spark.read.parquet(dir + 'vw_covid_hx_bill/', columns=bill_cols)

# Pulling in the additional diagnosis and procedure tables
pat_diag = spark.read.parquet(dir + 'vw_covid_paticd_diag/')
add_diag = spark.read.parquet(dir + 'vw_covid_additional_paticd_diag/')

pat_proc = spark.read.parquet(dir + 'vw_covid_paticd_proc/')
add_proc = spark.read.parquet(dir + 'vw_covid_additional_paticd_proc/')

icd = spark.read.parquet(dir + 'icdcode/')

# COMMAND ----------

# @get_samp_enc()
# - merges the encounter table to the sample tables
# - caches the new sample data
def get_samp_enc(sdf, pat, sdf_str = "dtst", cache = True):
    out = (pat
           .select("pat_key")
           .join(sdf, on = "pat_key", how = "inner")
          )
    if cache:
        out.cache()
    print(f" encounters kept from {sdf_str}: {out.count()}, from total {sdf.count()}")
    return(out)

# COMMAND ----------

# Apply

# Union tables that should be together
# genlabm = genlab.union(hx_genlab)
# vitalsm = vitals.union(hx_vitals)
# lab_resm = lab_res.union(hx_lab_res)
# billm = bill_lab.union(bill_pharm).union(bill_oth).union(hx_bill)
# diagm = pat_diag.union(add_diag)
# procm = pat_proc.union(add_proc)

# if SAMPLE:
#     genlab1 = get_samp_enc(genlabm, pat_f, "genlabm")
#     vitals1 = get_samp_enc(vitalsm, pat_f, "vitalsm")
#     lab_res1 = get_samp_enc(lab_resm, pat_f, "lab_resm")
#     bill1 = get_samp_enc(billm, pat_f, "billm")
#     diag1 = get_samp_enc(diagm, pat_f, "diagm")
#     proc1 = get_samp_enc(procm, pat_f, "procm")
# else:
#     genlab1 = genlabm
#     vitals1 = vitalsm
#     lab_res1 = lab_resm
#     bill1 = billm
#     diag1 = diagm
#     proc1 = procm

# COMMAND ----------



# COMMAND ----------

# Check Full Datasets
# display(genlab1)
# display(vitals1)
# display(bill1)
# display(diag1)
# display(proc1)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Cleaning
# MAGIC
# MAGIC - This includes a multi-level function.
# MAGIC   1. get_ftrs()
# MAGIC       - Main function and contain highest level logic
# MAGIC       - return the created df
# MAGIC       1. merge_cols()
# MAGIC           - gets the column list of the dataset input into get_ftrs
# MAGIC           - adjust to the options used in the main function
# MAGIC       2. get_quant()
# MAGIC           - gets the pandas_udf and the schema to be applied
# MAGIC       3. applies the pandas_udf function
# MAGIC       4. get_ftr_dict()
# MAGIC           - recodes the features generated in step 3 into ftr_key and ftr_value columns
# MAGIC           - these can be collected from the return dataframe and saved for reference
# MAGIC           - the ftr_key is kept in the return dataframe for Count Vectorization, having a benefit of reduced size
# MAGIC
# MAGIC - Using this function-generating function allows the parameters to be passed to create a custom pandas_udf for each datasets options
# MAGIC - This function generates text outptu for evaluating the ftrs created
# MAGIC         

# COMMAND ----------

# merg_cols()
# - constructs the list of columns based on presense of each
def merge_cols(base_cols, text_col, res_col, replace_col, time_cols):
    out = base_cols
    if text_col is not None:
        out += [text_col]
    if res_col is not None:
        out += [res_col]
    if replace_col is not None:
        out += [replace_col]
    if time_cols is not None:
        out += time_cols
    return out


# get_ftr_val()
# - returns the test and test results as quartiled representations
def get_ftr_val(ftr_dtst=None, test=None, result=None, replace=None):

    tst = F.col(test)
    # return test column as "ftr_val"
    if result is None:
        out = ftr_dtst.select("*", tst.alias("ftr_val"))
    # get quantiles
    else:
        res = F.col(result)
        perc = ftr_dtst.select(
            "*",
            F.percentile_approx(res, [0.0, 0.25, 0.5, 0.75, 1.0])
            .over(W.partitionBy(tst))
            .alias("p"),
        ).withColumn(
            "rank",
            F.when(((F.col("p").getItem(0) <= res) & (res < F.col("p").getItem(1))), 1)
            .when(((F.col("p").getItem(1) <= res) & (res < F.col("p").getItem(2))), 2)
            .when(((F.col("p").getItem(2) <= res) & (res < F.col("p").getItem(3))), 3)
            .when(((F.col("p").getItem(3) <= res) & (res <= F.col("p").getItem(4))), 4)
            .otherwise("none"),
        )
        # replace missing quantiles if Replace given
        if replace is not None:
            rep = F.col(replace)
            perc = perc.select("*", F.when(F.col("rank") == "none", rep).alias("rank"))

        out = perc.withColumn("ftr_val", F.concat_ws("_", tst, F.col("rank"))).drop(
            F.col("p"), F.col("rank")
        )

    return out


# get_ftr_dict
# - retruns the feature dict for each sdf
# - implement in pandas_udf
def get_ftr_dict(sdf=None, prefix=None):
    cols = sdf.columns + ["ftr_key"]
    out = (
        sdf.withColumn("tmp", F.lit(prefix))
        .withColumn("index", F.dense_rank().over(W.orderBy("ftr_val")))
        .withColumn("ftr_key", F.concat_ws("_", F.col("tmp"), F.col("index")))
        .select(*cols)
    )
    return out


# get_ftrs
# - applies all operations for each dataframe
def get_ftrs(
    sdf=None,
    base_cols=["pat_id"],
    text_col=None,
    res_col=None,
    replace_col=None,
    time_cols=None,
    time_types=None,
    prefix=None,
    verbose=True):

    # Get the selection columns
    sel_cols = merge_cols(base_cols, text_col, res_col, replace_col, time_cols) + ["dfi", "los"]

    # select only the columns required and remove and null text_col values
    sdf_select = (
        sdf
        # select columns needed
        .select(*[sel_cols])
        # ensure text column is a string
        .withColumn(text_col, F.col(text_col).cast("string"))
        .filter(F.col(text_col) != "")
    )

    # apply quantiles
    ftr_sdf = get_ftr_val(sdf_select, test=text_col, result=res_col)

    # get feature key
    ftr_dict = get_ftr_dict(sdf=ftr_sdf, prefix=prefix)

    # Review
    if verbose:
        print(f"Pre-tranformation select columns: {sel_cols}") 
        print(
            f"Distinct number of features {prefix}: {ftr_dict.select('ftr_key').distinct().count()}"
        )
        ftr_dict.groupBy("ftr_val").count().sort(F.col("count").desc()).show(
            truncate=False
        )

    return ftr_dict

# COMMAND ----------

# icd_enc2 
# proc_enc2
# patbill2 
# vitals2 
# genlab2 
# labres2 
VERBOSE = False

# COMMAND ----------

# Apply
vit_ftr = get_ftrs(sdf = vitals2,
                   base_cols = ["pat_key"],
                   text_col = "lab_test",
                   res_col = "test_result_numeric_value",
                   replace_col = None,
                   time_cols = ["observation_day_number", "observation_time_of_day"],
                   time_types = ["integer", "string"],
                   prefix = "vital", 
                   verbose = VERBOSE
                  )
bill_ftr = get_ftrs(sdf = patbill2,
                              base_cols = ["pat_key"],
                              text_col = "std_chg_desc",
                              res_col = None,
                              replace_col = None,
                              time_cols = ["serv_day"],
                              time_types = ["integer"],
                              prefix = "bill",
                    verbose = VERBOSE
                   )

genlab_ftr = get_ftrs(sdf = genlab2,
                                   base_cols = ["pat_key"],
                                   text_col = "lab_test_loinc_desc",
                                   res_col = "numeric_value",
                                   replace_col = "lab_test_result",
                                   time_cols = ["concept_set_day_number", "concept_set_time_of_day"],
                                   time_types = ["integer", "string"],
                                   prefix = "genl",
                      verbose = VERBOSE
                                  )

proc_ftr = get_ftrs(sdf = proc_enc2,
                               base_cols = ["pat_key"],
                               text_col = "icd_code",
                               res_col = None,
                               replace_col = None,
                               time_cols = ["proc_day"],
                               time_types = ["integer"],
                               prefix = "proc",
                    verbose = VERBOSE
                              )

diag_ftr = get_ftrs(sdf = icd_enc2,
                               base_cols = ["pat_key"],
                               text_col = "icd_code",
                               res_col = None,
                               replace_col = None,
                               time_cols = None,
                               time_types = None,
                               prefix = "diag",
                    verbose = VERBOSE
                              )

# Merge the test and observation columns together directly, since observation isn't a numeric column
lab_res_temp = labres2.withColumn("lab_text", F.concat_ws(" ", F.col("test"), F.col("observation")))
labres_ftr = get_ftrs(sdf = lab_res_temp,
                                   base_cols = ["pat_key"],
                                   text_col = "lab_text",
                                   res_col = None,
                                   replace_col = None,
                                   time_cols = ["spec_day_number", "spec_time_of_day"],
                                   time_types = ["integer", "string"],
                                   prefix = "labres",
                      verbose = VERBOSE
                                  )

# COMMAND ----------

# Check the created Datasets
# display(vit_ftr)
# display(bill_ftr)
# display(genlab_ftr)
# display(proc_ftr)
# display(diag_ftr)
# display(labres_ftr)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Adjust Times
# MAGIC
# MAGIC - Creates the dfi, hfi and mfi for each encounter observation based on given day, time columns associated with feature observation.
# MAGIC     - if no day or time column given, defaults to the encounter dfi and hfi at time 0 and mfi at time 0 for each day
# MAGIC
# MAGIC Proces
# MAGIC 1. input df and the day_dict
# MAGIC     - day_col, time_col, ftr_col, day_only=True
# MAGIC 2. join the dfi from the day_dict (p_id) as dfi
# MAGIC 3. if day_col not given then add hours and minutes (dfi * 24, dfh*60) as dfh, dfm and return pat_key, ftr_col, dfi, dfh, dfh
# MAGIC 4. else  
# MAGIC     - dfi = dfi from day_dict + day_col (this will be the day of event from encounter start)
# MAGIC     - get hfi and mfi as dfi * 24, hfi * 60
# MAGIC     - if time_col exists then calculate the hrs and minutes
# MAGIC          - hr is int[0:2] from time_col * 60
# MAGIC             - min is int[3:5] from time_col * 60
# MAGIC             - total_min is hr + min
# MAGIC             total_hr is np.round(total_min/60).astype(int) OR just use the hr column created
# MAGIC             hfi = hfi + total_hr
# MAGIC             mfi = mfi + total_min
# MAGIC
# MAGIC This will additionally add in the columns from the pat_df (post-exclusions)

# COMMAND ----------

# @get_times()
# - adjust the dfi (hfi, mfi) for each record for each encounter based on various associated day/time variables in the associated Feature files
def get_times(sdf = None,
              day_dict = None,
              day_col = None,
              time_col = None,
              adjusted_los = False
             ):
    # pat_f columns to keep
    drop = ["pat_key", "days_from_index"]
#     cols = [v for v in day_dict.columns if v not in drop]

    # new
    sdf_out = sdf
#     sdf_out = (sdf
#                .join(day_dict, on="pat_key", how="left")
#                .withColumnRenamed("days_from_index", "dfi")
#               )
    if day_col is not None:
        sdf_out = (sdf_out
                   .withColumn("dfi", F.col("dfi") + F.col(day_col) - 1)
                  )
    # create adjustment to move dfi to end of encounter (for the diagnoses set)
    if adjusted_los:
        sdf_out = (sdf_out
                   .withColumn("dfi", F.col("dfi") + F.col("los"))
                  )
    if time_col is not None:
        sdf_out = (sdf_out
                   .withColumn("hr", F.col(time_col).substr(1,2))
                   .withColumn("mn", F.col(time_col).substr(4,2))
                   .withColumn("hfi", (F.col("dfi")*24) + F.col("hr"))
                   .withColumn("mfi", (F.col("hfi")*60) + F.col("mn"))
                  )
    else:
        sdf_out = (sdf_out
                   .withColumn("hfi", F.col("dfi") * 24)
                   .withColumn("mfi", F.col("hfi") * 60)
                  )
#     return(sdf_out.select(*[["pat_key", "ftr_key", "ftr_val", "dfi", "hfi", "mfi"]+cols]))
    return sdf_out

# COMMAND ----------

pat_f = None
vit_tim = get_times(sdf = vit_ftr,
                    day_dict = pat_f,
                    day_col = "observation_day_number",
                    time_col = "observation_time_of_day"
                   )
genlab_tim = get_times(sdf = genlab_ftr,
                       day_dict = pat_f,
                       day_col = "concept_set_day_number",
                       time_col = "concept_set_time_of_day"
                      )
labres_tim = get_times(sdf = labres_ftr,
                    day_dict = pat_f,
                    day_col = "spec_day_number",
                    time_col = "spec_time_of_day"
                   )
bill_tim = get_times(sdf = bill_ftr,
                     day_dict = pat_f,
                     day_col = "serv_day",
                     time_col = None
                    )
proc_tim = get_times(sdf = proc_ftr,
                     day_dict = pat_f,
                     day_col = "proc_day"
                    )
diag_tim = get_times(sdf = diag_ftr,
                     day_dict = pat_f,
                     day_col = None,
                     time_col = None,
                     adjusted_los = True
                    )

# COMMAND ----------

# Check the times datasets
# display(vit_tim)
# display(genlab_tim)
# display(labres_tim)
# display(bill_tim)
# display(proc_tim)
# display(diag_tim)

# COMMAND ----------

# MAGIC %md
# MAGIC # Final Exclusion

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Remove Vitals
# MAGIC - This is optional, and not applied
# MAGIC - REMOVE_VITAS default = False

# COMMAND ----------

# Apply

# Unions all seperate feature datasets together 
if REMOVE_VITALS:
    ftr_tim = (bill_tim
           .union(labres_tim)
           .union(proc_tim)
           .union(diag_tim)
           .filter(F.col("ftr_key").isNotNull())
          )
else:
    ftr_tim = (vit_tim
           .union(bill_tim)
           .union(genlab_tim)
           .union(labres_tim)
           .union(proc_tim)
           .union(diag_tim)
           .filter(F.col("ftr_key").isNotNull())
          )

# COMMAND ----------

# check
# display(ftr_tim)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Features Exclusion
# MAGIC
# MAGIC - The last exclusion is based on excluding patients who where admitted to the ICU on the first day of the target visit.
# MAGIC     - Since this is the outcome we are looking for post 24 hours it needs to be removed.

# COMMAND ----------

# Final exclusion criteria

# @get_ftr_exclusion()
# - identifies patients to exclude based on ["ICU", "TCU", "STEP DOWN"] present in the ftr_vals for the first day of target encounter
def set_ftr_exclusion(sdf):
    excl = ["ICU", "TCU", "STEP DOWN"]
    out = (sdf
           .select("*", 
                   F.when((F.col("dfi") == F.col("target_dfi")) & 
                                ((F.col("ftr_val").contains("ICU") | 
                                  F.col("ftr_val").contains("TCU") | 
                                  F.col("ftr_val").contains("STEP DOWN"))), 1).otherwise(0)
                              .alias("ftr_excl"))
           .withColumn("ftr_excl", F.max(F.col("ftr_excl")).over(W.partitionBy(F.col("medrec_key"))))
          )    
    return(out)

# @excl_ftr_exclusion()
# - applies patient level exclusion based on the prior indicator
def excl_ftr_exclusion(sdf):
    out = (sdf
           .filter(F.col("ftr_excl") == 0)
           .drop("ftr_excl")
          )
    return(out)
 
# @filter_lessthanequal_target()
# - applies final encounter observation exclusions, removing any observations that occur at a dfi greater than the target first days dfi
def filter_lessthanequal_target(sdf):
    out = (sdf
           .filter(F.col("dfi") <= F.col('target_dfi'))
          )
    return(out)

# COMMAND ----------

# Apply
# get ftr exclusion indicator
ftr_tmp = set_ftr_exclusion(ftr_tim)
# apply patient level ftr exclusion filter
ftr_tmp2 = excl_ftr_exclusion(ftr_tmp)
# apply encounter > target dfi filter
ftr_final = filter_lessthanequal_target(ftr_tmp2)

# COMMAND ----------

# check
# check_exclusion(ftr_tim, ftr_tmp2)
# check_exclusion(ftr_tim, ftr_final)

# COMMAND ----------

# MAGIC %md
# MAGIC # Split into primary and secondary cohort
# MAGIC - This is optional only used if evaluating specifically the first 24 hours of target encounter ability to predict outcomes
# MAGIC - SENSITIVITY default = False

# COMMAND ----------

# @get_secondary()
def get_secondary(sdf):
    out = (sdf
           .filter(F.col("dfi") == F.col("target_dfi"))
          )
    return(out)

# COMMAND ----------

# Apply
pat_primary = ftr_final
if SENSITIVITY:
    pat_secondary = get_secondary(pat_primary)

# COMMAND ----------

# test
# if SENSITIVITY:
#     check_exclusion(pat_primary, pat_secondary)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Get Feature Dictionary
# MAGIC - Selects only the ftr_key, ftr_val to be saved independently

# COMMAND ----------

# @get_ftr_dict_out
# - seperates ftr_key and ftr_val and collects the distinct set to be saved seperately 
def get_ftr_dict_out(sdf):
    out = (sdf
           .select("ftr_key", "ftr_val")
           .distinct()
          )
    return(out)

# COMMAND ----------

# Apply
ftr_dict_out = get_ftr_dict_out(pat_primary)
if SENSITIVITY:
    ftr_dict_out2 = get_ftr_dict_out(pat_secondary)

# COMMAND ----------

# test
# display(ftr_dict_out)
# print(ftr_dict_out.count())

# COMMAND ----------

# MAGIC %md 
# MAGIC # Get Aggregate Features and Outcomes

# COMMAND ----------

# Aggregate features into a single column
# 1. input sdf, time_col, id_col, ft_col
# 2. groupby id_col, time_col (if time_col present else just groupby id_col)
# 3. agg group into a ftr dict in a single df column

# @get_agg_outcomes()
# - returns the sdf with the outcomes as an array
def get_agg_outcomes(sdf):
    out = (sdf
           .withColumn("outcomes", F.array(F.col("target_death"), F.col("target_misa_filled"), F.col("target_icu_visit")))
          )
    return(out)

# @get_agg_ftrs
# - return the ftr_key as a list/array for each dfi 
def get_agg_ftr(sdf = None,
            time_col = None,
            id_col = None,
            ftr_col = None
           ):
    out = (sdf
           .select("medrec_key", id_col, ftr_col, "dem_ftr", "outcomes", time_col, "target_dfi", "target")
           .withColumn(ftr_col, 
                       F.collect_list(F.col(ftr_col))
                       .over(W.partitionBy(F.col(id_col), F.col(time_col))))
           .distinct()
          )
    return(out)

# COMMAND ----------

# Apply
outc = get_agg_outcomes(pat_primary)
primary_agg = get_agg_ftr(sdf = outc, 
                      time_col = "dfi", 
                      id_col = "pat_key",
                      ftr_col = "ftr_key"
                     )

# COMMAND ----------

# test
primary_agg.cache()
primary_agg.count()

# COMMAND ----------

display(primary_agg)

# COMMAND ----------

primary_agg.select("medrec_key").distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC Original paper had about 58,000 patients, we have about 62,000. This is close enough to call accurate given the complete rewrite of the analyses

# COMMAND ----------

# MAGIC %md 
# MAGIC # Feature Aggregation
# MAGIC
# MAGIC At the moment I think the Count Vectorizer should be pushed to the modeling stage and wrapped in the final transformation pipeline.

# COMMAND ----------

# @pat_ftr_agg
# combine all features together per patient, add demographics to end
def pat_ftr_agg(sdf):
    out = (sdf
           .select("*",
                   F.flatten(F.collect_list(F.col("ftr_key")).over(W.partitionBy(F.col("medrec_key")))).alias("ftr")
                  )
           .withColumn("ftr_all", F.concat(F.col("ftr"), F.col("dem_ftr")))
           .select("medrec_key", "ftr_all", "outcomes")
           .distinct()
          )
    return(out)



# COMMAND ----------

# apply
AGG_ALL = False
if AGGR_ALL:
    primary_agg2 = pat_ftr_agg(primary_agg)
else:
    primary_agg2 = primary_agg

# COMMAND ----------

# display(primary_agg2)
# primary_agg2.count()

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# MAGIC %md 
# MAGIC # Write Datasets

# COMMAND ----------

# create a new catalog if one doesn't exist
# spark.sql("USE CATALOG hive_metastore")
# spark.sql("CREATE SCHEMA TWJ8_PREMIER")

# check tables in catalog
spark.catalog.listTables("twj8_premier")

# COMMAND ----------

# write table to catalog
primary_agg2.write.format("delta").mode("overwrite").saveAsTable("twj8_premier.20230120features")
ftr_dict_out.write.format("delta").mode("overwrite").saveAsTable("twj8_premier.20230120feature_dict")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Check existing tables
# MAGIC -- DESCRIBE DETAIL twj8_premier.ftr_dict
# MAGIC -- drop table
# MAGIC -- DROP TABLE IF EXISTS twj8_premier.ftr_dict

# COMMAND ----------

# MAGIC %md
# MAGIC # Data Directly from premier
# MAGIC - will require columns fixed

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.davsynapseanalyticsdev.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.davsynapseanalyticsdev.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-client-id"))
spark.conf.set("fs.azure.account.oauth2.client.secret.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-client-secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-tenant-id-endpoint"))

# COMMAND ----------

icd1 = spark.table("cdh_premier.icdcode")
# proc1 = ps.read_table("cdh_premier.paticd_proc")
# diag1 = ps.read_table("cdh_premier.paticd_diag")
# billing is weird, possibly hsopchg and patbill linked on hsop_chg_id
# bill1 = ps.read_table("cdh_premier.hospchg") and bill2 = read_table("cdh_premier.patbill")
# vitals1 = ps.read_table("cdh_premier.vitals")
# genlab1 = ps.read_table("cdh_premier.genlab")
# labres1 = ps.read_table("cdh_premier.lab_res")

# COMMAND ----------

display(icd1)

# COMMAND ----------

import mlflow
mlflow.__version__
