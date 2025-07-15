# Databricks notebook source
# MAGIC %md
# MAGIC # Create Time to (CCSR) Event Table
# MAGIC
# MAGIC This notebook generates a table with one unique patient per row and 400+ columns for each CCSR category. The value of in the cell of any given patient-CCSR pair indicates the number of days from that medrec_key's index date until their first post covid condition.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2022-08-08

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Contact Information
# MAGIC *Who to contact for questions about this notebook?*
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC - Author: Adewole Oyalowo
# MAGIC   - Email: [sve5@cdc.gov](sve5@cdc.gov) <!--- enter your email address in both the square brackets and the parens -->
# MAGIC
# MAGIC - Author: Ka'imi Kahihikolo
# MAGIC   - Email: [rvn4@cdc.gov](rvn4@cdc.gov) <!--- enter your email address in both the square brackets and the parens -->

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Configuration

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Imports and Settings

# COMMAND ----------

spark.version


# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.window as W
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import scipy.stats as ana

from pyspark.sql.functions import col, concat, expr, lit

# COMMAND ----------

# spark configuration
spark.conf.set('spark.sql.shuffle.partitions', 6400)
spark.conf.set('spark.databricks.adaptive.autoOptimizeShuffle.enabled', True)
spark.conf.set('spark.databricks.queryWatchdog.enabled', True)

# COMMAND ----------

import util.conditions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filters and Definitions

# COMMAND ----------

# MAGIC %run ./includes/0000-GLOBAL-NAMES

# COMMAND ----------

# this has been removed
#%run ../includes/0000-hv-filters

# COMMAND ----------

icd_desc=spark.read.table("cdh_premier.icdcode")
icd_code=spark.read.table("cdh_premier.paticd_diag")

patcpt=spark.read.table("cdh_premier.patcpt")
cptcode = spark.table("cdh_premier.cptcode")

patlabres=spark.read.table("cdh_premier.lab_res")
genlab=spark.read.table("cdh_premier.genlab")

disstat=spark.read.table("cdh_premier.disstat")

pattype=spark.read.table("cdh_premier.pattype")
providers=spark.read.table("cdh_premier.providers")
zip3=spark.read.table("cdh_premier.phd_providers_zip3")
prov=providers.join(zip3,"prov_id","inner")

patbill=spark.read.table("cdh_premier.patbill")
chgmstr=spark.read.table("cdh_premier.chgmstr")
hospchg = spark.table("cdh_premier.hospchg")

readmit=spark.read.table("cdh_premier.readmit")
patdemo=spark.read.table("cdh_premier.patdemo")

ccsr_lookup = spark.table("cdh_reference_data.icd10cm_diagnosis_codes_to_ccsr_categories_map")

# COMMAND ----------

# get the year/quarter/month vars from adm_mon
enc_date = (
    patdemo
    .select(
        "pat_key",
        # "medrec_key",
        # "adm_mon",
        F.substring(F.col("adm_mon"), 1,4).alias("year"),
        F.substring(F.col("adm_mon"), 5,1).alias("quarter"),
        F.substring(F.col("adm_mon"), 6,2).alias("month")
    )
)

display(enc_date.limit(10))

# COMMAND ----------

# DB_NAME = "cdh_premier"
# #DB_NAME = "cdh_hv_race_ethnicity_covid"
# #MED_TABLE_NAME = "medical_claims"
# #PHARMACY_TABLE_NAME = "pharmacy_claims"
# #ENROLL_TABLE_NAME = "enrollment"
# #LAB_TABLE_NAME = "lab_tests_cleansed"
# LONG_OUTPUT_NAME = "twj8_medrec_covid_ccsr"
# WIDE_OUTPUT_NAME = "twj8_medrec_ccsr_wide_cohort"
# TABLE1 = "twj8_medrec_icd"
# TABLE2 = "twj8_medrec_cpt"
# TABLE3 = "twj8_medrec_lab"

# COMMAND ----------

DATE_RANGE_COND = (
    # Date is in 2020 but in or after March
    (
        (F.col("year") == 2020) 
        & (F.col("adm_mon") >= 2020103)
    ) 
    # Or date is in 2021 but in or before November
    | (
        (F.col("year") == 2021)
        & (F.col('adm_mon') <= 2021411)
    )
)

CI_DATE_RANGE_COND = (
    # Date is in 2020 but in or after March
    (
        (F.col("year") == 2020) 
        & (F.col("adm_mon") >= 2020103)
    ) 
    | (
    (F.col("year") == 2021)
    )
    
    # Or date is in 2022 but in or before MAY
    | (
        (F.col("year") == 2022)
        & (F.col('adm_mon') <= 2022205)
    )
)

ENROLL_DATE_RANGE_COND = (
    # Date is in any month in 2019, 2020, or 2021
    F.col("year").isin([2019,2020,2021])
        
    # Or date is in 2022 but in or before May
    |(
        (F.col("year") == 2022)
        & (F.col('adm_mon') <= 2022205)
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Exclucsion

# COMMAND ----------

EXCLUSION_LAB = [
    "SARS coronavirus 2 RNA:PrThr:Pt:Respiratory:Ord:Probe.amp.tar",
    "SARS coronavirus 2 RNA:PrThr:Pt:XXX:Ord:Probe.amp.tar"
]

EXCLUSION_DIAG_CODES = [
    "U071",
    "U072",
    "U099",
    "B342",
    "B9721",
    "B9729",
    "J1281",
    "J1282",
    "M3581",
    "Z8616",
    "B948",
    "B949"   
]

EXCLUSION_NDC_CODES = [ 
    #not used in Premier
    "61755004202",
    "61755004502",
    "61755003502",
    "61755003608",
    "61755003705",
    "61755003805",
    "61755003901",
    "61755002501",
    "61755002701",
    "00002791001",
    "00002795001",
    "00002791001",
    "00002795001",
    "00173090186",
    "00002758901",
    "00069108530",
    "00069108506",
    "00006505506",
    "00006505507",
    "61958290202",
    "61968290101"
]

EXCLUSION_PROCEDURE_CODES = [
    #unchanged from HV
    "Q0240",
    "Q0243",
    "Q0244",
    "M0240",
    "M0241",
    "M0243",
    "M0244",
    "Q0245",
    "M0245",
    "M0246",
    "Q0247",
    "M0247",
    "M0248",
    "Q0222",
    "M0222",
    "M0223",
    "J0248"
]

EXCLUSION_TREATMENT_NAME = [
    #unchanged from HV
    "casirivimab",
    "imdevimab",
    "etesevimab",
    "bamlanivimab",
    "sotrovimab",
    "bebtelovimab",
    "paxlovid",
    "molnupiravir",
    "remdesivir"
]

# COMMAND ----------

# Get the ICD code dataset
# rename icd_code to icd_code_original
# select pat_key, ICD_PRI_SEC, year, quarter, icd_code_original
# Create new col icd_code from icd_cod_original, replace the "." with ""

# icd_code=(icd_code.withColumnRenamed('icd_code', 'icd_code_original')
#     .select("pat_key", 
#         "ICD_PRI_SEC", 
#         "year", 
#         "quarter", 
#         "icd_code_original", 
#         F.translate(F.col("icd_code_original"), ".", "").alias("icd_code")
#     )
# )


# COMMAND ----------

icd_code2=(
    icd_code
    # .join(enc_date, on="pat_key", how="left")
    .withColumnRenamed('icd_code', 'icd_code_original')
    .select("pat_key", 
        "ICD_PRI_SEC", 
        # "year", 
        # "quarter", 
        # "month",
        "icd_code_original", 
        F.translate(F.col("icd_code_original"), ".", "").alias("icd_code")
    )
)

patbill2 = (
    patbill
    # .join(enc_date, on ="pat_key", how="left")
    .join(chgmstr.select("std_chg_code", "std_chg_desc")
    , on = "std_chg_code", how = "left")
)

patcpt2 = (
    patcpt
    # .join(enc_date, on = "pat_key", how="left")
    .join(
        cptcode,
        on = "cpt_code",
        how = "left"
    )
)

pathospchg2 = (
    patbill
    # .join(enc_date, on="pat_key", how="left")
    .join(hospchg, on="hosp_chg_id", how="left")
)

# COMMAND ----------

# create indicator datasets for having the records

DRUGS = ["PAXLOVID", "MOLNUPIR","EVUSHELD","TIXAGEVIMAB","CILGAVIMAB","BEBTELOVIMA","SOTROVIMAB","BAMLANIVIMAB","ETESEVIMAB","REGEN-COV","CASIRIVIMAB","IMDEVIMAB","DEXAMETHASONE","TOFACITINIB", "TOCILIZUMAB","SARILUMAB","BARICITINIB","REMDESIVIR"]

filt_lambda = lambda i: i.isin(DRUGS)

patbill_drug_ind = (
    patbill2
    .select(
        "pat_key",
        "std_chg_desc"
    )
    .withColumn("desc", F.split(F.col("std_chg_desc"), " "))
    .withColumn(
        "drug_ind",
        F.exists(
            F.col("desc"),
            filt_lambda
        )
    )
    .filter(F.col("drug_ind") == True)
    .select(
        "pat_key",
        "drug_ind"
    )
)


patcpt_drug_ind = (
    patcpt2
    .select(
        "pat_key",
        "cpt_desc"
    )
    .withColumn("desc", F.split(F.col("cpt_desc"), " "))
    .withColumn(
        "drug_ind",
        F.exists(
            F.col("desc"),
            filt_lambda
        )
    )
    .filter(F.col("drug_ind") == True)
    .select(
        "pat_key",
        "drug_ind"
    )
)

hospchg_drug_ind = (
    pathospchg2
    .select(
        "pat_key",
        "hosp_chg_desc"
    )
    .withColumn("desc", F.split(F.col("hosp_chg_desc"), " "))
    .withColumn(
        "drug_ind",
        F.exists(
            F.col("desc"),
            filt_lambda
        )
    )
    .filter(F.col("drug_ind") == True)
    .select(
        "pat_key",
        "drug_ind"
    )
)



# COMMAND ----------

# join drug tables
drug_ind = (
    patbill_drug_ind
    .union(patcpt_drug_ind)
    .union(hospchg_drug_ind)
    .distinct()
)

drug_ind.display()

# need to do join to the patdemo to get the dates
    # select distinct MEDREC_KEY
    # from cdh_premier.patdemo pat
    # inner join meds on (pat.PAT_KEY=meds.PAT_KEY)
    # where meds.YEAR >= 2020 and pat.adm_mon >= 2020103 or pat.adm_mon <= 2022205

# COMMAND ----------

# Basically get a list of patients who have had the covid meds in the years 2020 -> 2022
# Using the patbill, chgmstr, patcpt, cptcode, hospchg, patdemo
# Filter patients who have the covid drugs from the chgmstr, cptcode, hospchg coded variables
# The patbill and patcpt has the patient identifier
# Completed in the following combinations
# patbill and chgmstr codes
# patcpt and cpt codes
# patbill and hospchg
# finally get medrec_key from the patdemo

# Create Meds table
# - from patbill, left join chgmstr on chrmstr.std_chg_code == pb.st_chg_code
# - filter on std_chg_desc like covid drugs and year >= 2020
# - Union with
# - from patpct, left join cptcode on patcpt.cpt_code == cptcode.cpt_code
# - filter cpt_desc like covid drugs 
# - Union with
# - patbill, left join hospchg on hosp_chg_id == hosp_chg_id
# - filter hosp_chg_id like covid drugs
# - inner join medrec

# Covid Mediations Table
# meds = spark.sql("""
# with meds as (
#         select
#           distinct pat_key, YEAR
#         from
#            cdh_premier.patbill pb
#            left join cdh_premier.chgmstr charge on charge.std_chg_code = pb.std_chg_code
#           where
#           upper(std_chg_desc) like '%PAXLOVID%'
#           or upper(std_chg_desc) like '%MOLNUPIR%'
#           or upper(std_chg_desc) like '%EVUSHELD%'
#           or upper(std_chg_desc) like '%TIXAGEVIMAB%'
#           or upper(std_chg_desc) like '%CILGAVIMAB%'
#           or upper(std_chg_desc) like '%BEBTELOVIMA%'
#           or upper(std_chg_desc) like '%SOTROVIMAB%'
#           or upper(std_chg_desc) like '%BAMLANIVIMAB%'
#           or upper(std_chg_desc) like '%ETESEVIMAB%'
#           or upper(std_chg_desc) like '%REGEN-COV%'
#           or upper(std_chg_desc) like '%CASIRIVIMAB%'
#           or upper(std_chg_desc) like '%IMDEVIMAB%'
# --           or upper(std_chg_desc) like '%DEXAMETHASONE%'
# --           or upper(std_chg_desc) like '%TOFACITINIB%'
# --           or upper(std_chg_desc) like '%TOCILIZUMAB%'
# --           or upper(std_chg_desc) like '%SARILUMAB%'
# --           or upper(std_chg_desc) like '%BARICITINIB%'
#           or upper(std_chg_desc) like '%REMDESIVIR%'
#           and YEAR>=2020
       
#         union



#        select
#           distinct pat_key, YEAR
#           from
#           cdh_premier.patcpt patcpt
#           left join cdh_premier.cptcode cpt_lookup on patcpt.cpt_code = cpt_lookup.cpt_code
      
#           where
#           upper(cpt_desc) like '%PAXLOVID%'
#           or upper(cpt_desc) like '%MOLNUPIR%'
#           or upper(cpt_desc) like '%EVUSHELD%'
#           or upper(cpt_desc) like '%TIXAGEVIMAB%'
#           or upper(cpt_desc) like '%CILGAVIMAB%'
#           or upper(cpt_desc) like '%BEBTELOVIMA%'
#           or upper(cpt_desc) like '%SOTROVIMAB%'
#           or upper(cpt_desc) like '%BAMLANIVIMAB%'
#           or upper(cpt_desc) like '%ETESEVIMAB%'
#           or upper(cpt_desc) like '%REGEN-COV%'
#           or upper(cpt_desc) like '%CASIRIVIMAB%'
#           or upper(cpt_desc) like '%IMDEVIMAB%'
# --           or upper(cpt_desc) like '%DEXAMETHASONE%'
# --           or upper(cpt_desc) like '%TOFACITINIB%'
# --           or upper(cpt_desc) like '%TOCILIZUMAB%'
# --           or upper(cpt_desc) like '%SARILUMAB%'
# --           or upper(cpt_desc) like '%BARICITINIB%'
#           or upper(cpt_desc) like '%REMDESIVIR%'



#        union



#        select
#           distinct pat_key, YEAR
#           from
#           cdh_premier.patbill pb
#           left join cdh_premier.hospchg hospcharge on hospcharge.hosp_chg_id = pb.hosp_chg_id
#           where
#           upper(hosp_chg_desc) like '%PAXLOVID%'
#           or upper(hosp_chg_desc) like '%MOLNUPIR%'
#           or upper(hosp_chg_desc) like '%EVUSHELD%'
#           or upper(hosp_chg_desc) like '%TIXAGEVIMAB%'
#           or upper(hosp_chg_desc) like '%CILGAVIMAB%'
#           or upper(hosp_chg_desc) like '%BEBTELOVIMA%'
#           or upper(hosp_chg_desc) like '%SOTROVIMAB%'
#           or upper(hosp_chg_desc) like '%BAMLANIVIMAB%'
#           or upper(hosp_chg_desc) like '%ETESEVIMAB%'
#           or upper(hosp_chg_desc) like '%REGEN-COV%'
#           or upper(hosp_chg_desc) like '%CASIRIVIMAB%'
#           or upper(hosp_chg_desc) like '%IMDEVIMAB%'
#           or upper(hosp_chg_desc) like '%REMDESIVIR%'



# )
    select distinct MEDREC_KEY
    from cdh_premier.patdemo pat
    inner join meds on (pat.PAT_KEY=meds.PAT_KEY)
    where meds.YEAR >= 2020 and pat.adm_mon >= 2020103 or pat.adm_mon <= 2022205
""")

# COMMAND ----------

# View each table 
# Using the patbill, chgmstr, patcpt, cptcode, hospchg, patdemo
t = spark.sql("""
    select *
    from cdh_premier.patbill
""")
display(t)
t = spark.sql("""
    select *
    from cdh_premier.chgmstr
""")
display(t)
t = spark.sql("""
    select *
    from cdh_premier.patcpt
""")
display(t)
t = spark.sql("""
    select *
    from cdh_premier.cptcode
""")
display(t)
t = spark.sql("""
    select *
    from cdh_premier.hospchg
""")
display(t)
t = spark.sql("""
    select *
    from cdh_premier.patdemo
""")
display(t)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Medrec_ICD

# COMMAND ----------

medrec_icd = (patdemo
        .join(prov, "prov_id", "inner")
        .join(icd_code, "pat_key", "inner")
        .join(readmit.select("pat_key", "days_from_index"), "pat_key", "inner")
        .select(
        'medrec_key',
        'pat_key',
        #'data_vendor',
        'race',
        'hispanic_ind',
        'gender', #'patient_gender',
        'age', #'patient_year_of_birth',
        'prov_region',
        'prov_division',
        'PROV_ZIP3',
        #'patient_state', Not available in Premier -- prov_region?
        #'claim_type', not available in Premier
        'adm_mon', #'date_service',
        'disc_mon', #'date_service_end',
        F.substring(F.col("adm_mon"), 1,4).alias('year'),
        #'inst_type_of_bill_std_id', -- patbill?
        'prov_id', #'place_of_service_std_id',
        #'ndc_code', Not available in Premier
        'icd_code', #'diagnosis_code'
        'pat_type',
        'i_o_ind',
        'days_from_index'
        )
           .distinct()
              # create adm_date as the yyyy - MM from adm_mon
           .withColumn('adm_date',
                       F.to_date(concat(F.substring(col('adm_mon'),1,4),
                                        lit("-"),
                                        F.substring(col('adm_mon'),6,7)), 'yyyy-MM')
                      )
              # recode inpatient outpatient
           .withColumn("IO_FLAG",
                       F.when(F.col("i_o_ind").isin("I") & F.col("pat_type").isin(8),1)
                       .when(F.col("i_o_ind").isin("O") & F.col("pat_type").isin(28, 34),0)
                       .otherwise(-1)
                      )
              # recode RaceEth
           .withColumn('RaceEth', #Create RaceEthnicity Categorical Variable
                        F.when(F.col('HISPANIC_IND')=="Y", "Hispanic")
                        .when((F.col('HISPANIC_IND')!="Y") & (F.col('RACE')=="W"), "NH_White")
                        .when((F.col('HISPANIC_IND')!="Y") & (F.col('RACE')=="B"), "NH_Black")
                        .when((F.col('HISPANIC_IND')!="Y") & (F.col('RACE')=="A"), "NH_Asian")
                        .when((F.col('HISPANIC_IND')!="Y") & (F.col('RACE')=="O"), "Other")
                        .when((F.col('HISPANIC_IND')!="Y") & (F.col('RACE')=="U"), "Unknown")
                        .otherwise("NA")
                      )
             )

# medrec_icd.display()

# COMMAND ----------

# Using the spark tables
# join patdemo, prov (provider), icd_code, readmit
# select vars
# recode inpatient outpatient
# create adm_date as the yyyy - MM from adm_mon
 # recode RaceEth

# ID field, date of service (adm_mon +1), diagnosis code - paired with CCSRs
medrec_icd = (patdemo
        .join(prov.withColumnRenamed('year','prov_year'), "prov_id", "inner")
        .join(icd_code.withColumnRenamed('year','icd_year'), "pat_key", "inner")
        .join(readmit.select('pat_key', 'days_from_index'), "pat_key", "inner")
        .select(
        'medrec_key',
        'pat_key',
        #'data_vendor',
        'race',
        'hispanic_ind',
        'gender', #'patient_gender',
        'age', #'patient_year_of_birth',
        'prov_region',
        'prov_division',
        'PROV_ZIP3',
        #'patient_state', Not available in Premier -- prov_region?
        #'claim_type', not available in Premier
        'adm_mon', #'date_service',
        'disc_mon', #'date_service_end',
        'year',
        #'inst_type_of_bill_std_id', -- patbill?
        'prov_id', #'place_of_service_std_id',
        #'ndc_code', Not available in Premier
        'icd_code', #'diagnosis_code'
        'pat_type',
        'i_o_ind',
        'days_from_index'
        )
           .distinct()
              # create adm_date as the yyyy - MM from adm_mon
           .withColumn('adm_date',
                       F.to_date(concat(F.substring(col('adm_mon'),1,4),
                                        lit("-"),
                                        F.substring(col('adm_mon'),6,7)), 'yyyy-MM')
                      )
              # recode inpatient outpatient
           .withColumn("IO_FLAG",
                       F.when(F.col("i_o_ind").isin("I") & F.col("pat_type").isin(8),1)
                       .when(F.col("i_o_ind").isin("O") & F.col("pat_type").isin(28, 34),0)
                       .otherwise(-1)
                      )
              # recode RaceEth
           .withColumn('RaceEth', #Create RaceEthnicity Categorical Variable
                        F.when(F.col('HISPANIC_IND')=="Y", "Hispanic")
                        .when((F.col('HISPANIC_IND')!="Y") & (F.col('RACE')=="W"), "NH_White")
                        .when((F.col('HISPANIC_IND')!="Y") & (F.col('RACE')=="B"), "NH_Black")
                        .when((F.col('HISPANIC_IND')!="Y") & (F.col('RACE')=="A"), "NH_Asian")
                        .when((F.col('HISPANIC_IND')!="Y") & (F.col('RACE')=="O"), "Other")
                        .when((F.col('HISPANIC_IND')!="Y") & (F.col('RACE')=="U"), "Unknown")
                        .otherwise("NA")
                      )
             )

# COMMAND ----------

display(medrec_icd)

# COMMAND ----------

# Not Run
#spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","false") #Revert to false after executed

# COMMAND ----------

# MAGIC %md
# MAGIC # Current Datasets are:
# MAGIC   "meds" - containing list of MEDREC_KEY of patients who had the covid drugs
# MAGIC   "medrec_icd" - visit, diagnoses code and demographic

# COMMAND ----------

# Write the medrec icd table
(
    medrec_icd
    .write
    .mode('overwrite')
    .format('parquet')
    .saveAsTable(f'{DB_NAME}_exploratory.{TABLE1}')
)

#spark.sql(f'CONVERT TO DELTA {DB_NAME}_exploratory.{TABLE1}')

#spark.sql(f'OPTIMIZE {DB_NAME}_exploratory.{TABLE1}')

# COMMAND ----------

# Convert to delta
spark.sql(f'CONVERT TO DELTA {DB_NAME}_exploratory.{TABLE1}')

# COMMAND ----------

medrec_icd = (
    spark.table(f'{DB_NAME}_exploratory.{TABLE1}')
)

# COMMAND ----------

spark.sql(f'OPTIMIZE {DB_NAME}_exploratory.{TABLE1}')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Medrec_CPT

# COMMAND ----------

# Join patdemo, prov (provider), patcpt
# select vars
# Create vars
# - adm_date
# - inpatient outpatient flag IO_flag
# ID field, date of service (adm_mon +1), diagnosis code - paired with CCSRs

medrec_cpt = (patdemo
        .join(prov.withColumnRenamed('year','prov_year'), "prov_id", "inner")
        .join(patcpt.withColumnRenamed('year','cpt_year'), "pat_key", "inner") #Put into a separate table
        .select(
        'medrec_key',
        'pat_key',
        #'data_vendor',
        'race',
        'hispanic_ind',
        'gender', #'patient_gender',
        'age', #'patient_year_of_birth',
        'prov_region',
        'prov_division',
        #'PROV_ZIP3',
        #'patient_state', Not available in Premier -- prov_region?
        #'claim_type', not available in Premier
        'adm_mon', #'date_service',
        'disc_mon', #'date_service_end',
        #'inst_type_of_bill_std_id', -- patbill?
        'prov_id', #'place_of_service_std_id',
        #'ndc_code', Not available in Premier
        'cpt_code', #'procedure_code',
        #'icd_code', #'diagnosis_code'
        #'test',
        #'observation',
        'pat_type',
        'i_o_ind'
        )
           .withColumn('adm_date',
                       F.to_date(concat(F.substring(col('adm_mon'),1,4),
                                        lit("-"),
                                        F.substring(col('adm_mon'),6,7)), 'yyyy-MM')
                      )
           .withColumn("IO_FLAG",
                       F.when(F.col("i_o_ind").isin("I") & F.col("pat_type").isin(8),1)
                       .when(F.col("i_o_ind").isin("O") & F.col("pat_type").isin(28),0)
                       .otherwise(-1)
                      )
           #.withColumn("CPT_FLAG",
           #            F.when(F.col("cpt_code").isin(EXCLUSION_PROCEDURE_CODES),1).otherwies(0)
           #  )
              
           #.groupBy("medrec_key", "pat_key")
           #.agg(*
           #    [F.max(c).alias(c) for c in[
           #        "CPT_FLAG"
           #    ]])
             )

# COMMAND ----------

# prov.display()
# n = patdemo.count()
n2 = (
    patdemo
    .join(prov, "prov_id", "inner")
).count()

# COMMAND ----------

print(n)
print(n2)

# COMMAND ----------

medrec_cpt.display()

# COMMAND ----------

# Writing these and rereading them causes an error downstream
# (
#    medrec_cpt
#    .write
#    .mode('overwrite')
#    .format('parquet')
#    .saveAsTable(f'{DB_NAME}_exploratory.{TABLE2}')
# )

# COMMAND ----------

# spark.sql(f'CONVERT TO DELTA {DB_NAME}_exploratory.{TABLE2}')

# COMMAND ----------

# medrec_cpt = (
#    spark.table(f'{DB_NAME}_exploratory.{TABLE2}')
# )

# COMMAND ----------

# MAGIC %md
# MAGIC #### Medrec_LAB

# COMMAND ----------

# Create lab table
# Join patdemo, prov, patlabres
# select vars
# Create adm_date, IO_FLAG

# ID field, date of service (adm_mon +1), diagnosis code - paired with CCSRs

medrec_lab = (patdemo
        .join(prov.withColumnRenamed('year','prov_year'), "prov_id", "inner")
        .join(patlabres.withColumnRenamed('year','lab_year'), "pat_key", "inner") #possible - unionByName( allowmissingcol)
        .select(
        'medrec_key',
        'pat_key',
        #'data_vendor',
        'race',
        'hispanic_ind',
        'gender', #'patient_gender',
        'age', #'patient_year_of_birth',
        'prov_region',
        'prov_division',
        #'PROV_ZIP3',
        #'patient_state', Not available in Premier -- prov_region?
        #'claim_type', not available in Premier
        'adm_mon', #'date_service',
        'disc_mon', #'date_service_end',
        #'inst_type_of_bill_std_id', -- patbill?
        'prov_id', #'place_of_service_std_id',
        #'ndc_code', Not available in Premier
        #'cpt_code', #'procedure_code',
        #'icd_code', #'diagnosis_code'
        'test',
        'observation',
        'pat_type',
        'i_o_ind'
        )
           .withColumn('adm_date',
                       F.to_date(concat(F.substring(col('adm_mon'),1,4),
                                        lit("-"),
                                        F.substring(col('adm_mon'),6,7)), 'yyyy-MM')
                      )
           .withColumn("IO_FLAG",
                       F.when(F.col("i_o_ind").isin("I") & F.col("pat_type").isin(8),1)
                       .when(F.col("i_o_ind").isin("O") & F.col("pat_type").isin(28),0)
                       .otherwise(-1)
                      )
             )

# COMMAND ----------

# Writing these and rereading them causes an error downstream
# (
#    medrec_lab
#    .write
#    .mode('overwrite')
#    .format('parquet')
#    .saveAsTable(f'{DB_NAME}_exploratory.{TABLE3}')
# )

# COMMAND ----------

# spark.sql(f'CONVERT TO DELTA {DB_NAME}_exploratory.{TABLE3}')

# COMMAND ----------

# medrec_lab = (
#    spark.table(f'{DB_NAME}_exploratory.{TABLE3}')
# )

# COMMAND ----------

#display(med_record, limit=10)

# COMMAND ----------

# med_claims.cache().count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### CCSR

# COMMAND ----------

ccsr_cond = (
    ccsr_lookup
    .withColumnRenamed("icd-10-cm_code", "icd_code")
    .withColumnRenamed("ccsr_category", "ccsr")
    .filter(
        ~F.col("icd_code").isin(EXCLUSION_DIAG_CODES)
    )
    .select("icd_code", "ccsr")
    # .groupBy("ccsr")
    # .agg(F.collect_set("icd_code").alias("icd_set"))
)

ccsr_cond.display()

# need the al aly subset

# COMMAND ----------

ccsr_lambda = lambda x: x.i

pat_ccsr = (
    icd_code
    .withColumn("icd_code", F.translate(F.col("icd_code"), ".",""))
    .join(ccsr_cond, on = "icd_code", how = "left")
    .select("pat_key", "icd_code", "ccsr")
)

pat_ccsr.display()

# COMMAND ----------

# Read in ccsr_codelist
# filter the Al_ALY ccsrs
# create a new icd_code column
# filter any icd_code in the EXCLUSION_DIAG_CODES
# conditions_df = (
#     spark
#     .table('cdh_reference_data.ccsr_codelist')
#     .filter(
#         'Al_Aly_plus = 1'
#     )
#     .withColumn(
#         'icd_code',
#         F.col('ICD-10-CM_Code')
#     )
#     .filter(
#         ~F.col('icd_code').isin(EXCLUSION_DIAG_CODES)
#     )  
# )

# COMMAND ----------

# Get the list of CCSR CATEGORIES
conditions_list = [row[0] for row in conditions_df.select('CCSR_Category').distinct().collect()]
conditions_list = sorted(conditions_list)

# COMMAND ----------

display(conditions_df, limit=10)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### COVID Indicators
# MAGIC
# MAGIC *note* : This dataframe should be renamed to something like "covid_indicators". This dataframe flags all unique hvids that had at least one indicator of a covid diagnosis based on ICD codes, procedure codes, NDC codes, and text from CDM.

# COMMAND ----------

# Prep meds 
# icd_code2=(
#     icd_code
#     # .join(enc_date, on="pat_key", how="left")
#     .withColumnRenamed('icd_code', 'icd_code_original')
#     .select("pat_key", 
#         "ICD_PRI_SEC", 
#         # "year", 
#         # "quarter", 
#         # "month",
#         "icd_code_original", 
#         F.translate(F.col("icd_code_original"), ".", "").alias("icd_code")
#     )
# )

patbill2 = (
    patbill
    # .join(enc_date, on ="pat_key", how="left")
    .join(chgmstr.select("std_chg_code", "std_chg_desc")
    , on = "std_chg_code", how = "left")
)

patcpt2 = (
    patcpt
    # .join(enc_date, on = "pat_key", how="left")
    .join(
        cptcode,
        on = "cpt_code",
        how = "left"
    )
)

pathospchg2 = (
    patbill
    # .join(enc_date, on="pat_key", how="left")
    .join(hospchg, on="hosp_chg_id", how="left")
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Get COVID encounters
# MAGIC - medrec_icd 
# MAGIC - meds
# MAGIC - medrec_labs
# MAGIC - medrec_cpt
# MAGIC
# MAGIC This identifies covid encounters in the full date range 2020-2022(eos)
# MAGIC These are used for exclusions of patients 
# MAGIC - we will additionally need to identify covid encounters in the inclusion date range

# COMMAND ----------

# cut on inclusion codes and on dates

def get_covid_icd(icd_code):
    EXCLUSION_DIAG_CODES = [
        "U071",
        "U072",
        "U099",
        "B342",
        "B9721",
        "B9729",
        "J1281",
        "J1282",
        "M3581",
        "Z8616",
        "B948",
        "B949"   
    ]

    out = (
        icd_code 
        .withColumn(
            "icd_code", 
            F.translate(F.col("icd_code"),".","")
        )
        .filter(F.col("icd_code").isin(EXCLUSION_DIAG_CODES))
        .select("pat_key", F.lit("icd").alias("cv_type"))
    )
    return out

def get_covid_meds(patbill2, patcpt2, pathospchg2):
    DRUGS = ["PAXLOVID", "MOLNUPIR","EVUSHELD","TIXAGEVIMAB","CILGAVIMAB","BEBTELOVIMA","SOTROVIMAB","BAMLANIVIMAB","ETESEVIMAB","REGEN-COV","CASIRIVIMAB","IMDEVIMAB","DEXAMETHASONE","TOFACITINIB", "TOCILIZUMAB","SARILUMAB","BARICITINIB","REMDESIVIR"]
    filt_lambda = lambda i: i.isin(DRUGS)

    patbill_drug_ind = (
        patbill2
        .select(
            "pat_key",
            "std_chg_desc"
        )
        .withColumn("desc", F.split(F.col("std_chg_desc"), " "))
        .withColumn(
            "drug_ind",
            F.exists(
                F.col("desc"),
                filt_lambda
            )
        )
        .filter(F.col("drug_ind") == True)
        .select(
            "pat_key",
            "drug_ind"
        )
    )
    patcpt_drug_ind = (
        patcpt2
        .select(
            "pat_key",
            "cpt_desc"
        )
        .withColumn("desc", F.split(F.col("cpt_desc"), " "))
        .withColumn(
            "drug_ind",
            F.exists(
                F.col("desc"),
                filt_lambda
            )
        )
        .filter(F.col("drug_ind") == True)
        .select(
            "pat_key",
            "drug_ind"
        )
    )

    hospchg_drug_ind = (
        pathospchg2
        .select(
            "pat_key",
            "hosp_chg_desc"
        )
        .withColumn("desc", F.split(F.col("hosp_chg_desc"), " "))
        .withColumn(
            "drug_ind",
            F.exists(
                F.col("desc"),
                filt_lambda
            )
        )
        .filter(F.col("drug_ind") == True)
        .select(
            "pat_key",
            "drug_ind"
        )
    )
    
    out = (
        patbill_drug_ind
        .union(patcpt_drug_ind)
        .union(hospchg_drug_ind)
        .distinct()
        .select("pat_key", F.lit("med").alias("cv_type"))
    )
    return out


def get_covid_labs(patlabres):
    EXCLUSION_LAB = [
        "SARS coronavirus 2 RNA:PrThr:Pt:Respiratory:Ord:Probe.amp.tar",
        "SARS coronavirus 2 RNA:PrThr:Pt:XXX:Ord:Probe.amp.tar"
    ]

    out = (
        patlabres
        .select("pat_key", "test", "observation")
        .filter(
            (F.col("test").isin(EXCLUSION_LAB)) 
            & 
            (F.col("observation") == "positive")
        )
        .select("pat_key", F.lit("lab").alias("cv_type"))
    )
    return out

def get_covid_cpt(patcpt):
    EXCLUSION_PROCEDURE_CODES = [
        #unchanged from HV
        "Q0240",
        "Q0243",
        "Q0244",
        "M0240",
        "M0241",
        "M0243",
        "M0244",
        "Q0245",
        "M0245",
        "M0246",
        "Q0247",
        "M0247",
        "M0248",
        "Q0222",
        "M0222",
        "M0223",
        "J0248"
    ]
    out = (
        patcpt
        .select("pat_key","cpt_code")
        .filter(
            F.col("cpt_code").isin(EXCLUSION_PROCEDURE_CODES)
        )
        .select("pat_key", F.lit("cpt").alias("cv_type"))
    )    
    return out

def get_all_cv_inc(cv_icd, cv_med, cv_lab, cv_cpt):
    out = (
        cv_icd
        .union(cv_med)
        .union(cv_lab)
        .union(cv_cpt)
    )
    return(out)

# COMMAND ----------

cv_icd = get_covid_icd(icd_code)
cv_med = get_covid_meds(patbill2, patcpt2, pathospchg2)
cv_lab = get_covid_labs(patlabres)
cv_cpt = get_covid_cpt(patcpt)

all_cv_ind = get_all_cv_inc(cv_icd, cv_med, cv_lab, cv_cpt)

dist_cv_ind = all_cv_ind.select("pat_key").distinct()

# COMMAND ----------

# apply date filtering
def get_cv_date_inc(dist_cv_ind, patdemo):
    CI_DATE_RANGE_COND = (
        # Date is in 2020 but in or after March
        ((F.col("year") == 2020) & (F.col("adm_mon") >= 2020103)) 
        | 
        ((F.col("year") == 2021))
        # Or date is in 2022 but in or before MAY
        | 
        ((F.col("year") == 2022) & (F.col('adm_mon') <= 2022205))
    )
    out = (
        dist_cv_ind
        .join(
            (
                patdemo
                .select(
                    "medrec_key", "pat_key", "adm_mon",
                    F.substring(
                        F.col("adm_mon"), 1,4
                    ).alias("year")
                )
            ), 
            on="pat_key", 
            how="left"
        )
        .filter(
            CI_DATE_RANGE_COND
        )
        .select("medrec_key", "pat_key")
    )
    return out

# COMMAND ----------

cv_ind = get_cv_date_inc(dist_cv_ind, patdemo)

# COMMAND ----------

# Create Covid_indicators
# Returns medrec_key from patients with the conditions meeting the EXCLUSION criteria variables using medrec_icd, medrec_lab, medrec_cpt

# From medrec_icd
# filter icd_code in EXLUSION_DIAG_CODES & CI_DATE_RANGE_COND
# select medrec_key
# union meds
# union medrec_lab
# filter test in EXCLUSION_LABS & "observation" == "Positive" and CI_DATE_RANGE_COND
# union medrec_cpt
# filter cpt_codes in EXCLUSION_PROCEDURE_CODES & CI_DATE_RANGE_COND
# select medrec_key and make distinct

covid_indicators = (
    
    # Load all medical claims and search for COVID indicators
    medrec_icd
    
    .filter(
        F.col('icd_code').isin(EXCLUSION_DIAG_CODES) & CI_DATE_RANGE_COND
        #| F.col('cpt_code').isin(EXCLUSION_PROCEDURE_CODES)
        #| F.col('test').isin(EXCLUSION_LAB) & F.col('observation').isin('POSITIVE')
        #| F.col('ndc_code').isin(EXCLUSION_NDC_CODES) -- no analogue in premier
    )
    
    .select(
        'medrec_key'
    )
#     .unionByName(
#         meds
#         .select(
#             'medrec_key'
#         )
#     )
    
    .unionByName(
        medrec_lab
        .select(
            'medrec_key'
        )
#       .filter(
#         F.col("test").isin(EXCLUSION_LAB) & (F.col("observation")=="positive") 
#         & CI_DATE_RANGE_COND)
    )
        
    .unionByName(
        medrec_cpt
        .select(
            'medrec_key'
        )
#     .filter(
#         F.col("cpt_code").isin(EXCLUSION_PROCEDURE_CODES)
#         & CI_DATE_RANGE_COND)
    )        
    .select('medrec_key')
    .distinct()
)


# COMMAND ----------

#Potentially saving the table out
(
    covid_indicators
    .write
    .mode('overwrite')
    .format('parquet')
    .saveAsTable(f'{DB_NAME}_exploratory.twj8_covid_indicators')
)


spark.sql(f'CONVERT TO DELTA {DB_NAME}_exploratory.twj8_covid_indicators')

# COMMAND ----------

covid_indicators = (
    spark.table(f'{DB_NAME}_exploratory.twj8_covid_indicators')
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Enrollment

# COMMAND ----------

def get_medical_enrollment(patdemo):
    ENROLL_DATE_RANGE_COND = (
        # Date is in any month in 2019, 2020, or 2021
        F.col("year").isin([2019,2020,2021])
            
        # Or date is in 2022 but in or before May
        |(
            (F.col("year") == 2022)
            & (F.col('adm_mon') <= 2022205)
        )
    )
    out = (
        patdemo
        .select("medrec_key", "pat_key", 
            F.substring(F.col("adm_mon"), 1, 4).alias("year")
        )
        .filter(ENROLL_DATE_RANGE_COND)
    ) 
    return out

# COMMAND ----------

enroll_period = get_medical_enrollment(patdemo)

# COMMAND ----------

#FLAGING FOR PREMIER CHECK
medical_enrollment = (
    medrec_icd
    # FLAG: Look at this data cond.
    # Limit file size by filtering based on the study period
    .filter(
        ENROLL_DATE_RANGE_COND
    )
    .select('medrec_key', "adm_date", "days_from_index")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### df

# COMMAND ----------



# COMMAND ----------

# Get the main DF
# With medrec_icd
# filter DATE_RANGE_COND
# Create new NULL column "covid"
# Union with medrec_lab with "covid" set to 1
# Filter "test" in EXCLUSION_LAB, "observation" = "positive", DATE_RANGE_COND 
# Group by medrec_key, adm_date, days_from_index
# aggregate columns and collect all icds 
# Flag COVID visits
# Assign rownumbers based on if patient had covid. If they had covid then we assign "date_rn" as ascending dates, else if no covid then assign "rand_rn" as a random observation
# If the pat has covid select the first rec as "date_rn" == 1, else if no covid then select a random rec as "rand_rn" == 1
# Select Vars
# Inner join with medical_enrollment on medrec_key match (with var name change) and the index_date - adm_date within 1-365
# Perform aggregate functions
# Join to covid_indicators
# Select Vars
# fill NA gender:unknown, and covid:0


df = (
  
    # Get all medical claims with standard HV filters applied
    medrec_icd
    .filter(
        DATE_RANGE_COND
    )    
    
    # Select necessary columns from medical claims. Create flags for covid and inpatient indicators.
    .select("*",
        F.lit(None).alias('covid'),
    )
    
    # Get lab information and look for positive lab tests
    .unionByName(
        medrec_lab
        .select(
            'medrec_key',
            F.lit(1).alias('covid'),
        )
        .filter(
            F.col("test").isin(EXCLUSION_LAB) & (F.col("observation")=="positive") 
            & DATE_RANGE_COND
            ),
        allowMissingColumns = True
    )
    
    # Generate one row per hvid-date-index trio
    .groupBy(
        'medrec_key',
        'adm_date', #YYYYQMM
        'days_from_index'
    )

    .agg(
        *[
            F.max(c).alias(c) for c in [ 
                'prov_division', #zip
                'prov_region',
                'prov_zip3',
                'io_flag',
                'age',
                'gender',
                'raceEth',
                'covid'
            ]
        ],
        *[
            F.collect_set(c).alias(c) for c in [
                'icd_code', 
            ]
        ],
    )
    # Flag COVID based on identifiers (diagnosis code, lab, etc.)
    .withColumn(
        'covid',
        F.when(
            (F.col('covid') == 1)
            | (
                F.array_contains('icd_code', 'U071')
                & F.col('adm_date').between(F.lit('2020-04-01').cast('date'), F.lit('2021-11-30').cast('date'))
            )
            | (
                F.array_contains('icd_code', 'B9729')
                & F.col('adm_date').between(F.lit('2020-03-01').cast('date'), F.lit('2020-04-30').cast('date'))
            ),
            1
        )
        .otherwise(0)
    )    
    # Assign a row number to a patient based on whether they had covid or not
    .select(
        '*',
        F.max('covid').over(W.Window.partitionBy('medrec_key')).alias('covid_patient'),
        F.row_number().over(W.Window.partitionBy('medrec_key').orderBy(F.desc('covid'),'adm_date')).alias('date_rn'),
        F.row_number().over(W.Window.partitionBy('medrec_key').orderBy('adm_date').orderBy(F.rand(42))).alias('rand_rn'),
        
    )    
    
    # If patient ever had covid, select earliest covid claim. If patient never had covid, select a random claim.
    .filter(
        (
            (F.col('covid_patient') == 1) 
            & (F.col('date_rn') == 1)
        )
        | (
            (F.col('covid_patient') == 0) 
            & (F.col('rand_rn') == 1)
        )
    )    
    
    # Clean up dataframe
    .select(
        'medrec_key',
        F.col('adm_date').alias('index_date'), #YYYYMMDD
        'covid',
        'io_flag',
        'age',
        'gender',
        'RaceEth',
        'prov_division',
        'prov_region',
        'prov_zip3',
        F.col('days_from_index').alias('index_dfi') ##### 
    )   
    # Add enrollment information #INCOMPLETE
    .join(
        medical_enrollment.withColumnRenamed('medrec_key','record_medrec_key'),
        [
            # Match hvid
            (F.col('medrec_key') == F.col('record_medrec_key')) &

            # Enrollment at least 1 year before covid index, and 0.5 year after. ### here we're more interested in the presence of an encounter
            (
                F.datediff('adm_date', 'index_date').between(-365, -1)
            )
        ],
        "inner"
    )
     .drop('record_medrec_key')
    
    .groupBy('medrec_key')
    .agg(
        F.countDistinct("adm_date").cast('integer').alias("days_enrolled"), ###Encoutner Count
        *[
            F.min(c).alias(c) for c in [
                'index_date',
                'covid',
                'io_flag',
                'age',
                'gender',
                'RaceEth',
                'prov_division',
                'prov_region',
                'prov_zip3',
                'index_dfi'
                
            ]
        ]
    )
    
    # Flag controls based on presence of covid indicators (e.g. medications)
    .join(
        covid_indicators.withColumn('covid_indicator', F.lit(1)).withColumnRenamed('medrec_key','ci_medrec_key'),
        [(F.col('medrec_key') == F.col('ci_medrec_key'))],
        'left'
    )
    .drop('ci_medrec_key')
    
    # Clean dataframe
    .select(
        'medrec_key',
        'days_enrolled',
        'index_date',
        'covid',
        'covid_indicator',
        'io_flag',
        'age',
        'gender',
        'RaceEth',
        'prov_division',
        'prov_region',
        'prov_zip3',
        'index_dfi'
    )
    
    .na.fill(
        {
            'covid_indicator':0,
            'gender':'Unknown',
        }
    )
    
)    #one row for every id per person 1 'covid' 'index_date' 'adm_date'

# COMMAND ----------

# %sql clear cache

# COMMAND ----------

df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Conditions & Claims

# COMMAND ----------

# Create ccrs_med_claims from medrec_icd
# select all non missing icd_code recs
# Select Vars
# Join DF and ccsr_med_claims

ccsr_med_claims = (
    # Use all medical claims
    medrec_icd

    # Require diganosis codes for claims
    .filter(
        ~F.col('icd_code').isin('', ' ')
        & F.col('icd_code').isNotNull()

    )

    # Select necessary columns
    .select(
        'medrec_key',
        'adm_date',
        'days_from_index', #ccsr_dfi 
        'icd_code',
    )
)

combined_df = (
    df
    .join(
        ccsr_med_claims.withColumnRenamed('medrec_key','ccsr_medrec_key')
        ,
        [
            (F.col('medrec_key') == F.col('ccsr_medrec_key')),
            F.datediff(F.col('adm_date'), F.col('index_date')).between(-365, 180)
        ]
        ,
        'left'
    )
    .drop('ccsr_medrec_key') 
    
    # Add pre-existing conditions
    .join(
        F.broadcast(conditions_df.select('icd_code','CCSR_Category')),
        'icd_code',
        'left'
    )
)

# COMMAND ----------

combined_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Write Combined DF

# COMMAND ----------

#Resolved error - table already exists
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

(
spark.createDataFrame(range(0),schema="`id` INTEGER")
    .write
    .mode('overwrite')
    .format('parquet')
    .saveAsTable(f'{DB_NAME}_exploratory.{LONG_OUTPUT_NAME}')
)

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","false")

# COMMAND ----------

(
    combined_df
    .write
    .mode('overwrite')
    .format('parquet')
    .saveAsTable(f'{DB_NAME}_exploratory.{LONG_OUTPUT_NAME}')
)
#5.26 minutes

# COMMAND ----------

spark.sql(f'CONVERT TO DELTA {DB_NAME}_exploratory.{LONG_OUTPUT_NAME}')

# COMMAND ----------

spark.sql(f'OPTIMIZE {DB_NAME}_exploratory.{LONG_OUTPUT_NAME}')

# COMMAND ----------

combined_df = (
    spark.table(f'{DB_NAME}_exploratory.{LONG_OUTPUT_NAME}')
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Write Wide DF

# COMMAND ----------

condition_claims_df = (
    
    combined_df

    # Flags
    .select(
        '*',
        days_elapsed := (F.col('days_from_index') - F.col('index_dfi')).alias('days_elapsed'),
        period := F.when(
            days_elapsed.between(-365, -7), 
            'a'
        ).when(
            days_elapsed.between(31, 180),
            'c'
        )
        .otherwise('o')
        .alias('period'),
        F.row_number().over(W.Window.partitionBy('medrec_key','CCSR_Category').orderBy(period,'index_date')).alias('rn')
    )
    
    .filter(
        (F.col('rn') == 1)
    )

    
    # Pivot table so that all CCSR categories are columns
    .groupBy(
        'medrec_key',
        'days_enrolled',
        'index_dfi',
        'index_date',
        'covid',
        'covid_indicator',
        'io_flag',
        'age',
        'gender',
        'RaceEth',
        'prov_division',
        'prov_region'
        
    )
    .pivot('CCSR_Category', conditions_list)
    .agg(
        F.min('days_elapsed'),
    )
    
)

# COMMAND ----------

display(condition_claims_df)

# COMMAND ----------

# spark configuration
spark.conf.set('spark.sql.shuffle.partitions', 6400)
spark.conf.set('spark.databricks.adaptive.autoOptimizeShuffle.enabled', True)
spark.conf.set('spark.databricks.queryWatchdog.enabled', True)

# COMMAND ----------

(
    condition_claims_df
    .write
    .mode('overwrite')
    .format('parquet')
    .saveAsTable(f'{DB_NAME}_exploratory.{WIDE_OUTPUT_NAME}')
)

# COMMAND ----------

spark.sql(f'CONVERT TO DELTA {DB_NAME}_exploratory.{WIDE_OUTPUT_NAME}')

# COMMAND ----------

spark.sql(f'OPTIMIZE {DB_NAME}_exploratory.{WIDE_OUTPUT_NAME}')

# COMMAND ----------

display(condition_claims_df)

# COMMAND ----------

condition_claims_df.select('medrec_key').count()
