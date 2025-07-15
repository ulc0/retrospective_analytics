# Databricks notebook source
# MAGIC %md
# MAGIC # Set-up

# COMMAND ----------


import pyspark.sql.functions as F

DATE_RANGE_COND = (
    # Date is in 2020 but in or after March
    (
        (F.col("year") == 2020) 
        & (F.col("admit_date") >= 2020103)
    ) 
    # Or date is in 2021 but in or before November
    | (
        (F.col("year") == 2021)
        & (F.col('admit_date') <= 2021411)
    )
)

CI_DATE_RANGE_COND = (
    # Date is in 2020 but in or after March
    (
        (F.col("year") == 2020) 
        & (F.col("admit_date") >= 2020103)
    ) 
    | (
    (F.col("year") == 2021)
    )
    
    # Or date is in 2022 but in or before MAY
    | (
        (F.col("year") == 2022)
        & (F.col('admit_date') <= 2022205)
    )
)

ENROLL_DATE_RANGE_COND = (
    # Date is in any month in 2019, 2020, or 2021
    F.col("year").isin([2019,2020,2021])
        
    # Or date is in 2022 but in or before May
    |(
        (F.col("year") == 2022)
        & (F.col('admit_date') <= 2022205)
    )
)

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

dbutils.widgets.text("catalog",defaultValue="edav_prd_cdh")
dbutils.widgets.text("schema",defaultValue="cdh_sandbox")
dbutils.widgets.text("src_catalog",defaultValue="edav_prd_cdh")
dbutils.widgets.text("src_schema",defaultValue="cdh_premier")
CATALOG=dbutils.widgets.get("catalog")
SCHEMA=dbutils.widgets.get("schema")
SRC_CATALOG=dbutils.widgets.get("src_catalog")
SRC_SCHEMA=dbutils.widgets.get("src_schema")

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.window as W
# import numpy as np
# import pandas as pd
# import matplotlib.pyplot as plt
# import scipy.stats as ana

# from pyspark.sql.functions import col, concat, expr, lit

# COMMAND ----------

DISPLAY = False
CACHE = False
SAMPLE = False

# COMMAND ----------

# MAGIC %md
# MAGIC # Data

# COMMAND ----------

icd_desc=spark.read.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.icdcode")
paticd=spark.read.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.paticd_diag")

patcpt=spark.read.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.patcpt")
cptcode = spark.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.cptcode")

patlabres=spark.read.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.lab_res")
genlab=spark.read.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.genlab")

disstat=spark.read.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.disstat")

pattype=spark.read.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.pattype")
#providers=spark.read.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.providers")
#zip3=spark.read.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.phd_providers_zip3")
#prov=providers.join(zip3,"prov_id","inner")
#payor = spark.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.payor")

patbill=spark.read.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.patbill")
chgmstr=spark.read.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.chgmstr")
hospchg = spark.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.hospchg")

#readmit=spark.read.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.readmit")
patdemo=spark.read.table(f"{SRC_CATALOG}.{SRC_SCHEMA}.patdemo")

ccsr_lookup = spark.table(f"{CATALOG}.cdh_reference_data.icd10cm_diagnosis_codes_to_ccsr_categories_map")

# COMMAND ----------

# MAGIC %md 
# MAGIC # Inclusion patients

# COMMAND ----------

# Apply initial exclusion period
inc_enc = (
        patdemo
        .select("pat_key", "medrec_key", "admit_date", 
    )
)

# prior inclusion
# 405595269
# post inclusion
# 174230455

# COMMAND ----------



# COMMAND ----------

# from the subset of encounters in the date_range_cond, identify records meeting the "primary covid event" criteria. These records identify patients who are in the covid_patient cohort



# identify covid encounters by labs in the date_range_cond
def get_covid_incl_lab(inc_dtst, patlab, DATE_RANGE_COND):
   
    return 

# COMMAND ----------

# identify covid encounters by icd in specified ranges
# apply covid encounter identification
cov_icd =  cov_cond = F.when(
#        (
            (F.col('icd_code') == 'U071')
#            & 
#            (F.col('admit_date').between(F.lit('2020-04-01').cast('date'), F.lit('2021-11-30').cast('date')))
#        )
        | 
        (
            (F.col('icd_code') == 'B9729')
#            & 
#            (F.col('admit_date').between(F.lit('2020-03-01').cast('date'), F.lit('2020-04-30').cast('date')))
        ),
        1
        ).otherwise(0)
    
icd = (
        paticd
        .select(
            "pat_key",
            F.translate(F.col("icd_code"), ".","").alias("icd_code")
        )
        .filter(F.col("icd_code").isin(["U071", "B9729"]))
    )
out = (
        icd
        .join(inc_enc, on = "pat_key", how = "inner")
        .withColumn("covid",
                    cov_cond
        )
        .filter(F.col("covid") == 1)
        .select("pat_key", "covid")
    )
#    return out
# def get_covid_incl_icd(inc_dtst, paticd):
#    get_covid_incl_icd(inc_enc, paticd)
display(cov_icd)


# COMMAND ----------


#get_covid_incl_lab(inc_enc, patlabres, DATE_RANGE_COND)

cov_lab =  EXCLUSION_LAB = [
        "SARS coronavirus 2 RNA:PrThr:Pt:Respiratory:Ord:Probe.amp.tar",
        "SARS coronavirus 2 RNA:PrThr:Pt:XXX:Ord:Probe.amp.tar"
    ]
    
    lab = (
        patlab
        .select("pat_key", "test", "observation")
        .filter(
            (F.col("test").isin(EXCLUSION_LAB))
            &
            (F.col("observation")=="positive")
        )
        .withColumn("covid", F.lit(1))
    )
    out = (
        lab
        .join(
            inc_enc, on="pat_key", how="inner"
        )
        .select("pat_key", "covid")
    )
    return out

def cov_icd_lab_union(cov_icd, cov_lab):
    out = (
        cov_icd
        .union(cov_lab)
        .distinct()
    )
    return out

def rejoin_inc_enc_cov_enc(inc_enc, cov_enc):
    out = (
        inc_enc
        .join(cov_enc, on = "pat_key", how="left")
        .fillna(0, "covid")
    )
display(cov_lab)
# union the covid encounters
# get unique encounter "pat_keys"
cov_enc = cov_icd_lab_union(cov_icd, cov_lab)

# Rejoin the cov_enc (covid encounters) to the inc_enc (encounters meeting primary inclusion range)
inc_enc2 = rejoin_inc_enc_cov_enc(inc_enc, cov_enc)

# COMMAND ----------

if DISPLAY:
    cov_icd.count()
    # all encounters with icd U071, B9729
    # 8039092
    # all encounters with icd code and in range
    # 4685402
    cov_lab.count()
    # 501287

# COMMAND ----------

if DISPLAY:
    print(inc_enc2.count())
    inc_enc2.display()

# COMMAND ----------

# Set the primary index encounter
# This involves identifying the first covid visit (if covid patient as defined above) or 
# identifying a random encounter (if non-covid patient) from the date_range_cond encounters subset
# Keep only the primary index encounter for each patient
def get_primary_date(inc_enc2):
    out = (
        inc_enc2
        .select(
            "*"
            , F.max('covid')
            .over(W.Window.partitionBy('medrec_key'))
            .alias('covid_patient')
            , F.row_number()
            .over(W.Window.partitionBy('medrec_key').orderBy(F.desc('covid'),'admit_date'))
            .alias('date_rn')
            , F.row_number()
            .over(W.Window.partitionBy('medrec_key').orderBy('admit_date').orderBy(F.rand(42)))
            .alias('rand_rn')
        )
        .filter(
            (
                (F.col('covid_patient') == 1) 
                & (F.col('date_rn') == 1)
            )
            | 
            (
                (F.col('covid_patient') == 0) 
                & (F.col('rand_rn') == 1)
            )
        ) 
    )
    return out

# COMMAND ----------

# identify and select the primary encounter for each patient
enc_primary = get_primary_date(inc_enc2)

# COMMAND ----------

if DISPLAY:
    print(enc_primary.count())
    enc_primary.display()

# COMMAND ----------

# Quality Check
if DISPLAY:
    enc_primary.select("medrec_key").distinct().count()
#     straight count
#     64609349
#     distinct count
#     64609349

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write encounters primary table

# COMMAND ----------

# write intermediate
enc_primary.write.mode("overwrite").format("parquet").saveAsTable(f"{CATALOG}.{SCHEMA}.01_enc_primary")

# COMMAND ----------

# read intermediate
enc_primary1 = spark.table(f"{CATALOG}.{SCHEMA}.01_enc_primary")

# COMMAND ----------

if DISPLAY:
    enc_primary.display()
    enc_primary.filter(F.col("covid_patient")==1).count()
    # 3252008 covid patients

# COMMAND ----------

# MAGIC %md
# MAGIC # Demographic Data
# MAGIC
# MAGIC - age, gender, race_ethnicity, zip3, payertype

# COMMAND ----------

def get_demographics(enc_primary, readmit, patdemo, prov, payor, pattype):
    out = (
        enc_primary
        .join(
            readmit
            .select("pat_key", 
                    "days_from_index", 
                    F.col("age").cast("integer").alias("age"), 
                    "gender",
                    "calc_los"
                   )
            ,on = "pat_key"
            ,how = "left"
        )
        .join(
            patdemo
            .select(
                "pat_key",
                "race",
                "hispanic_ind",
                "std_payor",
                "prov_id",
                "i_o_ind"
                ,"pat_type"
            )
            ,on="pat_key"
            ,how="left"
        )
        .join(
            prov
            ,on="prov_id"
            ,how="left"
        )
        .join(
            payor
            ,on="std_payor"
            ,how="left"
        )
        .join(
            pattype
            ,on="pat_type"
            ,how="left"
        )
       .withColumn("IO_FLAG",
                   F.when(F.col("i_o_ind").isin("I") & F.col("pat_type").isin(8),1)
                   .when(F.col("i_o_ind").isin("O") & F.col("pat_type").isin(28, 34),0)
                   .otherwise(-1)
          )
       .withColumn('RaceEth', #Create RaceEthnicity Categorical Variable
            F.when(F.col('HISPANIC_IND')=="Y", "Hispanic")
            .when((F.col('HISPANIC_IND')!="Y") & (F.col('RACE')=="W"), "NH_White")
            .when((F.col('HISPANIC_IND')!="Y") & (F.col('RACE')=="B"), "NH_Black")
            .when((F.col('HISPANIC_IND')!="Y") & (F.col('RACE')=="A"), "NH_Asian")
            .when((F.col('HISPANIC_IND')!="Y") & (F.col('RACE')=="O"), "Other")
            .when((F.col('HISPANIC_IND')!="Y") & (F.col('RACE')=="U"), "Unknown")
            .otherwise("NA")
          )
        .drop("teaching", "beds_grp", "cost_type", "i_o_ind", "race", "hispanic_ind", "std_payor", "pat_type", "pat_type_desc", "prov_id")
    )
    return out


# COMMAND ----------

# get demographic fields
enc_dem = get_demographics(enc_primary1, readmit, patdemo, prov, payor, pattype)

if DISPLAY:
    enc_dem.display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Exclusion

# COMMAND ----------

# MAGIC %md
# MAGIC ## Demographic Exclusion

# COMMAND ----------

# exclude on age and gender
def get_initial_exclusion(enc_dem): 
    out = (
        enc_dem
        .filter(
            (F.col("age") >= 18)
            &
            (F.col("age") < 65)
        )
        .filter(
            (F.col("gender").isin(["M","F"]))
        )
    ) 
    return out

# COMMAND ----------

# apply demographic exclusions
enc_excl1 = get_initial_exclusion(enc_dem)

if DISPLAY:
    enc_excl1.count()
    # with primary exclusion
    # 39141226 
    # 39141346 new

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write demographics dataset

# COMMAND ----------

# write demographics dataset
enc_excl1.write.mode("overwrite").format("parquet").saveAsTable(f"{CATALOG}.{SCHEMA}.02_demographics")
enc_excl2 = spark.table(f"{CATALOG}.{SCHEMA}.02_demographics")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sample

# COMMAND ----------

# get a sample and write
# SAMPLE default = False
if SAMPLE:
    samp1 = enc_excl2.sample(.001, seed=11)
#     print(samp1.count())
    samp1.write.mode("overwrite").format("parquet").saveAsTable(f"{CATALOG}.{SCHEMA}.sample1")
    enc_exl = spark.read.table(f"{CATALOG}.{SCHEMA}.sample1")
else:
    enc_exl = enc_excl2
    

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Enrollment Exclusion

# COMMAND ----------

# 1. set up p1,p2,p3 periods around primary dfi (prim_dfi)
# - with an adjustment of prim_dfi = 0 to prim_dfi - calc_los
# - this standardizes all prim_dfi to be set at the start of the encounter
# - - if this adjustment is not made then any prim_dfi == 0 (first encounter) will be offset to the encounter end. 
# - - this results in a different time measurement for these prim_dfi ==0 encounter than what 
# - - would be encountered in the other prim_dfi != 0
# - - basically this just makes sure all dfi are set to the admission date
# 2. join all encounters to the patients within the inclusion/exclusion set (w/ criteria applied to this point)
# - the same adjustment logic needs to be applied to days_from_index
# 3. set indicators if joined encounters are within the calculated dfi windows for p1,p2,p3
# 4. get total sum of num records in p1,p2,p3 dfi windows
def get_exclusion_enrollment(enc_primary2, readmit):
    ranges = (
        enc_primary2
        .select(
            "medrec_key",
            "admit_date",
            F.col("calc_los").cast("integer").alias("calc_los"),
            F.col("days_from_index").cast("integer").alias("prim_dfi")
            )
        .withColumn(
            "prim_dfi",
            F.when(
                F.col("prim_dfi") == 0
                , F.col("prim_dfi")-F.col("calc_los")
            ).otherwise(F.col("prim_dfi"))
        )
        .drop("calc_los")
        .withColumn(
            "prim_p1_start", 
            F.col("prim_dfi") -365
        )
        .withColumn(
            "prim_p1_end",
            F.col("prim_dfi") - 8
        )
        .withColumn(
            "prim_p2_start",
            F.col("prim_dfi") - 7
        )
        .withColumn(
            "prim_p2_end",
            F.col("prim_dfi") + 30
        )
        .withColumn(
            "prim_p3_start",
            F.col("prim_dfi") + 31
        )
        .withColumn(
            "prim_p3_end",
            F.col("prim_dfi") + 395
        )
    )
    
    ranges2 = (
        ranges
        .join(
            readmit
            .select("medrec_key", 
                    "pat_key",
                    "disc_mon",
                    "calc_los",
                    F.col("days_from_index").cast("integer").alias("days_from_index")
            )
            .withColumn(
                "days_from_index",
                F.when(
                    F.col("days_from_index") == 0
                    , F.col("days_from_index")-F.col("calc_los")
                ).otherwise(F.col("days_from_index"))
            )
            .drop("calc_los")
            ,
            on = "medrec_key",
            how = "left"
        )
        .withColumn(
            "p1_ind",
            F.when(
                (F.col("days_from_index") >= F.col("prim_p1_start")) &
                (F.col("days_from_index") <= F.col("prim_p1_end")),1).otherwise(0)
        )
        .withColumn(
            "p2_ind",
            F.when(
                (F.col("days_from_index") >= F.col("prim_p2_start"))
                &
                (F.col("days_from_index") <= F.col("prim_p2_end")),
                1)
            .otherwise(0)
        )
        .withColumn(
            "p3_ind",
            F.when(
                (F.col("days_from_index") >= F.col("prim_p3_start"))
                &
                (F.col("days_from_index") <= F.col("prim_p3_end")),
                1)
            .otherwise(0)
        )
    )
    
    
    ranges3 = (
        ranges2
        .groupBy("medrec_key")
        .agg(
            F.sum(F.col("p1_ind")).alias("p1_sum"),
            F.sum(F.col("p2_ind")).alias("p2_sum"),
            F.sum(F.col("p3_ind")).alias("p3_sum")
        )
    )
    out = (
        ranges2
        .join(
            ranges3,
            on="medrec_key",
            how="left"
        )
    )
    ranges2.unpersist()
    return out

    

# COMMAND ----------

# get enrollement exclusion fields
exl = get_exclusion_enrollment(enc_exl, readmit)


if DISPLAY:
    exl.cache()
    print(exl.count())
    exl.display()
    
# 17663 when not adjusted for prim_dfi==0
# 17663 when adjusted for prim_dfi==0

# COMMAND ----------

if DISPLAY:
    (
        exl
        .select("medrec_key","admit_date")
        .distinct()
        .groupby("admit_date")
        .agg(F.count(F.col("admit_date")))
    ).display()


    (
        exl
        .select("medrec_key","p1_sum")
        .distinct()
        .groupby("p1_sum")
        .agg(F.count(F.col("p1_sum")))
    ).display()


    (
        exl
        .select("medrec_key","p2_sum")
        .distinct()
        .groupby("p2_sum")
        .agg(F.count(F.col("p2_sum")))
    ).display()


    (
        exl
        .select("medrec_key","p3_sum")
        .distinct()
        .groupby("p3_sum")
        .agg(F.count(F.col("p3_sum")))
    ).display()
# Validation that admit_date is distrubited mostly uniform, with a slight taper off in the first months of the covid inclusion period
# Visualizations of the number of patients with specific frequencies of p1,p2,p3 encounters

# COMMAND ----------

# apply enrollement exclusions
def exclude_enc_not_range(exl):
    out = (
        exl
        .filter(
            ~(
                (F.col("p1_ind") == 0) &
                (F.col("p2_ind") == 0) &
                (F.col("p3_ind") == 0)
            )
        )
    )
    return(out)

def exclude_p3_miss(exl):
    out = (
        exl
        .filter(F.col("p3_sum")!=0)
    )
    return out

# sensitivity exclusion on p1 (requiring some records prior to primary encounter)
def exclude_p1_miss(exl):
    out = (
        exl
        .filter(F.col("p1_sum")!=0)
    )
    return out

# COMMAND ----------

# apply enroll exclusions
exl2 = exclude_enc_not_range(exl)
exl3 = exclude_p3_miss(exl2)
exl4 = exclude_p1_miss(exl3)

if DISPLAY:
    print(exl2.count())
    print(exl2.select("medrec_key").distinct().count())
    # 10390 encounters included
    # 3897 patients included

    print(exl3.count())
    print(exl3.select("medrec_key").distinct().count())
    # 5145 encounters included
    # 811 patients included

    print(exl4.count())
    print(exl4.select("medrec_key").distinct().count())
    # 4165 encounters included
    # 497 patient included

# COMMAND ----------


exl4.write.mode("overwrite").format("delta").saveAsTable(f"{CATALOG}.{SCHEMA}.03_enrollment_exclusion2")
exl4 = spark.table(f"{CATALOG}.{SCHEMA}.03_enrollment_exclusion2")

# COMMAND ----------

if DISPLAY:
    exl4.cache()
    print(exl4.count())
    exl4.display()

# COMMAND ----------

# MAGIC %md
# MAGIC exl4 
# MAGIC - this dataset is p1,p2,p3 encounters for the subset patients
# MAGIC - p1,p2,p3 are all calculated from the prim_dfi (primary date of tte count)
# MAGIC - since these ranges are already cut into this dtst, the medrec keys can be directly used to query the addl covid-exlcusions in any period for non-covid prim_dfi patients

# COMMAND ----------

# MAGIC %md
# MAGIC ## Covid DX/PROC/LAB/MED Exclusion
# MAGIC
# MAGIC This will create and apply exclusion criteria based on a covid type encounter applied to all of the encounters in p1,p2,p3.
# MAGIC
# MAGIC This is different than the covid logic applied in earlier steps as this has broader criteria and a broader range of encounters.
# MAGIC
# MAGIC The exclusion will applied to the subset of patients previously identified as non-covid

# COMMAND ----------

# create sub datasets
patbill2 = (
    exl4
    .select("pat_key")
    .join(
        patbill
        ,on = "pat_key"
        ,how = "inner"
    )
    .select("pat_key", "std_chg_code")
    .join(
        chgmstr.select("std_chg_code", "std_chg_desc")
        ,on = "std_chg_code"
        ,how = "left"
    )
)

patcpt2 = (
    exl4
    .select("pat_key")
    .join(
        patcpt
        ,on = "pat_key"
        ,how = "inner"
    )
    .select("pat_key", "cpt_code")
    .join(
        cptcode,
        on = "cpt_code",
        how = "left"
    )
)

pathospchg2 = (
    exl4
    .select("pat_key")
    .join(
        patbill
        ,on="pat_key"
        ,how="inner"
    )
    .select("pat_key", "hosp_chg_id")
    .join(
        hospchg, 
        on="hosp_chg_id", 
        how="left"
    )
)

paticd2 = (
    exl4
    .join(
        paticd
        ,on="pat_key"
        ,how="inner"
    )
)

patlabres2 = (
    exl4
    .join(
        patlabres
        ,on="pat_key"
        ,how="inner"
    )
)

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
  
def get_distinct_cv_ind(all_cv_ind):
    out = (
        all_cv_ind
        .select("pat_key")
        .distinct()
        .withColumn("cov_ind", F.lit(1))
    )
    return out

# COMMAND ----------

# get covid fields
cv_icd = get_covid_icd(paticd2)
cv_med = get_covid_meds(patbill2, patcpt2, pathospchg2)
cv_lab = get_covid_labs(patlabres2)
cv_cpt = get_covid_cpt(patcpt2)

# join all covid field
all_cv_ind = get_all_cv_inc(cv_icd, cv_med, cv_lab, cv_cpt)

# get the distinct covid_ind per encounter
dist_cv_ind = get_distinct_cv_ind(all_cv_ind)

# COMMAND ----------

def exclude_noncovpat_w_cov_broad(exl4, enc_primary2, dist_cv_ind):
    out = (
        exl4
        .join(enc_primary2
              .select("medrec_key", "covid_patient")
              .distinct(), 
              on="medrec_key", how = "left")
        .join(dist_cv_ind, on="pat_key", how="left")
        .fillna(0, "cov_ind")
        .withColumn(
            "any_cov_noncovpat",
            F.max(
                F.col("cov_ind")
            )
            .over(W.Window.partitionBy(F.col("medrec_key")))
        )
        .filter(
            ~((F.col("covid_patient") == 0) & 
              (F.col("any_cov_noncovpat") == 1)
             )
        )
    )
    return out

# COMMAND ----------

# apply the noncovpat_w_cov exclusion
# get the final cohort
coh = exclude_noncovpat_w_cov_broad(exl4, enc_exl, dist_cv_ind)

coh.write.mode("overwrite").format("delta").saveAsTable(f"{CATALOG}.{SCHEMA}.04_exclusion_noncov_cov2")
coh = spark.table(f"{CATALOG}.{SCHEMA}.04_exclusion_noncov_cov2")

if DISPLAY:
    coh.cache()
    coh.count()
    coh.display()

# COMMAND ----------

# coh = spark.table(f"{CATALOG}.{SCHEMA}.04_exclusion_noncov_cov")

# COMMAND ----------

coh.display()

# COMMAND ----------

# check that there are no duplicate encounters
if DISPLAY:
    (
        coh
        .select("pat_key")
        .distinct()
    ).count()

# COMMAND ----------

# MAGIC %md
# MAGIC # CCSR

# COMMAND ----------

# MAGIC %md
# MAGIC ## CCSR in range

# COMMAND ----------

# get ccsrs
def get_ccsrs(coh, paticd, ccsr_code):
    out = (
        coh
        .join(
            paticd
            .select("pat_key", "icd_code")
            .withColumn("icd_code", F.translate(F.col("icd_code"), ".", ""))
            ,on = "pat_key"
            ,how = "left"
        )
        .join(
            ccsr_code
            .withColumnRenamed("icd-10-cm_code", "icd_code")
            .select("icd_code", "ccsr_category")
            ,on = "icd_code"
            ,how = "left"
        )
        .withColumn(
            "ccsr_set",
            F.collect_set(F.col("ccsr_category"))
            .over(W.Window.partitionBy(F.col("pat_key")))
        )
        .drop("ccsr_category","icd_code")
        .distinct()
    )
    return out


# COMMAND ----------

DISPLAY = False

# COMMAND ----------

coh_ccsr = get_ccsrs(coh, paticd, ccsr_lookup)

if DISPLAY:
    coh_ccsr.cache()
    coh_ccsr.count()
    coh_ccsr.display()

# COMMAND ----------

(coh_ccsr
    .filter(F.col("p3_sum") > 3)
).display()

# COMMAND ----------

# get tte
def get_tte(coh_ccsr):
    out = (
        coh_ccsr
        .withColumn(
            "tte_ccsr"
            , F.col("days_from_index") - F.col("prim_dfi")
        )
    )
    return out

# COMMAND ----------

# apply tte
coh_tte = get_tte(coh_ccsr)

if DISPLAY:
    coh_tte.display()

# COMMAND ----------

coh_tte.write.mode("overwrite").format("delta").saveAsTable(f"{CATALOG}.{SCHEMA}.05_ccsrs_tte2")

# COMMAND ----------

# # Run all

# # Apply initial exclusion period
# inc_enc = get_inclusion_period_rec(patdemo, DATE_RANGE_COND)

# # apply covid encounter identification
# cov_icd = get_covid_incl_icd(inc_enc, paticd)
# cov_lab = get_covid_incl_lab(inc_enc, patlabres, DATE_RANGE_COND)

# # union the covid encounters
# # get unique encounter "pat_keys"
# cov_enc = (
#     cov_icd
#     .union(cov_lab)
#     .distinct()
# )

# # Rejoin the cov_enc (covid encounters) to the inc_enc (encounters meeting primary inclusion range)
# inc_enc2 = (
#     inc_enc
#     .join(cov_enc, on = "pat_key", how="left")
#     .fillna(0, "covid")
# )

# # get the primary dates for each patient
# enc_primary = get_primary_date(inc_enc2)

# # write primary dates
# (enc_primary.write.mode("overwrite")
# #  .format("parquet")
#  .format("delta")
#  .saveAsTable(f"{CATALOG}.{SCHEMA}.01_enc_primary")
# )
# # read in file
# enc_primary1 = spark.table(f"{CATALOG}.{SCHEMA}.01_enc_primary")

# # get demographic fields
# enc_dem = get_demographics(enc_primary1, readmit, patdemo, prov, payor, pattype)

# # apply demographic exclusions
# enc_excl1 = get_initial_exclusion(enc_dem)

# # write demographics dataset
# (
#     enc_excl1.write.mode("overwrite")
# #      .format("parquet")
#     .format("delta")
#     .saveAsTable(f"{CATALOG}.{SCHEMA}.02_demographics")
# )
# enc_excl2 = spark.table(f"{CATALOG}.{SCHEMA}.02_demographics")

# ################################################
# # get a sample and write
# if SAMPLE:
#     samp1 = enc_excl2.sample(.001, seed=11)
#     (samp1
#      .write.mode("overwrite")
#      .format("parquet")
#      .saveAsTable(f"{CATALOG}.{SCHEMA}.sample1")
#     )
#     enc_exl = spark.read.table(f"{CATALOG}.{SCHEMA}.sample1")
# else:
#     enc_exl = enc_excl2
    
# #########################################################   
# # get enrollement exclusion fields
# exl = get_exclusion_enrollment(enc_exl, readmit)

# # apply enroll exclusions
# exl2 = exclude_enc_not_range(exl)
# exl3 = exclude_p3_miss(exl2)
# exl4 = exclude_p1_miss(exl3)

# ###################################################
# # create sub datasets
# patbill2 = (
#     exl4
#     .select("pat_key")
#     .join(
#         patbill
#         ,on = "pat_key"
#         ,how = "inner"
#     )
#     .select("pat_key", "std_chg_code")
#     .join(
#         chgmstr.select("std_chg_code", "std_chg_desc")
#         ,on = "std_chg_code"
#         ,how = "left"
#     )
# )

# patcpt2 = (
#     exl4
#     .select("pat_key")
#     .join(
#         patcpt
#         ,on = "pat_key"
#         ,how = "inner"
#     )
#     .select("pat_key", "cpt_code")
#     .join(
#         cptcode,
#         on = "cpt_code",
#         how = "left"
#     )
# )

# pathospchg2 = (
#     exl4
#     .select("pat_key")
#     .join(
#         patbill
#         ,on="pat_key"
#         ,how="inner"
#     )
#     .select("pat_key", "hosp_chg_id")
#     .join(
#         hospchg, 
#         on="hosp_chg_id", 
#         how="left"
#     )
# )

# paticd2 = (
#     exl4
#     .join(
#         paticd
#         ,on="pat_key"
#         ,how="inner"
#     )
# )

# patlabres2 = (
#     exl4
#     .join(
#         patlabres
#         ,on="pat_key"
#         ,how="inner"
#     )
# )
# ####################################################
# # get covid fields
# cv_icd = get_covid_icd(paticd2)
# cv_med = get_covid_meds(patbill2, patcpt2, pathospchg2)
# cv_lab = get_covid_labs(patlabres2)
# cv_cpt = get_covid_cpt(patcpt2)

# # join all covid field
# all_cv_ind = get_all_cv_inc(cv_icd, cv_med, cv_lab, cv_cpt)

# # get the distinct covid_ind per encounter
# dist_cv_ind = (
#     all_cv_ind
#     .select("pat_key")
#     .distinct()
#     .withColumn("cov_ind", F.lit(1))
# )

# # apply exclusions
# coh = exclude_noncovpat_w_cov_broad(exl4, 
#                                     enc_exl, 
#                                     dist_cv_ind)

# # get coh ccrs
# coh_ccsr = get_ccsrs(coh, paticd, ccsr_lookup)

# # get tte
# coh_tte = get_tte(coh_ccsr)

# # write coh ccrs
# (coh_tte.write.mode("overwrite")
# #  .format("parquet")
#  .format("delta")
#  .saveAsTable(f"{CATALOG}.{SCHEMA}.03_ccsrs_tte")
# )
