# Databricks notebook source
#Initialize Functions and Libraries
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
patlabres=spark.read.table("cdh_premier.lab_res")
genlab=spark.read.table("cdh_premier.genlab")
disstat=spark.read.table("cdh_premier.disstat")
pattype=spark.read.table("cdh_premier.pattype")
patbill=spark.read.table("cdh_premier.patbill")
patdemo=spark.read.table("cdh_premier.patdemo")
providers=spark.read.table("cdh_premier.providers")
readmit=spark.read.table("cdh_premier.readmit")
ccsr_lookup = spark.table("cdh_reference_data.ccsr_codelist")

# COMMAND ----------

# MAGIC %run ./includes/Create_Covid_Cohort_sd

# COMMAND ----------

first_enc = first_covid_cohort(diagnosis=True, labs=True, loinc=False)

# COMMAND ----------

DB_WRITE = "cdh_premier_exploratory"

COVID_CODES = ["U07.1", "U07.2", "B97.29"]
EXCLUSION_DIAG_CODES = ["A4189", "B342", "B972", "B9721", "B948",  "J1282", "J1281", "J1289", "M358", "M303", "M3581", "U07.2"]
EXCLUSION_PROCEDURE_CODES = ["Q0240", "Q0243", "Q0244", "M0240", "M0241", "M0243", "M0244", "Q0245", "M0245", "M0246", "Q0247", "M0247", "M0248", "Q0222", "M0222", "M0223","J0248"]

# Date is after march 2020
DATE_RANGE_COND = (
    (
        (F.col("adm_mon") >= "2020-03-01")
    )
)

# Continuous Enrollment Gap
CONT_ENROLL_GAP = 1  # months

# COMMAND ----------

first_enc.write.mode("overwrite").partitionBy("adm_mon").saveAsTable(f"{DB_WRITE}.rvj5_first_enc")

# COMMAND ----------

#med_claims = spark.table(f"{DB_READ}.medical_claims")
#cdm_diag = spark.table(f"{DB_READ}.cdm_diag")
#lab_tests = spark.table(f"{DB_READ}.lab_tests_cleansed")
#enrollment = spark.table(f"{DB_READ}.enrollment")
#demographic_lookup = spark.table(f"{DB_WRITE}.covid_full_hvids_demographics_0715")
#ccsr_lookup = spark.table("cdh_reference_data.ccsr_codelist")

icd_desc=spark.read.table("cdh_premier.icdcode")
icd_code=spark.read.table("cdh_premier.paticd_diag")
patcpt=spark.read.table("cdh_premier.patcpt")
patlabres=spark.read.table("cdh_premier.lab_res")
genlab=spark.read.table("cdh_premier.genlab")
disstat=spark.read.table("cdh_premier.disstat")
pattype=spark.read.table("cdh_premier.pattype")
patbill=spark.read.table("cdh_premier.patbill")
patdemo=spark.read.table("cdh_premier.patdemo")
readmit=spark.read.table("cdh_premier.readmit")

# COMMAND ----------

patdemo = (patdemo
           .withColumnRenamed('adm_mon','adm_mon_original')
           .withColumn('adm_mon',
                       F.to_date(concat(F.substring(col('adm_mon_original'),1,4),
                                        lit("-"),
                                        F.substring(col('adm_mon_original'),6,7)), 'yyyy-MM')
                      )
)

# COMMAND ----------

display(patdemo.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## COVID Identification

# COMMAND ----------

window_spec = Window.partitionBy("medrec_key").orderBy(F.col("adm_mon").asc())

covid_cdm = (
    cdm_diag
        .select("medrec_key", F.col("adm_mon").alias("start"))
        .filter(DATE_RANGE_COND)
        .filter(
            F.col("icd_code").isin(COVID_CODES)
        )
        .withColumn(
            "rn",
            F.row_number().over(window_spec)
        )
        .filter(F.col("rn") == 1)
        .select("medrec_key", "adm_mon")
)

# covid_cdm.write.mode("overwrite").saveAsTable(f"{DB_WRITE}.rvn4_covid_cdm")

covid_claims = (
    med_claims
        .filter(DATE_RANGE_COND)
        .filter(
            F.col("icd_code").isin(COVID_CODES)
        )
        .withColumn(
            "rn",
            F.row_number().over(window_spec)
        )
        .filter(F.col("rn") == 1)
        .select("medrec_key", "adm_mon")
)

# covid_claims.write.mode("overwrite").saveAsTable(f"{DB_WRITE}.rvn4_covid_claims")

covid_labs = (
    lab_tests
        .filter(
            (F.col("observation") == "Positive") &
            (~F.lower(F.col("test")).rlike("covid antibody"))
        )
        .withColumn(
            "rn",
            F.row_number().over(window_spec)
        )
        .filter(F.col("rn") == 1)
        .select("medrec_key", "adm_mon")
)

# covid_labs.write.mode("overwrite").saveAsTable(f"{DB_WRITE}.rvn4_covid_labs")

# COMMAND ----------

# covid_cdm = spark.table(f"{DB_WRITE}.rvn4_covid_cdm")
# covid_claims = spark.table(f"{DB_WRITE}.rvn4_covid_claims")
# covid_labs = spark.table(f"{DB_WRITE}.rvn4_covid_labs")

covid_positive = (
    covid_cdm
        .union(covid_claims)
        .union(covid_labs)
    
    .groupBy("medrec_key")
    .agg(
        F.min("adm_mon").alias("covid_index_date")
    )
    .select(
        "medrec_key",
        "covid_index_date",
        F.year(F.col("covid_index_date")).alias("year"),
        F.month(F.col("covid_index_date")).alias("month")
    )
)

covid_positive.write.mode("overwrite").partitionBy("year", "month").saveAsTable(f"{DB_WRITE}.rvn4_covid_positive")

# COMMAND ----------

covid_positive = spark.table(f"{DB_WRITE}.rvn4_covid_positive")

covid_positive.count()

# COMMAND ----------

first_enc

#Variables required for child complexity algorithm: Need payor type, index date, prior to encoutner, inpatient variables

# COMMAND ----------

test=(first_enc
      .select("adm_mon")
      .withColumn("covid_month",(F.col("adm_mon")[len("adm_mon")-2:]))
     )

# COMMAND ----------

covid_positive = (first_enc
                  .select("medrec_key", "adm_mon", "year")
                  #.withColumnRenamed("medrec_key","covid_medrec_key")
                  .withColumnRenamed("year","covid_year")  
                  .withColumn('covid_index_date',
                              F.to_date(concat(F.substring(col('adm_mon'),1,4),
                                             lit("-"),
                                             F.substring(col('adm_mon'),6,7)), 'yyyy-MM')
                             )
                  #.withColumn('covid_index_date_test', F.date_format('covid_index_date', 'yyyy-MM'))
)          

#F.col("adm_mon").alias("covid_index_date"),
#F.col("year").alias("covid_year"),
#covid_positive.count()

# COMMAND ----------

display(covid_positive)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Construct Cases
# MAGIC - Age <= 64
# MAGIC - Continuous enrollment 1 year before + 6 months after, allow for 45 day gaps.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filter Continuous Enrollment

# COMMAND ----------

covid_positive

# COMMAND ----------

#TEST CELL. Outdated
covid_inclusion = (
    covid_positive
        .select("medrec_key", "covid_index_date", "covid_year")
        .join(
            patdemo,
            [
                # Match hvid - medrec_key
                (covid_positive["medrec_key"] == patdemo["medrec_key"]) &
                # Within a year (used just for partition operation boost)
                (F.abs(covid_positive["covid_year"] - patdemo["year"]) <= 1) #&  
                # Enrollment at least 1 year before covid index, and 0.5 year after.
                (
                    (F.datediff(covid_positive["covid_index_date"], patdemo["adm_mon"]) <= 365) &
                    (F.datediff(covid_positive["covid_index_date"], patdemo["adm_mon"]) >= -180)
                )
            ],
            "inner"
        )
        #.groupBy("medrec_key")
        #.agg(
        #    F.countDistinct("calendar_date").alias("day_enrolled")
        #)
)



#covid_enrollment.write.saveAsTable(f"{DB_WRITE}.rvn4_covid_enrollment_window")

# COMMAND ----------

covid_inclusion = (
    covid_positive.alias("a")
        .select("medrec_key", "covid_index_date", "covid_year")
        .join(
            patdemo.alias("b"),
            [
                # Match hvid - medrec_key
                (F.col('a.medrec_key') == F.col('b.medrec_key')) &
                # Within a year (used just for partition operation boost)
                ((F.col('a.covid_year') - F.col('b.year') <= 1)) &  
                # Enrollment at least 1 year before covid index, and 0.5 year after.
                (
                    (F.datediff(F.col('a.covid_index_date'), F.col('b.adm_mon')) <= 365) &
                    (F.datediff(F.col('a.covid_index_date'), F.col('b.adm_mon')) >= -180)
                )
             ],
            "inner"
        )
        .groupBy("a.medrec_key")
        .agg( #contrary to HV, this would count the number of months recorded encoutners relative to an index covid encounter instead of days
            F.countDistinct("adm_mon").alias("encounter_inclusion")
        )
)

# COMMAND ----------

display(covid_positive.agg(F.countDistinct("medrec_key")))
display(patdemo.agg(F.countDistinct("medrec_key")))
display(patdemo.agg(F.countDistinct("pat_key")))

# COMMAND ----------

display(patdemo.count())

display(patdemo.limit(10))
display(covid_inclusion.limit(10))

# COMMAND ----------

display(
    covid_inclusion
        .groupBy("encounter_inclusion")
        .count()
        .orderBy("encounter_inclusion")
)

# COMMAND ----------

#Not For Premier
#display(
#    spark.table(f"{DB_WRITE}.rvn4_covid_enrollment_window")
#        .filter(
#            F.col("day_enrolled") >= (365+180-CONT_ENROLL_GAP) # 1.5 year + 45 day gap
#        )
#        .select(F.count("hvid"))
#)

# COMMAND ----------

#covid_enrollment = spark.table(f"{DB_WRITE}.rvn4_covid_enrollment_window")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Get demographics and filter by Age

# COMMAND ----------

demo_prov = (patdemo
            .join(
            providers,
            "prov_id",
            "inner"
            )
            )

# COMMAND ----------

cases = (
    covid_positive
        .join(
            covid_inclusion,
                #.filter(
                #    F.col("day_enrolled") >= (365+180-CONT_ENROLL_GAP) # 1.5 year + 45 day gap
                #),
            "medrec_key",
            "inner"
        )
        # Retrieve demographic information
        .join(
            demo_prov
                .select(
                    "medrec_key",
                    F.col("AGE"),
                    F.col("GENDER"),
                    F.col("PROV_DIVISION"),
                    F.col("RACE"),
                    F.col("HISPANIC_IND")
                ),
            "medrec_key",
            "inner"
        )
        .filter(
            (F.col("age")) <= 64
        )
        .withColumn('Race_Eth', #Create RaceEthnicity Categorical Variable
                F.when(F.col('HISPANIC_IND')=="Y", "Hispanic")
                .when((F.col('HISPANIC_IND')!="Y") & (F.col('RACE')=="W"), "NH_White")
                .when((F.col('HISPANIC_IND')!="Y") & (F.col('RACE')=="B"), "NH_Black")
                .when((F.col('HISPANIC_IND')!="Y") & (F.col('RACE')=="A"), "NH_Asian")
                .when((F.col('HISPANIC_IND')!="Y") & (F.col('RACE')=="O"), "Other")
                .when((F.col('HISPANIC_IND')!="Y") & (F.col('RACE')=="U"), "Unknown")
                .otherwise("NA"))
)

#cases.write.mode("overwrite").saveAsTable(f"{DB_WRITE}.rvn4_covid_cases")

# COMMAND ----------

#cases = spark.table(f"{DB_WRITE}.rvn4_covid_cases")

display(cases.select(F.countDistinct("medrec_key")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collect Previous Conditions

# COMMAND ----------

display(ccsr_lookup.select(F.countDistinct("ICD-10-CM_Code")))
display(ccsr_lookup.select(F.countDistinct("CCSR_Category")))
display(ccsr_lookup.filter(F.col("Al_Aly") == "1").select(F.countDistinct("CCSR_Category")))
# swap to Al_Aly_plus

# COMMAND ----------

demo_icd = (
    patdemo
    .join(icd_code,"pat_key","inner")
    .filter(DATE_RANGE_COND)
)

# COMMAND ----------

# Filter down med claims for those in ccsr list
ccsr_subset = (
    ccsr_lookup
        .filter(
            F.col("Al_Aly") == 1
        )
        .select("ICD-10-CM_Code", "CCSR_Category")
)

ccsr_med_claims = (
    demo_icd
        .join(
            ccsr_subset,
            demo_icd["icd_code"] == ccsr_subset["ICD-10-CM_Code"],
            "inner"
        )
        .groupBy("medrec_key", "CCSR_Category")
        .agg(
            F.min("adm_mon").alias("date_service")
        )
        .select(
            F.col("medrec_key").alias("c_medrec_key"),
            "date_service",
            "CCSR_Category"
        )
)


cases_pccs = (
    cases
        .join(
            ccsr_med_claims,
            [
                (cases["medrec_key"] == ccsr_med_claims["c_medrec_key"]) &
                (
                    (F.datediff(ccsr_med_claims["date_service"], cases["covid_index_date"]) >= 31) &
                    (F.datediff(ccsr_med_claims["date_service"], cases["covid_index_date"]) <= 180)
                )
                
            ],
            "inner"
        )
        .groupBy("medrec_key")
        .agg(
            F.first("covid_index_date", ignorenulls=True).alias("covid_index_date"),
            F.collect_list(
                    F.struct(F.col("CCSR_Category"), F.col("date_service"))
                ).alias("pcc_list"),
            F.first("adm_mon", ignorenulls=True).alias("adm_mon"),
            F.first("age", ignorenulls=True).alias("age"),
            F.first("gender", ignorenulls=True).alias("gender"),
            F.first("prov_division", ignorenulls=True).alias("prov_division"),
            F.first("Race_Eth", ignorenulls=True).alias("Race_Eth")
        )
)

# COMMAND ----------

cases_pccs.write.saveAsTable(f"{DB_WRITE}.rvn4_cases_pccs")

# COMMAND ----------

cases_pccs = spark.table(f"{DB_WRITE}.rvn4_cases_pccs")

# COMMAND ----------

display(cases_pccs.limit(10))

# COMMAND ----------

cases_pccs.count()

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH sample as (
# MAGIC   SELECT
# MAGIC     explode_outer(pcc_list) as pcc_explode
# MAGIC   FROM
# MAGIC     cdh_hv_race_ethnicity_covid_exploratory.rvn4_cases_pccs
# MAGIC   LIMIT
# MAGIC     10
# MAGIC )
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM
# MAGIC   sample

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT COUNT(DISTINCT CCSR_Category) FROM cdh_reference_data.ccsr_codelist WHERE CCSR_Category rlike ("^PRG")

# COMMAND ----------


