# Databricks notebook source
# MAGIC %md
# MAGIC # Flowchart Counts (Revised)
# MAGIC
# MAGIC This notebook calculates the number of people in the cohort at various stages of inclusion/exclusion criteria. The notebook has been revised from the previous version to create counts for adult and pediatric cohorts separately. 

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
# MAGIC - Author: Adewole Oyalowo (Contractor, Booz Allen Hamilton)
# MAGIC   - Email: [sve5@cdc.gov](sve5@cdc.gov) <!--- enter your email address in both the square brackets and the parens -->
# MAGIC
# MAGIC - Author: Ka'imi Kahihikolo (Contractor, Booz Allen Hamilton)
# MAGIC   - Email: [rvn4@cdc.gov](rvn4@cdc.gov) <!--- enter your email address in both the square brackets and the parens -->
# MAGIC   
# MAGIC - Premier Edits: Joshua Bonner (Contractor, Booz Allen Hamilton)
# MAGIC   - Email: [rvj5@cdc.gov](rvn4@cdc.gov) <!--- enter your email address in both the square brackets and the parens -->

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
spark.conf.set('spark.databricks.adaptive.autoOptimizeShuffle.enabled', False)
spark.conf.set('spark.databricks.queryWatchdog.enabled', False)
spark.conf.set('spark.sql.files.maxPartitionBytes', 1024 * 1024 * 64)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filters and Definitions

# COMMAND ----------

# MAGIC %run ./includes/0000-GLOBAL-NAMES

# COMMAND ----------

DATE_RANGE_COND = (
    
    # Date is in 2020 but in or after March
    (
         (F.col("adm_mon") >= 2020103)
    ) 
    
    # Or date is in 2021 but in or before November
    | (
         (F.col('adm_mon') <= 2021411)
    )
)

ENROLL_DATE_RANGE_COND = (
    
    # Date is in 2019 but in or after March
    (
         (F.col("adm_mon") >= 2019103)
    )
    
    # Or date is in any month in 2020 or 2021
    | F.col("year").isin([2020,2021])
        
    # Or date is in 2022 but in or before May
    |(
         (F.col('adm_mon') <= 2022205)
    )
)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Collect Data
# MAGIC *What data do I need? From where?*

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Medical Claims
# MAGIC
# MAGIC `med_claims`: All closed source medical encounters from March 1, 2020 thru November 30, 2021 <br>
# MAGIC `eligible_med_claims`: All closed source medical encounters from March 1, 2019 thru May 31, 2022 <br>
# MAGIC --`claims`: In Premier, the best analogue to claims or continuous enrollment is pat_key, encounters. To align code and methodology, 'claims' will refer to Premier encounters

# COMMAND ----------

icd_desc=spark.read.table("cdh_premier.icdcode")
icd_code=spark.read.table("cdh_premier.paticd_diag")
patcpt=spark.read.table("cdh_premier.patcpt")
patlabres=spark.read.table("cdh_premier.lab_res")
genlab=spark.read.table("cdh_premier.genlab")
disstat=spark.read.table("cdh_premier.disstat")
pattype=spark.read.table("cdh_premier.pattype")
patbill=spark.read.table("cdh_premier.patbill")
chgmstr=spark.read.table("cdh_premier.chgmstr")
patdemo=spark.read.table("cdh_premier.patdemo")
providers=spark.read.table("cdh_premier.providers")
readmit=spark.read.table("cdh_premier.readmit")
zip3=spark.read.table("cdh_premier.phd_providers_zip3")
prov=providers.join(zip3,"prov_id","inner")


ccsr_lookup = spark.table("cdh_reference_data.ccsr_codelist")

# COMMAND ----------

icd_code=icd_code.withColumnRenamed('icd_code', 'icd_code_original').select("pat_key", "ICD_PRI_SEC", "year", "quarter", "icd_code_original", F.translate(F.col("icd_code_original"), ".", "").alias("icd_code"))
display(icd_code,limit=10)

# COMMAND ----------

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
           .withColumn('adm_date',
                       F.to_date(concat(F.substring(col('adm_mon'),1,4),
                                        lit("-"),
                                        F.substring(col('adm_mon'),6,7)), 'yyyy-MM')
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
             )

med_claims = (
    medrec_icd
    .filter(DATE_RANGE_COND)
)

eligible_med_claims = (
    medrec_icd
    .filter(ENROLL_DATE_RANGE_COND)
)

# COMMAND ----------

_condition_claims_df = (
    spark.table(f'{DB_WRITE}.{WIDE_OUTPUT_NAME}')
    .select(
        'medrec_key',
        'index_date',
        'days_enrolled', #Premier count of prior encounters
        'covid_indicator',
        'covid',
        'age',
        'gender',
        'prov_region',
        'prov_division',
        F.when(
            F.col('age').between(0,17),
            1
        ).otherwise(0).alias('age_0_17'),
        F.when(
            F.col('age').between(18,64),
            1
        ).otherwise(0).alias('age_18_64'),
        F.when(
            F.col('gender').isin('M','F'),
            1
        ).otherwise(0).alias('gender_m_f'),
#        F.when(
#            F.col('patient_state').isin(list(states_pydict.values())),
#            1
#        ).otherwise(0).alias('us_census'),
        F.when(
            F.col('days_enrolled')>=500,
            1
        ).otherwise(0).alias('cont_enroll'),
        F.when(
            ~((F.col('covid_indicator') == 1) & (F.col('covid') == 0)),
            1
        ).otherwise(0).alias('has_covid_indicator_and_no_covid')
    )
#     .join(
#         spark.table(f'{DB_WRITE}.{INPATIENT_STATUS_TABLE}').withColumn('inpatient_status', F.lit(1)),
#         'hvid',
#         'left'
#     )
#     .na.fill(
#         {'inpatient_status':0}
#     )
)

# Columns to match on
MATCHING_COLUMNS = ["age", "gender", "index_month", "inpatient_status"]

condition_claims_df = (
    _condition_claims_df
    .filter(
        ~((F.col('covid_indicator') == 1) & (F.col('covid') == 0))
        & F.col('age').between(0,64)
        & F.col('gender').isin('M','F')
#        & F.col('patient_state').isin(list(states_pydict.values()))
        & (F.col('days_enrolled') >= 1)
    )
#     .withColumn(
#         'index_month',
#         F.date_trunc("MM", F.col("index_date"))
#     )
#     .select(
#         "hvid", 
#         *MATCHING_COLUMNS,
#     )
)

# # Define cases as covid == 1
# cases = condition_claims_df.filter(F.col("covid") == 1).drop("covid")

# # Define cases as covid == 0
# controls = condition_claims_df.filter(F.col("covid") == 0).drop("covid")

# print(cases.count())
# print(controls.count())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Premier Closed Medical Claims: People with at least 1 medical claim 3/1/2020 - 11/30/2021

# COMMAND ----------

(
    med_claims
    .select('medrec_key')
    .distinct()
    .join(
        _condition_claims_df,
        'medrec_key',
        'inner'
    )
    .groupBy('age_18_64','age_0_17')
    .count()
    .orderBy('age_18_64','age_0_17')
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Premier Lab Results: People with a positive diagnostic lab 3/1/2020 - 11/30/2021

# COMMAND ----------

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
        'prov_id', #'place_of_service_std_id',
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


# This chunk wont run
# (medrec_lab
#     .filter(DATE_RANGE_COND)
#     .select(
#         'medrec_key',
#     )
#     .filter(
#         F.col('test').isin('COVID Diagnostic')
#         & F.col('observation').isin('Positive')
#     )
#     .join(
#         _condition_claims_df,
#         'medrec_key',
#         'inner'
#     )
#     .groupBy('age_18_64','age_0_17')
#     .count()
#     .orderBy('age_18_64','age_0_17')
#     .display()
# )


# COMMAND ----------

patlabres.display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Premier Merged: People with one medical claim or lab

# COMMAND ----------

# This won't run
(
  
    # Get all medical claims with standard HV filters applied
    med_claims
    .filter(
        ~F.col('diagnosis_code').isin('', ' ')
        & F.col('diagnosis_code').isNotNull()
    )
    
    # Select necessary columns from medical claims. Create flags for covid and inpatient indicators.
    .select(
        'medrec_key',
    )
    
    # Get lab information and look for positive diagnositc lab tests
    .unionByName(
        spark
        .table(f"{DB_READ}.{LAB_TABLE_NAME}")
        .filter(DATE_RANGE_COND)
        .select(
            'hvid',
        )
        .filter(
            F.col('test').isin('COVID Diagnostic')
            & F.col('observation').isin('Positive')
        )
        ,
        allowMissingColumns=True   
    )
    
    .distinct()
  
    .join(
        _condition_claims_df,
        'medrec_key',
        'inner'
    )
    .groupBy('age_18_64','age_0_17')
    .count()
    .orderBy('age_18_64','age_0_17')
    .display()
    
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exposed Unexposed Counts

# COMMAND ----------

(
    _condition_claims_df
    .select('*',F.lit(1).alias('n'))
    .groupBy('age_18_64','age_0_17')
    .sum()
    .display()
)

# COMMAND ----------

condition_claims_df = (
    _condition_claims_df
    .filter(
        ~((F.col('covid_indicator') == 1) & (F.col('covid') == 0))
        & F.col('age').between(0,64)
        & F.col('gender').isin('M','F')
#        & F.col('patient_state').isin(list(states_pydict.values()))
        & (F.col('days_enrolled') >= 1)
    )
#     .withColumn(
#         'index_month',
#         F.date_trunc("MM", F.col("index_date"))
#     )
#     .select(
#         "hvid", 
#         *MATCHING_COLUMNS,
#     )
)

# Define cases as covid == 1
cases = condition_claims_df.filter(F.col("covid") == 1).drop("covid")

# Define cases as covid == 0
controls = condition_claims_df.filter(F.col("covid") == 0).drop("covid")

display(cases.groupBy('age_18_64','age_0_17').count())
display(controls.groupBy('age_18_64','age_0_17').count())

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Final Cohort Count

# COMMAND ----------

(
    spark
    .table(f'{DB_WRITE}.{WIDE_OUTPUT_NAME_FINAL}')
    .select('medrec_key')
    .join(
        _condition_claims_df,
        'medrec_key',
        'inner'
    )
    .groupBy('age_18_64','age_0_17')
    .count()
    .orderBy('age_18_64','age_0_17')
    .display()
)

# COMMAND ----------

(
    spark
    .table(f'{DB_WRITE}.{WIDE_OUTPUT_NAME_FINAL}')
    .join(
        _condition_claims_df.drop('covid'),
        'medrec_key',
        'inner'
    )
    .groupBy('age_18_64','age_0_17','covid')
    .count()
    .orderBy('age_18_64','age_0_17','covid')
    .display()
)

# COMMAND ----------

(
    spark
    .table(f'{DB_WRITE}.{WIDE_OUTPUT_NAME_FINAL}')
    .join(
        _condition_claims_df.drop('covid'),
        'medrec_key',
        'inner'
    )
    .groupBy('age_18_64','age_0_17','inpatient_status')
    .count()
    .orderBy('age_18_64','age_0_17','inpatient_status')
    .display()
)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Model
# MAGIC *What analyses could solve my problem?*

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Evaluate
# MAGIC *Did I solve my problem? If not, why? (e.g. found a new problem that I'm stuck on, invalid assumptions, etc.)*

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### # of CCSRs evaluated

# COMMAND ----------

len(conditions_list)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Footer

# COMMAND ----------


