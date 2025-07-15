# Databricks notebook source
# MAGIC %md
# MAGIC # Create Incidence Rates
# MAGIC
# MAGIC This notebook creates incidence rates for cases and controls.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2022-08-18

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

# COMMAND ----------

# spark configuration
spark.conf.set('spark.sql.shuffle.partitions', 6400)
spark.conf.set('spark.databricks.adaptive.autoOptimizeShuffle.enabled', False)
spark.conf.set('spark.databricks.queryWatchdog.enabled', False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filters and Definitions

# COMMAND ----------

# MAGIC %run ./includes/0000-GLOBAL-NAMES

# COMMAND ----------

# DB_NAME = "cdh_hv_full_covid"
# DB_NAME = "cdh_premier_exploratory"
# WIDE_TABLE_NAME = "twj8_medrec_ccsr_wide_cohort"
# MATCHES_TABLE = 'twj8_case_control_matches'
# MATCHES_UID_TABLE = 'twj8_uid_case_control'

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
# MAGIC #### CCSR

# COMMAND ----------

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

# COMMAND ----------

# Get the ccsr_codelist
# filter by Al_Aly_plust
# Collect ICD-10-CM_Code
# Filter by EXLUSION_DIAG_CODES

conditions_df = (
    spark
    .table('cdh_reference_data.ccsr_codelist')
    .filter(
        'Al_Aly_plus = 1'
    )
    .withColumn(
        'diagnosis_code',
        F.col('ICD-10-CM_Code')
    )
    .filter(
        ~F.col('diagnosis_code').isin(EXCLUSION_DIAG_CODES)
    )
)



conditions_list = [row[0] for row in conditions_df.select('CCSR_Category').distinct().collect()]
conditions_list = sorted(conditions_list)

# COMMAND ----------

conditions_list

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Matches

# COMMAND ----------

# Read in matches table

matches = (
    spark
    .table(f"{DB_WRITE}.{MATCHES_TABLE}")
)

matched_medrec = (
    spark
    .table(f"{DB_WRITE}.{MATCHES_UID_TABLE}")

)

# COMMAND ----------

display(matches)

# COMMAND ----------

display(matched_medrec)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Wide Table

# COMMAND ----------

# Get Wide Table
# Filter 
# - NOT Covid_indicator == 1 and covid == 0
# - age 0-64
# - gender M,F
# - days_enrolled >= 1
# Create "adult" as 1 if >= 18, else 0
# Inner join to matched_medrec

condition_claims_df = (
    spark
    .table(f'{DB_WRITE}.{WIDE_OUTPUT_NAME}')
    .filter(
        ~((F.col('covid_indicator') == 1) & (F.col('covid') == 0))
        & F.col('age').between(0,64)
        & F.col('gender').isin('M','F')
        #& F.col('patient_state').isNotNull()
        & (F.col('days_enrolled') >= 1)
    )
    .withColumn(
        'adult',
        F.when(
            (F.col('age') >= 18),
            1
        )
        .otherwise(0)
    )
    .join(
        matched_medrec,
        'medrec_key',
        'inner'
    )
)

# COMMAND ----------

display(condition_claims_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Model
# MAGIC *What analyses could solve my problem?*

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Incidence Rates

# COMMAND ----------

# I think this is the important one
# Create Incidence Rates

# Use condition_claims_df
# Factor: adult, covid, io_flag
# groupby factors
# agg 
# - for c in conditions_list (conditions as columns in condition_claims_df)
# - - get sum of vals inbetween 31-149 (days from index), then 1
# - - as n_"c"
# - for c in conditions_list 
# - - set val if in 31-149 to val-31,
# - - if val between -365,-7 then 0, else 1
# - - as pd_"c"
# Select factors, and conditions_list as n_"c"/n_"pd"
factors = [
    'adult',
    'covid',
    'io_flag'
]

(
    condition_claims_df
    .groupBy(factors)
    .agg(
        *[
            F.sum(
                F.when(
                    F.col(c).between(31,149), #(31,180)
                    1
                )
                .otherwise(0)
            ).alias(f"n_{c}") for c in conditions_list
        ],
        *[
            F.sum(
                F.when(
                    F.col(c).between(31,149),
                    F.col(c)-31
                ) 
                .when(
                    F.col(c).between(-365,-7), #(-365,-7)
                    0
                ) 
                .otherwise(1) #Changed from 180 to 1 due to premier counts for proportion
            ).alias(f"pd_{c}") for c in conditions_list
        ]
    )
    .select(
        *factors,
        *[
          (
              F.col(f'n_{c}')/(F.col(f'pd_{c}'))
          ).alias(c) for c in conditions_list
        ]
        
    )
    .display()
)

# COMMAND ----------

# Check when conditions days from index is between 31, 180
factors = [
    'adult',
    'covid',
    'io_flag'
]

(
    condition_claims_df
    .groupBy(factors)
    .agg(
        *[
            F.sum(
                F.when(
                    F.col(c).between(31,180), #(31,180)
                    1
                )
                .otherwise(0)
            ).alias(f"n_{c}") for c in conditions_list
        ],
        

)
                .display()
)

# COMMAND ----------

# Check conditions days from index between 31-180, and the -365- -7 as 0
factors = [
    'adult',
    'covid',
    'io_flag'
]

(
    condition_claims_df
    .groupBy(factors)
    .agg(
*[
            F.sum(
                F.when(
                    F.col(c).between(31,180), #(31,180)
                    F.col(c) #Changed from column of cell to 1 due to premier counts for proportion
                ) 
                .when(
                    F.col(c).between(-365,-7), #(-365,-7)
                    0
                ) 
                .otherwise(1) #Changed from 180 to 1 due to premier counts for proportion
            ).alias(f"pd_{c}") for c in conditions_list
        ]
    )
    .display()
) #365.25 * 100,000

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
# MAGIC
# MAGIC ### Covariate Counts

# COMMAND ----------

# get frequencies for each covid, io_flag group
# counts of patients (medrec_key), covid_indicators, and adult
display(
    condition_claims_df
    .groupBy(
        'covid',
        'io_flag'
    )
    .agg(
        F.count('medrec_key'),
        *[F.sum(c).alias(c) for c in ['covid_indicator', 'adult']],
    )
)

# COMMAND ----------

condition_claims_df.groupBy('covid').count().display()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Test for Claims after 11/2021

# COMMAND ----------

(
    condition_claims_df
    .na.fill(0)
    .filter(F.col('index_date') >= '2021-11-01')
    .withColumn(
        'sum_elapsed',
        F.greatest(*conditions_list).cast('integer') + F.dayofyear('index_date')
    )
    .orderBy(F.desc('sum_elapsed'))
    .select(
        'medrec_key',
        'index_date',
        'covid',
        F.greatest(*conditions_list).cast('integer').alias('greatest'),
        'sum_elapsed'
    )
    .display()
)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Footer

# COMMAND ----------


