# Databricks notebook source
# MAGIC %md
# MAGIC # Create Prior Hospitalization Flag
# MAGIC This notebook generates PMCA scores based on ICD diagnoses codes

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2022-09-08

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Configuration

# COMMAND ----------

#import pyspark sql functions with alias
import pyspark.sql.functions as F
import pyspark.sql.window as W

#import specific functions from pyspark sql types
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from pyspark.sql.functions import col, concat, expr, lit

from operator import add
from functools import reduce

# COMMAND ----------

# spark configuration
spark.conf.set('spark.sql.shuffle.partitions', 1600)
spark.conf.set('spark.databricks.adaptive.autoOptimizeShuffle.enabled', False)
spark.conf.set('spark.databricks.queryWatchdog.enabled', False)
spark.conf.set('spark.sql.files.maxPartitionBytes', 1024 * 1024 * 128)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Definitions and Filters

# COMMAND ----------

#%run ../includes/0000-hv-filters

# COMMAND ----------

# MAGIC %run ./includes/0000-GLOBAL-NAMES

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Collect Data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### long_df

# COMMAND ----------

# Date Range Conditions
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

# COMMAND ----------

# In: patdemo
# Filter 
# - date_range_cond
# Join icd_code, inner
# Filter
# - medrec_key not null
# - I_O_IND = I
# - pat_type = 8
# Filter
# - NOT icd_cod = ""," "
# - icd_code not null
# Create "adm_date"
# Groupby medrec_key, adm_date
# agg
# - create "n_icd" as distinct number icd_codes
prior_hosp_claims = (
    spark
    .table(f"{DB_READ}.{patdemo}")
    .filter(DATE_RANGE_COND)
    .join(spark.table(f"{DB_READ}.{icd_code}"),
        'pat_key',
        'inner')
    # Apply HV-recommended filters
    .filter(
        F.col('medrec_key').isNotNull()
        & F.col('I_O_IND').isin("I")
        & F.col('pat_type').isin(8)
    )
    
    # Require diganosis codes for claims
    .filter(
        ~F.col('icd_code').isin('', ' ')
        & F.col('icd_code').isNotNull()
    )
    .withColumn('adm_date',
        F.to_date(concat(F.substring(col('adm_mon'),1,4),
                         lit("-"),
                         F.substring(col('adm_mon'),6,7)), 'yyyy-MM')
                      )  
  
    
    .groupBy(
        'medrec_key',
#         'claim_id',
#         'hv_enc_id',
#         'data_vendor',
#         'patient_gender',
#         'patient_year_of_birth',
#         'patient_zip3',
#         'patient_state',
#         'claim_type',
        'adm_date',
#         'date_service_end',
#         'inst_type_of_bill_std_id',
#         'place_of_service_std_id',
#         'ndc_code',
#         'procedure_code',
#         'diagnosis_code'
    )
        .agg(
        F.countDistinct('icd_code').cast('integer').alias('n_icd')
    ))

# COMMAND ----------

display(prior_hosp_claims)

# COMMAND ----------

# IN: twj8_medrec_ccsr_wide_cohort
# Filter
# - NOT covid_indicator = 1 * covid = 0
# - age 0,64
# - gender M,F
# Join twj8_uid_case_control, inner
cohort_df = (
    spark
    .table(
        f'{DB_WRITE}.{WIDE_OUTPUT_NAME}'
    )
    .filter(
        ~((F.col('covid_indicator') == 1) & (F.col('covid') == 0))
        & F.col('age').between(0,64)
        & F.col('gender').isin('M','F')
    )
    .select(
        'medrec_key',
        'index_date'
    )
    .join(
        F.broadcast(spark.table(f'{DB_WRITE}.{MATCHES_UID_TABLE}')),
        'medrec_key',
        'inner'
    )
)

# COMMAND ----------

display(cohort_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### long_df_scored

# COMMAND ----------

# In: prior_hosp_claims
# Join cohort_df, right on medrec_key, diff adm_date,index_date in -365,-7
# groupby medrec_key
# agg
# - create "n_prior_hosp" as sum "n_icd" (per patient)
# - - make NA = 0

long_df_scored = (
    prior_hosp_claims
    .join(
        cohort_df,
        [
            (prior_hosp_claims['medrec_key'] == cohort_df['medrec_key']),
            F.datediff('adm_date','index_date').between(-365,-7)
        ],
        'right'
    )
    .drop(
        prior_hosp_claims['medrec_key']
    )
    .groupBy(
        'medrec_key'
    )
    .agg(
        F.sum('n_icd').cast('integer').alias('n_prior_hosp')
    )
    .na.fill(
        {
            'n_prior_hosp': 0
        }
    )
)

# COMMAND ----------

display(long_df_scored)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Table

# COMMAND ----------

f'{DB_WRITE}.{HOSP_TABLE_NAME}'

# COMMAND ----------

spark.sql(f'DROP TABLE IF EXISTS {DB_WRITE}.{HOSP_TABLE_NAME}')

# COMMAND ----------

(
    long_df_scored
    .write
    .format('parquet')
    .mode('overwrite')
    .saveAsTable(f'{DB_WRITE}.{HOSP_TABLE_NAME}')
)

# COMMAND ----------

spark.sql(f'CONVERT TO DELTA {DB_WRITE}.{HOSP_TABLE_NAME}')

# COMMAND ----------

spark.sql(f'OPTIMIZE {DB_WRITE}.{HOSP_TABLE_NAME}')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Footer

# COMMAND ----------


