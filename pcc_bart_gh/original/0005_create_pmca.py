# Databricks notebook source
# MAGIC %md
# MAGIC # Create PMCA (Pediatric Medical Complexity Algorithm)
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

from operator import add
from functools import reduce

# COMMAND ----------

# spark configuration
spark.conf.set('spark.sql.shuffle.partitions', 1600)
spark.conf.set('spark.databricks.adaptive.autoOptimizeShuffle.enabled', False)
spark.conf.set('spark.databricks.queryWatchdog.enabled', False)
spark.conf.set('spark.sql.files.maxPartitionBytes', 1024 * 1024 * 64)

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
# MAGIC #### PMCA Mapping

# COMMAND ----------

spark.table("cdh_reference_data.pmca_list").show()

# COMMAND ----------

# Get pcma_list with cdh_reference_data.pcma_list
# remove icd_code "."
# create malignancy flag
pmca_df = (
#     spark
#     .createDataFrame(
#         [
#             ('A15.0', 'pulmonary_respiratory', 'no'),
#             ('A15.4', 'pulmonary_respiratory', 'no'),
#             ('A15.5', 'pulmonary_respiratory', 'no'),
#             ('C00.0', 'malignancy', 'na'),
#         ], 
#         schema="`ICD_CODE` STRING, `BODY_SYSTEM` STRING, `PROGRESSIVE` STRING"
#     )
    spark.table('cdh_reference_data.pmca_list')
    .withColumn(
        'ICD_CODE',
        F.regexp_replace('ICD_CODE','\.','')
    )
    .withColumn(
        'MALIGNANCY',
        F.when(
            F.col('BODY_SYSTEM').isin('malignancy'),
            1
        )
        .otherwise(0)
    )
#     .select(
#             # Remove '.' from ICD_CODE
#             pmca_icd := F.regexp_replace("ICD_CODE", "\.", "").alias("ICD_CODE"),
#             # Replace newlines `\n` from BODY_SYSTEM
#             F.regexp_replace("BODY_SYSTEM", "\n", "").alias("BODY_SYSTEM"),
#             # Cast _no/null to 0 and _yes to 1
#             F.when(
#                 F.col("BODY_PROGRESSIVE").rlike("yes"),
#                 1
#             ).otherwise(0).alias("BODY_PROGRESSIVE"),
#             # Cast no/null to 0 and yes to 1
#             F.when(
#                 F.col("PROGRESSIVE").rlike("yes"),
#                 1
#             ).otherwise(0),
#             # Create 0/1 flag for malignancy
#             F.max(
#                 F.when(
#                     (F.col("BODY_SYSTEM") == "malignancy"),
#                     1
#                 ).otherwise(0)
#             ).alias("MALIGNANCY"),
#         )
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Collect Data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### long_df

# COMMAND ----------

# Use twj8_medrec_covid_ccsr
# filter
# - NOT covid_indicator = 1 * covid = 0
# - age 0,17
# - gender M,F
# - adm_date - index_date in -365,-7
# left join twj8_uid_case_control, inner
long_df = (
    spark
    .table(
        f'{DB_WRITE}.{LONG_OUTPUT_NAME}'
    )
    .filter(
        ~((F.col('covid_indicator') == 1) & (F.col('covid') == 0))
        & F.col('age').between(0,17)
        & F.col('gender').isin('M','F')
        & F.datediff('adm_date','index_date').between(-365,-7)
    )
    .select(
        'medrec_key',
        'age',
        F.col('icd_code').alias("diagnosis_code"),
        'adm_date'
    )
    .join(
        F.broadcast(spark.table(f'{DB_WRITE}.{MATCHES_UID_TABLE}')),
        'medrec_key',
        'inner'
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### long_df_scored

# COMMAND ----------

# Use long_df
# join pmca_df, left on long_df.diagnosis_code startwith pcma_df.icd_code
# groupby medrec_key
# agg
# - count distinct body_system
# - max progressive
# - max malignancy
# Create pcma_score

long_df_scored = (
    
    long_df
    
    .join(
        F.broadcast(pmca_df),
        [
            long_df['diagnosis_code'].startswith(pmca_df['ICD_CODE'])
        ],
        'left'
    )
    .drop(
        pmca_df['ICD_CODE']
    )
    .groupBy(
        'medrec_key',
    )
    .agg(
        F.countDistinct('BODY_SYSTEM').alias('BODY_SYSTEM'),
        F.max('PROGRESSIVE').alias('PROGRESSIVE'),
        F.max('MALIGNANCY').alias('MALIGNANCY'),
    )
    
    .withColumn(
        'pmca_score',
        F.when(
            (F.col('PROGRESSIVE') == 'yes') 
            | (F.col('MALIGNANCY') == 1)
            | (F.col('BODY_SYSTEM') >= 2),
            3
        )
        .when(
            (F.col('PROGRESSIVE') == 'no') 
            & (F.col('MALIGNANCY') == 0)
            & (F.col('BODY_SYSTEM') == 1),
            2
        )
        .otherwise(1)
    )
    
    .select(
        'medrec_key',
        'pmca_score',
    )

)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Table

# COMMAND ----------

# f'{DB_WRITE}.{PMCA_TABLE_NAME}'

# COMMAND ----------

# remove old table. why??
spark.sql(f'DROP TABLE IF EXISTS {DB_WRITE}.{PMCA_TABLE_NAME}')

# COMMAND ----------

# twj8_medrec_pmca{tag}
(
    long_df_scored
    .write
    .format('parquet')
    .mode('overwrite')
    .saveAsTable(f'{DB_WRITE}.{PMCA_TABLE_NAME}')
)

# COMMAND ----------

spark.sql(f'CONVERT TO DELTA {DB_WRITE}.{PMCA_TABLE_NAME}')

# COMMAND ----------

spark.sql(f'OPTIMIZE {DB_WRITE}.{PMCA_TABLE_NAME}')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Footer

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The algorithm for the PMCA were adapted from the following resources:
# MAGIC
# MAGIC
# MAGIC <b> Pediatric Medical Complexity Algorithm: A New Method to Stratify Children by Medical Complexity (Simon et al., 2014) </b>
# MAGIC
# MAGIC https://publications.aap.org/pediatrics/article/133/6/e1647/76064/Pediatric-Medical-Complexity-Algorithm-A-New?autologincheck=redirected?nfToken=00000000-0000-0000-0000-000000000000

# COMMAND ----------


