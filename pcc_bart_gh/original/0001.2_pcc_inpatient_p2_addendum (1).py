# Databricks notebook source
# MAGIC %md
# MAGIC ## Notebook Configuration

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Imports and Settings

# COMMAND ----------

# spark configuration
spark.conf.set('spark.sql.shuffle.partitions', 6400)
spark.conf.set('spark.databricks.adaptive.autoOptimizeShuffle.enabled', True)
spark.conf.set('spark.databricks.queryWatchdog.enabled', False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filters and Definitions

# COMMAND ----------

#%run ../includes/0000-hv-filters

# COMMAND ----------

# MAGIC %run ./includes/0000-GLOBAL-NAMES

# COMMAND ----------

# Filter Function
P2_DATE_RANGE_COND = (
    
    # Date is in 2020 but in or after March 1 minus 7 days
    (
        (F.col("year") == 2020) 
        & (F.col("adm_mon") >= 2020102)
    ) 
    
    # Or date is in 2021 but in or before November 30 plus 1 month
    | (
        (F.col("year") == 2021)
        & (F.col('adm_mon') <= 2021412)
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

ccsr_df = (
    spark
    .table(f"{DB_WRITE}.{WIDE_OUTPUT_NAME}")
    .select('medrec_key','covid','index_date')
)

p2_med_claims = (
    spark
    .table(f"{DB_WRITE}.twj8_medrec_icd")
    .filter(P2_DATE_RANGE_COND)
    .filter(F.col("IO_FLAG").isin(1))
    .select(
        'medrec_key',
        'adm_mon',
        'adm_date',
        'year',
        F.when(
            (
                F.col('icd_code').isin('U071')
                & F.col('adm_date').between(F.lit('2020-04-01').cast('date'), F.lit('2021-11-30').cast('date'))
            )
            | (
                F.col('icd_code').isin('B9729')
                & F.col('adm_date').between(F.lit('2020-03-01').cast('date'), F.lit('2020-04-30').cast('date'))
            ),
            1
            )
        .otherwise(0)
        .alias('covid_claim')
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Inpatient Status

# COMMAND ----------

# Identify patients who experienced a P2 inpatient encounter
ccsr_df_inpatient_p2 = (
    
    p2_med_claims
    
    # Perform a conditional range join on medical claims
    .join(
        ccsr_df
        ,
        [
            # Match hvid
            (ccsr_df["medrec_key"] == p2_med_claims["medrec_key"]) 

#             # Match within 1 year window (just to use partition speed up)
#             & (
#                 F.abs(F.year(ccsr_df["index_date"]) - p2_med_claims["year"]) <= 1
#             ) 

#             # Match within 1 month window (just to use partition speed up)
#             & (
#                 F.abs(F.month(ccsr_df["index_date"]) - p2_med_claims["month"]) <= 1
#             )

            # Finally, match within p2 window (just to use partition speed up)
            & (
                F.datediff('adm_date','index_date').between(-6, 30)
            )

        ],
        "inner"
    )
    
    # For unexposed patients, find any inpatient claim during p2. For exposed patients, find inpatient claim during p2 that is attached to a covid diagnosis.
    .filter(
        """
        covid = 0
        OR (covid = 1 AND covid_claim = 1)
        """
    )
    
    .select(ccsr_df["medrec_key"])
    .distinct()
)

# COMMAND ----------

spark.sql(f'DROP TABLE IF EXISTS {DB_WRITE}.{INPATIENT_STATUS_TABLE}')

# COMMAND ----------

(
    ccsr_df_inpatient_p2
    .write
    .format('parquet')
    .mode("overwrite")
    .saveAsTable(f"{DB_WRITE}.{INPATIENT_STATUS_TABLE}")
)

# COMMAND ----------

spark.sql(f'CONVERT TO DELTA {DB_WRITE}.{INPATIENT_STATUS_TABLE}')

# COMMAND ----------

spark.sql(f'OPTIMIZE {DB_WRITE}.{INPATIENT_STATUS_TABLE}')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write new Wide Table

# COMMAND ----------

# spark.sql(f'DROP TABLE IF EXISTS {DB_WRITE}.{WIDE_OUTPUT_NAME_P2}')

# COMMAND ----------

# (
#     spark
#     .table(f"{DB_WRITE}.{WIDE_OUTPUT_NAME}")
#     .withColumnRenamed('inpatient', 'inpatient_index')
#     .join(
#         spark
#         .table(f"{DB_WRITE}.{INPATIENT_STATUS_TABLE}")
#         .withColumn('inpatient_status', F.lit(1))
#         ,
#         'hvid',
#         'left'
#     )
#     .na.fill(
#         {
#             'inpatient_status':0
#         }
#     )
#     .write
#     .format('delta')
#     .mode("overwrite")
#     .saveAsTable(f"{DB_WRITE}.{WIDE_OUTPUT_NAME_P2}")    
# )

# COMMAND ----------

# spark.sql(f'CONVERT TO DELTA {DB_WRITE}.{WIDE_OUTPUT_NAME_P2}')

# COMMAND ----------

# spark.sql(f'OPTIMIZE {DB_WRITE}.{WIDE_OUTPUT_NAME_P2}')

# COMMAND ----------


