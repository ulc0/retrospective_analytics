# Databricks notebook source
# MAGIC %md
# MAGIC # Create Time to (CCSR) Event Table
# MAGIC
# MAGIC This notebook generates a table with one unique hvid per row and 400+ columns for each CCSR category. The value of in the cell of any given hvid-CCSR pair indicates the number of days from that HVID's index date until their first post covid condition.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2022-09-20

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
spark.conf.set('spark.sql.shuffle.partitions', 1600)
spark.conf.set('spark.databricks.adaptive.autoOptimizeShuffle.enabled', False)
spark.conf.set('spark.databricks.queryWatchdog.enabled', False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filters and Definitions

# COMMAND ----------

#%run ../includes/0000-hv-filters

# COMMAND ----------

# MAGIC %run ./includes/0000-GLOBAL-NAMES

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
#conditions_list.append('INF012')
conditions_list = sorted(set(conditions_list))

# COMMAND ----------

conditions_list

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
# MAGIC #### Final Cohort DF

# COMMAND ----------

# IN: twj8_medrec_ccsr_wide_cohort
# Filter
# - NOT covid_indicator = 1 * covid = 0
# - age 0,64
# - gender M,F
# Join twj8_uid_case_control, inner
# Join inpatient status as twj8_medrec_inpatient_status_p2, + create "inpatient_status" = 1, left
# Join CCI scores as twj8_medrec_cci, left
# Join PMCA scores as twj8_medrec_pmca, left
# Join Hospital counts as twj8_medrec_hosp, left
# adjust cci_score, cci_score_age, pmca_score
# fill NA inpatient status 0
# Filter 
# - IO_flag in 1,0

final_cohort_df = (
    
    # Load the wide table
    spark
    .table(
        f"{DB_WRITE}.{WIDE_OUTPUT_NAME}"
    )
    
    # Filter wide table based on inclusion/exclusion criteria (speed up joins)
    .filter(
        ~((F.col('covid_indicator') == 1) & (F.col('covid') == 0))
        & F.col('age').between(0,64)
        & F.col('gender').isin('M','F')
    )
    
    # Join for only the HVIDs that were matched
    .join(
        F.broadcast(spark.table(f'{DB_WRITE}.{MATCHES_UID_TABLE}')),
        'medrec_key',
        'inner'
    )
    
    # Join for inpatient status
    .join(
        spark.table(f'{DB_WRITE}.{INPATIENT_STATUS_TABLE}').withColumn('inpatient_status', F.lit(1)),
        'medrec_key',
        'left'
    )    
     
    # Join CCI scores
    .join(
        spark.table(f'{DB_WRITE}.{CCI_TABLE_NAME}'),
        'medrec_key',
        'left'
    )
    
    # Join PMCA scores
    .join(
        spark.table(f'{DB_WRITE}.{PMCA_TABLE_NAME}'),
        'medrec_key',
        'left'
    )

    # Join HOSP count
    .join(
        spark.table(f'{DB_WRITE}.{HOSP_TABLE_NAME}'),
        'medrec_key',
        'left'
    )
    
    .select(
        'medrec_key',
        'days_enrolled',
        'index_date',
        'covid',
        'covid_indicator',
        'IO_FLAG',
        'inpatient_status',
        'age',
        F.when((F.col('age') >= 18), 1).otherwise(0).alias('adult'),
        'gender',
        'prov_division',
        'prov_region',
#         'cci_score',
        F.when((F.col('age') >= 18) & F.col('cci_score').isNull(), 0).otherwise(F.col('cci_score')).alias('cci_score'),
#         'cci_score_age',
        F.when((F.col('age') >= 18) & F.col('cci_score_age').isNull(), 0).otherwise(F.col('cci_score_age')).alias('cci_score_age'),
#         'pmca_score',
        F.when((F.col('age') < 18) & F.col('pmca_score').isNull(), 1).otherwise(F.col('pmca_score')).alias('pmca_score'),
        'n_prior_hosp',
        *conditions_list
    )
     .na.fill(
         {
             'inpatient_status':0
         }
     )
    .filter(F.col('IO_FLAG').isin(1,0))
)

# COMMAND ----------

display(final_cohort_df)

# COMMAND ----------

#TROUBLESHOOTING
#spark.sql(f'DROP TABLE IF EXISTS {DB_WRITE}.{WIDE_OUTPUT_NAME_FINAL}')
#spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","False")
#(
#spark.createDataFrame(range(0),schema="`id` INTEGER")
#    .write
#    .mode('overwrite')
#    .format('parquet')
#    .saveAsTable(f'{DB_WRITE}.{WIDE_OUTPUT_NAME_FINAL}')
#)
#spark.sql(f'REFRESH TABLE {DB_WRITE}.{MATCHES_UID_TABLE}')

# COMMAND ----------

(
    final_cohort_df
    .write
    .format('delta')
    .mode('overwrite')
    .saveAsTable(f'{DB_WRITE}.{WIDE_OUTPUT_NAME_FINAL}')
)

# COMMAND ----------



# COMMAND ----------

# Why didn't they make this into a spark table?
#spark.sql(f'CONVERT TO DELTA {DB_WRITE}.{WIDE_OUTPUT_NAME_FINAL}')

# COMMAND ----------

spark.sql(f'OPTIMIZE {DB_WRITE}.{WIDE_OUTPUT_NAME_FINAL}')

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Model
# MAGIC *What analyses could solve my problem?*

# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC ## Evaluate
# MAGIC *Did I solve my problem? If not, why? (e.g. found a new problem that I'm stuck on, invalid assumptions, etc.)*

# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC ## Footer

# COMMAND ----------


