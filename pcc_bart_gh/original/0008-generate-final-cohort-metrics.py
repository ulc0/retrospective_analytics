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
spark.conf.set('spark.sql.shuffle.partitions', 6400)
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

# In: twj8_medrec_ccsr_wide_cohort_final
final_cohort_df = (
    spark
    .table(
        f'{DB_WRITE}.{WIDE_OUTPUT_NAME_FINAL}'
    )
#     .select(
#         'hvid',
#         'days_enrolled',
#         'index_date',
#         'covid',
#         'covid_indicator',
#         'inpatient_index',
#         'inpatient_status',
#         'age',
#         'patient_gender',
#         'patient_state',
#         'cci_score',
#         'cci_score_age',
#         'pmca_score',
#         'n_prior_hosp'
#     )
)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Model
# MAGIC *What analyses could solve my problem?*

# COMMAND ----------

dbutils.data.summarize(final_cohort_df.filter('age >= 18 AND inpatient_status = 0'))

# COMMAND ----------

dbutils.data.summarize(final_cohort_df.filter('age >= 18 AND inpatient_status = 1'))

# COMMAND ----------

dbutils.data.summarize(final_cohort_df.filter('age < 18 AND inpatient_status = 0'))

# COMMAND ----------

dbutils.data.summarize(final_cohort_df.filter('age < 18 AND inpatient_status = 1'))

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Evaluate
# MAGIC *Did I solve my problem? If not, why? (e.g. found a new problem that I'm stuck on, invalid assumptions, etc.)*

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Footer

# COMMAND ----------


