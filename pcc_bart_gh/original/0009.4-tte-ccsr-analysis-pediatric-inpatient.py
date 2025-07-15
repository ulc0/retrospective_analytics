# Databricks notebook source
# %sh git clone https://github.com/autonlab/auton-survival.git

# COMMAND ----------

# %sh echo "mlflow" >> ./auton-survival/requirements.txt

# COMMAND ----------

# %pip install -r ./auton-survival/requirements.txt

# COMMAND ----------

# import sys

# sys.path.append("./auton-survival/")

# COMMAND ----------

# MAGIC %md
# MAGIC # Time to (CCSR) Event Analysis
# MAGIC
# MAGIC The value of in the cell of any given hvid-CCSR pair indicates the number of days from that HVID's index date until their first post covid condition.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2022-09-26

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

# MAGIC %pip install lifelines

# COMMAND ----------

spark.version

# COMMAND ----------

import pyspark.sql.functions as F
from lifelines import CoxPHFitter
# from auton_survival.models.cph import DeepCoxPH
# from auton_survival.preprocessing import Preprocessor
import pandas as pd
# Disabling a particular pandas warning
pd.options.mode.chained_assignment = None
import numpy as np
import matplotlib.pyplot as plt

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filters and Definitions

# COMMAND ----------

ADULT_STATUS = False
INPATIENT_STATUS = True

# COMMAND ----------

# MAGIC %run ./includes/0000-GLOBAL-NAMES

# COMMAND ----------

output_table_name = f"{DB_WRITE}.{cox_pre}{'adult' if ADULT_STATUS else 'pediatric'}_{'inpatient' if INPATIENT_STATUS else 'outpatient'}{tag}"
print(output_table_name)

# COMMAND ----------

#%run ../includes/0000-utils

# COMMAND ----------

#%run ../includes/0000-hv-filters

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Collect Data
# MAGIC *What data do I need? From where?*

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load Data

# COMMAND ----------

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

cohort_df = spark.table(f"{DB_WRITE}.{WIDE_OUTPUT_NAME_FINAL}")

# COMMAND ----------

display(cohort_df)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Preprocessing

# COMMAND ----------

if ADULT_STATUS:
    chronic_disease_score = (
        F.when(
                F.col("cci_score") >= 3,
                3  # 3+
        ).otherwise(
            F.col("cci_score")
        )
    )
else:
    chronic_disease_score = (
        F.col("pmca_score")
    )

cohort_df = (
    spark.table(f"{DB_WRITE}.{WIDE_OUTPUT_NAME_FINAL}")
    
    # Filter for adult/pediatric inpatient/outpatient
    .filter(
      (F.col("adult") == int(ADULT_STATUS)) &
      (F.col("inpatient_status") == int(INPATIENT_STATUS))
    )
    .select(
      # Create age group
      F.when(
          F.col("age").between(0,1),
              0
          ).when(
              F.col("age").between(2,4),
              1 
          ).when(
              F.col("age").between(5,11),
              2 
          ).when(
              F.col("age").between(12,15),
              3
          ).when(
              F.col("age").between(16,17),
              4 
          ).when(
              F.col("age").between(18,39),
              5
          ).when(
              F.col("age").between(40,54),
              6
          ).when(
              F.col("age") >= 55,
              7
          ).alias("age_group"),
      # Binary encode gender and alias as sex
      F.when(
          F.col("gender") == "M",
          1
      ).otherwise(0).alias("sex"),
      # Truncate cci_score (if applicable)
      chronic_disease_score.alias("chronic_disease_score"),
      F.when(
          F.col("n_prior_hosp") > 0,
          1
      ).otherwise(0).alias("prior_hosp"),
      F.when(
          F.col("prov_region")=="WEST",
          0
      ).when(
          F.col("prov_region")=='MIDWEST',
          1
      ).when(
          F.col("prov_region")=='SOUTH',
          2
      ).when(
          F.col("prov_region")=='NORTHEAST',
          3
      ).alias("region_code"),
      'index_date',
      'covid',
      'inpatient_status',
      'age',
      'adult',
      'gender',
      'prov_region',
      'cci_score',
      'pmca_score',
      'n_prior_hosp',
      *[F.when(
          F.col(c).between(31,180),
          1
      ).when(
          F.col(c).between(-6,30),
          (0)
      ).when(
          F.col(c).isNull(),
          (0)
      ).alias(f'e_{c}') for c in conditions_list],
      *[F.when(
          F.col(c).between(31,180),
          (F.col(c)-31)
      ).when(
          F.col(c).between(-6,30),
          (149)
      ).when(
          F.col(c).isNull(),
          (149)
      ).alias(f'd_{c}') for c in conditions_list]
    )
)

display(cohort_df.limit(100))

# COMMAND ----------

cohort_df.count()

# COMMAND ----------

# TODO: Add vaccination status
cat_feats = ["covid", "age_group", "sex", 
             "region_code", 
             "prior_hosp", "chronic_disease_score"]
event_columns = [f"e_{c}" for c in conditions_list]
duration_columns = [f"d_{c}" for c in conditions_list]

print(cat_feats)
print(len(event_columns))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Model
# MAGIC
# MAGIC *What analyses could solve my problem?*

# COMMAND ----------

cph_output_column_remap = {
    "covariate": "covariate",
    "coef": "coef",
    "exp(coef)": "exp_coef",
    "se(coef)": "se_coef",
    "coef lower 95%": "coef_lower_95",
    "coef upper 95%": "coef_upper_95",
    "exp(coef) lower 95%": "exp_coef_lower_95",
    "exp(coef) upper 95%": "exp_coef_upper_95",
    "cmp to": "cmp_to",
    "z": "z",
    "p": "p",
    "-log2(p)": "-log2_p"
}

# COMMAND ----------

import logging
logger = spark._jvm.org.apache.log4j
logging.getLogger("py4j.java_gateway").setLevel(logging.ERROR)

# COMMAND ----------

stats = []
failed_ccsrs = []

for i, event in enumerate(conditions_list):

    # Smoke test
    if i >= 0:
    
        print(i, end="")

        event_pdf = cohort_df.select(
            *cat_feats, 
            F.col(f"e_{event}").alias('event'),
            F.col(f"d_{event}").alias('duration')
        ).filter(
            F.col('event').isNotNull()
        ).groupBy(
            *cat_feats,
            'duration',
            'event'
        ).count().toPandas()

        try:
            # Fit CoxPH model
            cph = CoxPHFitter()
            cph.fit(event_pdf, duration_col='duration', event_col='event', weights_col='count', formula='covid + C(age_group) + C(region_code) + C(chronic_disease_score) + sex + prior_hosp')

            # Save summary stats
            summary_stats_pdf = cph.summary
            summary_stats_pdf.reset_index(inplace=True)
            summary_stats_pdf.rename(cph_output_column_remap, axis="columns", inplace=True)
            summary_stats_pdf.replace([np.inf, -np.inf], np.nan, inplace=True)
            
            summary_stats_pdf['n_event'] = cph.weights[cph.event_observed > 0].sum()
            summary_stats_pdf['n_at_risk'] = cph.weights.sum()
            summary_stats_pdf['error_flag'] = False

        except Exception as e:
            # Track failed ccsr
            print('<-error', end="")
            failed_ccsrs.append((event, e))
            summary_stats_pdf = pd.DataFrame(
                data=[[None]*len(cph_output_column_remap.values())],
                columns=cph_output_column_remap.values()
            )
            summary_stats_pdf['n_event'] = np.nan
            summary_stats_pdf['n_at_risk'] = np.nan
            summary_stats_pdf['error_flag'] = True
        
        finally:
            
            print(";", end = " ")
            
            summary_stats_pdf['category'] = event

            stats.append(summary_stats_pdf)
            
            del event_pdf, cph, summary_stats_pdf

# COMMAND ----------

refit_stats = []
refit_failed_ccsrs = []

for i, event in enumerate([failed[0] for failed in failed_ccsrs]):

    # Smoke test
    if i >= 0:
    
        print(i, end="")

        event_pdf = cohort_df.select(
            *cat_feats, 
            F.col(f"e_{event}").alias('event'),
            F.col(f"d_{event}").alias('duration')
        ).filter(
            F.col('event').isNotNull()
        ).groupBy(
            *cat_feats,
            'duration',
            'event'
        ).count().toPandas()

        try:
            # Fit CoxPH model
            cph = CoxPHFitter()
            cph.fit(event_pdf, duration_col='duration', event_col='event', weights_col='count', formula='covid + C(age_group) + C(region_code) + C(chronic_disease_score) + sex + prior_hosp', fit_options={'step_size':0.25})

            # Save summary stats
            summary_stats_pdf = cph.summary
            summary_stats_pdf.reset_index(inplace=True)
            summary_stats_pdf.rename(cph_output_column_remap, axis="columns", inplace=True)
            summary_stats_pdf.replace([np.inf, -np.inf], np.nan, inplace=True)
            
            summary_stats_pdf['n_event'] = cph.weights[cph.event_observed > 0].sum()
            summary_stats_pdf['n_at_risk'] = cph.weights.sum()
            summary_stats_pdf['error_flag'] = False

        except Exception as e:
            # Track failed ccsr
            print('<-error', end="")
            refit_failed_ccsrs.append((event, e))
            summary_stats_pdf = pd.DataFrame(
                data=[[None]*len(cph_output_column_remap.values())],
                columns=cph_output_column_remap.values()
            )
            summary_stats_pdf['n_event'] = np.nan
            summary_stats_pdf['n_at_risk'] = np.nan
            summary_stats_pdf['error_flag'] = True
        
        finally:
            
            print(";", end = " ")
            
            summary_stats_pdf['category'] = event

            refit_stats.append(summary_stats_pdf)
            
            del event_pdf, cph, summary_stats_pdf

# COMMAND ----------

stats[0]

# COMMAND ----------

schema = (
    spark
    .createDataFrame(
        pd.concat(stats)
    )
    .filter('covariate is not null')
    .schema
)

# COMMAND ----------

# Write summary statistics
(
    spark
    .createDataFrame(
        pd.concat(stats)
    )
    .filter('covariate is not null')
    .unionByName(
        spark
        .createDataFrame(
            pd.concat(refit_stats),
            schema=schema
        )
    )
    .join(
        spark
        .createDataFrame(
            pd.DataFrame(refit_failed_ccsrs, columns=['category','error']).astype({'error':str})
        ),
        'category',
        'left'
    )
    .write
    .mode("overwrite")
    .format("parquet")
#     .partitionBy("category")
    .saveAsTable(output_table_name)
)

# COMMAND ----------

(
    spark
    .table(output_table_name)
    .filter('covariate = "covid" or covariate is null')
    .select(
        'category',
        'n_event',
        'n_at_risk',
        'exp_coef',
        'exp_coef_lower_95',
        'exp_coef_upper_95',
        'p',
        'error'
    )
    .display()
)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Evaluate
# MAGIC *Did I solve my problem? If not, why? (e.g. found a new problem that I'm stuck on, invalid assumptions, etc.)*

# COMMAND ----------

# for i, event in enumerate(conditions_list):

#     # Smoke test
#     if i == 32:
    
#         print(i, end="")

#         event_pdf = cohort_df.select(
#             *cat_feats, 
#             F.col(f"e_{event}").alias('event'),
#             F.col(f"d_{event}").alias('duration')
#         ).filter(
#             F.col('event').isNotNull()
#         ).toPandas()

#         # Fit CoxPH model
#         cph = CoxPHFitter()
#         cph.fit(event_pdf, duration_col='duration', event_col='event', formula='covid + C(age_group) + C(region_code) + C(chronic_disease_score) + sex + prior_hosp', fit_options={'step_size':0.5})

#         # Save summary stats
#         summary_stats_pdf = cph.summary
#         summary_stats_pdf.reset_index(inplace=True)
#         summary_stats_pdf.rename(cph_output_column_remap, axis="columns", inplace=True)
#         summary_stats_pdf.replace([np.inf, -np.inf], np.nan, inplace=True)

#         summary_stats_pdf['n_event'] = cph.weights[cph.event_observed > 0].sum()
#         summary_stats_pdf['n_at_risk'] = cph.weights.sum()
#         summary_stats_pdf['error_flag'] = False

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Footer

# COMMAND ----------


