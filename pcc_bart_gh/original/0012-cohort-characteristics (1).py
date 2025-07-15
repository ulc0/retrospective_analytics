# Databricks notebook source
# MAGIC %md
# MAGIC # Time to (CCSR) Event Analysis
# MAGIC
# MAGIC The value of in the cell of any given hvid-CCSR pair indicates the number of days from that HVID's index date until their first post covid condition.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## 2022-10-12

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
from pyspark.sql.functions import col, concat, expr, lit

# spark configuration
spark.conf.set('spark.sql.shuffle.partitions', 1600)
spark.conf.set('spark.databricks.adaptive.autoOptimizeShuffle.enabled', False)
spark.conf.set('spark.databricks.queryWatchdog.enabled', False)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filters and Definitions

# COMMAND ----------

# MAGIC %run ./includes/0000-GLOBAL-NAMES

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
conditions_list.append('INF012')
conditions_list = sorted(set(conditions_list))

# COMMAND ----------

cohort_df = spark.table(f"{DB_WRITE}.{WIDE_OUTPUT_NAME_FINAL}")

# COMMAND ----------

# Using patdemo
# Select cols
# Create raceeth (combined race and ethnicity)
# Groupby medrec_key
# agg
# - get max of race and hispanic_ind
# get distinct
demo = (patdemo
.select('medrec_key', 'pat_key', 'hispanic_ind', 'race')
.withColumn('RaceEth', #Create RaceEthnicity Categorical Variable
    F.when(F.col('HISPANIC_IND')=="Y", "Hispanic")
    .when((F.col('HISPANIC_IND')!="Y") & (F.col('RACE')=="W"), "NH_White")
    .when((F.col('HISPANIC_IND')!="Y") & (F.col('RACE')=="B"), "NH_Black")
    .when((F.col('HISPANIC_IND')!="Y") & (F.col('RACE')=="A"), "NH_Asian")
    .when((F.col('HISPANIC_IND')!="Y") & (F.col('RACE')=="O"), "Other")
    .when((F.col('HISPANIC_IND')!="Y") & (F.col('RACE')=="U"), "Unknown")
    .otherwise("NA")
        )
    .groupBy(
        'medrec_key'
    )

    .agg(
        *[
            F.max(c).alias(c) for c in [ 
                'race',
                'hispanic_ind'
            ]
        ]
        )
    .distinct()
        
        
        
#    .groupBy('medrec_key')
#        .agg(
#            F.countDistinct('HISPANIC_IND').alias('HIS_IND_COUNT'),F.max('HISPANIC_IND').alias('HISPANIC_IND')
#            F.countDistinct('RACE').alias('RACE_IND_COUNT'),F.max('RACE').alias('RACE')
#            )  
#    .distinct()
)

# COMMAND ----------

display(demo)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Preprocessing

# COMMAND ----------

demo.groupBy('race', 'hispanic_ind').count().display()

# COMMAND ----------

# Set chronic disease condtion
chronic_disease_score = (
    F.when(
            F.col("cci_score") >= 3,
            3  # 3+
    ).when(
        F.col("cci_score").isNotNull(),
        F.col('cci_score')
    ).when(
        F.col('pmca_score').isNotNull(),
        F.col('pmca_score')
    )
)


# In: twj8_medrec_ccsr_wide_cohort_final
# Create: 
# - age_group
# - sex
# - chronic_disease_score
# - prior_hosp
# - region_code
# - select others
# Out: cohort_df2

cohort_df2 = (
    spark.table(f"{DB_WRITE}.{WIDE_OUTPUT_NAME_FINAL}")
    
    .select(
        'medrec_key',
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
          F.col("prov_region") == "WEST",
          0
      ).when(
          F.col("prov_region") == "MIDWEST",
          1
      ).when(
          F.col("prov_region") == "SOUTH",
          2
      ).when(
          F.col("prov_region") == "NORTHEAST",
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
    ))

# COMMAND ----------

# set chronic_disease_score function
chronic_disease_score = (
    F.when(
            F.col("cci_score") >= 3,
            3  # 3+
    ).when(
        F.col("cci_score").isNotNull(),
        F.col('cci_score')
    ).when(
        F.col('pmca_score').isNotNull(),
        F.col('pmca_score')
    )
)


# In: twj8_medrec_ccsr_wide_cohort_final
# Create: 
# - age_group
# - sex
# - chronic_disease_score
# - prior_hosp
# - region_code
# Select
# Join demo, left
# Out: cohort_df

cohort_df = (
    spark.table(f"{DB_WRITE}.{WIDE_OUTPUT_NAME_FINAL}")
    
    .select(
        'medrec_key',
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
          F.col("prov_region") == "WEST",
          0
      ).when(
          F.col("prov_region") == "MIDWEST",
          1
      ).when(
          F.col("prov_region") == "SOUTH",
          2
      ).when(
          F.col("prov_region") == "NORTHEAST",
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
    )
    
    .join(
        demo,
        'medrec_key',
        'left'
    )
    
)

display(cohort_df.limit(100))

# COMMAND ----------

# Check
cohort_df.filter(F.col('medrec_key')==-675046063).display()

# COMMAND ----------

cohort_df.filter(F.col('medrec_key')==-675046063).display()

# COMMAND ----------

# TODO: Add vaccination status
# Set cat_feats
cat_feats = ["covid", "age_group", "sex", "region_code", "prior_hosp", "chronic_disease_score", 'race', 'hispanic_ind']

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Frequency Checks
# MAGIC
# MAGIC *What analyses could solve my problem?*

# COMMAND ----------

# Generate counts adult vs. non-adult
(
    cohort_df

    .select('medrec_key',
            'covid',
            'age',
            'inpatient_status',
               F.when((F.col('age') >= 18), 1).otherwise(0).alias('age_group'))
    .groupBy('age_group', 'inpatient_status')
    .count()
    .display()
)

# COMMAND ----------

(
    cohort_df2

    .select('medrec_key',
            'covid',
            'age',
            'inpatient_status',
               F.when((F.col('age') >= 18), 1).otherwise(0).alias('age_group'))
    .groupBy('age_group', 'inpatient_status')
    .count()
    .display()
)

# COMMAND ----------

factors = ['adult', 'inpatient_status', 'covid']

(
    cohort_df
    .groupBy(factors)
    .count()
    .orderBy(
        *factors
    )
    .display()
)

# COMMAND ----------

factors = ['adult','inpatient_status',*cat_feats]

(
    cohort_df
    .groupBy(factors)
    .count()
    .orderBy(
        *factors
    )
    .display()
)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Footer

# COMMAND ----------


