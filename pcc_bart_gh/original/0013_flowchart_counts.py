# Databricks notebook source
# MAGIC %md
# MAGIC # Create Time to (CCSR) Event Table
# MAGIC
# MAGIC This notebook generates a table with one unique hvid per row and 400+ columns (i.e. each CCSR category). The value of in the cell of any given hvid-CCSR pair indicates the number of days from that HVID's index date until the first post covid condition (PCC) for the category. If the value is null, no PCC was observed. If the value is negative, then the individual had a history of the condition in the 1 year prior and should be excluded from analyses for that condition.

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

# COMMAND ----------

# spark configuration
spark.conf.set('spark.sql.shuffle.partitions', 6400)
spark.conf.set('spark.databricks.adaptive.autoOptimizeShuffle.enabled', False)
spark.conf.set('spark.databricks.queryWatchdog.enabled', False)
spark.conf.set('spark.sql.files.maxPartitionBytes', 1024 * 1024 * 128)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filters and Definitions

# COMMAND ----------

#%run ../includes/0000-hv-filters

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

# MAGIC %run ./includes/0000-GLOBAL-NAMES

# COMMAND ----------

# Set date_range_cond
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

# set ci_date_range_cond
CI_DATE_RANGE_COND = (
    
    # Date is in 2020 but in or after March
    (
        (F.col("year") == 2020) 
        & (F.col("adm_mon") >= 2020103)
    ) 
    | (
    (F.col("year") == 2021)
    )
    
    # Or date is in 2022 but in or before MAY
    | (
        (F.col("year") == 2022)
        & (F.col('adm_mon') <= 2022205)
    )
)

# set enroll_date_range_cond
ENROLL_DATE_RANGE_COND = (
    
    # Date is in any month in 2019, 2020, or 2021
    F.col("year").isin([2019,2020,2021])
        
    # Or date is in 2022 but in or before May
    |(
        (F.col("year") == 2022)
        & (F.col('adm_mon') <= 2022205)
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
# MAGIC `med_claims`: All closed source medical claims from March 1, 2020 thru November 30, 2021 <br>
# MAGIC `eligible_med_claims`: All closed source medical claims from March 1, 2019 thru May 31, 2022 

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
              .filter(DATE_RANGE_COND)
             )

eligible_med_claims = (
    medrec_icd
    .filter(ENROLL_DATE_RANGE_COND)
    
  
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
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## HV Closed Medical Claims: People with at least 1 medical claim 3/1/2020 - 11/30/2021

# COMMAND ----------

(
    med_claims
    .select('medrec_key')
    .distinct()
    .count()
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
        #'inst_type_of_bill_std_id', -- patbill?
        'prov_id', #'place_of_service_std_id',
        #'ndc_code', Not available in Premier
        #'cpt_code', #'procedure_code',
        #'icd_code', #'diagnosis_code'
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

(        
    medrec_lab
    .filter(DATE_RANGE_COND)
    .select(
        'medrec_key',
        'adm_mon',
        F.lit(0).alias('inpatient'),
        F.lit(1).alias('covid'),
    )
    .filter(
        F.col('test').isin(EXCLUSION_LAB)
        & F.col('observation').isin('positive')
    )
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Premier Merged: People with one medical claim or lab

# COMMAND ----------

(
  
    # Get all medical claims with standard HV filters applied
    med_claims
    .filter(
        ~F.col('icd_code').isin('', ' ')
        & F.col('icd_code').isNotNull()
    )
    
    # Select necessary columns from medical claims. Create flags for covid and inpatient indicators.
    .select(
        'medrec_key',
    )
    
    # Get lab information and look for positive diagnositc lab tests
    .unionByName(
        med_lab
        .filter(DATE_RANGE_COND)
        .select(
            'medrec_key',
        )
        .filter(
            F.col('test').isin(EXCLUSION_LAB)
            & F.col('observation').isin('positive')
        )
        ,
        allowMissingColumns=True   
    )
    
    .distinct()
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Exposed Unexposed Counts

# COMMAND ----------

 display(spark.table(f'{DB_WRITE}.{WIDE_OUTPUT_NAME}')).limit(10)

# COMMAND ----------

_condition_claims_df = (
    spark.table(f'{DB_WRITE}.{WIDE_OUTPUT_NAME}')
    .select(
        'medrec_key',
        'index_date',
        'days_enrolled',
        'covid_indicator',
        'covid',
        'age',
        'gender',
        'prov_region',
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

print(cases.count())
print(controls.count())

# COMMAND ----------

(
    _condition_claims_df
    .filter(
        ~(
            ~((F.col('covid_indicator') == 1) & (F.col('covid') == 0))
            & F.col('age').between(0,64)
            & F.col('patient_gender').isin('M','F')
            & F.col('patient_state').isin(list(states_pydict.values()))
            & (F.col('days_enrolled') >= 500)
        )
    )
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Final Cohort Count

# COMMAND ----------

(
    spark
    .table(f'{DB_WRITE}.{WIDE_OUTPUT_NAME_FINAL}')
    .select('medrec_key')
    .count()
)

# COMMAND ----------

(
    spark
    .table(f'{DB_WRITE}.{WIDE_OUTPUT_NAME_FINAL}')
    .select('medrec_key','covid')
    .groupBy('covid')
    .count()
    .display()
)

# COMMAND ----------

(
    spark
    .table(f'{DB_WRITE}.{WIDE_OUTPUT_NAME_FINAL}')

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

display(    spark
    .table(f'{DB_WRITE}.{WIDE_OUTPUT_NAME_FINAL}'))

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### CCSR
# MAGIC
# MAGIC `conditions_df`: look-up table of ICD codes and CCSR category

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
conditions_list.append('INF012')
conditions_list = sorted(set(conditions_list))

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### COVID Indicators
# MAGIC
# MAGIC `hvid_covid_indicators`: This dataframe flags all unique hvids that had at least one indicator of a covid diagnosis based on ICD codes, procedure codes, NDC codes, and text from CDM.

# COMMAND ----------

hvid_covid_indicators = (
    spark
    .table(
        f"{DB_WRITE}.{COVID_INDICATOR_TABLE}"
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Enrollment

# COMMAND ----------

medical_enrollment = (
    spark
    
    # Load enrollment table
    .table(
        f'{DB_READ}.{ENROLL_TABLE_NAME}'
    )

    # Limit file size by filtering based on the study period
    .filter(
        ENROLL_DATE_RANGE_COND
    )
    .filter(
        F.col("benefit_type") == "MEDICAL"
    )
    .select('hvid', "calendar_date", "year")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### df
# MAGIC
# MAGIC Generate a dataframe with one row for each unique ID, an index date, and a COVID flag.

# COMMAND ----------

med_claims

# COMMAND ----------

(

    # Get all medical claims with standard HV filters applied
    med_claims
    .filter(
        ~F.col('diagnosis_code').isin('', ' ')
        & F.col('diagnosis_code').isNotNull()
    )
    
    # Select necessary columns from medical claims. Create flags for covid and inpatient indicators.
    .select(
        'hvid',
        'date_service'
    )
    .distinct()
    
    # Add enrollment information
    .join(
        medical_enrollment.withColumnRenamed('hvid','e_hvid'),
        [
            # Match hvid
            (F.col('hvid') == F.col('e_hvid')) &

            # Enrollment at least 1 year before covid index, and 0.5 year after.
            (
                F.datediff('calendar_date', 'date_service').between(-365, 180)
            )
        ],
        "inner"
    )
    .drop('e_hvid')
    
    .select('hvid')
    .distinct()
    .count()
)

# COMMAND ----------

df = (
  
    # Get all medical claims with standard HV filters applied
    med_claims
    .filter(
        ~F.col('diagnosis_code').isin('', ' ')
        & F.col('diagnosis_code').isNotNull()
    )
    
    # Select necessary columns from medical claims. Create flags for covid and inpatient indicators.
    .select(
        'hvid',
        F.coalesce('claim_id','hv_enc_id').alias('claim_id'),
        'date_service',
        'diagnosis_code',
        'place_of_service_std_id',
        'inst_type_of_bill_std_id',
        'patient_year_of_birth',
        'patient_gender',
        'patient_state',
        F.lit(None).alias('covid'),
        
        F.when(
            inpatient_cond,
            1
        )
        .otherwise(0)
        .alias('inpatient_index'),
    )
    
    # Get lab information and look for positive diagnositc lab tests
    .unionByName(
        spark
        .table(f"{DB_READ}.{LAB_TABLE_NAME}")
        .filter(DATE_RANGE_COND)
        .select(
            'hvid',
            'date_service',
            F.lit(0).alias('inpatient'),
            F.lit(1).alias('covid'),
        )
        .filter(
            F.col('hv_test_type').isin('COVID Diagnostic')
            & F.col('hv_result').isin('Positive')
        )
        ,
        allowMissingColumns=True   
    )
    
    # Generate one row per hvid-date pair
    .groupBy(
        'hvid',
        'date_service'
    )
    
    .agg(
        *[
            F.max(c).alias(c) for c in [
                'inpatient_index',
                'patient_year_of_birth',
                'patient_gender',
                'covid',
                'patient_state'
            ]
        ],
        *[
            F.collect_set(c).alias(c) for c in [
                'diagnosis_code', 
            ]
        ],
    )
    
    # Flag COVID based on identifiers (diagnosis code, lab, etc.)
    .withColumn(
        'covid',
        F.when(
            (F.col('covid') == 1)
            | (
                F.array_contains('diagnosis_code', 'U071')
                & F.col('date_service').between(F.lit('2020-04-01').cast('date'), F.lit('2021-11-30').cast('date'))
            )
            | (
                F.array_contains('diagnosis_code', 'B9729')
                & F.col('date_service').between(F.lit('2020-03-01').cast('date'), F.lit('2020-04-30').cast('date'))
            ),
            1
        )
        .otherwise(0)
    )

    .drop('diagnosis_code')
    
    # Assign a row number to a patient based on whether they had covid or not
    .select(
        '*',
        F.max('covid').over(W.Window.partitionBy('hvid')).alias('covid_patient'),
        F.row_number().over(W.Window.partitionBy('hvid').orderBy(F.desc('covid'),'date_service')).alias('date_rn'),
        F.row_number().over(W.Window.partitionBy('hvid').orderBy('date_service').orderBy(F.rand(42))).alias('rand_rn'),
        
    )
    
    # If patient ever had covid, select earliest covid claim. If patient never had covid, select a random claim.
    .filter(
        (
            (F.col('covid_patient') == 1) 
            & (F.col('date_rn') == 1)
        )
        | (
            (F.col('covid_patient') == 0) 
            & (F.col('rand_rn') == 1)
        )
    )
    
    # Clean up dataframe
    .select(
        'hvid',
        F.col('date_service').alias('index_date'),
        'covid',
        'inpatient_index',
        'patient_year_of_birth',
        'patient_gender',
        'patient_state',
        (
            F.datediff(
                F.col('date_service'),
                F.concat_ws('-','patient_year_of_birth',F.lit('07'),F.lit('02')).cast('date')
            )/365.25
        ).alias('age').cast('integer')
        
    )
    
    # Add enrollment information
    .join(
        medical_enrollment.withColumnRenamed('hvid','e_hvid'),
        [
            # Match hvid
            (F.col('hvid') == F.col('e_hvid')) &

            # Enrollment at least 1 year before covid index, and 0.5 year after.
            (
                F.datediff('calendar_date', 'index_date').between(-365, 180)
            )
        ],
        "inner"
    )
    .drop('e_hvid')
    
    .groupBy('hvid')
    .agg(
        F.countDistinct("calendar_date").cast('integer').alias("days_enrolled"),
        *[
            F.first(c, ignorenulls=True).alias(c) for c in [
                'index_date',
                'covid',
                'inpatient_index',
                'patient_year_of_birth',
                'patient_gender',
                'age',
                'patient_state'
            ]
        ]
    )
    
    # Flag controls based on presence of covid indicators (e.g. medications)
    .join(
        hvid_covid_indicators.withColumn('covid_indicator', F.lit(1)).withColumnRenamed('hvid','ci_hvid'),
        [(F.col('hvid') == F.col('ci_hvid'))],
        'left'
    )
    .drop('ci_hvid')
    
    # Clean dataframe
    .select(
        'hvid',
        'days_enrolled',
        'index_date',
        'covid',
        'covid_indicator',
        'inpatient_index',
        'age',
        'patient_gender',
        'patient_state',
    )
    
    .na.fill(
        {
            'covid_indicator':0,
            'patient_gender':'U',
        }
    )
    
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Conditions & Claims

# COMMAND ----------

ccsr_med_claims = (
    # Use all medical claims
    eligible_med_claims

    # Require diganosis codes for claims
    .filter(
        ~F.col('diagnosis_code').isin('', ' ')
        & F.col('diagnosis_code').isNotNull()

    )

    # Select necessary columns
    .select(
        'hvid',
        'date_service',
        'diagnosis_code',
    )
  
    .distinct()
)

combined_df = (
    df
    .join(
        ccsr_med_claims.withColumnRenamed('hvid','ccsr_hvid')
        ,
        [
            (F.col('hvid') == F.col('ccsr_hvid')),
            F.datediff(F.col('date_service'), F.col('index_date')).between(-365, 180)
        ]
        ,
        'left'
    )
    .drop('ccsr_hvid')    
    
    # Add pre-existing conditions
    .join(
        F.broadcast(conditions_df.select('diagnosis_code','CCSR_Category')),
        'diagnosis_code',
        'left'
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Write Combined DF

# COMMAND ----------

f'{DB_WRITE}.{LONG_OUTPUT_NAME}'

# COMMAND ----------

# spark.sql(f'DROP TABLE IF EXISTS {DB_WRITE}.{LONG_OUTPUT_NAME}')

# COMMAND ----------

(
    combined_df
    .write
    .mode('overwrite')
    .format('parquet')
    .saveAsTable(f'{DB_WRITE}.{LONG_OUTPUT_NAME}')
)

# COMMAND ----------

# spark configuration
spark.conf.set('spark.sql.shuffle.partitions', 6400)
spark.conf.set('spark.databricks.adaptive.autoOptimizeShuffle.enabled', False)
spark.conf.set('spark.databricks.queryWatchdog.enabled', False)

# COMMAND ----------

spark.sql(f'CONVERT TO DELTA {DB_WRITE}.{LONG_OUTPUT_NAME}')

# COMMAND ----------

spark.sql(f'OPTIMIZE {DB_WRITE}.{LONG_OUTPUT_NAME}')

# COMMAND ----------

combined_df = (
    spark.table(f'{DB_WRITE}.{LONG_OUTPUT_NAME}')
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Write Wide DF

# COMMAND ----------

condition_claims_df = (
    
    combined_df

    # Flags
    .select(
        '*',
        days_elapsed := F.datediff('date_service','index_date').alias('days_elapsed'),
        period := F.when(
            days_elapsed.between(-365, -7),
            'a'
        ).when(
            days_elapsed.between(31, 180),
            'c'
        )
        .otherwise('o')
        .alias('period'),
        F.row_number().over(W.Window.partitionBy('hvid','CCSR_Category').orderBy(period,'index_date')).alias('rn')
    )
    
    .filter(
        (F.col('rn') == 1)
    )

    
    # Pivot table so that all CCSR categories are columns
    .groupBy(
        'hvid',
        'days_enrolled',
        'index_date',
        'covid',
        'covid_indicator',
        'inpatient_index',
        'age',
        'patient_gender',
        'patient_state',
        
    )
    .pivot('CCSR_Category', conditions_list)
    .agg(
        F.min('days_elapsed'),
    )
    
)

# COMMAND ----------

# spark configuration
spark.conf.set('spark.sql.shuffle.partitions', 6400)
spark.conf.set('spark.databricks.adaptive.autoOptimizeShuffle.enabled', True)
spark.conf.set('spark.databricks.queryWatchdog.enabled', False)

# COMMAND ----------

# spark.sql(f'DROP TABLE IF EXISTS {DB_WRITE}.{WIDE_OUTPUT_NAME}')

# COMMAND ----------

(
    condition_claims_df
    .write
    .mode('overwrite')
    .format('parquet')
    .saveAsTable(f'{DB_WRITE}.{WIDE_OUTPUT_NAME}')
)

# COMMAND ----------

spark.sql(f'CONVERT TO DELTA {DB_WRITE}.{WIDE_OUTPUT_NAME}')

# COMMAND ----------

spark.sql(f'OPTIMIZE {DB_WRITE}.{WIDE_OUTPUT_NAME}')

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


