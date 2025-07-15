# Databricks notebook source
# MAGIC %md
# MAGIC # Create Time to (CCSR) Event Table
# MAGIC
# MAGIC This notebook generates a table with one unique patient per row and 400+ columns for each CCSR category. The value of in the cell of any given patient-CCSR pair indicates the number of days from that medrec_key's index date until their first post covid condition.

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
# MAGIC - Author: Adewole Oyalowo
# MAGIC   - Email: [sve5@cdc.gov](sve5@cdc.gov) <!--- enter your email address in both the square brackets and the parens -->
# MAGIC
# MAGIC - Author: Ka'imi Kahihikolo
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
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import scipy.stats as ana

from pyspark.sql.functions import col, concat, expr, lit

# COMMAND ----------

# spark configuration
spark.conf.set('spark.sql.shuffle.partitions', 6400)
spark.conf.set('spark.databricks.adaptive.autoOptimizeShuffle.enabled', True)
spark.conf.set('spark.databricks.queryWatchdog.enabled', True)

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
ccsr_lookup = spark.table("cdh_reference_data.ccsr_codelist")

prov=providers.join(zip3,"prov_id","inner")

# COMMAND ----------

DB_NAME = "cdh_premier"
#DB_NAME = "cdh_hv_race_ethnicity_covid"
#MED_TABLE_NAME = "medical_claims"
#PHARMACY_TABLE_NAME = "pharmacy_claims"
#ENROLL_TABLE_NAME = "enrollment"
#LAB_TABLE_NAME = "lab_tests_cleansed"
LONG_OUTPUT_NAME = "rvj5_medrec_covid_ccsr"
WIDE_OUTPUT_NAME = "rvj5_medrec_ccsr_wide_cohort"
TABLE1 = "rvj5_medrec_icd"
TABLE2 = "rvj5_medrec_cpt"
TABLE3 = "rvj5_medrec_lab"

# COMMAND ----------

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
# MAGIC #### Exclucsion

# COMMAND ----------

EXCLUSION_LAB = [
    "SARS coronavirus 2 RNA:PrThr:Pt:Respiratory:Ord:Probe.amp.tar",
    "SARS coronavirus 2 RNA:PrThr:Pt:XXX:Ord:Probe.amp.tar"
]

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

EXCLUSION_NDC_CODES = [ 
    #not used in Premier
    "61755004202",
    "61755004502",
    "61755003502",
    "61755003608",
    "61755003705",
    "61755003805",
    "61755003901",
    "61755002501",
    "61755002701",
    "00002791001",
    "00002795001",
    "00002791001",
    "00002795001",
    "00173090186",
    "00002758901",
    "00069108530",
    "00069108506",
    "00006505506",
    "00006505507",
    "61958290202",
    "61968290101"
]
EXCLUSION_PROCEDURE_CODES = [
    #unchanged from HV
    "Q0240",
    "Q0243",
    "Q0244",
    "M0240",
    "M0241",
    "M0243",
    "M0244",
    "Q0245",
    "M0245",
    "M0246",
    "Q0247",
    "M0247",
    "M0248",
    "Q0222",
    "M0222",
    "M0223",
    "J0248"
]
EXCLUSION_TREATMENT_NAME = [
    #unchanged from HV
    "casirivimab",
    "imdevimab",
    "etesevimab",
    "bamlanivimab",
    "sotrovimab",
    "bebtelovimab",
    "paxlovid",
    "molnupiravir",
    "remdesivir"
]

# COMMAND ----------

icd_code=icd_code.withColumnRenamed('icd_code', 'icd_code_original').select("pat_key", "ICD_PRI_SEC", "year", "quarter", "icd_code_original", F.translate(F.col("icd_code_original"), ".", "").alias("icd_code"))
display(icd_code,limit=10)

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

# COMMAND ----------

#Covid Mediations Table
meds = spark.sql("""

with meds as (
        select
          distinct pat_key, YEAR
        from
           cdh_premier.patbill pb
           left join cdh_premier.chgmstr charge on charge.std_chg_code = pb.std_chg_code
          where
          upper(std_chg_desc) like '%PAXLOVID%'
          or upper(std_chg_desc) like '%MOLNUPIR%'
          or upper(std_chg_desc) like '%EVUSHELD%'
          or upper(std_chg_desc) like '%TIXAGEVIMAB%'
          or upper(std_chg_desc) like '%CILGAVIMAB%'
          or upper(std_chg_desc) like '%BEBTELOVIMA%'
          or upper(std_chg_desc) like '%SOTROVIMAB%'
          or upper(std_chg_desc) like '%BAMLANIVIMAB%'
          or upper(std_chg_desc) like '%ETESEVIMAB%'
          or upper(std_chg_desc) like '%REGEN-COV%'
          or upper(std_chg_desc) like '%CASIRIVIMAB%'
          or upper(std_chg_desc) like '%IMDEVIMAB%'
--           or upper(std_chg_desc) like '%DEXAMETHASONE%'
--           or upper(std_chg_desc) like '%TOFACITINIB%'
--           or upper(std_chg_desc) like '%TOCILIZUMAB%'
--           or upper(std_chg_desc) like '%SARILUMAB%'
--           or upper(std_chg_desc) like '%BARICITINIB%'
          or upper(std_chg_desc) like '%REMDESIVIR%'
          and YEAR>=2020
        union



       select
          distinct pat_key, YEAR
          from
          cdh_premier.patcpt patcpt
          left join cdh_premier.cptcode cpt_lookup on patcpt.cpt_code = cpt_lookup.cpt_code
      
          where
          upper(cpt_desc) like '%PAXLOVID%'
          or upper(cpt_desc) like '%MOLNUPIR%'
          or upper(cpt_desc) like '%EVUSHELD%'
          or upper(cpt_desc) like '%TIXAGEVIMAB%'
          or upper(cpt_desc) like '%CILGAVIMAB%'
          or upper(cpt_desc) like '%BEBTELOVIMA%'
          or upper(cpt_desc) like '%SOTROVIMAB%'
          or upper(cpt_desc) like '%BAMLANIVIMAB%'
          or upper(cpt_desc) like '%ETESEVIMAB%'
          or upper(cpt_desc) like '%REGEN-COV%'
          or upper(cpt_desc) like '%CASIRIVIMAB%'
          or upper(cpt_desc) like '%IMDEVIMAB%'
--           or upper(cpt_desc) like '%DEXAMETHASONE%'
--           or upper(cpt_desc) like '%TOFACITINIB%'
--           or upper(cpt_desc) like '%TOCILIZUMAB%'
--           or upper(cpt_desc) like '%SARILUMAB%'
--           or upper(cpt_desc) like '%BARICITINIB%'
          or upper(cpt_desc) like '%REMDESIVIR%'



       union



       select
          distinct pat_key, YEAR
          from
          cdh_premier.patbill pb
          left join cdh_premier.hospchg hospcharge on hospcharge.hosp_chg_id = pb.hosp_chg_id
          where
          upper(hosp_chg_desc) like '%PAXLOVID%'
          or upper(hosp_chg_desc) like '%MOLNUPIR%'
          or upper(hosp_chg_desc) like '%EVUSHELD%'
          or upper(hosp_chg_desc) like '%TIXAGEVIMAB%'
          or upper(hosp_chg_desc) like '%CILGAVIMAB%'
          or upper(hosp_chg_desc) like '%BEBTELOVIMA%'
          or upper(hosp_chg_desc) like '%SOTROVIMAB%'
          or upper(hosp_chg_desc) like '%BAMLANIVIMAB%'
          or upper(hosp_chg_desc) like '%ETESEVIMAB%'
          or upper(hosp_chg_desc) like '%REGEN-COV%'
          or upper(hosp_chg_desc) like '%CASIRIVIMAB%'
          or upper(hosp_chg_desc) like '%IMDEVIMAB%'
          or upper(hosp_chg_desc) like '%REMDESIVIR%'



)
    select distinct MEDREC_KEY
    from cdh_premier.patdemo pat
    inner join meds on (pat.PAT_KEY=meds.PAT_KEY)
    where meds.YEAR >= 2020 and pat.adm_mon >= 2020103 or pat.adm_mon <= 2022205
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Medrec_ICD

# COMMAND ----------

readmit

# COMMAND ----------

# ID field, date of service (adm_mon +1), diagnosis code - paired with CCSRs

medrec_icd = (patdemo
        .join(prov.withColumnRenamed('year','prov_year'), "prov_id", "inner")
        .join(icd_code.withColumnRenamed('year','icd_year'), "pat_key", "inner")
        #.unionByName(
        #    readmit
        #    .select('medrec_key', 'pat_key', 'days_from_index'))
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
        #'cpt_code', #'procedure_code',
        'icd_code', #'diagnosis_code'
        #'test',
        #'observation',
        'pat_type',
        'i_o_ind'
        #'days_from_index'
        )
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

# COMMAND ----------

display(medrec_icd)

# COMMAND ----------

#spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","false") #Revert to false after executed

# COMMAND ----------

(
    medrec_icd
    .write
    .mode('overwrite')
    .format('parquet')
    .saveAsTable(f'{DB_NAME}_exploratory.{TABLE1}')
)

#spark.sql(f'CONVERT TO DELTA {DB_NAME}_exploratory.{TABLE1}')

#spark.sql(f'OPTIMIZE {DB_NAME}_exploratory.{TABLE1}')

# COMMAND ----------

spark.sql(f'CONVERT TO DELTA {DB_NAME}_exploratory.{TABLE1}')

# COMMAND ----------

medrec_icd = (
    spark.table(f'{DB_NAME}_exploratory.{TABLE1}')
)

# COMMAND ----------

spark.sql(f'OPTIMIZE {DB_NAME}_exploratory.{TABLE1}')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Medrec_CPT

# COMMAND ----------

# ID field, date of service (adm_mon +1), diagnosis code - paired with CCSRs

medrec_cpt = (patdemo
        .join(prov.withColumnRenamed('year','prov_year'), "prov_id", "inner")
        .join(patcpt.withColumnRenamed('year','cpt_year'), "pat_key", "inner") #Put into a separate table
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
        'cpt_code', #'procedure_code',
        #'icd_code', #'diagnosis_code'
        #'test',
        #'observation',
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
           #.withColumn("CPT_FLAG",
           #            F.when(F.col("cpt_code").isin(EXCLUSION_PROCEDURE_CODES),1).otherwies(0)
           #  )
              
           #.groupBy("medrec_key", "pat_key")
           #.agg(*
           #    [F.max(c).alias(c) for c in[
           #        "CPT_FLAG"
           #    ]])
             )

# COMMAND ----------

#(
#    medrec_cpt
#    .write
#    .mode('overwrite')
#    .format('parquet')
#    .saveAsTable(f'{DB_NAME}_exploratory.{TABLE2}')
#)

# COMMAND ----------

#spark.sql(f'CONVERT TO DELTA {DB_NAME}_exploratory.{TABLE2}')

# COMMAND ----------

#medrec_cpt = (
#    spark.table(f'{DB_NAME}_exploratory.{TABLE2}')
#)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Medrec_LAB

# COMMAND ----------

# ID field, date of service (adm_mon +1), diagnosis code - paired with CCSRs

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

# COMMAND ----------

#(
#    medrec_lab
#    .write
#    .mode('overwrite')
#    .format('parquet')
#    .saveAsTable(f'{DB_NAME}_exploratory.{TABLE3}')
#)

# COMMAND ----------

#spark.sql(f'CONVERT TO DELTA {DB_NAME}_exploratory.{TABLE3}')

# COMMAND ----------

#medrec_lab = (
#    spark.table(f'{DB_NAME}_exploratory.{TABLE3}')
#)

# COMMAND ----------

#display(med_record, limit=10)

# COMMAND ----------

# med_claims.cache().count()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### CCSR

# COMMAND ----------

conditions_df = (
    spark
    .table('cdh_reference_data.ccsr_codelist')
    .filter(
        'Al_Aly_plus = 1'
    )
    .withColumn(
        'icd_code',
        F.col('ICD-10-CM_Code')
    )
    .filter(
        ~F.col('icd_code').isin(EXCLUSION_DIAG_CODES)
    )  
)

# COMMAND ----------

conditions_list = [row[0] for row in conditions_df.select('CCSR_Category').distinct().collect()]
conditions_list = sorted(conditions_list)

# COMMAND ----------

display(conditions_df, limit=10)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### COVID Indicators
# MAGIC
# MAGIC *note* : This dataframe should be renamed to something like "covid_indicators". This dataframe flags all unique hvids that had at least one indicator of a covid diagnosis based on ICD codes, procedure codes, NDC codes, and text from CDM.

# COMMAND ----------

covid_indicators = (
    
    # Load all medical claims and search for COVID indicators
    medrec_icd
    
    .filter(
        F.col('icd_code').isin(EXCLUSION_DIAG_CODES) & CI_DATE_RANGE_COND
        #| F.col('cpt_code').isin(EXCLUSION_PROCEDURE_CODES)
        #| F.col('test').isin(EXCLUSION_LAB) & F.col('observation').isin('POSITIVE')
        #| F.col('ndc_code').isin(EXCLUSION_NDC_CODES) -- no analogue in premier
    )
    
    .select(
        'medrec_key'
    )
    .unionByName(
        meds
        .select(
            'medrec_key'
        )
    )
        
    .unionByName(
        medrec_lab
        .select(
            'medrec_key'
        )
    .filter(
        F.col("test").isin(EXCLUSION_LAB) & (F.col("observation")=="positive") 
        & CI_DATE_RANGE_COND)
    )
        
    .unionByName(
        medrec_cpt
        .select(
            'medrec_key'
        )
    .filter(
        F.col("cpt_code").isin(EXCLUSION_PROCEDURE_CODES)
        & CI_DATE_RANGE_COND)
    )        
    .select('medrec_key')
    .distinct()
)

# COMMAND ----------

covid_indicators.agg(F.countDistinct('medrec_key')).display()

# COMMAND ----------

display(covid_indicators)

# COMMAND ----------

#Potentially saving the table out
##

(
    covid_indicators
    .write
    .mode('overwrite')
    .format('parquet')
    .saveAsTable(f'{DB_NAME}_exploratory.covid_indicators')
)


spark.sql(f'CONVERT TO DELTA {DB_NAME}_exploratory.covid_indicators')

# COMMAND ----------

covid_indicators = (
    spark.table(f'{DB_NAME}_exploratory.covid_indicators')
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Enrollment

# COMMAND ----------

#FLAGING FOR PREMIER CHECK
medical_enrollment = (
    medrec_icd
    # FLAG: Look at this data cond.
    # Limit file size by filtering based on the study period
    .filter(
        ENROLL_DATE_RANGE_COND
    )
    .select('medrec_key', "adm_date")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### df

# COMMAND ----------

df = (
  
    # Get all medical claims with standard HV filters applied
    medrec_icd
    .filter(
        DATE_RANGE_COND
    )    
    
    # Select necessary columns from medical claims. Create flags for covid and inpatient indicators.
    .select("*",
        F.lit(None).alias('covid'),
    )
    
    # Get lab information and look for positive lab tests
    .unionByName(
        medrec_lab
        .select(
            'medrec_key',
            F.lit(1).alias('covid'),
        )
        .filter(
            F.col("test").isin(EXCLUSION_LAB) & (F.col("observation")=="positive") 
            & DATE_RANGE_COND
            ),
        allowMissingColumns = True
    )
    
    # Generate one row per hvid-date pair
    .groupBy(
        'medrec_key',
        'adm_date'
    )

    .agg(
        *[
            F.max(c).alias(c) for c in [ 
                'prov_division', #zip
                'prov_zip3',
                'io_flag',
                'age',
                'gender',
                'raceEth',
                'covid'
            ]
        ],
        *[
            F.collect_set(c).alias(c) for c in [
                'icd_code', 
            ]
        ],
    )
    # Flag COVID based on identifiers (diagnosis code, lab, etc.)
    .withColumn(
        'covid',
        F.when(
            (F.col('covid') == 1)
            | (
                F.array_contains('icd_code', 'U071')
                & F.col('adm_date').between(F.lit('2020-04-01').cast('date'), F.lit('2021-11-30').cast('date'))
            )
            | (
                F.array_contains('icd_code', 'B9729')
                & F.col('adm_date').between(F.lit('2020-03-01').cast('date'), F.lit('2020-04-30').cast('date'))
            ),
            1
        )
        .otherwise(0)
    )    
    # Assign a row number to a patient based on whether they had covid or not
    .select(
        '*',
        F.max('covid').over(W.Window.partitionBy('medrec_key')).alias('covid_patient'),
        F.row_number().over(W.Window.partitionBy('medrec_key').orderBy(F.desc('covid'),'adm_date')).alias('date_rn'),
        F.row_number().over(W.Window.partitionBy('medrec_key').orderBy('adm_date').orderBy(F.rand(42))).alias('rand_rn'),
        
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
        'medrec_key',
        F.col('adm_date').alias('index_date'),
        'covid',
        'io_flag',
        'age',
        'gender',
        'RaceEth',
        'prov_division',
        'prov_zip3'
    )   
    # Add enrollment information #INCOMPLETE
    .join(
        medical_enrollment.withColumnRenamed('medrec_key','record_medrec_key'),
        [
            # Match hvid
            (F.col('medrec_key') == F.col('record_medrec_key')) &

            # Enrollment at least 1 year before covid index, and 0.5 year after. ### here we're more interested in the presence of an encounter
            (
                F.datediff('adm_date', 'index_date').between(-365, -1)
            )
        ],
        "inner"
    )
     .drop('record_medrec_key')
    
    .groupBy('medrec_key')
    .agg(
        F.countDistinct("adm_date").cast('integer').alias("days_enrolled"), ###Premier Analogue?
        *[
            F.min(c).alias(c) for c in [
                'index_date',
                'covid',
                'io_flag',
                'age',
                'gender',
                'RaceEth',
                'prov_division',
                'prov_zip3',
                
            ]
        ]
    )
    
    # Flag controls based on presence of covid indicators (e.g. medications)
    .join(
        covid_indicators.withColumn('covid_indicator', F.lit(1)).withColumnRenamed('medrec_key','ci_medrec_key'),
        [(F.col('medrec_key') == F.col('ci_medrec_key'))],
        'left'
    )
    .drop('ci_medrec_key')
    
    # Clean dataframe
    .select(
        'medrec_key',
        'days_enrolled',
        'index_date',
        'covid',
        'covid_indicator',
        'io_flag',
        'age',
        'gender',
        'RaceEth',
        'prov_division',
        'prov_zip3'
    )
    
    .na.fill(
        {
            'covid_indicator':0,
            'gender':'Unknown',
        }
    )
    
)    #one row for every id per person 1 'covid' 'index_date' 'adm_date'

# COMMAND ----------

display(df)

# COMMAND ----------

### --- Kaimi testing code here --- ###
# df.write.saveAsTable(f"{DB_NAME}_exploratory.rvn4_df_test")

# display(
#     spark.table(f"{DB_NAME}_exploratory.rvn4_df_test").select("covid").distinct()
# )

# display(
#     spark.table(f"{DB_NAME}_exploratory.rvn4_df_test").select("covid_indicator").distinct()
# )

display(
    spark.table(f"{DB_NAME}_exploratory.rvn4_df_test").limit(10)
)

# COMMAND ----------

display(df.filter(F.col("covid")==1))

# COMMAND ----------

display(df.filter(F.col("covid_indicator")==1))

# COMMAND ----------

# %sql clear cache

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Conditions & Claims

# COMMAND ----------

ccsr_med_claims = (
    # Use all medical claims
    medrec_icd

    # Require diganosis codes for claims
    .filter(
        ~F.col('icd_code').isin('', ' ')
        & F.col('icd_code').isNotNull()

    )

    # Select necessary columns
    .select(
        'medrec_key',
        'adm_date',
        'icd_code',
    )
)

combined_df = (
    df
    .join(
        ccsr_med_claims.withColumnRenamed('medrec_key','ccsr_medrec_key')
        ,
        [
            (F.col('medrec_key') == F.col('ccsr_medrec_key')),
            F.datediff(F.col('adm_date'), F.col('index_date')).between(-365, 180)
        ]
        ,
        'left'
    )
    .drop('ccsr_medrec_key')    
    
    # Add pre-existing conditions
    .join(
        F.broadcast(conditions_df.select('icd_code','CCSR_Category')),
        'icd_code',
        'left'
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Write Combined DF

# COMMAND ----------

#Resolved error - table already exists
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")

(
spark.createDataFrame(range(0),schema="`id` INTEGER")
    .write
    .mode('overwrite')
    .format('parquet')
    .saveAsTable(f'{DB_NAME}_exploratory.{LONG_OUTPUT_NAME}')
)

spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","false")

# COMMAND ----------

(
    combined_df
    .write
    .mode('overwrite')
    .format('parquet')
    .saveAsTable(f'{DB_NAME}_exploratory.{LONG_OUTPUT_NAME}')
)
#5.26 minutes

# COMMAND ----------

spark.sql(f'CONVERT TO DELTA {DB_NAME}_exploratory.{LONG_OUTPUT_NAME}')

# COMMAND ----------

spark.sql(f'OPTIMIZE {DB_NAME}_exploratory.{LONG_OUTPUT_NAME}')

# COMMAND ----------

combined_df = (
    spark.table(f'{DB_NAME}_exploratory.{LONG_OUTPUT_NAME}')
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
        days_elapsed := F.datediff('adm_date','index_date').alias('days_elapsed'),
        period := F.when(
            days_elapsed.between(-365, -7), #Change from () to ()
            'a'
        ).when(
            days_elapsed.between(31, 180), #Change from () to ()
            'c'
        )
        .otherwise('o')
        .alias('period'),
        F.row_number().over(W.Window.partitionBy('medrec_key','CCSR_Category').orderBy(period,'index_date')).alias('rn')
    )
    
    .filter(
        (F.col('rn') == 1)
    )

    
    # Pivot table so that all CCSR categories are columns
    .groupBy(
        'medrec_key',
        'days_enrolled',
        'index_date',
        'covid',
        'covid_indicator',
        'io_flag',
        'age',
        'gender',
        'RaceEth',
        'prov_division',
        
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
spark.conf.set('spark.databricks.queryWatchdog.enabled', True)

# COMMAND ----------

(
    condition_claims_df
    .write
    .mode('overwrite')
    .format('parquet')
    .saveAsTable(f'{DB_NAME}_exploratory.{WIDE_OUTPUT_NAME}')
)

# COMMAND ----------

spark.sql(f'CONVERT TO DELTA {DB_NAME}_exploratory.{WIDE_OUTPUT_NAME}')

# COMMAND ----------

spark.sql(f'OPTIMIZE {DB_NAME}_exploratory.{WIDE_OUTPUT_NAME}')

# COMMAND ----------

display(condition_claims_df)

# COMMAND ----------

condition_claims_df.select('medrec_key').count()
