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

# MAGIC %md
# MAGIC ../includes/0000-hv-filters

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
#ccsr_lookup = spark.table("cdh_reference_data.ccsr_codelist")

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

ENROLL_DATE_RANGE_COND = (
    
    # Date is in any month in 2019, 2020, or 2021
    F.col("year").isin([2019,2020,2021])
        
    # Or date is in 2022 but in or before July
    |(
        (F.col("year") == 2022)
        & (F.col('adm_mon') <= 2022307)
    )
)

# COMMAND ----------

EXCLUSION_LAB = [
    "SARS coronavirus 2 RNA:PrThr:Pt:Respiratory:Ord:Probe.amp.tar",
    "SARS coronavirus 2 RNA:PrThr:Pt:XXX:Ord:Probe.amp.tar"
]

EXCLUSION_DIAG_CODES = [
  #changing to doted-code to be Premier-compatible. Verified with icd10data.com
    "A4189",
    "B342",
    "B9729",
    "B9721",
    "B948",
    "J1282",
    "J1281",
    "J1289",
    "M303",
    "M3581",
    "U071",
    "U072"
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

display(icd_code)

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
    where meds.YEAR >= 2020
""")




# COMMAND ----------

prov

# COMMAND ----------

# MAGIC %md
# MAGIC #### Medrec_ICD

# COMMAND ----------

# ID field, date of service (adm_mon +1), diagnosis code - paired with CCSRs

medrec_icd = (patdemo.join(prov.withColumnRenamed('year','prov_year'), "prov_id", "inner")
        .join(icd_code.withColumnRenamed('year','icd_year'), "pat_key", "inner")
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
        'icd_code', #'diagnosis_code'
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
             )

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

medrec_cpt = patdemo.join(prov.withColumnRenamed('year','prov_year'), "prov_id", "inner").join(patcpt.withColumnRenamed('year','cpt_year'), "pat_key", "inner") 
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
        ).withColumn('adm_date',
                       F.to_date(concat(F.substring(col('adm_mon'),1,4),
                                        lit("-"),
                                        F.substring(col('adm_mon'),6,7)), 'yyyy-MM')
                      ).withColumn("IO_FLAG",
                       F.when(F.col("i_o_ind").isin("I") & F.col("pat_type").isin(8),1)
                       .when(F.col("i_o_ind").isin("O") & F.col("pat_type").isin(28),0)
                       .otherwise(-1)
                      ).withColumn("CPT_FLAG",
                       F.when(F.col("cpt_code").isin(EXCLUSION_PROCEDURE_CODES),1).otherwies(0)
             ).groupBy("medrec_key", "pat_key").agg(*[F.max(c).alias(c) for c in[
                   "CPT_FLAG"
               ]]).agg(
        *[
            F.max(c).alias(c) for c in [
                'prov_division', #zip
                'prov_zip3',
                'io_flag',
                'age',
                'gender',
                'covid',
            ] ] )

# COMMAND ----------

(
    medrec_cpt
    .write
    .mode('overwrite')
    .format('parquet')
    .saveAsTable(f'{DB_NAME}_exploratory.{TABLE2}')
)

# COMMAND ----------

spark.sql(f'CONVERT TO DELTA {DB_NAME}_exploratory.{TABLE2}')

# COMMAND ----------

medrec_cpt = (
    spark.table(f'{DB_NAME}_exploratory.{TABLE2}')
)

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

(
    medrec_lab
    .write
    .mode('overwrite')
    .format('parquet')
    .saveAsTable(f'{DB_NAME}_exploratory.{TABLE3}')
)

# COMMAND ----------

spark.sql(f'CONVERT TO DELTA {DB_NAME}_exploratory.{TABLE3}')

# COMMAND ----------

medrec_lab = (
    spark.table(f'{DB_NAME}_exploratory.{TABLE3}')
)

# COMMAND ----------

med_record = (patdemo
    .join(patcpt.withColumnRenamed('year','cpt_year'), "pat_key", "inner") #Put into a separate table
    .join(icd_code.withColumnRenamed('year','icd_year'), "pat_key", "inner")
    .join(prov.withColumnRenamed('year','prov_year'), "prov_id", "inner")
    .join(patlabres.withColumnRenamed('year','lab_year'), "pat_key", "inner") #possible - unionByName( allowmissingcol)
    .filter(DATE_RANGE_COND)
     
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
        #'inst_type_of_bill_std_id', -- patbill?
        'prov_id', #'place_of_service_std_id',
        #'ndc_code', Not available in Premier
        'cpt_code', #'procedure_code',
        'icd_code', #'diagnosis_code'
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

display(med_record, limit=10)

# COMMAND ----------



# COMMAND ----------

all_med_record = (patdemo
    .join(patcpt.withColumnRenamed('year','cpt_year'), "pat_key", "inner")
    .join(icd_code.withColumnRenamed('year','icd_year'), "pat_key", "inner")
    .join(prov.withColumnRenamed('year','prov_year'), "prov_id", "inner")
    .join(patlabres.withColumnRenamed('year','lab_year'), "pat_key", "inner")
  
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
        #'inst_type_of_bill_std_id', -- patbill?
        'prov_id', #'place_of_service_std_id',
        #'ndc_code', Not available in Premier
        'cpt_code', #'procedure_code',
        'icd_code', #'diagnosis_code'
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
)

# COMMAND ----------

conditions_list = [row[0] for row in conditions_df.select('CCSR_Category').distinct().collect()]

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
    med_record
    
    .filter(
        F.col('icd_code').isin(EXCLUSION_DIAG_CODES)
        | F.col('cpt_code').isin(EXCLUSION_PROCEDURE_CODES)
        | F.col('test').isin(EXCLUSION_LAB) & F.col('observation').isin('POSITIVE')
        #| F.col('ndc_code').isin(EXCLUSION_NDC_CODES) -- no analogue in premier
    )
    
    .select(
        'medrec_key'
    )
    .union(
        meds
        .select(
            'medrec_key'
        )
    )
    .select('medrec_key')
    .distinct()
)

# COMMAND ----------

covid_indicators.agg(F.countDistinct('medrec_key')).display()

# COMMAND ----------

display(covid_indicators)

# COMMAND ----------

medrec_covid_indicators = (
    
    # Load all medical claims and search for COVID indicators
    med_record
    
    .filter(
        F.col('icd_code').isin(EXCLUSION_DIAG_CODES)
        | F.col('cpt_code').isin(EXCLUSION_PROCEDURE_CODES)
        #| F.col('ndc_code').isin(EXCLUSION_NDC_CODES) -- no analogue in premier
    )
    
    .select(
        'medrec_key'
    )
    
    # Search all pharmacy claims for COVID indicators
    .unionByName(
        spark

        .table(f"{DB_NAME}.{PHARMACY_TABLE_NAME}")
        .filter(DATE_RANGE_COND)

        .select(
            'medrec_key'
        )

        # Apply HV-recommended filters
        #.filter(
        #    (pharm_cond)
        #    & ~F.col('hvid').isin('', ' ')
        #    & F.col('hvid').isNotNull()
        #    & F.col('data_vendor').isin('Private Source 20')
        #)

        # Apply pharm claim filters
        .filter(
            F.col('diagnosis_code').isin(EXCLUSION_DIAG_CODES)
            | F.col('procedure_code').isin(EXCLUSION_PROCEDURE_CODES)
            | F.col('ndc_code').isin(EXCLUSION_NDC_CODES)
        )
        
    )
    
    # Search all cdm dtl for COVID indicators
    .unionByName(
        spark

        .table(f"{DB_NAME}.cdm_enc_dtl")
        .filter(DATE_RANGE_COND)

        .select(
            'medrec_key'
        )

        # Apply HV-recommended filters
        #.filter(
        #    ~F.col('medrec_key').isin('', ' ')
        #    & F.col('medrec_key').isNotNull()
        #)

        # Apply cdm filters
        .filter(
            F.col('proc_cd').isin(EXCLUSION_PROCEDURE_CODES)
            | F.lower(F.col("vdr_chg_desc")).rlike("|".join(EXCLUSION_TREATMENT_NAME))
            | F.lower(F.col("std_chg_desc")).rlike("|".join(EXCLUSION_TREATMENT_NAME))
        )
        
    )
    
    # Search all cdm diag for COVID indicators
    .unionByName(
        spark

        .table(f"{DB_NAME}.cdm_diag")
        .filter(DATE_RANGE_COND)

        .select(
            'medrec_key'
        )

        # Apply HV-recommended filters
        .filter(
            ~F.col('medrec_key').isin('', ' ')
            & F.col('medrec_key').isNotNull()
        )

        # Apply pharm claim filters
        .filter(
            F.col('diag_cd').isin(EXCLUSION_DIAG_CODES)
        )
        
    )
    
    .select('medrec_key')
    
    .distinct()
    
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Enrollment

# COMMAND ----------

#FLAGING FOR PREMIER CHECK
medical_enrollment = (
    med_record
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
    med_record
    
    # Select necessary columns from medical claims. Create flags for covid and inpatient indicators.
    .select(
        'medrec_key',
        'pat_key',
        #F.coalesce('claim_id','hv_enc_id').alias('claim_id'), not necessary in premier
        'adm_mon', #'date_service',
        'adm_date',
        'icd_code', #'diagnosis_code',
        'cpt_code',
        'test',
        'observation',
        #'place_of_service_std_id', prov_id?
        #'inst_type_of_bill_std_id',
        'age', #'patient_year_of_birth',
        'gender', #'patient_gender',
        'race',
        'hispanic_ind',
        #'patient_state',
        'prov_id',
        'prov_zip3',
        'prov_region',
        'prov_division',
        'io_flag',
        F.lit(None).alias('covid'),
    )
    
    # Get lab information and look for positive lab tests
    .withColumn('covid',
        F.when(F.col('TEST').isin(EXCLUSION_LAB) & F.col('OBSERVATION').isin('POSITIVE'),1).otherwise(0)
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
                'covid',
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
                F.array_contains('icd_code', 'U07.1')
                & F.col('adm_date').between(F.lit('2020-04-01').cast('date'), F.lit('2021-11-30').cast('date'))
            )
            | (
                F.array_contains('icd_code', 'B97.29')
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
                F.datediff('adm_date', 'index_date').between(-365, 0)
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
                'prov_division',
                'prov_zip3'
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

df = (
  
    # Get all medical claims with standard HV filters applied
    med_record
    
    # Select necessary columns from medical claims. Create flags for covid and inpatient indicators.
    .select(
        'medrec_key',
        F.coalesce('claim_id','hv_enc_id').alias('claim_id'),
        'adm_mon', #'date_service',
        'icd_code', #'diagnosis_code',
        'place_of_service_std_id',
        'inst_type_of_bill_std_id',
        'age', #'patient_year_of_birth',
        'gender', #'patient_gender',
        #'patient_state',
        'prov_id'
        'prov_region',
        'prov_district',
        'pat_type',
        'i_o_ind'
        F.lit(None).alias('covid'),
    )
    
    # Get lab information and look for positive lab tests
    .unionByName(
        spark
        .table(f"{DB_NAME}.{LAB_TABLE_NAME}")
        .filter(DATE_RANGE_COND)
        .select(
            'medrec_key',
            'adm_mon',
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
        'medrec_key',
        'adm_mon'
    )
    
    .agg(
        *[
            F.max(c).alias(c) for c in [
                'inpatient',
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
        F.row_number().over(W.Window.partitionBy('hvid').orderBy(F.rand(42))).alias('rand_rn'),
        
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
        'inpatient',
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
            F.min(c).alias(c) for c in [
                'index_date',
                'covid',
                'inpatient',
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
        'inpatient',
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

# %sql clear cache

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Conditions & Claims

# COMMAND ----------

ccsr_med_claims = (
    # Use all medical claims
    all_med_record

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
            F.datediff(F.col('adm_date'), F.col('index_date')).between(-365, 0)
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

# MAGIC %md 
# MAGIC
# MAGIC ## Model
# MAGIC *What analyses could solve my problem?*

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Incidence Rates

# COMMAND ----------

condition_claims_df = (
    spark.table(f'{DB_NAME}_exploratory.{WIDE_OUTPUT_NAME}')
)

# COMMAND ----------

(
    condition_claims_df
    .filter(
        ~((F.col('covid_indicator') == 1) & (F.col('covid') == 0))
        & F.col('age').between(0,64)
        & F.col('gender').isin('M','F')
        & F.col('prov_division').isNotNull()
        & (F.col('days_enrolled') >= 500)
    )
    .groupBy(
        'covid',
    )
    .agg(
        *[
            F.sum(
                F.when(
                    F.col(c).between(31,180),
                    1
                )
                .otherwise(0)
            ).alias(f"n_{c}") for c in conditions_list
        ],
        *[
            F.sum(
                F.when(
                    F.col(c).between(31,180),
                    F.col(c)
                )
                .when(
                    F.col(c).between(-365,0),
                    0
                )
                .otherwise(180)
            ).alias(f"pd_{c}") for c in conditions_list
        ]
    )
    .select(
        'covid',
        *[
          (
              F.col(f'n_{c}')/(F.col(f'pd_{c}'))
          ).alias(c) for c in conditions_list
        ]
        
    )
    .display()
)

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
# MAGIC ### Wide Table Preview

# COMMAND ----------

display(
    condition_claims_df
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Inclusion and Exclusion Counts

# COMMAND ----------

display(
    condition_claims_df
    .groupBy(
        'covid'
    )
    .agg(
        F.count('medrec_key'),
        F.sum(
            F.when(
                (
                    ~((F.col('covid_indicator') == 1) & (F.col('covid') == 0))
                    & F.col('age').between(0,64)
                    & F.col('gender').isin('M','F')
                    & F.col('prov_division').isNotNull() #zip3
                    & (F.col('days_enrolled') >= 1)
                ),
                1
            ).otherwise(0)
        ).alias('cohort_count'),
    )
)

display(
    condition_claims_df
    .groupBy(
        'covid'
    )
    .agg(
        F.sum(
            F.when(
                ~((F.col('covid_indicator') == 1) & (F.col('covid') == 0)),
                0
            ).otherwise(1)
        ).alias('exclude_covid'),
        F.sum(
            F.when(
                F.col('age').between(0,64),
                0
            ).otherwise(1)
        ).alias('exclude_age'),
        F.sum(
            F.when(
                F.col('patient_gender').isin('M','F'),
                0
            ).otherwise(1)
        ).alias('exclude_gender'),
        F.sum(
            F.when(
                F.col('prov_division').isNotNull(), #zip
                0
            ).otherwise(1)
        ).alias('exclude_state'),
        F.sum(
            F.when(
                F.col('days_enrolled') >= 1,
                0
            ).otherwise(1)
        ).alias('exclude_enroll'),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Covariate Counts

# COMMAND ----------

display(
    condition_claims_df
    .filter(
        ~((F.col('covid_indicator') == 1) & (F.col('covid') == 0))
        & F.col('age').between(0,64)
        & F.col('gender').isin('M','F')
        & F.col('prov_division').isNotNull()
        & (F.col('days_enrolled') >= 1)
    )
    .groupBy(
        'covid'
    )
    .agg(
        F.count('medrec_key'),
        *[F.sum(c).alias(c) for c in ['covid_indicator', 'inpatient']],        
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Enrollment

# COMMAND ----------

display(
    condition_claims_df
    .filter(
        ~((F.col('covid_indicator') == 1) & (F.col('covid') == 0))
        & F.col('age').between(0,64)
        & F.col('gender').isin('M','F')
        & F.col('prov_division').isNotNull()
    )
    .groupBy('days_enrolled')
    .count()
    .orderBy('days_enrolled')
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Demographics

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

condition_claims_df = (
    spark.table(f'{DB_NAME}_exploratory.{WIDE_OUTPUT_NAME}')
)

# COMMAND ----------

(
    condition_claims_df
    .filter(
        ~((F.col('covid_indicator') == 1) & (F.col('covid') == 0))
        & F.col('age').between(0,64)
        & F.col('gender').isin('M','F')
        & F.col('prov_division').isNotNull()
        & (F.col('days_enrolled') >= 1)
    )
    .groupBy(
        'covid',
        'age'
    )
    .count()
    .orderBy('age')
    .display()
)

# COMMAND ----------

(
    condition_claims_df
    .filter(
        ~((F.col('covid_indicator') == 1) & (F.col('covid') == 0))
        & F.col('age').between(0,64)
        & F.col('gender').isin('M','F')
        & F.col('prov_division').isNotNull()
        & (F.col('days_enrolled') >= 1)
    )
    .groupBy(
        'covid',
        'gender'
    )
    .count()
    .orderBy('gender')
    .display()
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC #### Potential Matching

# COMMAND ----------

(
    condition_claims_df
    .filter(
        ~((F.col('covid_indicator') == 1) & (F.col('covid') == 0))
        & F.col('age').between(0,64)
        & F.col('gender').isin('M','F')
        & F.col('prov_division').isNotNull()
        & (F.col('days_enrolled') >= 500)
    )
    .groupBy(
        F.year('index_date').alias('year'),
        F.month('index_date').alias('month'),
        'gender',
        'io_flag',
        'age'
    )
    .pivot(
        'covid',
        [0,1]
    )
    .agg(
        F.count('medrec_key')
    )
    .orderBy('year','month','io_flag','gender', 'age')
    .display()
)

# COMMAND ----------

(
    condition_claims_df
    .filter(
        ~((F.col('covid_indicator') == 1) & (F.col('covid') == 0))
        & F.col('age').between(0,64)
        & F.col('gender').isin('M','F')
        & F.col('prov_division').isNotNull()
        & (F.col('days_enrolled') >= 500)
    )
    .filter(
        'covid = 1'
    )
    .select('medrec_key')
    .count()
)

# COMMAND ----------

(
    condition_claims_df
    .filter(
        ~((F.col('covid_indicator') == 1) & (F.col('covid') == 0))
        & F.col('age').between(0,64)
        & F.col('gender').isin('M','F')
        & F.col('prov_division').isNotNull()
        & (F.col('days_enrolled') >= 500)
    )
    .groupBy(
        'gender',
        'io_flag',
        'age'
    )
    .pivot(
        'covid',
        [0,1]
    )
    .agg(
        F.count('medrec_key')
    )
    .orderBy('io_flag','gender', 'age')
    .display()
)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Footer

# COMMAND ----------


