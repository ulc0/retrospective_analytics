# Databricks notebook source
# MAGIC %md
# MAGIC # COVID-19 Mortality

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date
# MAGIC 04-28-2022

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Contact Information
# MAGIC *Who to contact for questions about this notebook?*
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC - Author: Heartley Egwuogu, Kira Gurganus, Jon Starnes
# MAGIC - Email:  [tog0@cdc.gov](), [soj3@cdc.gov](), [tmk0@cdc.gov]() <!--- enter your email address in both the square brackets and the parens -->

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Configuration

# COMMAND ----------

#%run /CDH/Analytics/Premier/Projects/cdh-premier-core/tmk0_2022-04-28_premier_COVID-19_Mortality/includes/Create_Covid_Cohort_mort
%run ./Create_Covid_Cohort_mort

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Purpose
# MAGIC * Gap: Need for improved visibility on mortality
# MAGIC * Request: Evaluate newer healthcare datasets to determine ability to describe characteristics of persons dying of COVID-19 
# MAGIC <br>
# MAGIC Inclusion criteria:
# MAGIC <br>
# MAGIC * All persons who died who have COVID-19 diagnosis at the encounter of death 
# MAGIC
# MAGIC Time frame? 
# MAGIC <br>
# MAGIC * Consider variant waves:  
# MAGIC   - 
# MAGIC   - Delta: July 2021 – November 2021 
# MAGIC   - Omicron: January 2022 – February 2022 

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Collect Data
# MAGIC *What data do I need? From where?*

# COMMAND ----------

spark.conf.set('spark.databricks.adaptive.autoOptimizeShuffle.enabled', True)

# COMMAND ----------

from pyspark.sql import SQLContext 
sqlContext = SQLContext(sc)

# Maximize Pandas output text width.
import pandas as pd
pd.set_option('display.max_colwidth', 200)

#ready for advanced plotting
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np

#import pyspark sql functions with alias
import pyspark.sql.functions as F
import pyspark.sql.window as W

#import specific functions from pyspark sql types
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType
from functools import reduce
from operator import add # added by JS

#import specific functions from pyspark sql types
from pyspark.sql.functions import col, concat, lit, to_date, date_format 
import pyspark.sql.functions as F
from pyspark.sql.functions import when, lag, concat, lit, date_add, date_sub, col
from pyspark.sql.window import Window
import pandas as pd
import numpy as np

# COMMAND ----------

icd_desc=spark.read.table("edav_prd_cdh.cdh_premier_v2.icdcode")
#icd_code=spark.read.table("edav_prd_cdh.cdh_premier_v2.paticd_diag") -- imported above with includes
# Main tables 
patdemo=spark.read.table("edav_prd_cdh.cdh_premier_v2.patdemo")
patlabres=spark.read.table("edav_prd_cdh.cdh_premier_v2.lab_res") # used for create covid cohort - lab results (covid-19)
genlab=spark.read.table("edav_prd_cdh.cdh_premier_v2.genlab") # used for create covid cohort
paticd_proc=spark.read.table("edav_prd_cdh.cdh_premier_v2.paticd_proc") # procedure codes
censusdiv=spark.read.table("edav_prd_cdh.cdh_premier_v2.pattype")
provider_df=spark.read.table("edav_prd_cdh.cdh_premier_v2.providers")
# Secondary tables 
disstat=spark.read.table("edav_prd_cdh.cdh_premier_v2.disstat") # lookup table for discharge status 
pattype=spark.read.table("edav_prd_cdh.cdh_premier_v2.pattype") # patient type settings 
patcpt=spark.read.table("edav_prd_cdh.cdh_premier_v2.patcpt") # HCPCs codes

chargemaster = spark.read.table("edav_prd_cdh.cdh_premier_v2.chgmstr")
hospcharge = spark.read.table("edav_prd_cdh.cdh_premier_v2.hospchg")
patcpt = spark.read.table("edav_prd_cdh.cdh_premier_v2.patcpt")
cpt_lookup = spark.read.table("edav_prd_cdh.cdh_premier_v2.cptcode")
patbill = spark.read.table("edav_prd_cdh.cdh_premier_v2.patbill")

patcpt.createOrReplaceTempView("patcpt")
patbill.createOrReplaceTempView("patbill")
chargemaster.createOrReplaceTempView("chargemaster")
hospcharge.createOrReplaceTempView("hospcharge")
cpt_lookup.createOrReplaceTempView("cpt_lookup")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load COVID-19 cohort 

# COMMAND ----------

# Use the uncommented options for the mortality analysis - the first cohort is for single person centered counts  
covid_pop=covid_cohort(diagnosis=True, labs=True, loinc=False) # Diagnosis and labs 
#covid_pop_all = covid_cohort(diagnosis=True, labs=True, loinc=True) # all 


# COMMAND ----------

# Add columns for string dates (for LOS later), agecat, inpatient, covid and deaths sums 
windowDept=W.Window.partitionBy(['MEDREC_KEY','PAT_KEY']).orderBy(F.col("ADM_MON"),F.col("DISC_MON"),F.col("DISC_MON_SEQ"))
 
covid_pop = (covid_pop.filter( (col('DISC_MON') > 2020204) )  # & (col('DISC_MON') < 2022204 )
                      .withColumn('Adm_month', 
                                     to_date(
                                         concat(F.substring(col('ADM_MON'),1,4),
                                                lit("-"), F.substring(col('ADM_MON'),6,7)
                                               ), 'yyyy-MM')
                                     )
                      .withColumn('Adm_month', date_format('Adm_month', 'yyyy-MM'))
                      .withColumn('Disc_month', 
                                     to_date(
                                         concat(F.substring(col('DISC_MON'),1,4),
                                                lit("-"), F.substring(col('DISC_MON'),6,7)
                                               ), 'yyyy-MM')
                                     )
                      .withColumn('Disc_month', date_format('Disc_month', 'yyyy-MM'))
                      .withColumn("agecat", 
                                F.when(col('age') < 0, "unknown")
                                    .when(col('age') < 5, "0-04")
                                    .when(col('age') < 12, "05-11")
                                    .when(col('age') < 18, "12-17")
                                    .when(col('age') < 30, "18-29")
                                    .when(col('age') < 40, "30-39")
                                    .when(col('age') < 50, "40-49")
                                    .when(col('age') < 65, "50-64")
                                    .when(col('age') < 75, "65-74")
                                    .when(col('age') >= 75, "75+")
                                .otherwise("unknown"))
                      .withColumn("COV_Wave", 
                            F.when( ( (F.col("ADM_MON") > 2021206) & (F.col("ADM_MON") < 2021412) ) # Delta July 2021 through Nov 2021  
                                      , "Delta") # add dates in legends or footnotes?
                                .when( ( (F.col("ADM_MON") > 2021412) & (F.col("ADM_MON") < 2022103) ) # Omicron > Jan 2022 < March 2022 or less than 4/2022?  
                                     , "Omicron")
                                .otherwise("Period Other"))
                      .withColumn(
                            "COVID_FLAG",
                                F.when( col('Type').isin(['DX', 'LAB'])
                                       , 1).otherwise(0))
                      .withColumn('Deaths_Total', 
                                 F.when( (col('disc_status').isin(20,40,41,42)) 
                                        , 1).otherwise(0))
                      .sort(['MEDREC_KEY', 'Deaths_Total'], ascending=[True, False])
                      .withColumn("RN",F.row_number().over(windowDept))
                                   .filter((col("RN")==1)) # & (col('DISC_MON') > 2020204) & (col('DISC_MON') < 2022204 )
            )
 
# replace with covid_pop.columns later
pop_cols=['PAT_KEY', 'ICD_PRI_SEC', 'ICD_CODE', 'MEDREC_KEY', 'DISC_MON', 'DISC_MON_SEQ', 'ADM_MON', 'PROV_ID', 
          'I_O_IND', 'PAT_TYPE', 'DISC_STATUS', 'MART_STATUS', 'AGE', 'GENDER', 'RACE', 'HISPANIC_IND', 'LOS', 
          'year', 'quarter', 'Type', 'Adm_month', 'Disc_month', 'agecat', 'COV_Wave', 'COVID_FLAG', 'Deaths_Total', 'Type',
          'MS_DRG', 'MS_DRG_MDC', 'ADM_SOURCE', 'POINT_OF_ORIGIN', 'ADM_TYPE',  
          'ADMPHY_SPEC', 'ADM_PHY', 'ATTPHY_SPEC', 'ATT_PHY', 'STD_PAYOR', 'PROJ_WGT', 'PAT_CHARGES', 
          'PAT_COST', 'PAT_FIX_COST', 'PAT_VAR_COST', 'PUBLISH_TYPE', 'Cancer1', 'Cancer2', 'Down_Syndrome', 
          'Heart_condition', 'Immun_Comp_Solid_Organ', 'Overweight', 'Obesity', 'Pregnancy', 
          'Cerebrovascular_Disease', 'Immun_Comp_Other', 'Neuro_Cond', 'C_Kidney_Dis', 'COPD', 'Asthma', 
          'Diabetes', 'Hypertension', 'Cystic_Fibrosis_ODD', 'Drug_Use', 'HIV_AIDS', 'Liver_Dis_Hep', 'Sickle_cell', 'Tobacco_use']
 
underCond_cols = ['PAT_KEY', 'Cancer1', 'Cancer2', 'Down_Syndrome', 'Heart_condition', 'Immun_Comp_Solid_Organ', 'Overweight', 'Obesity', 'Pregnancy', 
                   'Cerebrovascular_Disease', 'Immun_Comp_Other', 'Neuro_Cond', 'C_Kidney_Dis', 'COPD', 'Asthma', 'Diabetes', 'Hypertension', 
                   'Cystic_Fibrosis_ODD', 'Drug_Use', 'HIV_AIDS', 'Liver_Dis_Hep', 'Sickle_cell', 'Tobacco_use']
 
 
covid_pop = (covid_pop.filter(col('disc_status').isin(20,40,41,42) )
                      .join(icd_code.select(underCond_cols), "PAT_KEY", "INNER")
                      .drop_duplicates(['MEDREC_KEY', 'PAT_KEY'])
            )
 
print("Total Population rows after filtering: " + str(covid_pop.count())) # 147397
 
covid_pop.createOrReplaceTempView("covid_pop_tab")
 
# Duplicates df 
covid_pop_dupes = spark.sql("""select distinct MEDREC_KEY, count(MEDREC_KEY) FROM covid_pop_tab GROUP BY MEDREC_KEY HAVING count(MEDREC_KEY) >1 """)
 
covid_pop = covid_pop.join(covid_pop_dupes, "MEDREC_KEY", how='leftanti')
 

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Test cohort counts
# MAGIC Were duplicates removed? 
# MAGIC Did we miss any of the cohort ?

# COMMAND ----------

print("Total Population rows after filtering by death?: " + str(covid_pop.count()))

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Methods 
# MAGIC
# MAGIC *Add methods steps as they are developed*
# MAGIC
# MAGIC ### Overall Ns
# MAGIC * N of Facilities (PROVIDERS or patdemo)
# MAGIC * N of deaths   (patdemo)
# MAGIC
# MAGIC Match the patient type codes in the patdemo table on:
# MAGIC
# MAGIC
# MAGIC #### Subgroup Tables: 
# MAGIC * patdemo 
# MAGIC   - Inpatient (pat_type)
# MAGIC   - ED (pat_type, label:'Emergency')
# MAGIC   - Hospice (pat_type)
# MAGIC   - Gender 
# MAGIC   - Age 
# MAGIC   - Marital Staus 
# MAGIC   - Race 
# MAGIC * Providers
# MAGIC   - Census Division  
# MAGIC * *Vax Status - Where to find?
# MAGIC * *Prior Infection - Where to find? diagnosis paticd_diag (medication) 
# MAGIC * icd_code
# MAGIC   - Underlying Conditions 
# MAGIC * Do not resuscitate (icd_code == Z66)
# MAGIC * Severity at presentation (code to work on)
# MAGIC * Severity scale 
# MAGIC * ICU Admissions (admit_type)
# MAGIC * Treatment Received (procedure code) 
# MAGIC     -- Dexamethasone
# MAGIC     --  Remdesivir
# MAGIC     --  Baricitinib
# MAGIC     --  Tofacitinib
# MAGIC     --  Tocilizumab
# MAGIC     --  Sarilumab
# MAGIC     --  Supplemental oxygen
# MAGIC     --  Non-invasive ventilation (CPAP / BIPAP)
# MAGIC     --  IMV
# MAGIC
# MAGIC * Length of Stay mean/median (DISC_MON - ADM_MON?) 

# COMMAND ----------

## Set table data for Overall Facilities and Deaths
covid_pop.createOrReplaceTempView("covid_pop_tab")
## Set table data for icd_codes 
icd_code.createOrReplaceTempView("icd_code_tab")
# Lookup table
icd_desc.createOrReplaceTempView("icd_lookup_tab")
# Labs
patlabres.createOrReplaceTempView("labresults_tab") # used for create covid cohort - lab results (covid-19)
genlab.createOrReplaceTempView("genlab_tab") # used for create covid cohort
paticd_proc.createOrReplaceTempView("procedure_tab") # procedure codes
# Provider info
censusdiv.createOrReplaceTempView("censusdiv_tab")
provider_df.createOrReplaceTempView("provider_tab")
# Secondary tables 
disstat.createOrReplaceTempView("disstat_tab") # lookup table for discharge status 
pattype.createOrReplaceTempView("pattype_tab") # patient type settings 
patcpt.createOrReplaceTempView("patcodes_tab") # HCPCs codes

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Lookup Columns for covid_pop and icd_table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- # Visually inspect that the data loaded correctly
# MAGIC --print('covid population')
# MAGIC --display(covid_popD.limit(10))
# MAGIC
# MAGIC select * 
# MAGIC FROM covid_pop_tab
# MAGIC LIMIT 10; 

# COMMAND ----------

# MAGIC %sql
# MAGIC --print('ICD_CODES')
# MAGIC --display(icd_code.limit(5))
# MAGIC
# MAGIC select * 
# MAGIC FROM icd_code_tab
# MAGIC LIMIT 5; 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### prior infection

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### vax status

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### len of icu adm (imv)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### length IMV

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### other diagnoses at time of death

# COMMAND ----------

Sepsis/shock
Pneumonia
Acute Respiratory Failure
Heart Failure
Acute kidney injury/failure
Arrhythmia
Myocardial infarction/acute coronary syndrome
Pulmonary embolism
Ischemic stroke

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Overall Counts

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Facilities N 

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT 
# MAGIC   (
# MAGIC     case
# MAGIC       when PAT_TYPE = 8 then "Inpatient" 
# MAGIC       when PAT_TYPE = 28 then "Emergency"
# MAGIC       when PAT_TYPE = 25 then "Hospice"
# MAGIC       when PAT_TYPE = 10 then  "SNF"
# MAGIC       when PAT_TYPE = 22 then  "Long Term Care"
# MAGIC       when PAT_TYPE = 33 then "Home Health"
# MAGIC       else "Other"
# MAGIC     end
# MAGIC   ) as Patient_Setting,
# MAGIC   count(distinct PROV_ID)
# MAGIC FROM covid_pop_tab
# MAGIC WHERE (PAT_TYPE in (8,28,25, 10, 22, 33))
# MAGIC       and (DISC_MON > 2020204)
# MAGIC GROUP BY
# MAGIC     (
# MAGIC     case
# MAGIC       when PAT_TYPE = 8 then "Inpatient" 
# MAGIC       when PAT_TYPE = 28 then "Emergency"
# MAGIC       when PAT_TYPE = 25 then "Hospice"
# MAGIC       when PAT_TYPE = 10 then  "SNF"
# MAGIC       when PAT_TYPE = 22 then  "Long Term Care"
# MAGIC       when PAT_TYPE = 33 then "Home Health"
# MAGIC       else "Other" end)
# MAGIC ORDER BY
# MAGIC   Patient_Setting;

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Death N 

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT 
# MAGIC   (
# MAGIC     case
# MAGIC       when PAT_TYPE = 8 then "Inpatient" 
# MAGIC       when PAT_TYPE = 28 then "Emergency"
# MAGIC       when PAT_TYPE = 25 then "Hospice"
# MAGIC       when PAT_TYPE = 10 then  "SNF"
# MAGIC       when PAT_TYPE = 22 then  "Long Term Care"
# MAGIC       when PAT_TYPE = 33 then "Home Health"
# MAGIC       else "Other"
# MAGIC     end
# MAGIC   ) as Patient_Setting,
# MAGIC   sum(Deaths_Total) as Death_flags, 
# MAGIC   count(distinct MEDREC_KEY) as Pat_Count
# MAGIC FROM covid_pop_tab
# MAGIC WHERE (PAT_TYPE in (8,28,25, 10, 22, 33))
# MAGIC GROUP BY
# MAGIC     (
# MAGIC     case
# MAGIC       when PAT_TYPE = 8 then "Inpatient" 
# MAGIC       when PAT_TYPE = 28 then "Emergency"
# MAGIC       when PAT_TYPE = 25 then "Hospice"
# MAGIC       when PAT_TYPE = 10 then  "SNF"
# MAGIC       when PAT_TYPE = 22 then  "Long Term Care"
# MAGIC       when PAT_TYPE = 33 then "Home Health"
# MAGIC       else "Other" end);

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Gender

# COMMAND ----------

# Gender 

# Total Inpatient cohort count by Gender 
patientSetting_N_Gen=(covid_pop.filter( #(F.col('DISC_MON') < 2022204 ) & 
                              (F.col('PAT_TYPE').isin([8,28,25,10, 22, 33])) )
                         .withColumn('Patient_Setting', 
                                    F.when( col('PAT_TYPE')==8, "Inpatient")
                                    .when( col('PAT_TYPE')==28, "Emergency")
                                    .when( col('PAT_TYPE')==25, "Hospice")
                                    .when( F.col('PAT_TYPE') == 10, "SNF")
                                    .when( F.col('PAT_TYPE') == 22, "Long Term Care")
                                    .when( F.col('PAT_TYPE') == 33, "Home Health")
                                .otherwise("Other"))
                        .groupBy("GENDER", "Patient_Setting")
                        .agg(F.countDistinct("MEDREC_KEY").alias("unique_patients"),
                             F.sum("Deaths_Total").alias("n_DeathFlags"))
                        .orderBy(F.col("Patient_Setting")))

display(patientSetting_N_Gen)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Age Group

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT 
# MAGIC   agecat as Age_Group, 
# MAGIC   (
# MAGIC     case
# MAGIC       when PAT_TYPE = 8 then "Inpatient" 
# MAGIC       when PAT_TYPE = 28 then "Emergency"
# MAGIC       when PAT_TYPE = 25 then "Hospice"
# MAGIC       when PAT_TYPE = 10 then  "SNF"
# MAGIC       when PAT_TYPE = 22 then  "Long Term Care"
# MAGIC       when PAT_TYPE = 33 then "Home Health"
# MAGIC       else "Other"
# MAGIC     end
# MAGIC   ) as Patient_Setting,
# MAGIC   count(distinct MEDREC_KEY)
# MAGIC FROM covid_pop_tab
# MAGIC WHERE (PAT_TYPE in (8,28,25,10,22,33)) and 
# MAGIC       (DISC_MON > 2020204)
# MAGIC GROUP BY
# MAGIC   agecat, 
# MAGIC     (
# MAGIC     case
# MAGIC       when PAT_TYPE = 8 then "Inpatient" 
# MAGIC       when PAT_TYPE = 28 then "Emergency"
# MAGIC       when PAT_TYPE = 25 then "Hospice"
# MAGIC       when PAT_TYPE = 10 then  "SNF"
# MAGIC       when PAT_TYPE = 22 then  "Long Term Care"
# MAGIC       when PAT_TYPE = 33 then "Home Health"
# MAGIC       else "Other" end)
# MAGIC ORDER BY
# MAGIC   Patient_Setting, Age_Group; 

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Marital Status

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT 
# MAGIC   MART_STATUS as Marital_Status, 
# MAGIC   (
# MAGIC     case
# MAGIC       when PAT_TYPE = 8 then "Inpatient" 
# MAGIC       when PAT_TYPE = 28 then "Emergency"
# MAGIC       when PAT_TYPE = 25 then "Hospice"
# MAGIC       when PAT_TYPE = 10 then  "SNF"
# MAGIC       when PAT_TYPE = 22 then  "Long Term Care"
# MAGIC       when PAT_TYPE = 33 then "Home Health"
# MAGIC       else "Other"
# MAGIC     end
# MAGIC   ) as Patient_Setting,
# MAGIC   count(distinct MEDREC_KEY)
# MAGIC FROM covid_pop_tab
# MAGIC WHERE (PAT_TYPE in (8,28,25,10,22,33)) and 
# MAGIC       (DISC_MON > 2020204)
# MAGIC GROUP BY
# MAGIC   Marital_Status, 
# MAGIC     (
# MAGIC     case
# MAGIC       when PAT_TYPE = 8 then "Inpatient" 
# MAGIC       when PAT_TYPE = 28 then "Emergency"
# MAGIC       when PAT_TYPE = 25 then "Hospice"
# MAGIC       when PAT_TYPE = 10 then  "SNF"
# MAGIC       when PAT_TYPE = 22 then  "Long Term Care"
# MAGIC       when PAT_TYPE = 33 then "Home Health"
# MAGIC       else "Other" end)
# MAGIC ORDER BY
# MAGIC   Patient_Setting;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Race/Ethnicity

# COMMAND ----------

# Count combined Race+Ethnicity by Facility Type
covid_pop_Race = (covid_pop.filter("PAT_TYPE in (8,28,25,10,22,33)")
                              .withColumn('Race_Eth',
                                     F.when(F.col('HISPANIC_IND')=="Y", "Hispanic")
                                      .when((F.col('HISPANIC_IND')!="Y") & (F.col('RACE')=="W"), "NH_White")
                                      .when((F.col('HISPANIC_IND')!="Y") & (F.col('RACE')=="B"), "NH_Black")
                                      .when((F.col('HISPANIC_IND')!="Y") & (F.col('RACE')=="A"), "NH_Asian")
                                      .when((F.col('HISPANIC_IND')!="Y") & (F.col('RACE')=="O"), "Other")
                                      .when((F.col('HISPANIC_IND')!="Y") & (F.col('RACE')=="U"), "Unknown")
                                      .otherwise("NA"))
                              .withColumn('Facility',
                                     F.when(F.col('PAT_TYPE')==8, "Inpatient")
                                      .when(F.col('PAT_TYPE')==28, "Emergency")
                                      .when(F.col('PAT_TYPE')==25, "Hospice")
                                      .when(F.col('PAT_TYPE') == 10, "SNF")
                                      .when(F.col('PAT_TYPE') == 22, "Long Term Care")
                                      .when(F.col('PAT_TYPE') == 33, "Home Health")
                                      .otherwise("NA"))
                                    .groupBy("Facility","Race_Eth")
                                      .agg(F.countDistinct("MEDREC_KEY").alias("n_patients"))
                                    .orderBy(F.col("Facility"))
                 )

display(covid_pop_Race)


# COMMAND ----------

# MAGIC %sql -- Change below to NOT 'Y' for Non-hispanic  
# MAGIC SELECT 
# MAGIC   (
# MAGIC     case
# MAGIC       when HISPANIC_IND = 'Y' THEN "Hispanic" 
# MAGIC       when RACE = 'W' and HISPANIC_IND != 'Y' THEN "NH_White"
# MAGIC       when RACE = 'B' and HISPANIC_IND != 'Y' THEN "NH_Black"
# MAGIC       when RACE = 'A' and HISPANIC_IND != 'Y' THEN "NH_Asian"
# MAGIC       when RACE = 'O' and HISPANIC_IND != 'Y' then "Other"
# MAGIC       when RACE = 'U' and HISPANIC_IND != 'Y' then "Unknown"
# MAGIC       else "NA" 
# MAGIC       end
# MAGIC     ) as Race_Eth,
# MAGIC   (
# MAGIC     case
# MAGIC       when PAT_TYPE = 8 then "Inpatient" 
# MAGIC       when PAT_TYPE = 28 then "Emergency"
# MAGIC       when PAT_TYPE = 25 then "Hospice"
# MAGIC       when PAT_TYPE = 10 then  "SNF"
# MAGIC       when PAT_TYPE = 22 then  "Long Term Care"
# MAGIC       when PAT_TYPE = 33 then "Home Health"
# MAGIC       else "Other"
# MAGIC     end
# MAGIC   ) as Patient_Setting, 
# MAGIC   count(distinct MEDREC_KEY)
# MAGIC FROM covid_pop_tab
# MAGIC WHERE (
# MAGIC         (PAT_TYPE in (8,28,25,10,22,33)) and 
# MAGIC         (RACE in ('W', 'B', 'A', 'O', 'U'))
# MAGIC       )
# MAGIC GROUP BY
# MAGIC   (
# MAGIC     case
# MAGIC       when HISPANIC_IND = 'Y' THEN "Hispanic" 
# MAGIC       when RACE = 'W' and HISPANIC_IND != 'Y' THEN "NH_White"
# MAGIC       when RACE = 'B' and HISPANIC_IND != 'Y' THEN "NH_Black"
# MAGIC       when RACE = 'A' and HISPANIC_IND != 'Y' THEN "NH_Asian"
# MAGIC       when RACE = 'O' and HISPANIC_IND != 'Y' then "Other"
# MAGIC       when RACE = 'U' and HISPANIC_IND != 'Y' then "Unknown"
# MAGIC       else "NA" 
# MAGIC       end
# MAGIC     ),
# MAGIC     (
# MAGIC     case
# MAGIC       when PAT_TYPE = 8 then "Inpatient" 
# MAGIC       when PAT_TYPE = 28 then "Emergency"
# MAGIC       when PAT_TYPE = 25 then "Hospice"
# MAGIC       when PAT_TYPE = 10 then  "SNF"
# MAGIC       when PAT_TYPE = 22 then  "Long Term Care"
# MAGIC       when PAT_TYPE = 33 then "Home Health"
# MAGIC       else "Other" end)
# MAGIC ORDER BY
# MAGIC   Patient_Setting, Race_Eth;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Census Division

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT 
# MAGIC   prov.PROV_DIVISION as Census_Division, 
# MAGIC   (
# MAGIC     case
# MAGIC       when cov.PAT_TYPE = 8 then "Inpatient" 
# MAGIC       when cov.PAT_TYPE = 28 then "Emergency"
# MAGIC       when cov.PAT_TYPE = 25 then "Hospice"
# MAGIC       when cov.PAT_TYPE = 10 then  "SNF"
# MAGIC       when cov.PAT_TYPE = 22 then  "Long Term Care"
# MAGIC       when cov.PAT_TYPE = 33 then "Home Health"
# MAGIC       else "Other"
# MAGIC     end
# MAGIC   ) as Patient_Setting,
# MAGIC   count(distinct MEDREC_KEY)
# MAGIC FROM covid_pop_tab cov 
# MAGIC     left join provider_tab prov 
# MAGIC     on cov.PROV_ID=prov.PROV_ID
# MAGIC WHERE (cov.PAT_TYPE in (8,25,28,10,22,33) and Deaths_Total=1) 
# MAGIC GROUP BY
# MAGIC   Census_Division, 
# MAGIC     (
# MAGIC     case
# MAGIC       when cov.PAT_TYPE = 8 then "Inpatient" 
# MAGIC       when cov.PAT_TYPE = 28 then "Emergency"
# MAGIC       when cov.PAT_TYPE = 25 then "Hospice"
# MAGIC       when cov.PAT_TYPE = 10 then  "SNF"
# MAGIC       when cov.PAT_TYPE = 22 then  "Long Term Care"
# MAGIC       when cov.PAT_TYPE = 33 then "Home Health"
# MAGIC       else "Other" end)
# MAGIC ORDER BY
# MAGIC   Patient_Setting, Census_Division;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Medications

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct
# MAGIC   (
# MAGIC     case
# MAGIC       when PAT_TYPE = 8 then "Inpatient" 
# MAGIC       when PAT_TYPE = 28 then "Emergency"
# MAGIC       when PAT_TYPE = 25 then "Hospice"
# MAGIC       when PAT_TYPE = 10 then  "SNF"
# MAGIC       when PAT_TYPE = 22 then  "Long Term Care"
# MAGIC       when PAT_TYPE = 33 then "Home Health"
# MAGIC       else "Other"
# MAGIC     end
# MAGIC   ) as Patient_Setting,
# MAGIC   
# MAGIC   treatment,
# MAGIC   
# MAGIC   count(distinct medrec_key)
# MAGIC
# MAGIC from
# MAGIC   (
# MAGIC     select
# MAGIC       distinct pat_type, medrec_key,
# MAGIC       (
# MAGIC         case
# MAGIC           when upper(std_chg_desc) like '%DEXAMETHASONE%' then 'DEXAMETHASONE'
# MAGIC           when upper(std_chg_desc) like '%TOFACITINIB%' then 'TOFACITINIB'
# MAGIC           when upper(std_chg_desc) like '%TOCILIZUMAB%' then 'TOCILIZUMAB'
# MAGIC           when upper(std_chg_desc) like '%SARILUMAB%' then 'SARILUMAB'
# MAGIC           when upper(std_chg_desc) like '%BARICITINIB%' then 'BARICITINIB'
# MAGIC           when (
# MAGIC             upper(std_chg_desc) like '%CPAP%'
# MAGIC             or upper(std_chg_desc) like '%BIPAP%'
# MAGIC             or upper(std_chg_desc) like '%NON-INVASIVE VENT%'
# MAGIC           ) then 'CPAP'
# MAGIC           when upper(std_chg_desc) like '%REMDESIVIR%' then 'REMDESIVIR'
# MAGIC           when clin_sum_code = 1101 then 'ICU'
# MAGIC           when clin_dtl_code = 410412946570007 then 'IMV'
# MAGIC         end
# MAGIC       ) as treatment
# MAGIC     from
# MAGIC       covid_pop_tab
# MAGIC       left join patbill on patbill.pat_key = covid_pop_tab.pat_key
# MAGIC       left join chargemaster on chargemaster.std_chg_code = patbill.std_chg_code
# MAGIC       
# MAGIC     union
# MAGIC     
# MAGIC     select
# MAGIC       distinct pat_type, medrec_key,
# MAGIC       (
# MAGIC         case
# MAGIC           when upper(cpt_desc) like '%DEXAMETHASONE%' then 'DEXAMETHASONE'
# MAGIC           when upper(cpt_desc) like '%TOFACITINIB%' then 'TOFACITINIB'
# MAGIC           when upper(cpt_desc) like '%TOCILIZUMAB%' then 'TOCILIZUMAB'
# MAGIC           when upper(cpt_desc) like '%SARILUMAB%' then 'SARILUMAB'
# MAGIC           when upper(cpt_desc) like '%BARICITINIB%' then 'BARICITINIB'
# MAGIC           when (
# MAGIC             upper(cpt_desc) like '%CPAP%'
# MAGIC             or upper(cpt_desc) like '%BIPAP%'
# MAGIC             or upper(cpt_desc) like '%NON-INVASIVE VENT%'
# MAGIC           ) then 'CPAP'
# MAGIC           when upper(cpt_desc) like '%REMDESIVIR%' then 'REMDESIVIR'
# MAGIC           when clin_sum_code = 1101 then 'ICU'
# MAGIC           when clin_dtl_code = 410412946570007 then 'IMV'
# MAGIC         end
# MAGIC       ) as treatment
# MAGIC     from
# MAGIC       covid_pop_tab
# MAGIC       left join patcpt on patcpt.pat_key = covid_pop_tab.pat_key
# MAGIC       left join cpt_lookup on patcpt.cpt_code = cpt_lookup.cpt_code
# MAGIC       left join patbill on patbill.pat_key = covid_pop_tab.pat_key
# MAGIC       left join chargemaster on chargemaster.std_chg_code = patbill.std_chg_code
# MAGIC   
# MAGIC     union
# MAGIC     
# MAGIC     select
# MAGIC       distinct pat_type, medrec_key,
# MAGIC       (
# MAGIC         case
# MAGIC           when upper(hosp_chg_desc) like '%DEXAMETHASONE%' then 'DEXAMETHASONE'
# MAGIC           when upper(hosp_chg_desc) like '%TOFACITINIB%' then 'TOFACITINIB'
# MAGIC           when upper(hosp_chg_desc) like '%TOCILIZUMAB%' then 'TOCILIZUMAB'
# MAGIC           when upper(hosp_chg_desc) like '%SARILUMAB%' then 'SARILUMAB'
# MAGIC           when upper(hosp_chg_desc) like '%BARICITINIB%' then 'BARICITINIB'
# MAGIC           when (
# MAGIC             upper(hosp_chg_desc) like '%CPAP%'
# MAGIC             or upper(hosp_chg_desc) like '%BIPAP%'
# MAGIC             or upper(hosp_chg_desc) like '%NON-INVASIVE VENT%'
# MAGIC           ) then 'CPAP'
# MAGIC           when upper(hosp_chg_desc) like '%REMDESIVIR%' then 'REMDESIVIR'
# MAGIC           when clin_sum_code = 1101 then 'ICU'
# MAGIC           when clin_dtl_code = 410412946570007 then 'IMV'
# MAGIC         end
# MAGIC       ) as treatment
# MAGIC     from
# MAGIC       covid_pop_tab
# MAGIC       left join patbill on patbill.pat_key = covid_pop_tab.pat_key
# MAGIC       left join hospcharge on hospcharge.hosp_chg_id = patbill.hosp_chg_id
# MAGIC       left join chargemaster on chargemaster.std_chg_code = patbill.std_chg_code
# MAGIC   )
# MAGIC   
# MAGIC   group by treatment, 
# MAGIC   (
# MAGIC     case
# MAGIC       when PAT_TYPE = 8 then "Inpatient" 
# MAGIC       when PAT_TYPE = 28 then "Emergency"
# MAGIC       when PAT_TYPE = 25 then "Hospice"
# MAGIC       when PAT_TYPE = 10 then  "SNF"
# MAGIC       when PAT_TYPE = 22 then  "Long Term Care"
# MAGIC       when PAT_TYPE = 33 then "Home Health"
# MAGIC       else "Other"
# MAGIC     end
# MAGIC   ) 

# COMMAND ----------

chgmstr=spark.read.table("edav_prd_cdh.cdh_premier_v2.chgmstr")
patbill=spark.read.table("edav_prd_cdh.cdh_premier_v2.patbill")
 
#ICU=chgmstr.filter("clin_sum_code in ('110108','110102')")  # Prior definition
ICU=chgmstr.filter("clin_sum_code == 110102")
 
ICU = ICU.join(patbill.select("PAT_KEY", "STD_CHG_CODE", "year", "quarter"), "STD_CHG_CODE", "inner")
 
ICU.createOrReplaceTempView("icu_tab")
 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   --MEDREC_KEY,
# MAGIC   --PAT_TYPE,
# MAGIC   (
# MAGIC     case
# MAGIC       when PAT_TYPE = 8 then "Inpatient" 
# MAGIC       when PAT_TYPE = 28 then "Emergency"
# MAGIC       when PAT_TYPE = 25 then "Hospice"
# MAGIC       when PAT_TYPE = 10 then  "SNF"
# MAGIC       when PAT_TYPE = 22 then  "Long Term Care"
# MAGIC       when PAT_TYPE = 33 then "Home Health"
# MAGIC       else "Other"
# MAGIC     end
# MAGIC   ) as Patient_Setting,
# MAGIC   count(distinct MEDREC_KEY) as Pat_Count,
# MAGIC   sum(Deaths_Total) as Total_DFlags
# MAGIC FROM
# MAGIC   covid_pop_tab
# MAGIC WHERE
# MAGIC   (
# MAGIC     PAT_KEY in (
# MAGIC       SELECT
# MAGIC         PAT_KEY
# MAGIC       from
# MAGIC         icu_tab
# MAGIC     )
# MAGIC   and (PAT_TYPE in (8,28,25,10,22,33))
# MAGIC   and (Deaths_Total = 1)
# MAGIC   )
# MAGIC GROUP BY
# MAGIC   (
# MAGIC     case
# MAGIC       when PAT_TYPE = 8 then "Inpatient" 
# MAGIC       when PAT_TYPE = 28 then "Emergency"
# MAGIC       when PAT_TYPE = 25 then "Hospice"
# MAGIC       when PAT_TYPE = 10 then  "SNF"
# MAGIC       when PAT_TYPE = 22 then  "Long Term Care"
# MAGIC       when PAT_TYPE = 33 then "Home Health"
# MAGIC       else "Other"
# MAGIC     end
# MAGIC   )
# MAGIC ORDER BY
# MAGIC   Patient_Setting;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Length of Stay

# COMMAND ----------

# LOS df then stats

LOS_df = spark.sql("""

SELECT
   PAT_KEY, 
   MEDREC_KEY,
   PROV_ID, 
   PAT_TYPE, 
   Adm_month, 
   Disc_month,
   DISC_MON, 
   LOS, 
   DATEDIFF(cast(Disc_month as date), cast(Adm_month as date)) as LOS_nonI,
  (
    case
      when PAT_TYPE = 8 then "Inpatient" 
      when PAT_TYPE = 28 then "Emergency"
      when PAT_TYPE = 25 then "Hospice"
      when PAT_TYPE = 10 then "SNF" 
      when PAT_TYPE = 22 then "Long Term Care"
      when PAT_TYPE = 33 then "Home Health"
      else "Other"
    end
  ) as Patient_Setting
FROM
  covid_pop_tab
WHERE (
        (PAT_TYPE in (8,28,25,10,22,33)) and 
        (DISC_MON is not null)
      )
ORDER BY
  MEDREC_KEY
  
""")


display(LOS_df.limit(10))

LOS_stats1 = (LOS_df.filter((F.col('LOS') > 0) & (F.col('PAT_TYPE').isin(8,25)))
                   .sort('LOS', ascending=True)
                   .groupBy("Patient_Setting")
                   .agg(F.min("LOS").alias("Min LOS"), 
                        F.round(F.mean("LOS"), 2).alias("Mean_LOS"),
                        F.percentile_approx("LOS", 0.5, accuracy=100000).alias("Median LOS"), 
                        F.max("LOS").alias("Max LOS")
                        )
            )
LOS_stats2 = (LOS_df.filter((F.col('LOS_nonI') > 0) & (F.col('PAT_TYPE').isin(28,10,33,22)))
                   .sort('LOS_nonI', ascending=True)
                   .groupBy("Patient_Setting")
                   .agg(F.min("LOS_nonI").alias("Min LOS"), 
                        F.round(F.mean("LOS_nonI"), 2).alias("Mean_LOS"),
                        F.percentile_approx("LOS_nonI", 0.50, accuracy=100000).alias("Median LOS"), 
                        F.max("LOS_nonI").alias("Max LOS")
                        )
            )


LOS_stats = LOS_stats1.unionAll(LOS_stats2)

display(LOS_stats2)

# COMMAND ----------

LOS_stats = LOS_stats1.unionAll(LOS_stats2)

display(LOS_stats)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC #### Do Not Resusitate

# COMMAND ----------

paticd_proc.createOrReplaceTempView("proc_tab")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   (
# MAGIC     case
# MAGIC       when ICD_CODE like "%Z66%" then "DNR Yes" 
# MAGIC       when ICD_CODE not like "%Z66%" then "DNR No"
# MAGIC       else "Unkown"
# MAGIC     end
# MAGIC   ) as DNR_Order,
# MAGIC   (
# MAGIC     case
# MAGIC       when PAT_TYPE = 8 then "Inpatient" 
# MAGIC       when PAT_TYPE = 28 then "Emergency"
# MAGIC       when PAT_TYPE = 25 then "Hospice"
# MAGIC       when PAT_TYPE = 10 then  "SNF"
# MAGIC       when PAT_TYPE = 22 then  "Long Term Care"
# MAGIC       when PAT_TYPE = 33 then "Home Health"
# MAGIC       else "Other"
# MAGIC     end
# MAGIC   ) as Patient_Setting,
# MAGIC   COUNT(distinct MEDREC_KEY)
# MAGIC FROM
# MAGIC   covid_pop_tab 
# MAGIC   
# MAGIC   WHERE(
# MAGIC     PAT_KEY in (
# MAGIC       SELECT
# MAGIC         PAT_KEY
# MAGIC       from
# MAGIC         proc_tab
# MAGIC     )
# MAGIC   and (PAT_TYPE in (8,28,25,10,22,33))
# MAGIC   )
# MAGIC GROUP BY 
# MAGIC   (
# MAGIC     case
# MAGIC       when ICD_CODE like "%Z66%" then "DNR Yes" 
# MAGIC       when ICD_CODE not like "%Z66%" then "DNR No"
# MAGIC       else "Unkown"
# MAGIC     end
# MAGIC   ),
# MAGIC   (
# MAGIC     case
# MAGIC       when PAT_TYPE = 8 then "Inpatient" 
# MAGIC       when PAT_TYPE = 28 then "Emergency"
# MAGIC       when PAT_TYPE = 25 then "Hospice"
# MAGIC       when PAT_TYPE = 10 then  "SNF"
# MAGIC       when PAT_TYPE = 22 then  "Long Term Care"
# MAGIC       when PAT_TYPE = 33 then "Home Health"
# MAGIC       else "Other"
# MAGIC     end
# MAGIC   ) 
# MAGIC ORDER BY
# MAGIC   Patient_Setting
# MAGIC ;

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Underlying conditions 
# MAGIC * 0
# MAGIC * 1–3
# MAGIC * 4–6
# MAGIC * 7+
# MAGIC * Cerebrovascular disease
# MAGIC * Cancer
# MAGIC * Chronic kidney disease
# MAGIC * Chronic lung disease
# MAGIC * Chronic liver disease
# MAGIC * Cystic fibrosis
# MAGIC * Diabetes mellitus (types 1 and 2)
# MAGIC * Disabilities
# MAGIC * Heart conditions
# MAGIC * HIV
# MAGIC * Mental health disorders
# MAGIC * Dementia
# MAGIC * Obesity
# MAGIC * Primary immunodeficiences
# MAGIC * Pregnancy or recent pregnancy
# MAGIC * Transplantation
# MAGIC * Tuberculosis 

# COMMAND ----------

"""Underlying conditions count - total n """ 

underCond_cols = ['Cancer1', 'Cancer2', 'Down_Syndrome', 'Heart_condition', 'Immun_Comp_Solid_Organ', 'Overweight', 'Obesity', 'Pregnancy', 
                   'Cerebrovascular_Disease', 'Immun_Comp_Other', 'Neuro_Cond', 'C_Kidney_Dis', 'COPD', 'Asthma', 'Diabetes', 'Hypertension', 
                   'Cystic_Fibrosis_ODD', 'Drug_Use', 'HIV_AIDS', 'Liver_Dis_Hep', 'Sickle_cell', 'Tobacco_use']

covid_pop = covid_pop.withColumn("UC_count", reduce(add, [F.col(c) for c in underCond_cols ] ))

print("Patients with no underlying conditions: " +  str(covid_pop.select(['MEDREC_KEY', 'UC_count']).filter("UC_count == 0").count()) )
print("Patients with 1-3: " + str(covid_pop.select(['MEDREC_KEY', 'UC_count']).filter("UC_count < 4 and UC_count > 0").count()) )
print("Patients with 4-6: " + str(covid_pop.select(['MEDREC_KEY', 'UC_count']).filter("UC_count < 7 and UC_count > 3").count()) )
print("Patients with 1-3: " + str(covid_pop.select(['MEDREC_KEY', 'UC_count']).filter("UC_count >= 7").count()) )


# COMMAND ----------

"""Underlying condition Rankings 0-7+ counting all specific underlying conditions by setting """  

underC_N=(covid_pop.filter( col('PAT_TYPE').isin([10,22,33,8,28,25]) )
                        .withColumn('Patient_Setting',
                                    F.when( col('PAT_TYPE')==8, "Inpatient")
                                    .when( col('PAT_TYPE')==28, "Emergency")
                                    .when( col('PAT_TYPE')==25, "Hospice")
                                    .when(col('PAT_TYPE')==10, "SNF")
                                    .when(col('PAT_TYPE')==22, "Long Term Care")
                                    .when(col("PAT_TYPE")==33, "Home Health")
                                .otherwise("Other"))
                        .withColumn('Underlying_Group',
                                    F.when( col('UC_count')==0, "No UC")
                                    .when( (col('UC_count') < 4) & (col('UC_count') > 0), "UC 1-3")
                                    .when( (col('UC_count') < 7) & (col('UC_count') > 3), "UC 4-6")
                                    .when( (col('UC_count') >= 7), "UC 7+")
                                .otherwise("Other"))
                        .groupBy("Patient_Setting", "Underlying_Group")
                        .agg( 
                             F.countDistinct(col('MEDREC_KEY')).alias("n_Underlying_Conditions"), 
                            )
                        .orderBy(col("Patient_Setting")))


# COMMAND ----------

""" all underlying conditions by setting """

UC_counts = (covid_pop.filter((col('PAT_TYPE').isin([10, 22, 33,8,28,25])) )
                         .withColumn('Patient_Setting', 
                                    F.when( col('PAT_TYPE')==8, "Inpatient")
                                    .when( col('PAT_TYPE')==28, "Emergency")
                                    .when( col('PAT_TYPE')==25, "Hospice")
                                    .when(col('PAT_TYPE')==10, "SNF")
                                    .when(col('PAT_TYPE')==22, "Long Term Care")
                                    .when(col("PAT_TYPE")==33, "Home Health")
                                .otherwise("Other"))
                         .groupBy("Patient_Setting")
                         .agg(*[ F.sum(c).alias(c) for c in underCond_cols])
            )

display(UC_counts)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Other conditions

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Mean los IMV

# COMMAND ----------

LOS_stats1 = (LOS_df.filter((F.col('LOS') > 0) & (F.col('PAT_TYPE').isin(8,25)))
                   .sort('LOS', ascending=True)
                   .groupBy("Patient_Setting")
                   .agg(F.min("LOS").alias("Min LOS"), 
                        F.round(F.mean("LOS"), 2).alias("Mean_LOS"),
                        F.percentile_approx("LOS", 0.5, accuracy=100000).alias("Median LOS"), 
                        F.max("LOS").alias("Max LOS")
                        )
            )
LOS_stats2 = (LOS_df.filter((F.col('LOS_nonI') > 0) & (F.col('PAT_TYPE').isin(28,10,33,22)))
                   .sort('LOS_nonI', ascending=True)
                   .groupBy("Patient_Setting")
                   .agg(F.min("LOS_nonI").alias("Min LOS"), 
                        F.round(F.mean("LOS_nonI"), 2).alias("Mean_LOS"),
                        F.percentile_approx("LOS_nonI", 0.50, accuracy=100000).alias("Median LOS"), 
                        F.max("LOS_nonI").alias("Max LOS")
                        )
            )


LOS_stats = LOS_stats1.unionAll(LOS_stats2)

display(LOS_stats2)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Mean los ICU 

# COMMAND ----------

LOS_stats1 = (LOS_df.filter((F.col('LOS') > 0) & (F.col('PAT_TYPE').isin(8,25)))
                   .sort('LOS', ascending=True)
                   .groupBy("Patient_Setting")
                   .agg(F.min("LOS").alias("Min LOS"), 
                        F.round(F.mean("LOS"), 2).alias("Mean_LOS"),
                        F.percentile_approx("LOS", 0.5, accuracy=100000).alias("Median LOS"), 
                        F.max("LOS").alias("Max LOS")
                        )
            )
LOS_stats2 = (LOS_df.filter((F.col('LOS_nonI') > 0) & (F.col('PAT_TYPE').isin(28,10,33,22)))
                   .sort('LOS_nonI', ascending=True)
                   .groupBy("Patient_Setting")
                   .agg(F.min("LOS_nonI").alias("Min LOS"), 
                        F.round(F.mean("LOS_nonI"), 2).alias("Mean_LOS"),
                        F.percentile_approx("LOS_nonI", 0.50, accuracy=100000).alias("Median LOS"), 
                        F.max("LOS_nonI").alias("Max LOS")
                        )
            )


LOS_stats = LOS_stats1.unionAll(LOS_stats2)

display(LOS_stats2)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Prior Infection

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from covid_pop_tab

# COMMAND ----------

patlabres=spark.read.table("edav_prd_cdh.cdh_premier_v2.lab_res")
patlabres.createOrReplaceTempView("patlabres")

# COMMAND ----------

# MAGIC %sql
# MAGIC     
# MAGIC     select
# MAGIC       medrec_key,
# MAGIC       (
# MAGIC         case
# MAGIC           when PAT_TYPE = 8 then "Inpatient"
# MAGIC           when PAT_TYPE = 28 then "Emergency"
# MAGIC           when PAT_TYPE = 25 then "Hospice"
# MAGIC           when PAT_TYPE = 10 then "SNF"
# MAGIC           when PAT_TYPE = 22 then "Long Term Care"
# MAGIC           when PAT_TYPE = 33 then "Home Health"
# MAGIC           else "Other"
# MAGIC         end
# MAGIC       ) as Patient_Setting, 
# MAGIC       count(distinct a.adm_mon) adm_count
# MAGIC     from
# MAGIC       covid_pop_tab a
# MAGIC       group by medrec_key, (
# MAGIC         case
# MAGIC           when PAT_TYPE = 8 then "Inpatient"
# MAGIC           when PAT_TYPE = 28 then "Emergency"
# MAGIC           when PAT_TYPE = 25 then "Hospice"
# MAGIC           when PAT_TYPE = 10 then "SNF"
# MAGIC           when PAT_TYPE = 22 then "Long Term Care"
# MAGIC           when PAT_TYPE = 33 then "Home Health"
# MAGIC           else "Other"
# MAGIC         end
# MAGIC       )

# COMMAND ----------

case
    when
        (icd_code in ('U07.1', 'U07.2', 'J12.82')
        and (cpt_date >= icd_date or chg_date >=icd_date)) or 
        (upper(test) in (upper('SARS coronavirus 2 RNA:PrThr:Pt:Respiratory:Ord:Probe.amp.tar'),upper('SARS coronavirus 2 RNA:PrThr:Pt:XXX:Ord:Probe.amp.tar')) 
        and upper(observation) like '%POSITIVE%'
        and (cpt_date >= lab_date or chg_date >= lab_date))
     then 1
    else 0
  end as COVID

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Vaccination Status

# COMMAND ----------

case 
    when (cpt_code in ("91300", "0002A", "91301", "0012A", "91303", "0031A")) 
    or  (std_chg_code in (510771000020000, 510771000120000, 51077100031000))
    then 1 else 0 end as vac_full,
case
    when cpt_code in ("0001A", "0011A") and vac_full != 1 then 1, else 0 end as vac_part
case
    when (cpt_code in ("91300", "0001A", "0002A", "91301", "0011A", "0012A", "91303", "0031A"))
    or (std_chg_code in (510771000010000, 510771000020000, 510771000110000, 510771000120000, 510771000310000))
    then 1, else 0 end as vac_any
case 
    when (vac_any == 0 and vac_full == 0 and vac_part == 0) then 1 else 0 end as vac_unknown
