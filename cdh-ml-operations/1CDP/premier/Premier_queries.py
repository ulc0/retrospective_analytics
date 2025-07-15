# Databricks notebook source
# MAGIC %md
# MAGIC ## Contact Information
# MAGIC *Who to contact for questions about this notebook?*
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC - Author: Stacey Adjei 
# MAGIC - Email: [Ltq3@cdc.gov](Ltq3@cdc.gov) <!--- enter your email address in both the square brackets and the parens -->

# COMMAND ----------

# MAGIC %md
# MAGIC # Premier Data Extraction for Weighting (Marker)
# MAGIC The purpose of this notebook is to provide guidance on how the data was extracted from Premier in order to be applied to GTRI's weighting methodology. This notebook also provides an examples of how a cohort within the Premier data can be applied to GTRI's weighting methodology. For further review on the data itself, please visit the <em>premier_data_review</em> notebook within the Analytics workspace (Workspaces/CDH/Anlaytics/Premier/Projects/cdh-premier-core/premier_data_review). The data review below was pulled from the <em>premier_data_review</em> notebook.
# MAGIC
# MAGIC <strong>Premier Data Overview</strong>
# MAGIC
# MAGIC Description: Longitudinal inpatient and hospital-based outpatient discharge containing claims, laboratory and pharmacy line-level data
# MAGIC
# MAGIC Strengths: Contains data beginning in 2016; Includes data on comorbidities and from long-term care facilities; Uses ICD-10, CPT, LOINC codes
# MAGIC
# MAGIC The Premier Healthcare Database (PHD) is comprised of data from more than 1 billion patient encounters, which equates to approximately one in every five of all inpatient discharges in the U.S. All patient-related data in the PHD is de-identified and HIPAA-compliant from both the inpatient and hospital-based outpatient settings, including demographic and disease state, and information on billed services, including medications, laboratory tests performed, diagnostics and therapeutic services. Additionally, information on hospital characteristics, including geographic location, bed size and teaching status, is also included. (https://www.premierinc.com/newsroom/press-releases/premier-healthcare-database-being-used-by-national-institutes-of-health-to-evaluate-impact-of-covid-19-on-patients-across-the-u-s)
# MAGIC
# MAGIC <strong>Steps to obtain data for the weighting process</strong>
# MAGIC - Run <em>Required Imports</em>
# MAGIC - Run <em>1) Premier Providers Table</em>: hospital charicteristics are used in the weighting variables
# MAGIC   - save as pandas dataframe
# MAGIC - Run <em>2) Inpatient Discharge Counts by Year and Provider</em>: discharges are binned and then used in the weighting variables
# MAGIC   - save as pandas dataframe
# MAGIC - Run <em>3) Project Example: Inpatient PAT_KEY Counts by Year, Provider, demographics, and where Primary Diaganoses Corresponds with Pericarditis in the year 2020 </em>: Reshape your data into the format of the final cell from the displayed table under 3.3
# MAGIC     - save as pandas dataframe
# MAGIC     - Save all 3 tables as CSV file in your analytic folder for use in weighting process
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # Required Imports

# COMMAND ----------

import pandas as pd
import matplotlib
import seaborn as sns
import os
from pyspark.sql import SparkSession

# COMMAND ----------

# MAGIC %md
# MAGIC # 1) Premier Providers Table
# MAGIC The query below calls the Premier providers table that contains hospital characteristics. The number of hospitals within the Providers table differs from the number of hospitals that have inpatient data.
# MAGIC
# MAGIC For further details, view the Premier Data Extract Dictionary.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC         pr.prov_id, 
# MAGIC         pd.PROV_ID, URBAN_RURAL, TEACHING, BEDS_GRP, PROV_REGION, PROV_DIVISION, COST_TYPE,
# MAGIC         COUNT(CASE WHEN pd.PAT_TYPE = 8 THEN 1 END) AS c_INPATIENT,
# MAGIC         COUNT(CASE WHEN pd.ADM_TYPE = 4 THEN 1 END) AS NEWBORN,
# MAGIC         COUNT(CASE WHEN pd.ADM_TYPE = 1 THEN 1 END) AS EMERGENCY,
# MAGIC         COUNT(CASE WHEN pd.ADM_TYPE = 2 THEN 1 END) AS URGENT,
# MAGIC         COUNT(CASE WHEN pd.ADM_TYPE = 3 THEN 1 END) AS ELECTIVE,
# MAGIC         COUNT(CASE WHEN pd.ADM_TYPE = 5 THEN 1 END) AS TRAUMA_CENTER,
# MAGIC         COUNT(CASE WHEN pd.ADM_TYPE = 9 THEN 1 END) AS NA,
# MAGIC         COUNT(CASE WHEN pd.PAT_TYPE = 10 THEN 1 END) AS c_SKILLED_NURSING,
# MAGIC         COUNT(CASE WHEN pd.PAT_TYPE = 22 THEN 1 END) AS c_LONG_TERM_CARE,
# MAGIC         COUNT(CASE WHEN pd.PAT_TYPE = 23 THEN 1 END) AS c_REHABILITATION,
# MAGIC         COUNT(CASE WHEN pd.PAT_TYPE = 24 THEN 1 END) AS c_PSYCHIATRIC,
# MAGIC         COUNT(CASE WHEN pd.PAT_TYPE = 25 THEN 1 END) AS c_HOSPICE,
# MAGIC         COUNT(CASE WHEN pd.PAT_TYPE = 26 THEN 1 END) AS c_CHEMICAL_DEPENDENCY,
# MAGIC         COUNT(CASE WHEN pd.PAT_TYPE = 27 THEN 1 END) AS c_SAME_DAY_SURGERY,
# MAGIC         COUNT(CASE WHEN pd.PAT_TYPE = 28 THEN 1 END) AS c_EMERGENCY,
# MAGIC         COUNT(CASE WHEN pd.PAT_TYPE = 29 THEN 1 END) AS c_OBSERVATION,
# MAGIC         COUNT(CASE WHEN pd.PAT_TYPE = 30 THEN 1 END) AS c_DIAGNOSTIC_TESTING,
# MAGIC         COUNT(CASE WHEN pd.PAT_TYPE = 31 THEN 1 END) AS c_RECURRING_SERIES,
# MAGIC         COUNT(CASE WHEN pd.PAT_TYPE = 32 THEN 1 END) AS c_PRE_SURGICAL_TESTING,
# MAGIC         COUNT(CASE WHEN pd.PAT_TYPE = 33 THEN 1 END) AS c_HOME_HEALTH,
# MAGIC         COUNT(CASE WHEN pd.PAT_TYPE = 34 THEN 1 END) AS c_CLINIC,
# MAGIC         COUNT(CASE WHEN pd.PAT_TYPE = 35 THEN 1 END) AS c_ORGAN_DONOR,
# MAGIC         COUNT(CASE WHEN pd.PAT_TYPE = 90 THEN 1 END) AS c_OTHER 
# MAGIC     FROM 
# MAGIC         edav_prd_cdh.cdh_premier_v2.patdemo AS pd
# MAGIC     LEFT JOIN 
# MAGIC         edav_prd_cdh.cdh_premier_v2.providers AS pr
# MAGIC     ON 
# MAGIC         pr.PROV_ID = pd.PROV_ID
# MAGIC     WHERE 
# MAGIC         SUBSTRING(CAST(pd.DISC_MON AS STRING), 1, 4) = '2020'
# MAGIC     GROUP BY  
# MAGIC        pr.PROV_ID, pd.PROV_ID, URBAN_RURAL, TEACHING, BEDS_GRP, PROV_REGION, PROV_DIVISION, COST_TYPE
# MAGIC     ORDER BY  
# MAGIC         pd.PROV_ID DESC

# COMMAND ----------

df_providers = spark.sql("""
    SELECT 
        pr.prov_id, 
        pd.PROV_ID, URBAN_RURAL, TEACHING, BEDS_GRP, PROV_REGION, PROV_DIVISION, COST_TYPE,
        COUNT(CASE WHEN pd.PAT_TYPE = 8 THEN 1 END) AS c_INPATIENT,
        COUNT(CASE WHEN pd.ADM_TYPE = 4 THEN 1 END) AS NEWBORN,
        COUNT(CASE WHEN pd.ADM_TYPE = 1 THEN 1 END) AS EMERGENCY,
        COUNT(CASE WHEN pd.ADM_TYPE = 2 THEN 1 END) AS URGENT,
        COUNT(CASE WHEN pd.ADM_TYPE = 3 THEN 1 END) AS ELECTIVE,
        COUNT(CASE WHEN pd.ADM_TYPE = 5 THEN 1 END) AS TRAUMA_CENTER,
        COUNT(CASE WHEN pd.ADM_TYPE = 9 THEN 1 END) AS NA,
        COUNT(CASE WHEN pd.PAT_TYPE = 10 THEN 1 END) AS c_SKILLED_NURSING,
        COUNT(CASE WHEN pd.PAT_TYPE = 22 THEN 1 END) AS c_LONG_TERM_CARE,
        COUNT(CASE WHEN pd.PAT_TYPE = 23 THEN 1 END) AS c_REHABILITATION,
        COUNT(CASE WHEN pd.PAT_TYPE = 24 THEN 1 END) AS c_PSYCHIATRIC,
        COUNT(CASE WHEN pd.PAT_TYPE = 25 THEN 1 END) AS c_HOSPICE,
        COUNT(CASE WHEN pd.PAT_TYPE = 26 THEN 1 END) AS c_CHEMICAL_DEPENDENCY,
        COUNT(CASE WHEN pd.PAT_TYPE = 27 THEN 1 END) AS c_SAME_DAY_SURGERY,
        COUNT(CASE WHEN pd.PAT_TYPE = 28 THEN 1 END) AS c_EMERGENCY,
        COUNT(CASE WHEN pd.PAT_TYPE = 29 THEN 1 END) AS c_OBSERVATION,
        COUNT(CASE WHEN pd.PAT_TYPE = 30 THEN 1 END) AS c_DIAGNOSTIC_TESTING,
        COUNT(CASE WHEN pd.PAT_TYPE = 31 THEN 1 END) AS c_RECURRING_SERIES,
        COUNT(CASE WHEN pd.PAT_TYPE = 32 THEN 1 END) AS c_PRE_SURGICAL_TESTING,
        COUNT(CASE WHEN pd.PAT_TYPE = 33 THEN 1 END) AS c_HOME_HEALTH,
        COUNT(CASE WHEN pd.PAT_TYPE = 34 THEN 1 END) AS c_CLINIC,
        COUNT(CASE WHEN pd.PAT_TYPE = 35 THEN 1 END) AS c_ORGAN_DONOR,
        COUNT(CASE WHEN pd.PAT_TYPE = 90 THEN 1 END) AS c_OTHER 
    FROM 
        edav_prd_cdh.cdh_premier_v2.patdemo AS pd
    LEFT JOIN 
        edav_prd_cdh.cdh_premier_v2.providers AS pr
    ON 
        pr.PROV_ID = pd.PROV_ID
    WHERE 
        SUBSTRING(CAST(pd.DISC_MON AS STRING), 1, 4) = '2020'
    GROUP BY  
       pr.PROV_ID, pd.PROV_ID, URBAN_RURAL, TEACHING, BEDS_GRP, PROV_REGION, PROV_DIVISION, COST_TYPE
    ORDER BY  
        pd.PROV_ID DESC
""")




# converting table to pandas df to visualize variable distributions
d_providers = df_providers.toPandas()




# COMMAND ----------


d_providers.fillna(0, inplace=True)
d_providers['PROV_ID'] = d_providers['PROV_ID'].copy()
d_providers['Admin total\n(sum adm_type vals)'] = (
    d_providers['NEWBORN'] + d_providers['EMERGENCY'] + d_providers['URGENT'] + 
    d_providers['ELECTIVE'] + d_providers['TRAUMA_CENTER'] + d_providers['NA']
)
d_providers['p_NEWBORN'] = d_providers['NEWBORN'] / d_providers['Admin total\n(sum adm_type vals)']
d_providers['p_EMERGENCY'] = d_providers['EMERGENCY'] / d_providers['Admin total\n(sum adm_type vals)']
d_providers['p_URGENT'] = d_providers['URGENT'] / d_providers['Admin total\n(sum adm_type vals)']
d_providers['p_ELECTIVE'] = d_providers['ELECTIVE'] / d_providers['Admin total\n(sum adm_type vals)']
d_providers['p_TRAUMA_CENTER'] = d_providers['TRAUMA_CENTER'] / d_providers['Admin total\n(sum adm_type vals)']
d_providers['p_NA'] = d_providers['NA'] / d_providers['Admin total\n(sum adm_type vals)']
d_providers['pat_type total\n(sum pat_type vals)'] = (
    d_providers['c_INPATIENT'] + d_providers['c_SKILLED_NURSING'] + d_providers['c_LONG_TERM_CARE'] + 
    d_providers['c_REHABILITATION'] + d_providers['c_PSYCHIATRIC'] + d_providers['c_HOSPICE'] + 
    d_providers['c_CHEMICAL_DEPENDENCY'] + d_providers['c_SAME_DAY_SURGERY'] + d_providers['c_EMERGENCY'] + 
    d_providers['c_OBSERVATION'] + d_providers['c_DIAGNOSTIC_TESTING'] + d_providers['c_RECURRING_SERIES'] + 
    d_providers['c_PRE_SURGICAL_TESTING'] + d_providers['c_HOME_HEALTH'] + d_providers['c_CLINIC'] + 
    d_providers['c_ORGAN_DONOR'] + d_providers['c_OTHER']
)
d_providers['p_INPATIENT'] = d_providers['c_INPATIENT'] / d_providers['Admin total\n(sum adm_type vals)']
d_providers['p_SKILLED_NURSING'] = d_providers['c_SKILLED_NURSING'] / d_providers['Admin total\n(sum adm_type vals)']
d_providers['p_LONG_TERM_CARE'] = d_providers['c_LONG_TERM_CARE'] / d_providers['Admin total\n(sum adm_type vals)']
d_providers['p_REHABILITATION'] = d_providers['c_REHABILITATION'] / d_providers['Admin total\n(sum adm_type vals)']
d_providers['p_PSYCHIATRIC'] = d_providers['c_PSYCHIATRIC'] / d_providers['Admin total\n(sum adm_type vals)']
d_providers['p_HOSPICE'] = d_providers['c_HOSPICE'] / d_providers['Admin total\n(sum adm_type vals)']
d_providers['p_CHEMICAL_DEPENDENCY'] = d_providers['c_CHEMICAL_DEPENDENCY'] / d_providers['Admin total\n(sum adm_type vals)']
d_providers['p_SAME_DAY_SURGERY'] = d_providers['c_SAME_DAY_SURGERY'] / d_providers['Admin total\n(sum adm_type vals)']
d_providers['p_EMERGENCY.1'] = d_providers['c_EMERGENCY'] / d_providers['Admin total\n(sum adm_type vals)']
d_providers['p_OBSERVATION'] = d_providers['c_OBSERVATION'] / d_providers['Admin total\n(sum adm_type vals)']
d_providers['p_DIAGNOSTIC_TESTING'] = d_providers['c_DIAGNOSTIC_TESTING'] / d_providers['Admin total\n(sum adm_type vals)']
d_providers['p_RECURRING_SERIES'] = d_providers['c_RECURRING_SERIES'] / d_providers['Admin total\n(sum adm_type vals)']
d_providers['p_PRE_SURGICAL_TESTING'] = d_providers['c_PRE_SURGICAL_TESTING'] / d_providers['Admin total\n(sum adm_type vals)']
d_providers['p_HOME_HEALTH'] = d_providers['c_HOME_HEALTH'] / d_providers['Admin total\n(sum adm_type vals)']
d_providers['p_CLINIC'] = d_providers['c_CLINIC'] / d_providers['Admin total\n(sum adm_type vals)']
d_providers['p_ORGAN_DONOR'] = d_providers['c_ORGAN_DONOR'] / d_providers['Admin total\n(sum adm_type vals)']
d_providers['p_OTHER'] = d_providers['c_OTHER'] / d_providers['Admin total\n(sum adm_type vals)']

d_providers = d_providers[[
    'prov_id', 'PROV_ID', 'URBAN_RURAL', 'TEACHING', 'BEDS_GRP', 'PROV_REGION', 'PROV_DIVISION', 'COST_TYPE', 
    'PROV_ID', 'Admin total\n(sum adm_type vals)', 'p_NEWBORN', 'p_EMERGENCY', 'p_URGENT', 'p_ELECTIVE', 
    'p_TRAUMA_CENTER', 'p_NA', 'NEWBORN', 'EMERGENCY', 'URGENT', 'ELECTIVE', 'TRAUMA_CENTER', 'NA', 
    'pat_type total\n(sum pat_type vals)', 'p_INPATIENT', 'p_SKILLED_NURSING', 'p_LONG_TERM_CARE', 'p_REHABILITATION', 
    'p_PSYCHIATRIC', 'p_HOSPICE', 'p_CHEMICAL_DEPENDENCY', 'p_SAME_DAY_SURGERY', 'p_EMERGENCY.1', 'p_OBSERVATION', 
    'p_DIAGNOSTIC_TESTING', 'p_RECURRING_SERIES', 'p_PRE_SURGICAL_TESTING', 'p_HOME_HEALTH', 'p_CLINIC', 
    'p_ORGAN_DONOR', 'p_OTHER', 'c_INPATIENT', 'c_SKILLED_NURSING', 'c_LONG_TERM_CARE', 'c_REHABILITATION', 
    'c_PSYCHIATRIC', 'c_HOSPICE', 'c_CHEMICAL_DEPENDENCY', 'c_SAME_DAY_SURGERY', 'c_EMERGENCY', 'c_OBSERVATION', 
    'c_DIAGNOSTIC_TESTING', 'c_RECURRING_SERIES', 'c_PRE_SURGICAL_TESTING', 'c_HOME_HEALTH', 'c_CLINIC', 
    'c_ORGAN_DONOR', 'c_OTHER'
]]

d_providers.fillna(0, inplace=True)
display(d_providers)

print('Number of hospitals:', len(d_providers))

# COMMAND ----------

# MAGIC %md
# MAGIC # 2) Inpatient Discharge Counts by Year and Provider
# MAGIC
# MAGIC The query below extracts a count of all discharge encounters from 2019 by provider. In order to coordinate with AHA inpatient dataset, the query filters on PAT_TYPE = 8 and i_o_ind = 'I' and year as 2019 as that year overlaps with AHA data. The discharge counts are later binned and used as a variable within the wieghting process
# MAGIC
# MAGIC Columns:['PI', ' discharge_patdemo']
# MAGIC
# MAGIC For further details, view the Premier Data Extract Dictionary.
# MAGIC
# MAGIC

# COMMAND ----------

df_inpatient =spark.sql("""
select  pd.PI as PROV_ID, pd.year AS year, count(count_pat) as discharge_patdemo
  from
  (select pd.PROV_ID as PI, pd.PAT_KEY as count_pat,  SUBSTRING(cast(pd.DISC_MON AS string), 1, 4) AS year
from 
    edav_prd_cdh.cdh_premier_v2.patdemo pd 
where 
    pd.PAT_TYPE = 8 and pd.i_o_ind = 'I'
    and year = '2020') pd

group by  
    pd.PI, pd.year
;
""")

# converting table to pandas df to visualize variable distributions
inpatients= df_inpatient.toPandas()

print('Number of hospitals with inpatient data:', len(providers))

display(inpatients)



# COMMAND ----------

# MAGIC %md
# MAGIC # 3) Sample project: Pericarditis Demographics
# MAGIC   Pericarditis demographics by race, sex, age group, and insurance status as well as severity status, ICU and IMV admissions

# COMMAND ----------

# Create table of patients with pericarditis
Percodes= spark.sql(""" select PAT_KEY, ICD_CODE  from edav_prd_cdh.cdh_premier_v2.paticd_diag where ICD_CODE in ('A39.53','B33.23','I01.0','I09.2','I30.0','I30.1','I30.8','I30.9','I31.0','I31.1','I31.2','I31.3','I31.31','I31.39','I31.4','I31.8','I31.9','I32','M32.12') and ICD_PRI_SEC IN ('P') and year = '2020' """)


# Create table of patients with admitted to the ICU or used a ventilator
Charges= spark.sql(""" select PAT_KEY, CLIN_SUM_CODE, CLIN_DTL_CODE, F.STD_CHG_CODE
                    FROM edav_prd_cdh.cdh_premier_v2.patbill as e 
                    INNER JOIN edav_prd_cdh.cdh_premier_v2.chgmstr as F
                    ON    e.std_chg_code=f.std_chg_code 
                    WHERE (f.clin_sum_code = "110102" or f.clin_dtl_code ="410412946570007") and year = '2020' """)


# Create table a demographic table with provider information and selecting demogrphaics of interest
Demographic= spark.sql(""" SELECT DISTINCT PAT_KEY, MEDREC_KEY, DISC_MON, I_O_IND, PAT_TYPE, DISC_STATUS, std_payor,  AGE, GENDER, RACE, HISPANIC_IND, a.PROV_ID, prov_region, prov_division, URBAN_RURAL
                        FROM edav_prd_cdh.cdh_premier_v2.patdemo as a
                        LEFT JOIN edav_prd_cdh.cdh_premier_v2.providers as b
                        ON a.prov_id = b.prov_id 
                        WHERE YEAR = '2020' and a.PAT_TYPE = 08 AND a.I_O_IND = 'I' """)



# Register the percodes, charges, and demographics DataFrame as a temporary view to allow for use in joins
Percodes.createOrReplaceTempView("Percodes")
Charges.createOrReplaceTempView("Charges")
Demographic.createOrReplaceTempView("Demographic")


# Merge all 3 tables, percodes, charges, and demographics to create cohort of interest along with any new variables needed for analysis
pericarditis = spark.sql(""" SELECT DISTINCT  c.PAT_KEY, MEDREC_KEY, DISC_MON, I_O_IND, PAT_TYPE, DISC_STATUS, std_payor, AGE, GENDER, RACE, HISPANIC_IND, PROV_ID, prov_region, prov_division, URBAN_RURAL, CLIN_SUM_CODE, CLIN_DTL_CODE, ICD_CODE,
 
        CASE 
            WHEN ICD_CODE in ('A39.53','B33.23','I01.0','I09.2','I30.0','I30.1','I30.8','I30.9','I31.0','I31.1','I31.2','I31.3','I31.31','I31.39','I31.4','I31.8','I31.9','I32','M32.12') THEN 'Pericarditis' else 'NA_pericarditis'
            END AS Pericarditis,
        CASE 
            WHEN disc_status IN (20,40,41,42) THEN 'Expired' else 'NA_expired'
            END AS expired,
        CASE 
            WHEN clin_sum_code = 110102 THEN 'ICU' else 'NA_icu'
            END AS ICU,
        CASE 
            WHEN clin_dtl_code = 410412946570007 THEN 'VENT' else 'NA_vent'
            END AS Vent,
        CASE 
            WHEN AGE <= 17 THEN '0-17'
            WHEN AGE >= 18 AND a.AGE <= 49 THEN '18-49'  
            WHEN AGE >= 50 AND a.AGE <= 64 THEN '50-64' 
            WHEN AGE >= 65  THEN '65+' 
        END AS agecat2,
        
        CASE 
            WHEN hispanic_ind = 'Y' THEN 'Hispanic'
            WHEN hispanic_ind = 'U' THEN 'Unknown'
            WHEN RACE = 'W' AND a.HISPANIC_IND = 'N' THEN 'nH White'
            WHEN RACE = 'B' AND a.HISPANIC_IND = 'N' THEN 'nH Black' 
            WHEN RACE = 'A' AND a.HISPANIC_IND = 'N' THEN 'nH Asian'
            WHEN RACE = 'O' AND a.HISPANIC_IND = 'N' THEN 'nH Other'
            WHEN RACE = 'U' AND a.HISPANIC_IND = 'N' THEN 'Unknown'
        END AS raceeth,

           case 
       when std_payor in (360,370,380) then 'Private'
       when STD_PAYOR in (300,310,320) then 'Medicare'
       when std_payor in (330,340,350) then 'Medicaid'
       when std_payor in (410) then 'dSelf_pay'
       else 'NA_payor'
       END AS payor

    FROM Demographic as a
    left join charges as b
    on a.pat_key = b.pat_key
    left join Percodes as c
    on a.pat_key = c.pat_key
""")

# Register the pericarditis DataFrame as a temporary view
pericarditis.createOrReplaceTempView("pericarditis")


# Create one row of demorgraphic variables for each hospitilization in the cohort
Pericarditis2 = spark.sql("""
    select pat_key, max(agecat2) as agecat2, max(raceeth) as raceeth, max(expired) as expired, max(ICU) as ICU, max(Vent) as Vent, max(PROV_ID) as PROV_ID, max(Pericarditis) as Pericarditis, max(URBAN_RURAL) as URBAN, max(prov_region) as prov_region, max(prov_division) as prov_division, max(payor) as payor
    from Pericarditis 
    group by pat_key
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.1) Pericarditis Analytics
# MAGIC Analysis needed for your project can be done in this section. For this sample project, Pericardities counts by demographics and provider is used. Each demographic is saved as a separate table for ease of final formating. All analysis can be done here or the finalized portions of analysis from a different notebook can be put here. Alternatiively, you may choose to do all analyisi in a diifferent notebook as long a the finalized table is formatted to match the final table in this section and saved as a CSV to the analytic folder, everything will still run as expected. 

# COMMAND ----------

# Register the pericarditis2 DataFrame as a temporary view
Pericarditis2.createOrReplaceTempView("Pericarditis2")

age = spark.sql("""
  SELECT PROV_ID, agecat2, COUNT(agecat2)
  FROM Pericarditis2 
  GROUP BY PROV_ID, agecat2
""")

race = spark.sql("""
  SELECT PROV_ID, raceeth, COUNT(raceeth)
  FROM Pericarditis2 
  GROUP BY PROV_ID, raceeth
""")

death = spark.sql("""
  SELECT PROV_ID, expired, COUNT(expired)
  FROM Pericarditis2 
  GROUP BY PROV_ID, expired
""")

critical = spark.sql("""
  SELECT PROV_ID, ICU, COUNT(ICU)
  FROM Pericarditis2 
  GROUP BY PROV_ID, ICU
""")

ventilation = spark.sql("""
  SELECT PROV_ID, VENT, COUNT(VENT)
  FROM Pericarditis2 
  GROUP BY PROV_ID, VENT
""")

per = spark.sql("""
  SELECT PROV_ID, Pericarditis, COUNT(Pericarditis)
  FROM Pericarditis2 
  GROUP BY PROV_ID, Pericarditis
""")

urb_rural = spark.sql("""
  SELECT PROV_ID, URBAN, COUNT(URBAN)
  FROM Pericarditis2 
  GROUP BY PROV_ID, URBAN
""")

region = spark.sql("""
  SELECT PROV_ID, prov_region, COUNT(prov_region)
  FROM Pericarditis2 
  GROUP BY PROV_ID, prov_region
""")

division = spark.sql("""
  SELECT PROV_ID, prov_division, COUNT(prov_division)
  FROM Pericarditis2 
  GROUP BY PROV_ID, prov_division
""")

ins = spark.sql("""
  SELECT PROV_ID, payor, COUNT(payor)
  FROM Pericarditis2 
  GROUP BY PROV_ID, payor
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.2) Formating
# MAGIC Tranform all analytical tables, demographic count tables for this project. In this section each table is transformed into a pandas dataframe, and transposed to fit the formating required weighting

# COMMAND ----------

# Perert to pandas df
age2 = age.toPandas()  
# transpose df on pivot
age2 = age2.pivot_table('count(agecat2)', ['PROV_ID'],'agecat2')
# flatten to one row of columns
age2 = age2.reset_index().rename_axis(None, axis=1)
# set index
age2.index = age2['PROV_ID']

race2 = race.toPandas()  
# transpose df on pivot
race2 = race2.pivot_table('count(raceeth)', ['PROV_ID'],'raceeth')
# flatten to one row of columns
race2 = race2.reset_index().rename_axis(None, axis=1)
# set index
race2.index = race2['PROV_ID']

death2 = death.toPandas()  
# transpose df on pivot
death2 = death2.pivot_table('count(expired)', ['PROV_ID'],'expired')
# flatten to one row of columns
death2 = death2.reset_index().rename_axis(None, axis=1)
# set index
death2.index = death2['PROV_ID']

critical2 = critical.toPandas()  
# transpose df on pivot
critical2 = critical2.pivot_table('count(ICU)', ['PROV_ID'],'ICU')
# flatten to one row of columns
critical2 = critical2.reset_index().rename_axis(None, axis=1)
# set index
critical2.index = critical2['PROV_ID']

ventilation2 = ventilation.toPandas()  
# transpose df on pivot
ventilation2 = ventilation2.pivot_table('count(VENT)', ['PROV_ID'],'VENT')
# flatten to one row of columns
ventilation2 = ventilation2.reset_index().rename_axis(None, axis=1)
# set index
ventilation2.index = ventilation2['PROV_ID']

per2 = per.toPandas()  
# transpose df on pivot
per2 = per2.pivot_table('count(Pericarditis)', ['PROV_ID'],'Pericarditis')
# flatten to one row of columns
per2 = per2.reset_index().rename_axis(None, axis=1)
# set index
per2.index = per2['PROV_ID']

urb_rural2 = urb_rural.toPandas()  
# transpose df on pivot
urb_rural2 = urb_rural2.pivot_table('count(URBAN)', ['PROV_ID'],'URBAN')
# flatten to one row of columns
urb_rural2 = urb_rural2.reset_index().rename_axis(None, axis=1)
# set index
urb_rural2.index = urb_rural2['PROV_ID']

region2 = region.toPandas()  
# transpose df on pivot
region2 = region2.pivot_table('count(prov_region)', ['PROV_ID'],'prov_region')
# flatten to one row of columns
region2 = region2.reset_index().rename_axis(None, axis=1)
# set index
region2.index = region2['PROV_ID']

division2 = division.toPandas()  
# transpose df on pivot
division2 = division2.pivot_table('count(prov_division)', ['PROV_ID'],'prov_division')
# flatten to one row of columns
division2 = division2.reset_index().rename_axis(None, axis=1)
# set index
division2.index = division2['PROV_ID']

ins2 = ins.toPandas()  
# transpose df on pivot
ins2 = ins2.pivot_table('count(payor)', ['PROV_ID'],'payor')
# flatten to one row of columns
ins2 = ins2.reset_index().rename_axis(None, axis=1)
# set index
ins2.index = ins2['PROV_ID']



# COMMAND ----------

# MAGIC %md
# MAGIC ## 3.3) Final table for Weighting process
# MAGIC Join all relevant tables where necessary to create final table with the below format for the weighting process.

# COMMAND ----------

# Assuming you are using pandas DataFrames instead of Spark DataFrames
# If so, you should convert them to Spark DataFrames first
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# Convert pandas DataFrames to Spark DataFrames
age2_spark = spark.createDataFrame(age2)
race2_spark = spark.createDataFrame(race2)
death2_spark = spark.createDataFrame(death2)
critical2_spark = spark.createDataFrame(critical2)
ventilation2_spark = spark.createDataFrame(ventilation2)
per2_spark = spark.createDataFrame(per2)
urb_rural2_spark = spark.createDataFrame(urb_rural2)
region2_spark = spark.createDataFrame(region2)
division2_spark = spark.createDataFrame(division2)
ins2_spark = spark.createDataFrame(ins2)

# Create a temporary view of each Spark DataFrame to enable joins
age2_spark.createOrReplaceTempView("age2")
race2_spark.createOrReplaceTempView("race2")
death2_spark.createOrReplaceTempView("death2")
critical2_spark.createOrReplaceTempView("critical2")
ventilation2_spark.createOrReplaceTempView("ventilation2")
per2_spark.createOrReplaceTempView("per2")
urb_rural2_spark.createOrReplaceTempView("urb_rural2")
region2_spark.createOrReplaceTempView("region2")
division2_spark.createOrReplaceTempView("division2")
ins2_spark.createOrReplaceTempView("ins2")

Pericarditis_demographics = spark.sql("""
    SELECT 
*
    FROM 
        age2 a 
    LEFT JOIN 
        race2 b ON a.PROV_ID= b.PROV_ID
    LEFT JOIN 
        death2 c ON a.PROV_ID= c.PROV_ID
    LEFT JOIN 
        critical2 d ON a.PROV_ID= d.PROV_ID
    LEFT JOIN 
        ventilation2 e ON a.PROV_ID= e.PROV_ID
    LEFT JOIN 
        per2 f ON a.PROV_ID= f.PROV_ID
    LEFT JOIN 
        urb_rural2 g ON a.PROV_ID= g.PROV_ID
    LEFT JOIN 
        region2 h ON a.PROV_ID= h.PROV_ID
    LEFT JOIN 
        division2 i ON a.PROV_ID= i.PROV_ID
    LEFT JOIN 
        ins2 j ON a.PROV_ID= j.prov_id
""")



# Convert table to pandas dataframe to enable python funtions and to save as CSV
Pericarditis_demo = Pericarditis_demographics.toPandas()

# Convert null values to 0 using fillna()
Pericarditis_demo.fillna(0, inplace=True)

display(Pericarditis_demo)

# COMMAND ----------


#Save all three dataframes to csv in analytic folder for use in weighting process
d_providers.to_csv('/Workspace/Users/ltq3@cdc.gov/Weight_test/Pericarditis and pericardial disease/premier_2020.csv', index=False)
inpatients.to_csv('/Workspace/Users/ltq3@cdc.gov/Weight_test/Pericarditis and pericardial disease/premier_discharge_counts_2020.csv', index=False)
Pericarditis_demo.to_csv('/Workspace/Users/ltq3@cdc.gov/Weight_test/Pericarditis and pericardial disease/all_provid_all_ccsr_counts_2020.csv', index=False)
