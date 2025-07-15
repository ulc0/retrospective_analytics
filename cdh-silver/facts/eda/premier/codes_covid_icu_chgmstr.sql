-- Databricks notebook source
-- MAGIC %python
-- MAGIC import pandas as pd
-- MAGIC import pyspark.sql.functions as F
-- MAGIC import itertools

-- COMMAND ----------

-- MAGIC %python
-- MAGIC EXCLUSION_LOINC = [
-- MAGIC     "SARS coronavirus 2 RNA:PrThr:Pt:Respiratory:Ord:Probe.amp.tar",
-- MAGIC     "SARS coronavirus 2 RNA:PrThr:Pt:XXX:Ord:Probe.amp.tar"
-- MAGIC ]
-- MAGIC
-- MAGIC EXCLUSION_ICD = [
-- MAGIC     "U07.1",
-- MAGIC     "U07.2",
-- MAGIC     "U09.9",
-- MAGIC     "B34.2",
-- MAGIC     "B97.21",
-- MAGIC     "B97.29",
-- MAGIC     "J12.81",
-- MAGIC     "J12.82",
-- MAGIC     "M35.81",
-- MAGIC     "Z86.16",
-- MAGIC     "B94.8",
-- MAGIC     "B94.9"   
-- MAGIC ]
-- MAGIC
-- MAGIC EXCLUSION_NDC_CODES = [ 
-- MAGIC     #not used in Premier
-- MAGIC     "61755004202",
-- MAGIC     "61755004502",
-- MAGIC     "61755003502",
-- MAGIC     "61755003608",
-- MAGIC     "61755003705",
-- MAGIC     "61755003805",
-- MAGIC     "61755003901",
-- MAGIC     "61755002501",
-- MAGIC     "61755002701",
-- MAGIC     "00002791001",
-- MAGIC     "00002795001",
-- MAGIC     "00002791001",
-- MAGIC     "00002795001",
-- MAGIC     "00173090186",
-- MAGIC     "00002758901",
-- MAGIC     "00069108530",
-- MAGIC     "00069108506",
-- MAGIC     "00006505506",
-- MAGIC     "00006505507",
-- MAGIC     "61958290202",
-- MAGIC     "61968290101"
-- MAGIC ]
-- MAGIC
-- MAGIC EXCLUSION_HPCPS = [
-- MAGIC     #unchanged from HV
-- MAGIC     "Q0240",
-- MAGIC     "Q0243",
-- MAGIC     "Q0244",
-- MAGIC     "M0240",
-- MAGIC     "M0241",
-- MAGIC     "M0243",
-- MAGIC     "M0244",
-- MAGIC     "Q0245",
-- MAGIC     "M0245",
-- MAGIC     "M0246",
-- MAGIC     "Q0247",
-- MAGIC     "M0247",
-- MAGIC     "M0248",
-- MAGIC     "Q0222",
-- MAGIC     "M0222",
-- MAGIC     "M0223",
-- MAGIC     "J0248"
-- MAGIC ]
-- MAGIC
-- MAGIC EXCLUSION_TREATMENT_DESC = [
-- MAGIC     #unchanged from HV
-- MAGIC     "casirivimab",
-- MAGIC     "imdevimab",
-- MAGIC     "etesevimab",
-- MAGIC     "bamlanivimab",
-- MAGIC     "sotrovimab",
-- MAGIC     "bebtelovimab",
-- MAGIC     "paxlovid",
-- MAGIC     "molnupiravir",
-- MAGIC     "remdesivir"
-- MAGIC ]

-- COMMAND ----------

-- MAGIC %python
-- MAGIC CCSRS=['END005',
-- MAGIC 'CIR013',
-- MAGIC 'RSP010',
-- MAGIC 'NVS014',
-- MAGIC 'NEO059',
-- MAGIC 'NEO064',
-- MAGIC 'CIR005',
-- MAGIC 'NVS018',
-- MAGIC 'CIR015',
-- MAGIC 'RSP002',
-- MAGIC 'INF002',]

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Codes used to identify COVID tests
-- MAGIC covid_tests={"LOINC" = ['959429', '959411', '953802', '954230', '954222', '959718', '959742', '959726', '959734', '952093',
-- MAGIC                '947630', '946616', '958256', '947622', '947697', '961185', '945048', '945030', '945584', '961193',
-- MAGIC                '960948', '945626', '947689', '954271', '947200', '951251', '947614', '945634', '945071', '954297', 
-- MAGIC                '945055', '945477', '955427', '954164', '945642', '945089', '954289', '945063', '955211', '945105', 
-- MAGIC                '943118', '943126', '955229', '947606', '954099', '945337', '947564', '947572', '954255', '964486', 
-- MAGIC                '947663', '943167', '943076', '943084', '954115', '954107', '946442', '945113', '945592', '958249', 
-- MAGIC                '946392', '946467', '946459', '961201', '945345', '960914', '943142', '961235', '947457', '947465',
-- MAGIC                '948190', '945659', '947598', '954065', '956086', '945006', '954248', '948455', '948224', '946608', 
-- MAGIC                '943092', '945311', '958264', '943068', '946426', '946434', '946400', '956094', '947671', '946418', 
-- MAGIC                '959700', '947648', '943134', '943100', '945097', '961219', '947580', '958231', '947655', '943159', 
-- MAGIC                '961227', '945022', '946475', '945329'],
-- MAGIC
-- MAGIC # procedure_code + procedure_code_qual = 'CPT'
-- MAGIC "CPT":['86318', '86328', '86408', '86409', '86413', '86769', '87635', '0098U', '0099U', '0100U', '0202U', '0223U', '0224U', '0225U', '0226U']
-- MAGIC ,
-- MAGIC # procedure_code + procedure_code_qual = 'HCPCS'
-- MAGIC "HCPCS" :['C9803', 'G2023', 'G2024', 'G2025', 'U0001', 'U0002', 'U0003', 'U0004'],
-- MAGIC "ICD":['B97.29','U07.1']
-- MAGIC }

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #covid1='B97.29' # adm >=20200102+<2020204 disch >=2020103 <2020204
-- MAGIC #covid2='U07.1' # adm >=2020204 <=20201204 
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC # MISA Hyperinflammatory Syndrome
-- MAGIC
-- MAGIC Identify encounters that meet this criteria
-- MAGIC - identify criteria 2 and 3 in encounters
-- MAGIC
-- MAGIC - Meets criteria 1,2, and 3
-- MAGIC    1. Hospitalization (inpat) and > 18y
-- MAGIC         - adm: Feb-Apr 2020 
-- MAGIC         - disch: March-Apr 2020
-- MAGIC           - Primary of Secondary ICD-10 code for B97.29 
-- MAGIC         - disch: after April 2020 
-- MAGIC           - primary or secondary ICD-10 cod U07.1 "COVID-19 identified"
-- MAGIC     2. Marker of inflamation (atleast 1)
-- MAGIC         - IL-6 > 46.5 pg/ml
-- MAGIC         - C-reactive protein >= 30mg/L
-- MAGIC         - Ferritin >= 1500 ng/ml
-- MAGIC         - Erythrocyte sedimentation rate >= 45 mm/hour
-- MAGIC         - Procalcitonin >= 0.3 ng/ml
-- MAGIC     3. greater than or equal to 2 categoris of clinical complications
-- MAGIC           - Cardiac, defined by at least one of the following ICD-10 codes
-- MAGIC             1. I30: Acute pericarditis
-- MAGIC             2. I40: Infective myocarditis
-- MAGIC             3. I25.41: Coronary artery aneurysm
-- MAGIC           - Shock or hypotension, defined by at least one of the following
-- MAGIC             1. Systolic blood pressure <90 mmHg on at least 2 days
-- MAGIC             2. At least one of the following ICD-10 codes:
-- MAGIC                 1. R57, Shock, not elsewhere classified (includes all subcodes)
-- MAGIC                 2. R65.21, Severe sepsis with septic shock
-- MAGIC                 3. I95, Hypotension shock (includes all subcodes)
-- MAGIC             3. Vasopressor use on at least 2 days
-- MAGIC           - Gastrointestinal, defined by at least one of the following ICD-10 codes
-- MAGIC             1. R10.0, Acute abdomen
-- MAGIC             2. R19.7, Diarrhea, unspecified
-- MAGIC             3. A09, Infectious gastroenteritis and colitis, unspecified
-- MAGIC             4. A08.39, Other viral enteritis
-- MAGIC             5. A08.4, Viral intestinal infection, unspecified
-- MAGIC             6. R11.2, Nausea with vomiting, unspecified
-- MAGIC             7. R11.10, Vomiting, unspecified
-- MAGIC           - Thrombocytopenia or elevated D-dimer
-- MAGIC             1. Thrombocytopenia, defined by one of the following ICD-10 codes
-- MAGIC                 1. D69.6, Thrombocytopenia, unspecified
-- MAGIC                 2. D69.59, Other secondary thrombocytopenia
-- MAGIC             2. D-dimer ≥ 1200 FEU
-- MAGIC             

-- COMMAND ----------

-- MAGIC %python
-- MAGIC #Cardiac, defined by at least one of the following ICD-10 codes
-- MAGIC #I30: Acute pericarditis
-- MAGIC #I40: Infective myocarditis
-- MAGIC #I25.41: Coronary artery aneurysm
-- MAGIC cardiac_icd={'pericarditis_acute':['I30'],'myocarditis_infective':['I40'],'aneurysm_coronary_artery':['I25']}
-- MAGIC #icd_codes=spark.table("edav_prd_cdh.cdh_edav_prd_cdh.cdh__prd_cdh.cdh_premier_exploratory.icd_list")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # get chg codes
-- MAGIC chgmstr=spark.table("edav_prd_cdh.cdh_edav_prd_cdh.cdh__prd_cdh.cdh_premier_v2.chgmstr")
-- MAGIC icu_code = chgmstr.filter(F.lower(F.col("std_chg_desc")).contains("r&b icu"))
-- MAGIC #.select("std_chg_code").toPandas()["std_chg_code"])
-- MAGIC step_code = chgmstr.filter(F.lower(F.col("std_chg_desc")).contains("r&b step down"))
-- MAGIC #.select("std_chg_code").toPandas()["std_chg_code"])
-- MAGIC
-- MAGIC #codes = icu_code + step_code
-- MAGIC #print(codes)
-- MAGIC print(icu_code)
-- MAGIC print(step_code)

-- COMMAND ----------

select * from edav_prd_cdh.cdh_edav_prd_cdh.cdh__prd_cdh.cdh_premier_v2.chgmstr 
where lower(STD_CHG_DESC) like "%r&b icu%" or
lower(STD_CHG_DESC) like "%r&b step%"

-- COMMAND ----------

select * from edav_prd_cdh.cdh_edav_prd_cdh.cdh__prd_cdh.cdh_premier_v2.chgmstr 
where (lower(STD_CHG_DESC) like "%r&b%" and (lower(STD_CHG_DESC) like "%icu%"  or
lower(STD_CHG_DESC) like " step%"))
order by STD_CHG_DESC

-- COMMAND ----------

select * from edav_prd_cdh.cdh_edav_prd_cdh.cdh__prd_cdh.cdh_premier_v2.chgmstr 
where CLIN_SUM_CODE in (110108,110102)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Join the disstat
-- MAGIC target_enc = target_enc.join(disstat, on = "disc_status", how = "left")
-- MAGIC def get_death(sdf):
-- MAGIC     out = (sdf
-- MAGIC            .filter(F.lower(F.col("disc_status_desc")).contains("expired"))
-- MAGIC            .select("pat_key", "medrec_key")
-- MAGIC            .withColumn("death_ind", F.lit(1))
-- MAGIC           )
-- MAGIC     return out
