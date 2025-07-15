# Databricks notebook source
# MAGIC %python
# MAGIC import pandas as pd
# MAGIC import pyspark.sql.functions as F
# MAGIC import itertools

# COMMAND ----------

# LOINC

EXCLUSION_LOINC = [
    "SARS coronavirus 2 RNA:PrThr:Pt:Respiratory:Ord:Probe.amp.tar",
    "SARS coronavirus 2 RNA:PrThr:Pt:XXX:Ord:Probe.amp.tar"
]

# COMMAND ----------



EXCLUSION_ICD = [
    "U07.1",
    "U07.2",
    "U09.9",
    "B34.2",
    "B97.21",
    "B97.29",
    "J12.81",
    "J12.82",
    "M35.81",
    "Z86.16",
    "B94.8",
    "B94.9"   
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

EXCLUSION_HPCPS = [
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

EXCLUSION_TREATMENT_DESC = [
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

# MAGIC %python
# MAGIC CCSRS=['END005',
# MAGIC 'CIR013',
# MAGIC 'RSP010',
# MAGIC 'NVS014',
# MAGIC 'NEO059',
# MAGIC 'NEO064',
# MAGIC 'CIR005',
# MAGIC 'NVS018',
# MAGIC 'CIR015',
# MAGIC 'RSP002',
# MAGIC 'INF002',]

# COMMAND ----------

# MAGIC %python
# MAGIC #Codes used to identify COVID tests
# MAGIC covid_tests={"LOINC" = ['959429', '959411', '953802', '954230', '954222', '959718', '959742', '959726', '959734', '952093',
# MAGIC                '947630', '946616', '958256', '947622', '947697', '961185', '945048', '945030', '945584', '961193',
# MAGIC                '960948', '945626', '947689', '954271', '947200', '951251', '947614', '945634', '945071', '954297', 
# MAGIC                '945055', '945477', '955427', '954164', '945642', '945089', '954289', '945063', '955211', '945105', 
# MAGIC                '943118', '943126', '955229', '947606', '954099', '945337', '947564', '947572', '954255', '964486', 
# MAGIC                '947663', '943167', '943076', '943084', '954115', '954107', '946442', '945113', '945592', '958249', 
# MAGIC                '946392', '946467', '946459', '961201', '945345', '960914', '943142', '961235', '947457', '947465',
# MAGIC                '948190', '945659', '947598', '954065', '956086', '945006', '954248', '948455', '948224', '946608', 
# MAGIC                '943092', '945311', '958264', '943068', '946426', '946434', '946400', '956094', '947671', '946418', 
# MAGIC                '959700', '947648', '943134', '943100', '945097', '961219', '947580', '958231', '947655', '943159', 
# MAGIC                '961227', '945022', '946475', '945329'],
# MAGIC
# MAGIC # procedure_code + procedure_code_qual = 'CPT'
# MAGIC "CPT":['86318', '86328', '86408', '86409', '86413', '86769', '87635', '0098U', '0099U', '0100U', '0202U', '0223U', '0224U', '0225U', '0226U']
# MAGIC ,
# MAGIC # procedure_code + procedure_code_qual = 'HCPCS'
# MAGIC "HCPCS" :['C9803', 'G2023', 'G2024', 'G2025', 'U0001', 'U0002', 'U0003', 'U0004'],
# MAGIC "ICD":['B97.29','U07.1']
# MAGIC }

# COMMAND ----------

# MAGIC %python
# MAGIC #covid1='B97.29' # adm >=20200102+<2020204 disch >=2020103 <2020204
# MAGIC #covid2='U07.1' # adm >=2020204 <=20201204 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # MISA Hyperinflammatory Syndrome
# MAGIC
# MAGIC Identify encounters that meet this criteria
# MAGIC - identify criteria 2 and 3 in encounters
# MAGIC
# MAGIC - Meets criteria 1,2, and 3
# MAGIC    1. Hospitalization (inpat) and > 18y
# MAGIC         - adm: Feb-Apr 2020 
# MAGIC         - disch: March-Apr 2020
# MAGIC           - Primary of Secondary ICD-10 code for B97.29 
# MAGIC         - disch: after April 2020 
# MAGIC           - primary or secondary ICD-10 cod U07.1 "COVID-19 identified"
# MAGIC     2. Marker of inflamation (atleast 1)
# MAGIC         - IL-6 > 46.5 pg/ml
# MAGIC         - C-reactive protein >= 30mg/L
# MAGIC         - Ferritin >= 1500 ng/ml
# MAGIC         - Erythrocyte sedimentation rate >= 45 mm/hour
# MAGIC         - Procalcitonin >= 0.3 ng/ml
# MAGIC     3. greater than or equal to 2 categoris of clinical complications
# MAGIC           - Cardiac, defined by at least one of the following ICD-10 codes
# MAGIC             1. I30: Acute pericarditis
# MAGIC             2. I40: Infective myocarditis
# MAGIC             3. I25.41: Coronary artery aneurysm
# MAGIC           - Shock or hypotension, defined by at least one of the following
# MAGIC             1. Systolic blood pressure <90 mmHg on at least 2 days
# MAGIC             2. At least one of the following ICD-10 codes:
# MAGIC                 1. R57, Shock, not elsewhere classified (includes all subcodes)
# MAGIC                 2. R65.21, Severe sepsis with septic shock
# MAGIC                 3. I95, Hypotension shock (includes all subcodes)
# MAGIC             3. Vasopressor use on at least 2 days
# MAGIC           - Gastrointestinal, defined by at least one of the following ICD-10 codes
# MAGIC             1. R10.0, Acute abdomen
# MAGIC             2. R19.7, Diarrhea, unspecified
# MAGIC             3. A09, Infectious gastroenteritis and colitis, unspecified
# MAGIC             4. A08.39, Other viral enteritis
# MAGIC             5. A08.4, Viral intestinal infection, unspecified
# MAGIC             6. R11.2, Nausea with vomiting, unspecified
# MAGIC             7. R11.10, Vomiting, unspecified
# MAGIC           - Thrombocytopenia or elevated D-dimer
# MAGIC             1. Thrombocytopenia, defined by one of the following ICD-10 codes
# MAGIC                 1. D69.6, Thrombocytopenia, unspecified
# MAGIC                 2. D69.59, Other secondary thrombocytopenia
# MAGIC             2. D-dimer ≥ 1200 FEU
# MAGIC             

# COMMAND ----------

# MAGIC %sql
# MAGIC --c-reactive
# MAGIC select * from edav_dev_edav_prd_cdh.cdh_test.dev_edav_prd_cdh.cdh_ml_test.ml_loinc_list
# MAGIC where loinc_desc like '%reactive%'
