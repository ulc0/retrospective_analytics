# Databricks notebook source
#version = '@v8' 
tag = '_221003'

icd_desc= f"icdcode"
icd_code=f"paticd_diag"
patcpt=f"patcpt"
patlabres=f"lab_res"
genlab=f"genlab"
disstat=f"disstat"
pattype=f"pattype"
patbill=f"patbill"
chgmstr=f"chgmstr"
patdemo=f"patdemo"
providers=f"providers"
readmit=f"readmit"
zip3=f"phd_providers_zip3"

ccsr_lookup = "cdh_reference_data.ccsr_codelist"

DB_READ = "cdh_premier"
DB_WRITE = "cdh_premier_exploratory"

#####RENAME FILE NAMES FOR OWNERSHIP#####

#COVID_INDICATOR_NAME = f"twj8_hvid_covid_indicator{tag}"
#COVID_INDICATOR_TABLE = COVID_INDICATOR_NAME
LONG_OUTPUT_NAME = f"twj8_medrec_covid_ccsr"
WIDE_OUTPUT_NAME = f"twj8_medrec_ccsr_wide_cohort"
# WIDE_OUTPUT_NAME_P2 = f"sve5_hvid_ccsr_wide_cohort_p2{tag}"
MATCHES_TABLE = f'twj8_case_control_matches'
MATCHES_UID_TABLE = f'twj8_uid_case_control'
INPATIENT_STATUS_TABLE = f"twj8_medrec_inpatient_status_p2{tag}"
CCI_TABLE_NAME = f"twj8_medrec_cci{tag}"
PMCA_TABLE_NAME = f"twj8_medrec_pmca{tag}"
HOSP_TABLE_NAME = f"twj8_medrec_hosp{tag}"
WIDE_OUTPUT_NAME_FINAL = f"twj8_medrec_ccsr_wide_cohort_final{tag}"

cox_pre = "twj8_coxph_"

# COMMAND ----------

displayHTML(f"""
Imported the following GLOBAL_NAMES: <br>

<b>DB_READ</b> = {DB_READ} <br>
<b>DB_WRITE</b> = {DB_WRITE} <br>

<b>LONG_OUTPUT_NAME</b> = {LONG_OUTPUT_NAME} <br>
<b>WIDE_OUTPUT_NAME</b> = {WIDE_OUTPUT_NAME} <br>
<b>MATCHES_TABLE</b> = {MATCHES_TABLE} <br>
<b>MATCHES_UID_TABLE</b> = {MATCHES_UID_TABLE} <br>
<b>CCI_TABLE_NAME</b> = {CCI_TABLE_NAME} <br>
<b>PMCA_TABLE_NAME</b> = {PMCA_TABLE_NAME} <br>
<b>HOSP_TABLE_NAME</b> = {HOSP_TABLE_NAME} <br>
<b>WIDE_OUTPUT_NAME_FINAL</b> = {WIDE_OUTPUT_NAME_FINAL} <br>
"""
           )

# COMMAND ----------

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

# COMMAND ----------

EXCLUSION_LAB = [
    "SARS coronavirus 2 RNA:PrThr:Pt:Respiratory:Ord:Probe.amp.tar",
    "SARS coronavirus 2 RNA:PrThr:Pt:XXX:Ord:Probe.amp.tar"
]
