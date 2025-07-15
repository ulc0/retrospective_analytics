# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ### Fact Tables  
# MAGIC #### Basic Fact Tables  
# MAGIC patbill  
# MAGIC patcpt  
# MAGIC patdemo  
# MAGIC paticd_diag  
# MAGIC paticd_proc  
# MAGIC
# MAGIC #### Complex Fact Tables  
# MAGIC genlab  
# MAGIC lab_res  
# MAGIC lab_sens  
# MAGIC vitals  
# MAGIC
# MAGIC ### Dimension Tables
# MAGIC admtype  
# MAGIC chgmstr  
# MAGIC cptcode  
# MAGIC de_admin  
# MAGIC disstat  
# MAGIC hospchg  
# MAGIC icdcode  
# MAGIC icdpoa  
# MAGIC msdrg  
# MAGIC msdrgmdc  
# MAGIC pattype  
# MAGIC payor    
# MAGIC physpec  
# MAGIC poorigin  
# MAGIC prov_digit_zip  
# MAGIC prov_enrollment
# MAGIC providers  
# MAGIC

# COMMAND ----------

 
#from shared.etl_utils import *


# COMMAND ----------


core_fact=['patbill',
'patcpt',
'patdemo',
'paticd_diag',
'paticd_proc',]
complex_fact=['genlab',
'lab_res',
'lab_sens',
'vitals', 
#'readmit',
]
dim=[
    #'admsrc',
'admtype',
'chgmstr',
'cptcode',
'de_admin',
'disstat',
'hospchg',
'icdcode',
'icdpoa',
'msdrg',
'msdrgmdc',
#'pat_noapr',
'pattype',
'payor',
#'phd_providers_zip3',
'physpec',
'poorigin',
'prov_digit_zip',
'prov_enrollment', 'providers']



# COMMAND ----------

def table_tb(tablename,schema="cdh_premier_v2"):
    byte_size = spark.sql(f"describe detail {schema}.{tablename}").select("sizeInBytes").collect()
    byte_size = (byte_size[0]["sizeInBytes"])
    kb_size = byte_size/1024
    mb_size = kb_size/1024 
    gb_size = mb_size/1024
    tb_size = gb_size/1024
    return gb_size

# COMMAND ----------



core_size=0
cmp_size=0
dim_size=0
for tbl in complex_fact:
    cmp_size+=table_tb(tbl)
    print(f"{tbl} {cmp_size}")
for tbl in core_fact:
    core_size+=table_tb(tbl)
    print(f"{tbl} {core_size}")
for tbl in dim:
    dim_size+=table_tb(tbl)
    print(f"{tbl} {dim_size}")
total_size=core_size+cmp_size+dim_size



# COMMAND ----------

print(f"Core Size: {core_size} GB, {core_size/total_size*100}%")
print(f"Complex Size: {cmp_size} GB, {cmp_size/total_size*100}%")
print(f"Total Size: {cmp_size+core_size} GB, {(cmp_size+core_size)/total_size*100}%")
print(f"Dimensions Size: {dim_size} GB, {dim_size/total_size*100}%")


# COMMAND ----------


patient_size=table_tb("fact_person","cdh_premier_ra")
encounter_size=table_tb("fact_encounter_index","cdh_premier_ra")
fact_size=patient_size+encounter_size
print(f"Fact Table: {fact_size} TB")
print(f"Reduction: {100-(fact_size/(core_size+cmp_size)*100)}%")
