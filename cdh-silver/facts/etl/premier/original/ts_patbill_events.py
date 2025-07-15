# Databricks notebook source
dbutils.widgets.text("DBCATALOG","edav_prd_cdh")
dbutils.widgets.text("SRC_SCHEMA","cdh_premier_v2")
dbutils.widgets.text("DEST_SCHEMA","cdh_premier_ra")
dbutils.widgets.text("table_name","ts_patbill_events")

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table ${DBCATALOG}.${DEST_SCHEMA}.${table_name} as
# MAGIC SELECT
# MAGIC h.pat_key as visit_occurrence_number,
# MAGIC h.serv_date as visit_start_date,
# MAGIC case 
# MAGIC  when (std_dept_code='250') then 'STDRX'
# MAGIC  when (std_dept_code='300') then 'STDLAB'
# MAGIC  else "STDCHGCODE"
# MAGIC end as stdchg_vocabulary_id,
# MAGIC case 
# MAGIC  when (std_dept_code='250') then 'HCHGRX'
# MAGIC  when (std_dept_code='300') then 'HCHGLAB'
# MAGIC  else "HCHGCODE"
# MAGIC end as hospchg_vocabulary_id,
# MAGIC c.std_chg_code,
# MAGIC hosp_chg_id 
# MAGIC FROM ${DBCATALOG}.${SRC_SCHEMA}.patbill h
# MAGIC join ${DBCATALOG}.${SRC_SCHEMA}.chgmstr c
# MAGIC on h.std_chg_code=c.std_chg_code
