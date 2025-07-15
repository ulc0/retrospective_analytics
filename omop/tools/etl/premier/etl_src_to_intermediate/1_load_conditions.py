# Databricks notebook source
dbutils.widgets.text("source_catalog",defaultValue="HIVE_METASTORE")
dbutils.widgets.text("source_schema",defaultValue="cdh_premier_v2")
#mandatory parameters and names. The orchestrator will always pass these
dbutils.widgets.text("etl_catalog",defaultValue="HIVE_METASTORE")
dbutils.widgets.text("etl_schema",defaultValue="cdh_premier_etl_v2")
dbutils.widgets.text("omop_catalog",defaultValue="HIVE_METASTORE")
dbutils.widgets.text("omop_schema",defaultValue="cdh_premier_omop_v2")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO ${etl_catalog}.${etl_schema}.stage_condition_temp
# MAGIC (
# MAGIC     condition_code_source_type,
# MAGIC     condition_source_value,
# MAGIC     condition_source_type_value,
# MAGIC     start_date,
# MAGIC     visit_source_value,
# MAGIC     person_source_value,
# MAGIC     load_id,
# MAGIC     loaded
# MAGIC )
# MAGIC SELECT
# MAGIC     case d.icd_version 
# MAGIC       when '10' then 'ICD10CM'
# MAGIC       else 'ICD9CM'
# MAGIC     end AS condition_code_source_type,
# MAGIC     replace(d.icd_code, '.','') AS condition_source_value,
# MAGIC     case d.icd_pri_sec 
# MAGIC       when 'A' then 42894222  
# MAGIC       when 'P' then 44786627
# MAGIC       when 'S' then 44786629
# MAGIC     end AS condition_source_type_value,
# MAGIC     p.admit_date AS start_date,
# MAGIC     d.pat_key AS visit_source_value,
# MAGIC     p.medrec_key AS person_source_value,
# MAGIC     1 AS load_id,
# MAGIC     0 AS loaded
# MAGIC FROM ${source_catalog}.${source_schema}.paticd_diag d
# MAGIC join ${source_catalog}.${source_schema}.patdemo p on d.pat_key = p.pat_key
# MAGIC ;
