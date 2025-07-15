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
# MAGIC -- load procedures from icd
# MAGIC
# MAGIC
# MAGIC INSERT INTO ${etl_catalog}.${etl_schema}.stage_procedure_temp
# MAGIC (
# MAGIC     procedure_code_source_type,
# MAGIC     procedure_source_value,
# MAGIC     procedure_source_type_value,
# MAGIC     procedure_date,
# MAGIC     visit_source_value,
# MAGIC     person_source_value,
# MAGIC     load_id,
# MAGIC     loaded
# MAGIC )
# MAGIC SELECT
# MAGIC     'ICD10PCS' AS procedure_code_source_type,
# MAGIC     d.icd_code AS procedure_source_value,
# MAGIC     case d.icd_pri_sec 
# MAGIC       when 'P' then 44786630
# MAGIC       when 'S' then 44786631
# MAGIC     end AS procedure_source_type_value,
# MAGIC     d.proc_date AS procedure_date,
# MAGIC     d.pat_key AS visit_source_value,
# MAGIC     p.medrec_key AS person_source_value,
# MAGIC     1 AS load_id,
# MAGIC     0 AS loaded
# MAGIC FROM ${source_catalog}.${source_schema}.paticd_proc d
# MAGIC join ${source_catalog}.${source_schema}.patdemo p on d.pat_key = p.pat_key
# MAGIC ;
# MAGIC
