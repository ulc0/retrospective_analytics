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
# MAGIC -- load visit
# MAGIC INSERT INTO ${etl_catalog}.${etl_schema}.stage_visit
# MAGIC   (
# MAGIC     visit_source_value,
# MAGIC     visit_type,
# MAGIC     visit_start_date,
# MAGIC     visit_end_date,
# MAGIC     person_source_value,
# MAGIC     load_id,
# MAGIC     loaded
# MAGIC   )
# MAGIC select 
# MAGIC     a2.pat_key as visit_source_value,
# MAGIC     case 
# MAGIC       when a2.pat_type = 28
# MAGIC         then 'ER'
# MAGIC       when a2.i_o_ind = 'I'
# MAGIC         then 'IN'
# MAGIC       when a2.i_o_ind = 'O'
# MAGIC         then 'OUT'
# MAGIC       else null 
# MAGIC     end as visit_type,
# MAGIC     admit_date as visit_start_date,
# MAGIC     discharge_date as visit_end_date,
# MAGIC     a2.medrec_key as person_source_value, 
# MAGIC     1 as load_id,
# MAGIC     0 as loaded
# MAGIC from ${source_catalog}.${source_schema}.patdemo a2
# MAGIC ;
