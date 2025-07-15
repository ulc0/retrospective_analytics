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
# MAGIC insert overwrite ${omop_catalog}.${omop_schema}.condition_occurrence
# MAGIC (
# MAGIC   condition_occurrence_id,
# MAGIC   person_id,
# MAGIC   condition_concept_id,
# MAGIC   condition_start_date,
# MAGIC   condition_start_datetime,
# MAGIC   condition_end_date,
# MAGIC   condition_end_datetime,
# MAGIC   condition_type_concept_id,
# MAGIC   condition_status_concept_id,
# MAGIC   stop_reason,
# MAGIC   provider_id,
# MAGIC   visit_occurrence_id,
# MAGIC   visit_detail_id,
# MAGIC   condition_source_value,
# MAGIC   condition_source_concept_id,
# MAGIC   condition_status_source_value,
# MAGIC   x_srcid,
# MAGIC   x_srcloadid,
# MAGIC   x_srcfile,
# MAGIC   x_createdate,
# MAGIC   x_updatedate
# MAGIC )
# MAGIC   select 
# MAGIC   condition_occurrence_id,
# MAGIC   person_id,
# MAGIC   condition_concept_id,
# MAGIC   condition_start_date,
# MAGIC   condition_start_datetime,
# MAGIC   condition_end_date,
# MAGIC   condition_end_datetime,
# MAGIC   condition_type_concept_id,
# MAGIC   condition_status_concept_id,
# MAGIC   stop_reason,
# MAGIC   provider_id,
# MAGIC   visit_occurrence_id,
# MAGIC   visit_detail_id,
# MAGIC   condition_source_value,
# MAGIC   condition_source_concept_id,
# MAGIC   condition_status_source_value,
# MAGIC   x_srcid,
# MAGIC   x_srcloadid,
# MAGIC   x_srcfile,
# MAGIC   x_createdate,
# MAGIC   x_updatedate
# MAGIC from ${omop_catalog}.${omop_schema}.condition_occurrence_temp 
# MAGIC where x_srcloadid = 1;
