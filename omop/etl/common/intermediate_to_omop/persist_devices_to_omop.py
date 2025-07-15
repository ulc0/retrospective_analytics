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
# MAGIC insert overwrite ${omop_catalog}.${omop_schema}.device_exposure
# MAGIC (
# MAGIC   device_exposure_id,
# MAGIC   person_id,
# MAGIC   device_concept_id,
# MAGIC   device_exposure_start_date,
# MAGIC   device_exposure_start_datetime,
# MAGIC   device_exposure_end_date,
# MAGIC   device_exposure_end_datetime,
# MAGIC   device_type_concept_id,
# MAGIC   unique_device_id,
# MAGIC   quantity,
# MAGIC   provider_id,
# MAGIC   visit_occurrence_id,
# MAGIC   visit_detail_id,
# MAGIC   device_source_value,
# MAGIC   device_source_concept_id,
# MAGIC   x_srcid,
# MAGIC   x_srcloadid,
# MAGIC   x_srcfile,
# MAGIC   x_createdate,
# MAGIC   x_updatedate
# MAGIC )
# MAGIC select 
# MAGIC   device_exposure_id,
# MAGIC   person_id,
# MAGIC   device_concept_id,
# MAGIC   device_exposure_start_date,
# MAGIC   device_exposure_start_datetime,
# MAGIC   device_exposure_end_date,
# MAGIC   device_exposure_end_datetime,
# MAGIC   device_type_concept_id,
# MAGIC   unique_device_id,
# MAGIC   quantity,
# MAGIC   provider_id,
# MAGIC   visit_occurrence_id,
# MAGIC   visit_detail_id,
# MAGIC   device_source_value,
# MAGIC   device_source_concept_id,
# MAGIC   x_srcid,
# MAGIC   x_srcloadid,
# MAGIC   x_srcfile,
# MAGIC   x_createdate,
# MAGIC   x_updatedate
# MAGIC from ${omop_catalog}.${omop_schema}.device_exposure_temp
# MAGIC where x_srcloadid = 1;
