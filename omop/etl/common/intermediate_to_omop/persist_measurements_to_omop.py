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
# MAGIC insert overwrite ${omop_catalog}.${omop_schema}.measurement
# MAGIC (
# MAGIC   measurement_id,
# MAGIC   person_id,
# MAGIC   measurement_concept_id,
# MAGIC   measurement_date,
# MAGIC   measurement_datetime,
# MAGIC   measurement_time,
# MAGIC   measurement_type_concept_id,
# MAGIC   operator_concept_id,
# MAGIC   value_as_number,
# MAGIC   value_as_concept_id,
# MAGIC   unit_concept_id,
# MAGIC   range_low,
# MAGIC   range_high,
# MAGIC   provider_id,
# MAGIC   visit_occurrence_id,
# MAGIC   visit_detail_id,
# MAGIC   measurement_source_value,
# MAGIC   measurement_source_concept_id,
# MAGIC   unit_source_value,
# MAGIC   value_source_value,
# MAGIC   x_srcid,
# MAGIC   x_srcloadid,
# MAGIC   x_srcfile,
# MAGIC   x_createdate,
# MAGIC   x_updatedate
# MAGIC )
# MAGIC select 
# MAGIC   measurement_id,
# MAGIC   person_id,
# MAGIC   measurement_concept_id,
# MAGIC   measurement_date,
# MAGIC   measurement_datetime,
# MAGIC   measurement_time,
# MAGIC   measurement_type_concept_id,
# MAGIC   operator_concept_id,
# MAGIC   value_as_number,
# MAGIC   value_as_concept_id,
# MAGIC   unit_concept_id,
# MAGIC   range_low,
# MAGIC   range_high,
# MAGIC   provider_id,
# MAGIC   visit_occurrence_id,
# MAGIC   visit_detail_id,
# MAGIC   measurement_source_value,
# MAGIC   measurement_source_concept_id,
# MAGIC   unit_source_value,
# MAGIC   value_source_value,
# MAGIC   x_srcid,
# MAGIC   x_srcloadid,
# MAGIC   x_srcfile,
# MAGIC   x_createdate,
# MAGIC   x_updatedate
# MAGIC from ${omop_catalog}.${omop_schema}.measurement_temp 
# MAGIC where x_srcloadid = 1;
