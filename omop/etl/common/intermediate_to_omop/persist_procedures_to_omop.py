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
# MAGIC insert overwrite ${omop_catalog}.${omop_schema}.procedure_occurrence
# MAGIC (
# MAGIC   procedure_occurrence_id,
# MAGIC   person_id,
# MAGIC   procedure_concept_id,
# MAGIC   procedure_date,
# MAGIC   procedure_datetime,
# MAGIC   procedure_type_concept_id,
# MAGIC   modifier_concept_id,
# MAGIC   quantity,
# MAGIC   provider_id,
# MAGIC   visit_occurrence_id,
# MAGIC   visit_detail_id,
# MAGIC   procedure_source_value,
# MAGIC   procedure_source_concept_id,
# MAGIC   modifier_source_value,
# MAGIC   x_srcid,
# MAGIC   x_srcloadid,
# MAGIC   x_srcfile,
# MAGIC   x_createdate,
# MAGIC   x_updatedate
# MAGIC )
# MAGIC select 
# MAGIC   procedure_occurrence_id,
# MAGIC   person_id,
# MAGIC   procedure_concept_id,
# MAGIC   procedure_date,
# MAGIC   procedure_datetime,
# MAGIC   procedure_type_concept_id,
# MAGIC   modifier_concept_id,
# MAGIC   quantity,
# MAGIC   provider_id,
# MAGIC   visit_occurrence_id,
# MAGIC   visit_detail_id,
# MAGIC   procedure_source_value,
# MAGIC   procedure_source_concept_id,
# MAGIC   modifier_source_value,
# MAGIC   x_srcid,
# MAGIC   x_srcloadid,
# MAGIC   x_srcfile,
# MAGIC   x_createdate,
# MAGIC   x_updatedate
# MAGIC from ${omop_catalog}.${omop_schema}.procedure_occurrence_temp
# MAGIC where x_srcloadid = 1;
