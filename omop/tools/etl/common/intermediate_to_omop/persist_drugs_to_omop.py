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
# MAGIC insert overwrite ${omop_catalog}.${omop_schema}.drug_exposure
# MAGIC (
# MAGIC   drug_exposure_id,
# MAGIC   person_id,
# MAGIC   drug_concept_id,
# MAGIC   drug_exposure_start_date,
# MAGIC   drug_exposure_start_datetime,
# MAGIC   drug_exposure_end_date,
# MAGIC   drug_exposure_end_datetime,
# MAGIC   verbatim_end_date,
# MAGIC   drug_type_concept_id,
# MAGIC   stop_reason,
# MAGIC   refills,
# MAGIC   quantity,
# MAGIC   days_supply,
# MAGIC   sig,
# MAGIC   route_concept_id,
# MAGIC   lot_number,
# MAGIC   provider_id,
# MAGIC   visit_occurrence_id,
# MAGIC   visit_detail_id,
# MAGIC   drug_source_value,
# MAGIC   drug_source_concept_id,
# MAGIC   route_source_value,
# MAGIC   dose_unit_source_value,
# MAGIC   x_srcid,
# MAGIC   x_srcloadid,
# MAGIC   x_srcfile,
# MAGIC   x_createdate,
# MAGIC   x_updatedate
# MAGIC )
# MAGIC select 
# MAGIC   drug_exposure_id,
# MAGIC   person_id,
# MAGIC   drug_concept_id,
# MAGIC   drug_exposure_start_date,
# MAGIC   drug_exposure_start_datetime,
# MAGIC   drug_exposure_end_date,
# MAGIC   drug_exposure_end_datetime,
# MAGIC   verbatim_end_date,
# MAGIC   drug_type_concept_id,
# MAGIC   stop_reason,
# MAGIC   refills,
# MAGIC   quantity,
# MAGIC   days_supply,
# MAGIC   sig,
# MAGIC   route_concept_id,
# MAGIC   lot_number,
# MAGIC   provider_id,
# MAGIC   visit_occurrence_id,
# MAGIC   visit_detail_id,
# MAGIC   drug_source_value,
# MAGIC   drug_source_concept_id,
# MAGIC   route_source_value,
# MAGIC   dose_unit_source_value,
# MAGIC   x_srcid,
# MAGIC   x_srcloadid,
# MAGIC   x_srcfile,
# MAGIC   x_createdate,
# MAGIC   x_updatedate
# MAGIC from ${omop_catalog}.${omop_schema}.drug_exposure_temp 
# MAGIC where x_srcloadid = 1;
