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
# MAGIC with matches as
# MAGIC (
# MAGIC   select 
# MAGIC     x_srcid as id
# MAGIC     from ${omop_catalog}.${omop_schema}.observation_temp
# MAGIC     where x_srcfile = 'STAGE_LAB'
# MAGIC     and x_srcloadid = 1
# MAGIC   union
# MAGIC     select 
# MAGIC     x_srcid as id
# MAGIC     from ${omop_catalog}.${omop_schema}.procedure_occurrence_temp
# MAGIC     where x_srcfile = 'STAGE_LAB'
# MAGIC     and x_srcloadid = 1
# MAGIC   union
# MAGIC     select 
# MAGIC     x_srcid as id
# MAGIC     from ${omop_catalog}.${omop_schema}.condition_occurrence_temp
# MAGIC     where x_srcfile = 'STAGE_LAB'
# MAGIC     and x_srcloadid = 1
# MAGIC   union
# MAGIC     select 
# MAGIC     x_srcid as id
# MAGIC     from ${omop_catalog}.${omop_schema}.drug_exposure_temp
# MAGIC     where x_srcfile = 'STAGE_LAB'
# MAGIC     and x_srcloadid = 1
# MAGIC   union
# MAGIC     select 
# MAGIC     x_srcid as id
# MAGIC     from ${omop_catalog}.${omop_schema}.device_exposure_temp
# MAGIC     where x_srcfile = 'STAGE_LAB'
# MAGIC     and x_srcloadid = 1
# MAGIC )
# MAGIC update ${etl_catalog}.${etl_schema}.stage_lab_temp sc
# MAGIC set loaded = 1
# MAGIC where exists ( select id from matches where sc.id = matches.id and sc.load_id = 1);

# COMMAND ----------

# MAGIC %sql
# MAGIC   -- copy records that did not make it over to error
# MAGIC insert into ${etl_catalog}.${etl_schema}.stage_lab_error
# MAGIC (
# MAGIC     id,
# MAGIC     measurement_source_type,
# MAGIC     measurement_source_value,
# MAGIC     measurement_source_type_value,
# MAGIC     measurement_date,
# MAGIC     operator_source_value,
# MAGIC     unit_source_value,
# MAGIC     value_source_value,
# MAGIC     value_as_number,
# MAGIC     value_as_string,
# MAGIC     range_low,
# MAGIC     range_high,
# MAGIC     visit_source_value,
# MAGIC     person_source_value,
# MAGIC     provider_source_value,
# MAGIC     load_id,
# MAGIC     loaded
# MAGIC )
# MAGIC select 
# MAGIC   s.id,
# MAGIC   s.measurement_source_type,
# MAGIC   s.measurement_source_value,
# MAGIC   s.measurement_source_type_value,
# MAGIC   s.measurement_date,
# MAGIC   s.operator_source_value,
# MAGIC   s.unit_source_value,
# MAGIC   s.value_source_value,
# MAGIC   s.value_as_number,
# MAGIC   s.value_as_string,
# MAGIC   s.range_low,
# MAGIC   s.range_high,
# MAGIC   s.visit_source_value,
# MAGIC   s.person_source_value,
# MAGIC   s.provider_source_value,
# MAGIC   s.load_id,
# MAGIC   s.loaded
# MAGIC from ${etl_catalog}.${etl_schema}.stage_lab_temp s
# MAGIC join ${omop_catalog}.${omop_schema}.measurement_temp o on 
# MAGIC         s.id = o.x_srcid
# MAGIC         and s.load_id = o.x_srcloadid
# MAGIC where s.loaded <> 1 -- not matched in other tables
# MAGIC and s.load_id = 1
# MAGIC and o.measurement_source_concept_id is null; 

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into ${omop_catalog}.${omop_schema}.measurement_temp co
# MAGIC using ${etl_catalog}.${etl_schema}.stage_lab_temp sl
# MAGIC on ( co.measurement_source_concept_id is null
# MAGIC         and co.x_srcfile = 'STAGE_LAB'
# MAGIC         and co.x_srcid = sl.id
# MAGIC         and sl.loaded = 1 )
# MAGIC when matched then delete;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into ${etl_catalog}.${etl_schema}.stage_lab
# MAGIC (
# MAGIC     id,
# MAGIC     measurement_source_type,
# MAGIC     measurement_source_value,
# MAGIC     measurement_source_type_value,
# MAGIC     measurement_date,
# MAGIC     operator_source_value,
# MAGIC     unit_source_value,
# MAGIC     value_source_value,
# MAGIC     value_as_number,
# MAGIC     value_as_string,
# MAGIC     range_low,
# MAGIC     range_high,
# MAGIC     visit_source_value,
# MAGIC     person_source_value,
# MAGIC     provider_source_value,
# MAGIC     load_id,
# MAGIC     loaded
# MAGIC )
# MAGIC select 
# MAGIC   s.id,
# MAGIC   s.measurement_source_type,
# MAGIC   s.measurement_source_value,
# MAGIC   s.measurement_source_type_value,
# MAGIC   s.measurement_date,
# MAGIC   s.operator_source_value,
# MAGIC   s.unit_source_value,
# MAGIC   s.value_source_value,
# MAGIC   s.value_as_number,
# MAGIC   s.value_as_string,
# MAGIC   s.range_low,
# MAGIC   s.range_high,
# MAGIC   s.visit_source_value,
# MAGIC   s.person_source_value,
# MAGIC   s.provider_source_value,
# MAGIC   s.load_id,
# MAGIC   s.loaded
# MAGIC from ${etl_catalog}.${etl_schema}.stage_lab_temp s

# COMMAND ----------


