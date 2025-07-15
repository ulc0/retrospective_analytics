# Databricks notebook source
dbutils.widgets.text("source_catalog",defaultValue="HIVE_METASTORE")
dbutils.widgets.text("source_schema",defaultValue="cdh_premier_v2")
#mandatory parameters and names. The orchestrator will always pass these
dbutils.widgets.text("etl_catalog",defaultValue="HIVE_METASTORE")
dbutils.widgets.text("etl_schema",defaultValue="cdh_premier_etl_v2")
dbutils.widgets.text("omop_catalog",defaultValue="HIVE_METASTORE")
dbutils.widgets.text("omop_schema",defaultValue="${omop_catalog}.${omop_schema}_v2")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC with matches as
# MAGIC (
# MAGIC   select 
# MAGIC     x_srcid as id
# MAGIC     from ${omop_catalog}.${omop_schema}.observation_temp
# MAGIC     where x_srcfile = 'STAGE_CONDITION'
# MAGIC     and x_srcloadid = 1
# MAGIC   union
# MAGIC     select 
# MAGIC     x_srcid as id
# MAGIC     from ${omop_catalog}.${omop_schema}.measurement_temp
# MAGIC     where x_srcfile = 'STAGE_CONDITION'
# MAGIC     and x_srcloadid = 1
# MAGIC /* not needed with premier
# MAGIC   union
# MAGIC     select 
# MAGIC     x_srcid as id
# MAGIC     from ${omop_catalog}.${omop_schema}.procedure_occurrence_temp
# MAGIC     where x_srcfile = 'STAGE_CONDITION'
# MAGIC     and x_srcloadid = 1
# MAGIC   union
# MAGIC     select 
# MAGIC     x_srcid as id
# MAGIC     from ${omop_catalog}.${omop_schema}.drug_exposure_temp
# MAGIC     where x_srcfile = 'STAGE_CONDITION'
# MAGIC     and x_srcloadid = 1
# MAGIC   union
# MAGIC     select 
# MAGIC     x_srcid as id
# MAGIC     from ${omop_catalog}.${omop_schema}.device_exposure_temp
# MAGIC     where x_srcfile = 'STAGE_CONDITION'
# MAGIC     and x_srcloadid = 1
# MAGIC */
# MAGIC )
# MAGIC update  ${etl_catalog}.${etl_schema}.stage_condition_temp sc
# MAGIC set loaded = 1
# MAGIC where exists ( select id from matches where sc.id = matches.id and sc.load_id = 1);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- copy records that did not make it over to error
# MAGIC insert into ${etl_catalog}.${etl_schema}.stage_condition_error
# MAGIC (
# MAGIC   id,
# MAGIC   condition_code_source_type,
# MAGIC   condition_source_value,
# MAGIC   condition_source_type_value,
# MAGIC   start_date,
# MAGIC   end_date,
# MAGIC   stop_reason,
# MAGIC   visit_source_value,
# MAGIC   person_source_value,
# MAGIC   provider_source_value,
# MAGIC   load_id,
# MAGIC   loaded
# MAGIC )
# MAGIC select s.id,
# MAGIC   s.condition_code_source_type,
# MAGIC   s.condition_source_value,
# MAGIC   s.condition_source_type_value,
# MAGIC   s.start_date,
# MAGIC   s.end_date,
# MAGIC   s.stop_reason,
# MAGIC   s.visit_source_value,
# MAGIC   s.person_source_value,
# MAGIC   s.provider_source_value,
# MAGIC   s.load_id,
# MAGIC   s.loaded
# MAGIC from ${etl_catalog}.${etl_schema}.stage_condition_temp s
# MAGIC join ${omop_catalog}.${omop_schema}.condition_occurrence_temp o on 
# MAGIC                     s.id = o.x_srcid
# MAGIC                     and s.load_id = o.x_srcloadid
# MAGIC where s.loaded <> 1  -- not matched in other tables
# MAGIC and s.load_id = 1
# MAGIC and o.condition_source_concept_id = 0;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- delete records which matched under a non-condition table (loaded = 1) that did not match to a concept code in conditon (source concept = 0)
# MAGIC merge into ${omop_catalog}.${omop_schema}.condition_occurrence_temp co
# MAGIC using ${etl_catalog}.${etl_schema}.stage_condition_temp sl
# MAGIC on ( co.condition_source_concept_id = 0 ---- this should be condition_concept_id
# MAGIC         and co.x_srcfile = 'STAGE_CONDITION'
# MAGIC         and co.x_srcid = sl.id
# MAGIC         and sl.loaded = 1 )
# MAGIC when matched then delete;

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into ${etl_catalog}.${etl_schema}.stage_condition
# MAGIC (
# MAGIC   id,
# MAGIC   condition_code_source_type,
# MAGIC   condition_source_value,
# MAGIC   condition_source_type_value,
# MAGIC   start_date,
# MAGIC   end_date,
# MAGIC   stop_reason,
# MAGIC   visit_source_value,
# MAGIC   person_source_value,
# MAGIC   provider_source_value,
# MAGIC   load_id,
# MAGIC   loaded
# MAGIC )
# MAGIC select id,
# MAGIC   condition_code_source_type,
# MAGIC   condition_source_value,
# MAGIC   condition_source_type_value,
# MAGIC   start_date,
# MAGIC   end_date,
# MAGIC   stop_reason,
# MAGIC   visit_source_value,
# MAGIC   person_source_value,
# MAGIC   provider_source_value,
# MAGIC   load_id,
# MAGIC   loaded
# MAGIC from ${etl_catalog}.${etl_schema}.stage_condition_temp
# MAGIC where load_id = 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC /*
# MAGIC delete from ${etl_catalog}.${etl_schema}.stage_condition_temp
# MAGIC where load_id = 1;
# MAGIC */
