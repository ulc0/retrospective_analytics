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
# MAGIC insert into ${omop_catalog}.${omop_schema}.condition_occurrence_temp
# MAGIC (
# MAGIC         person_id,
# MAGIC         condition_concept_id,
# MAGIC         condition_start_date,
# MAGIC         condition_start_datetime,
# MAGIC         condition_end_date,
# MAGIC         condition_type_concept_id,
# MAGIC --       condition_status_concept_id,
# MAGIC         stop_reason,
# MAGIC --      provider_id,
# MAGIC         visit_occurrence_id,
# MAGIC --        visit_detail_id,
# MAGIC         condition_source_value,
# MAGIC         condition_source_concept_id,
# MAGIC         x_srcid,
# MAGIC         x_srcloadid,
# MAGIC         x_srcfile
# MAGIC )
# MAGIC select
# MAGIC     person_id
# MAGIC     , condition_concept_id
# MAGIC     , condition_start_date
# MAGIC     , condition_start_datetime
# MAGIC     , condition_end_date
# MAGIC     , condition_type_concept_id
# MAGIC --	, condition_status_concept_id
# MAGIC     , stop_reason
# MAGIC     --   , provider_id
# MAGIC     , visit_occurrence_id
# MAGIC --     , visit_detail_id
# MAGIC     , condition_source_value
# MAGIC     , condition_source_concept_id
# MAGIC     , x_srcid
# MAGIC     , x_srcloadid
# MAGIC     , x_srcfile
# MAGIC from
# MAGIC (
# MAGIC     select 
# MAGIC         p.person_id as person_id
# MAGIC         , coalesce(src.tar_concept_id, 0 ) as condition_concept_id
# MAGIC         , s.start_date as condition_start_date
# MAGIC         , s.start_date as condition_start_datetime
# MAGIC         , s.end_date as condition_end_date
# MAGIC         , cast(s.condition_source_type_value as int) as condition_type_concept_id
# MAGIC --          , condition_status_concept_id
# MAGIC         , null as stop_reason
# MAGIC     --     , coalesce( pr.provider_id, v.provider_id ) as provider_id
# MAGIC         , v.visit_occurrence_id as visit_occurrence_id
# MAGIC     --      , coalesce( vd.visit_detail_id ) as visit_detail_id
# MAGIC         , s.condition_source_value as condition_source_value
# MAGIC         , coalesce(src.src_concept_id, 0 ) as condition_source_concept_id
# MAGIC         , s.id as x_srcid
# MAGIC         , s.load_id as x_srcloadid
# MAGIC         , 'STAGE_CONDITION' as x_srcfile
# MAGIC     from ${etl_catalog}.${etl_schema}.stage_condition_temp s
# MAGIC     join ${omop_catalog}.${omop_schema}.person p on p.person_source_value = s.person_source_value
# MAGIC     left join ${omop_catalog}.${omop_schema}.visit_occurrence v on s.visit_source_value = v.visit_source_value and p.person_id = v.person_id
# MAGIC     --   left join ${omop_catalog}.${omop_schema}.visit_detail vd on s.visit_detail_source_value = vd.visit_detail_source_value and p.person_id = vd.person_id
# MAGIC     left join ${omop_catalog}.${omop_schema}.concept_condition src on s.condition_source_value = src.clean_concept_code
# MAGIC         and s.condition_code_source_type = src.src_vocabulary_id
# MAGIC     -- left join ${omop_catalog}.${omop_schema}.provider_source_value pr on s.provider_source_value = pr.provider_source_value
# MAGIC where s.start_date is not null
# MAGIC and s.load_id = 1
# MAGIC ) a
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- labs -> conditions
# MAGIC
# MAGIC insert into ${omop_catalog}.${omop_schema}.condition_occurrence_temp
# MAGIC (
# MAGIC         person_id,
# MAGIC         condition_concept_id,
# MAGIC         condition_start_date,
# MAGIC         condition_start_datetime,
# MAGIC --       condition_end_date,
# MAGIC         condition_type_concept_id,
# MAGIC --       condition_status_concept_id,
# MAGIC         stop_reason,
# MAGIC --      provider_id,
# MAGIC         visit_occurrence_id,
# MAGIC --        visit_detail_id,
# MAGIC         condition_source_value,
# MAGIC         condition_source_concept_id,
# MAGIC         x_srcid,
# MAGIC         x_srcloadid,
# MAGIC         x_srcfile
# MAGIC )
# MAGIC select
# MAGIC     person_id
# MAGIC     , condition_concept_id
# MAGIC     , condition_start_date
# MAGIC     , condition_start_datetime
# MAGIC   --  , condition_end_date
# MAGIC     , condition_type_concept_id
# MAGIC --	, condition_status_concept_id
# MAGIC     , stop_reason
# MAGIC     --   , provider_id
# MAGIC     , visit_occurrence_id
# MAGIC --     , visit_detail_id
# MAGIC     , condition_source_value
# MAGIC     , condition_source_concept_id
# MAGIC     , x_srcid
# MAGIC     , x_srcloadid
# MAGIC     , x_srcfile
# MAGIC from
# MAGIC (
# MAGIC     select 
# MAGIC         p.person_id as person_id
# MAGIC         , coalesce(src.tar_concept_id, 0 ) as condition_concept_id
# MAGIC         , s.measurement_date as condition_start_date
# MAGIC         , cast(s.measurement_date as timestamp) as condition_start_datetime
# MAGIC         -- , s.end_date as condition_end_date
# MAGIC         , cast(s.measurement_source_type_value as int) as condition_type_concept_id
# MAGIC --          , condition_status_concept_id
# MAGIC         , null as stop_reason
# MAGIC     --     , coalesce( pr.provider_id, v.provider_id ) as provider_id
# MAGIC         , v.visit_occurrence_id as visit_occurrence_id
# MAGIC     --      , coalesce( vd.visit_detail_id ) as visit_detail_id
# MAGIC         , s.measurement_source_value as condition_source_value
# MAGIC         , coalesce(src.src_concept_id, 0 ) as condition_source_concept_id
# MAGIC         , s.id as x_srcid
# MAGIC         , s.load_id as x_srcloadid
# MAGIC         , 'STAGE_LAB' as x_srcfile
# MAGIC     from ${etl_catalog}.${etl_schema}.stage_lab_temp s
# MAGIC     join ${omop_catalog}.${omop_schema}.person p on p.person_source_value = s.person_source_value
# MAGIC     left join ${omop_catalog}.${omop_schema}.visit_occurrence v on s.visit_source_value = v.visit_source_value and p.person_id = v.person_id
# MAGIC     --   left join ${omop_catalog}.${omop_schema}.visit_detail vd on s.visit_detail_source_value = vd.visit_detail_source_value and p.person_id = vd.person_id
# MAGIC     join ${omop_catalog}.${omop_schema}.concept_condition src on s.measurement_source_value = src.clean_concept_code
# MAGIC         and s.measurement_source_type = src.src_vocabulary_id
# MAGIC     -- left join ${omop_catalog}.${omop_schema}.provider_source_value pr on s.provider_source_value = pr.provider_source_value
# MAGIC where s.measurement_date is not null
# MAGIC and s.load_id = 1
# MAGIC ) a
# MAGIC ;

# COMMAND ----------


