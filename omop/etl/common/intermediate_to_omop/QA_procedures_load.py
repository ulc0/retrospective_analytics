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
# MAGIC -- check procedures
# MAGIC -- new method
# MAGIC
# MAGIC with matches as
# MAGIC (
# MAGIC   select 
# MAGIC     x_srcid as id
# MAGIC     from ${omop_catalog}.${omop_schema}.observation_temp
# MAGIC     where x_srcfile = 'STAGE_PROCEDURE'
# MAGIC     and x_srcloadid = 1
# MAGIC   union
# MAGIC     select 
# MAGIC     x_srcid as id
# MAGIC     from ${omop_catalog}.${omop_schema}.measurement_temp
# MAGIC     where x_srcfile = 'STAGE_PROCEDURE'
# MAGIC     and x_srcloadid = 1
# MAGIC /*
# MAGIC   union
# MAGIC     select 
# MAGIC     x_srcid as id
# MAGIC     from ${omop_catalog}.${omop_schema}.condition_occurrence_temp
# MAGIC     where x_srcfile = 'STAGE_PROCEDURE'
# MAGIC     and x_srcloadid = 1
# MAGIC */
# MAGIC   union
# MAGIC     select 
# MAGIC     x_srcid as id
# MAGIC     from ${omop_catalog}.${omop_schema}.drug_exposure_temp
# MAGIC     where x_srcfile = 'STAGE_PROCEDURE'
# MAGIC     and x_srcloadid = 1
# MAGIC   union
# MAGIC     select 
# MAGIC     x_srcid as id
# MAGIC     from ${omop_catalog}.${omop_schema}.device_exposure_temp
# MAGIC     where x_srcfile = 'STAGE_PROCEDURE'
# MAGIC     and x_srcloadid = 1
# MAGIC )
# MAGIC update  ${etl_catalog}.${etl_schema}.stage_procedure_temp sc
# MAGIC set loaded = 1
# MAGIC where exists ( select id from matches where sc.id = matches.id and sc.load_id = 1);

# COMMAND ----------

# MAGIC %sql
# MAGIC -- copy records that did not make it over to error
# MAGIC insert into ${etl_catalog}.${etl_schema}.stage_procedure_error
# MAGIC (
# MAGIC     id,
# MAGIC     procedure_code_source_type,
# MAGIC     procedure_source_value,
# MAGIC     procedure_source_type_value,
# MAGIC     code_modifier,
# MAGIC     procedure_date,
# MAGIC     quantity,
# MAGIC     stop_reason,
# MAGIC     total_charge,
# MAGIC     total_cost,
# MAGIC     total_paid,
# MAGIC     paid_by_payer,
# MAGIC     paid_by_patient,
# MAGIC     paid_patient_copay,
# MAGIC     paid_patient_coinsurance,
# MAGIC     paid_patient_deductible,
# MAGIC     paid_by_primary,
# MAGIC     visit_source_value,
# MAGIC     person_source_value,
# MAGIC     provider_source_value,
# MAGIC     load_id,
# MAGIC     loaded
# MAGIC )
# MAGIC select
# MAGIC   s.id,
# MAGIC   s.procedure_code_source_type,
# MAGIC   s.procedure_source_value,
# MAGIC   s.procedure_source_type_value,
# MAGIC   s.code_modifier,
# MAGIC   s.procedure_date,
# MAGIC   s.quantity,
# MAGIC   s.stop_reason,
# MAGIC   s.total_charge,
# MAGIC   s.total_cost,
# MAGIC   s.total_paid,
# MAGIC   s.paid_by_payer,
# MAGIC   s.paid_by_patient,
# MAGIC   s.paid_patient_copay,
# MAGIC   s.paid_patient_coinsurance,
# MAGIC   s.paid_patient_deductible,
# MAGIC   s.paid_by_primary,
# MAGIC   s.visit_source_value,
# MAGIC   s.person_source_value,
# MAGIC   s.provider_source_value,
# MAGIC   s.load_id,
# MAGIC   s.loaded
# MAGIC from ${etl_catalog}.${etl_schema}.stage_procedure_temp s
# MAGIC join ${omop_catalog}.${omop_schema}.procedure_occurrence_temp o on 
# MAGIC                 s.id = o.x_srcid
# MAGIC                 and s.load_id = o.x_srcloadid
# MAGIC where s.loaded <> 1  -- not matched in other tables
# MAGIC and s.load_id = 1
# MAGIC and o.procedure_concept_id is null; --- this should be procedure_concept_id -- fixed from proc_source_concept_id

# COMMAND ----------

# MAGIC %sql
# MAGIC -- delete records which matched under a non-procedure table (loaded = 1) that did not match to a concept code in procedure (source concept = 0)
# MAGIC merge into ${omop_catalog}.${omop_schema}.procedure_occurrence_temp co
# MAGIC using ${etl_catalog}.${etl_schema}.stage_procedure_temp sl
# MAGIC on ( co.procedure_source_concept_id is null
# MAGIC         and co.x_srcfile = 'STAGE_PROCEDURE'
# MAGIC         and co.x_srcid = sl.id
# MAGIC         and sl.loaded = 1 )
# MAGIC when matched then delete;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- move to etl stage permanent home
# MAGIC insert into ${etl_catalog}.${etl_schema}.stage_procedure
# MAGIC (
# MAGIC     id,
# MAGIC     procedure_code_source_type,
# MAGIC     procedure_source_value,
# MAGIC     procedure_source_type_value,
# MAGIC     code_modifier,
# MAGIC     procedure_date,
# MAGIC     quantity,
# MAGIC     stop_reason,
# MAGIC     total_charge,
# MAGIC     total_cost,
# MAGIC     total_paid,
# MAGIC     paid_by_payer,
# MAGIC     paid_by_patient,
# MAGIC     paid_patient_copay,
# MAGIC     paid_patient_coinsurance,
# MAGIC     paid_patient_deductible,
# MAGIC     paid_by_primary,
# MAGIC     visit_source_value,
# MAGIC     person_source_value,
# MAGIC     provider_source_value,
# MAGIC     load_id,
# MAGIC     loaded
# MAGIC )
# MAGIC select
# MAGIC   id,
# MAGIC   procedure_code_source_type,
# MAGIC   procedure_source_value,
# MAGIC   procedure_source_type_value,
# MAGIC   code_modifier,
# MAGIC   procedure_date,
# MAGIC   quantity,
# MAGIC   stop_reason,
# MAGIC   total_charge,
# MAGIC   total_cost,
# MAGIC   total_paid,
# MAGIC   paid_by_payer,
# MAGIC   paid_by_patient,
# MAGIC   paid_patient_copay,
# MAGIC   paid_patient_coinsurance,
# MAGIC   paid_patient_deductible,
# MAGIC   paid_by_primary,
# MAGIC   visit_source_value,
# MAGIC   person_source_value,
# MAGIC   provider_source_value,
# MAGIC   load_id,
# MAGIC   loaded
# MAGIC from ${etl_catalog}.${etl_schema}.stage_procedure_temp
# MAGIC where load_id = 1;
