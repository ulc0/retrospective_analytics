# Databricks notebook source
dbutils.widgets.text("source_catalog",defaultValue="edav_prd_cdh")
dbutils.widgets.text("source_schema",defaultValue="cdh_ml")
#mandatory parameters and names. The orchestrator will always pass these
dbutils.widgets.text("omop_catalog",defaultValue="edav_prd_cdh")
dbutils.widgets.text("omop_schema",defaultValue="cdh_ml")

# COMMAND ----------

# MAGIC %md
# MAGIC ICD10
# MAGIC ICD10CM
# MAGIC ICD10PCS
# MAGIC ICD9CM
# MAGIC ICD9Proc
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create
# MAGIC or replace table ${omop_catalog}.${omop_schema}.concept_cdh as
# MAGIC select  * from
# MAGIC   ${source_catalog}.${source_schema}.concept src
# MAGIC where
# MAGIC   src.domain_id in (
# MAGIC     'Measurement',
# MAGIC     'Observation',
# MAGIC     'Device',
# MAGIC     'Drug',
# MAGIC     'Condition',
# MAGIC     'Procedure',
# MAGIC     'Meas Value',
# MAGIC     'Meas Value Operator',
# MAGIC     'Condition/Meas',
# MAGIC     'Meas Value Operator',
# MAGIC     'Condition/Device'
# MAGIC   )
# MAGIC   and src.vocabulary_id in (
# MAGIC     'CVX',
# MAGIC     'HCPCS',
# MAGIC     'ICD10',
# MAGIC     'ICD10CM',
# MAGIC     'ICD10PCS',
# MAGIC     'ICD9CM',
# MAGIC     'ICD9Proc',
# MAGIC     '~~ICDO3~~',
# MAGIC     'LOINC',
# MAGIC     'NDC',
# MAGIC     'OMOP Extension',
# MAGIC     'OMOP',
# MAGIC     'SNOMED',
# MAGIC     'PCORNet',
# MAGIC     'RxNorm',
# MAGIC     'UB04 Pri Typ of Adm',
# MAGIC     'UB04 Typ bill',
# MAGIC     'VA Class'
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   domain_id,
# MAGIC   vocabulary_id,
# MAGIC   count(concept_id) as concept_counts_like
# MAGIC from
# MAGIC  ${source_catalog}.${source_schema}.concept src
# MAGIC where
# MAGIC   src.domain_id like '%Dev%'
# MAGIC   or src.domain_id like '%Drug%'
# MAGIC   or src.domain_id like '%Cond%'
# MAGIC   or src.domain_id like '%Meas%'
# MAGIC   or src.domain_id like '%Obs%'
# MAGIC   or src.domain_id like '%Proc%'
# MAGIC group by
# MAGIC   domain_id,
# MAGIC   vocabulary_id
# MAGIC order by
# MAGIC   vocabulary_id,
# MAGIC   domain_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC   domain_id,
# MAGIC   vocabulary_id,
# MAGIC   count(concept_id) as concept_counts
# MAGIC from
# MAGIC   ${source_catalog}.${source_schema}.concept src
# MAGIC group by 
# MAGIC   domain_id,
# MAGIC   vocabulary_id
# MAGIC order by
# MAGIC   vocabulary_id,
# MAGIC   domain_id

# COMMAND ----------

# MAGIC %sql
# MAGIC create
# MAGIC or replace table ${source_catalog}.${source_schema}.concept_xref as --select * from
# MAGIC --(
# MAGIC select
# MAGIC   src.code as src_code,
# MAGIC   tar.code as tar_code,
# MAGIC   --    replace(src.code, '.', '' ) as clean_code,
# MAGIC   replace(src.vocabulary_id, 'CPT4', 'HCPCS') as src_vocabulary_id,
# MAGIC   src.vocabulary_id as src_vocabulary_id,
# MAGIC   tar.vocabulary_id as tar_vocabulary_id,
# MAGIC   src.domain_id as src_domain_id,
# MAGIC   src.concept_id as src_concept_id,
# MAGIC   tar.concept_id as tar_concept_id,
# MAGIC   src.valid_end_date as src_end_date,
# MAGIC   cr.valid_end_date as cr_end_date,
# MAGIC   tar.valid_end_date as tar_end_date,
# MAGIC   src.invalid_reason as src_invalid_reason,
# MAGIC   cr.invalid_reason as cr_invalid_reason,
# MAGIC   tar.invalid_reason as tar_invalid_reason,
# MAGIC   coalesce(
# MAGIC     src.invalid_reason,
# MAGIC     cr.invalid_reason,
# MAGIC     tar.invalid_reason
# MAGIC   ) as invalid --,
# MAGIC   --row_number() over ( partition by src.code  , src.vocabulary_id, src.domain_id
# MAGIC   --                    order by coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason ) nulls first,
# MAGIC   --                             tar.valid_end_date desc nulls last, cr.valid_end_date desc nulls last,
# MAGIC   --                             src.valid_end_date desc nulls last
# MAGIC   --                  )  as rn
# MAGIC from
# MAGIC   ${omop_catalog}.${omop_schema}.concept src
# MAGIC   left join ${omop_catalog}.${omop_schema}.concept_relationship cr on src.concept_id = cr.concept_id_1
# MAGIC   and cr.relationship_id = 'Maps to'
# MAGIC   left join ${omop_catalog}.${omop_schema}.concept tar on cr.concept_id_2 = tar.concept_id
# MAGIC   and tar.standard_concept = 'S'
# MAGIC where
# MAGIC   tar.valid_end_date > '20240101'
# MAGIC   and src.domain_id in (
# MAGIC     'Measurement',
# MAGIC     'Observation',
# MAGIC     'Device',
# MAGIC     'Drug',
# MAGIC     'Condition',
# MAGIC     'Procedure',
# MAGIC     'Meas Value',
# MAGIC     'Meas Value Operator',
# MAGIC     'Condition/Meas',
# MAGIC     'Meas Value Operator',
# MAGIC     'Condition/Device'
# MAGIC   )
# MAGIC   and tar.vocabulary_id in (
# MAGIC     'CVX',
# MAGIC     'DRG',
# MAGIC     'HCPCS',
# MAGIC     'CPT4',
# MAGIC     'ICD10',
# MAGIC     'ICD10CM',
# MAGIC     'ICD10CPS',
# MAGIC     'ICD9CM',
# MAGIC     'ICD9Proc',
# MAGIC     'ICDO3',
# MAGIC     'LOINC',
# MAGIC     'NDC',
# MAGIC     'OMOP',
# MAGIC     'OMOP Extension',
# MAGIC     'SNOMED',
# MAGIC     'PCORNet',
# MAGIC     'RxNorm',
# MAGIC     'UB04 Pri Typ of Adm',
# MAGIC     'UB04 Typ bill',
# MAGIC     'VA Class'
# MAGIC   ) --) a
# MAGIC   --where rn=1
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC CREATE WIDGET TEXT omop_catalog DEFAULT "edav_dev_cdh"
# MAGIC CREATE WIDGET TEXT omop_schema DEFAULT "cdh_ml"

# COMMAND ----------

# MAGIC %md
# MAGIC -- create and populate the OMOP tables and default OHDSI datasets
# MAGIC -- drop table if exists  ${omop_catalog}.${omop_schema}.concept;
# MAGIC create or replace table ${omop_catalog}.${omop_schema}.concept
# MAGIC as select concept_id,
# MAGIC concept_name,
# MAGIC domain_id,
# MAGIC vocabulary_id,
# MAGIC concept_class_id,
# MAGIC standard_concept,
# MAGIC code,
# MAGIC to_date(valid_start_date, 'yyyyMMdd') valid_start_date,
# MAGIC to_date(valid_end_date, 'yyyyMMdd') valid_end_date,
# MAGIC invalid_reason
# MAGIC  from ${omop_catalog}.${omop_schema}.concept;
# MAGIC
# MAGIC
# MAGIC --drop table if exists ${omop_catalog}.${omop_schema}.concept_ancestor;
# MAGIC create or replace table ${omop_catalog}.${omop_schema}.concept_ancestor
# MAGIC as select * from ${omop_catalog}.${omop_schema}.concept_ancestor;
# MAGIC
# MAGIC --drop table if exists ${omop_catalog}.${omop_schema}.concept_class;
# MAGIC create or replace table ${omop_catalog}.${omop_schema}.concept_class
# MAGIC as select * from ${omop_catalog}.${omop_schema}.concept_class;
# MAGIC
# MAGIC --drop table if exists ${omop_catalog}.${omop_schema}.concept_relationship;
# MAGIC create or replace table ${omop_catalog}.${omop_schema}.concept_relationship
# MAGIC as select 
# MAGIC concept_id_1,
# MAGIC concept_id_2,
# MAGIC relationship_id,
# MAGIC to_date(valid_start_date, 'yyyyMMdd') valid_start_date,
# MAGIC to_date(valid_end_date, 'yyyyMMdd') valid_end_date,
# MAGIC invalid_reason 
# MAGIC from ${omop_catalog}.${omop_schema}.concept_relationship;
# MAGIC
# MAGIC
# MAGIC --drop table if exists  ${omop_catalog}.${omop_schema}.concept_synonym;
# MAGIC create or replace table ${omop_catalog}.${omop_schema}.concept_synonym
# MAGIC as select * from ${omop_catalog}.${omop_schema}.concept_synonym;
# MAGIC
# MAGIC
# MAGIC drop  table if exists ${omop_catalog}.${omop_schema}.domain;
# MAGIC create or replace table ${omop_catalog}.${omop_schema}.domain
# MAGIC as select * from ${omop_catalog}.cdh_global_reference_etl.bronze_athena_domain;
# MAGIC
# MAGIC drop table if exists ${omop_catalog}.${omop_schema}.drug_strength;
# MAGIC create or replace table ${omop_catalog}.${omop_schema}.drug_strength
# MAGIC as select 
# MAGIC drug_concept_id,
# MAGIC ingredient_concept_id,
# MAGIC amount_value,
# MAGIC amount_unit_concept_id,
# MAGIC numerator_value,
# MAGIC numerator_unit_concept_id,
# MAGIC denominator_value,
# MAGIC denominator_unit_concept_id,
# MAGIC box_size,
# MAGIC to_date(valid_start_date, 'yyyyMMdd') valid_start_date,
# MAGIC to_date(valid_end_date, 'yyyyMMdd') valid_end_date,
# MAGIC invalid_reason
# MAGIC  from ${omop_catalog}.cdh_global_reference_etl.bronze_athena_drug_strength;
# MAGIC
# MAGIC drop table if exists ${omop_catalog}.${omop_schema}.vocabulary;
# MAGIC create or replace table ${omop_catalog}.${omop_schema}.vocabulary
# MAGIC as select * from ${omop_catalog}.cdh_global_reference_etl.bronze_athena_vocabulary;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table if exists ${omop_catalog}.${omop_schema}.concept_measurement;
# MAGIC create
# MAGIC or replace table ${omop_catalog}.${omop_schema}.concept_measurement as --select * from
# MAGIC --(
# MAGIC select
# MAGIC   src.code as src_code,
# MAGIC   tar.code as tar_code,
# MAGIC   --  replace(src.code, '.', '' ) as clean_code,
# MAGIC   replace(src.vocabulary_id, 'CPT4', 'HCPCS') as src_vocabulary_id,
# MAGIC   src.vocabulary_id as src_vocabulary_id,
# MAGIC   tar.vocabulary_id as tar_vocabulary_id,
# MAGIC   src.domain_id as src_domain_id,
# MAGIC   src.concept_id as src_concept_id,
# MAGIC   tar.concept_id as tar_concept_id,
# MAGIC   val.concept_id as val_concept_id,
# MAGIC   src.valid_end_date as src_end_date,
# MAGIC   cr.valid_end_date as cr_end_date,
# MAGIC   tar.valid_end_date as tar_end_date,
# MAGIC   src.invalid_reason as src_invalid_reason,
# MAGIC   cr.invalid_reason as cr_invalid_reason,
# MAGIC   tar.invalid_reason as tar_invalid_reason,
# MAGIC   coalesce(
# MAGIC     src.invalid_reason,
# MAGIC     cr.invalid_reason,
# MAGIC     tar.invalid_reason
# MAGIC   ) as invalid --,
# MAGIC   --    row_number() over ( partition by src.code  , src.vocabulary_id, src.domain_id
# MAGIC   --                        order by coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason, val.invalid_reason ) nulls first,
# MAGIC   --                                 tar.valid_end_date desc nulls last, cr.valid_end_date desc nulls last,
# MAGIC   --                                 src.valid_end_date desc nulls last , val.concept_id nulls last
# MAGIC   --                      )  as rn
# MAGIC from
# MAGIC   ${omop_catalog}.${omop_schema}.concept src
# MAGIC   left join ${omop_catalog}.${omop_schema}.concept_relationship cr on src.concept_id = cr.concept_id_1
# MAGIC   and cr.relationship_id = 'Maps to'
# MAGIC   left join ${omop_catalog}.${omop_schema}.concept tar on cr.concept_id_2 = tar.concept_id
# MAGIC   and tar.standard_concept = 'S'
# MAGIC   left join ${omop_catalog}.${omop_schema}.concept_relationship crv on src.concept_id = crv.concept_id_1
# MAGIC   and crv.relationship_id = 'Maps to value'
# MAGIC   left join ${omop_catalog}.${omop_schema}.concept val on crv.concept_id_2 = val.concept_id
# MAGIC   and val.standard_concept = 'S'
# MAGIC   and val.domain_id = 'Meas Value'
# MAGIC where
# MAGIC   src.domain_id like '%Meas%'
# MAGIC   and tar.code is not null
# MAGIC   and tar.valid_end_date > '20240101' --) a
# MAGIC   --where rn=1
# MAGIC ;
# MAGIC   /*
# MAGIC   create index idx_concept_meas_raw on ${omop_catalog}.${omop_schema}.concept_measurement( raw_code, src_vocabulary_id );
# MAGIC   create index idx_concept_meas on ${omop_catalog}.${omop_schema}.concept_measurement( clean_code, src_vocabulary_id );
# MAGIC   */

# COMMAND ----------

# MAGIC %sql
# MAGIC ---drop table if exists ${omop_catalog}.${omop_schema}.concept_observation;
# MAGIC create
# MAGIC or replace table ${omop_catalog}.${omop_schema}.concept_observation as --select * from
# MAGIC --(
# MAGIC select
# MAGIC   src.code as src_code,
# MAGIC   --    replace(src.code, '.', '' ) as clean_code,
# MAGIC   tar.code as tar_code,
# MAGIC   replace(src.vocabulary_id, 'CPT4', 'HCPCS') as src_vocabulary_id,
# MAGIC   src.vocabulary_id as src_vocabulary_id,
# MAGIC   tar.vocabulary_id as tar_vocabulary_id,
# MAGIC   src.domain_id as src_domain_id,
# MAGIC   src.concept_id as src_concept_id,
# MAGIC   tar.concept_id as tar_concept_id,
# MAGIC   val.concept_id as val_concept_id,
# MAGIC   src.valid_end_date as src_end_date,
# MAGIC   cr.valid_end_date as cr_end_date,
# MAGIC   tar.valid_end_date as tar_end_date,
# MAGIC   src.invalid_reason as src_invalid_reason,
# MAGIC   cr.invalid_reason as cr_invalid_reason,
# MAGIC   tar.invalid_reason as tar_invalid_reason,
# MAGIC   coalesce(
# MAGIC     src.invalid_reason,
# MAGIC     cr.invalid_reason,
# MAGIC     tar.invalid_reason
# MAGIC   ) as invalid --,
# MAGIC   --    row_number() over ( partition by src.code  , src.vocabulary_id, src.domain_id
# MAGIC   --                        order by coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason, val.invalid_reason ) nulls first,
# MAGIC   --                                 tar.valid_end_date desc nulls last, cr.valid_end_date desc nulls last,
# MAGIC   --                                 src.valid_end_date desc nulls last , val.concept_id nulls last
# MAGIC   --                      )  as rn
# MAGIC from
# MAGIC   ${omop_catalog}.${omop_schema}.concept src
# MAGIC   left join ${omop_catalog}.${omop_schema}.concept_relationship cr on src.concept_id = cr.concept_id_1
# MAGIC   and cr.relationship_id = 'Maps to'
# MAGIC   left join ${omop_catalog}.${omop_schema}.concept tar on (
# MAGIC     cr.concept_id_2 = tar.concept_id
# MAGIC     and tar.standard_concept = 'S'
# MAGIC   ) --- this is a join of relationship maps to value ...
# MAGIC   left join ${omop_catalog}.${omop_schema}.concept_relationship crv on src.concept_id = crv.concept_id_1
# MAGIC   and crv.relationship_id = 'Maps to value'
# MAGIC   left join ${omop_catalog}.${omop_schema}.concept val on (
# MAGIC     crv.concept_id_2 = val.concept_id
# MAGIC     and val.standard_concept = 'S'
# MAGIC   )
# MAGIC where
# MAGIC   src.domain_id like '%Obs%'
# MAGIC   and tar.code is not null
# MAGIC   and tar.valid_end_date > '20240101' --) a
# MAGIC   --where rn = 1
# MAGIC ;
# MAGIC   /*
# MAGIC   create index idx_concept_obs_raw on ${omop_catalog}.${omop_schema}.concept_observation( raw_code, src_vocabulary_id );
# MAGIC   create index idx_concept_obs on ${omop_catalog}.${omop_schema}.concept_observation( clean_code, src_vocabulary_id );
# MAGIC   */

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table if exists ${omop_catalog}.${omop_schema}.concept_procedure;
# MAGIC create
# MAGIC or replace table ${omop_catalog}.${omop_schema}.concept_procedure as --select * from
# MAGIC --(
# MAGIC select
# MAGIC   src.code as src_code,
# MAGIC   -- replace(src.code, '.', '' ) as clean_code,
# MAGIC   tar.code as tar_code,
# MAGIC   replace(src.vocabulary_id, 'CPT4', 'HCPCS') as src_vocabulary_id,
# MAGIC   src.vocabulary_id as src_vocabulary_id,
# MAGIC   tar.vocabulary_id as tar_vocabulary_id,
# MAGIC   src.domain_id as src_domain_id,
# MAGIC   src.concept_id as src_concept_id,
# MAGIC   tar.concept_id as tar_concept_id,
# MAGIC   src.valid_end_date as src_end_date,
# MAGIC   cr.valid_end_date as cr_end_date,
# MAGIC   tar.valid_end_date as tar_end_date,
# MAGIC   src.invalid_reason as src_invalid_reason,
# MAGIC   cr.invalid_reason as cr_invalid_reason,
# MAGIC   tar.invalid_reason as tar_invalid_reason,
# MAGIC   coalesce(
# MAGIC     src.invalid_reason,
# MAGIC     cr.invalid_reason,
# MAGIC     tar.invalid_reason
# MAGIC   ) as invalid --,
# MAGIC   -- row_number() over ( partition by src.code  , src.vocabulary_id, src.domain_id
# MAGIC   --                     order by coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason ) nulls first,
# MAGIC   --                              tar.valid_end_date desc nulls last, cr.valid_end_date desc nulls last,
# MAGIC   --                              src.valid_end_date desc nulls last
# MAGIC   --                   )  as rn
# MAGIC from
# MAGIC   ${omop_catalog}.${omop_schema}.concept src
# MAGIC   left join ${omop_catalog}.${omop_schema}.concept_relationship cr on (
# MAGIC     src.concept_id = cr.concept_id_1
# MAGIC     and cr.relationship_id = 'Maps to'
# MAGIC   )
# MAGIC   left join ${omop_catalog}.${omop_schema}.concept tar on (
# MAGIC     cr.concept_id_2 = tar.concept_id
# MAGIC     and tar.standard_concept = 'S'
# MAGIC   )
# MAGIC where
# MAGIC   src.domain_id like '%Proc%'
# MAGIC   and tar.code is not null
# MAGIC   and tar.valid_end_date > '20240101' --) a
# MAGIC   --where rn=1
# MAGIC ;
# MAGIC   /*
# MAGIC   create index idx_concept_proc_raw on ${omop_catalog}.${omop_schema}.concept_procedure( raw_code, src_vocabulary_id );
# MAGIC   create index idx_concept_proc on ${omop_catalog}.${omop_schema}.concept_procedure( clean_code, src_vocabulary_id );
# MAGIC   */

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table if exists ${omop_catalog}.${omop_schema}.concept_drug;
# MAGIC create
# MAGIC or replace table ${omop_catalog}.${omop_schema}.concept_drug as --select * from
# MAGIC --(
# MAGIC select
# MAGIC   src.code as src_code,
# MAGIC   tar.code as tar_code,
# MAGIC   --    replace(src.code, '.', '' ) as clean_code,
# MAGIC   replace(src.vocabulary_id, 'CPT4', 'HCPCS') as src_vocabulary_id,
# MAGIC   src.vocabulary_id as src_vocabulary_id,
# MAGIC   tar.vocabulary_id as tar_vocabulary_id,
# MAGIC   src.domain_id as src_domain_id,
# MAGIC   tar.domain_id as tar_domain_id,
# MAGIC   src.concept_id as src_concept_id,
# MAGIC   tar.concept_id as tar_concept_id,
# MAGIC   src.valid_end_date as src_end_date,
# MAGIC   cr.valid_end_date as cr_end_date,
# MAGIC   tar.valid_end_date as tar_end_date,
# MAGIC   src.invalid_reason as src_invalid_reason,
# MAGIC   cr.invalid_reason as cr_invalid_reason,
# MAGIC   tar.invalid_reason as tar_invalid_reason,
# MAGIC   coalesce(
# MAGIC     src.invalid_reason,
# MAGIC     cr.invalid_reason,
# MAGIC     tar.invalid_reason
# MAGIC   ) as invalid --,
# MAGIC   -- row_number() over ( partition by src.code  , src.vocabulary_id, src.domain_id
# MAGIC   --                    order by coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason ) nulls first,
# MAGIC   --                              tar.valid_end_date desc nulls last, cr.valid_end_date desc nulls last,
# MAGIC   --                              src.valid_end_date desc nulls last
# MAGIC   --                   )  as rn
# MAGIC from
# MAGIC   ${omop_catalog}.${omop_schema}.concept src
# MAGIC   left join ${omop_catalog}.${omop_schema}.concept_relationship cr on (
# MAGIC     src.concept_id = cr.concept_id_1
# MAGIC     and cr.relationship_id = 'Maps to'
# MAGIC   )
# MAGIC   left join ${omop_catalog}.${omop_schema}.concept tar on (
# MAGIC     cr.concept_id_2 = tar.concept_id
# MAGIC     and tar.standard_concept = 'S'
# MAGIC   )
# MAGIC where
# MAGIC   src.domain_id like '%Drug%'
# MAGIC   and src.valid_end_date > '20240101'
# MAGIC   and tar.concept_id is not null
# MAGIC   and tar.valid_end_date > '20240101' --) a
# MAGIC   --where rn = 1
# MAGIC ;
# MAGIC   /*
# MAGIC   create index idx_concept_drug_raw on ${omop_catalog}.${omop_schema}.concept_drug( raw_code, src_vocabulary_id );
# MAGIC   create index idx_concept_drug on ${omop_catalog}.${omop_schema}.concept_drug( clean_code, src_vocabulary_id );
# MAGIC   */

# COMMAND ----------

# MAGIC %sql
# MAGIC -- create domain concept source specific tables
# MAGIC --drop table if exists ${omop_catalog}.${omop_schema}.concept_device;
# MAGIC create
# MAGIC or replace table ${omop_catalog}.${omop_schema}.concept_device as --select * from
# MAGIC --(
# MAGIC select
# MAGIC   src.code as src_code,
# MAGIC   tar.code as tar_code,
# MAGIC   --replace(src.code, '.', '' ) as clean_code,
# MAGIC   --replace(src.vocabulary_id, 'CPT4', 'HCPCS') as src_vocabulary_id,
# MAGIC   src.vocabulary_id as src_vocabulary_id,
# MAGIC   tar.code as code,
# MAGIC   tar.vocabulary_id as tar_vocabulary_id,
# MAGIC   src.domain_id as src_domain_id,
# MAGIC   src.concept_id as src_concept_id,
# MAGIC   tar.concept_id as tar_concept_id,
# MAGIC   src.valid_end_date as src_end_date,
# MAGIC   cr.valid_end_date as cr_end_date,
# MAGIC   tar.valid_end_date as tar_end_date,
# MAGIC   src.invalid_reason as src_invalid_reason,
# MAGIC   cr.invalid_reason as cr_invalid_reason,
# MAGIC   tar.invalid_reason as tar_invalid_reason,
# MAGIC   coalesce(
# MAGIC     src.invalid_reason,
# MAGIC     cr.invalid_reason,
# MAGIC     tar.invalid_reason
# MAGIC   ) as invalid --,
# MAGIC   --   row_number() over ( partition by src.code  , src.vocabulary_id, src.domain_id
# MAGIC   --                       order by coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason ) nulls first,
# MAGIC   --                                tar.valid_end_date desc nulls last, cr.valid_end_date desc nulls last,
# MAGIC   --                                src.valid_end_date desc nulls last
# MAGIC   --                    )  as rn
# MAGIC from
# MAGIC   ${omop_catalog}.${omop_schema}.concept src
# MAGIC   left join ${omop_catalog}.${omop_schema}.concept_relationship cr on src.concept_id = cr.concept_id_1
# MAGIC   and cr.relationship_id = 'Maps to'
# MAGIC   left join ${omop_catalog}.${omop_schema}.concept tar on (
# MAGIC     cr.concept_id_2 = tar.concept_id
# MAGIC     and tar.standard_concept = 'S'
# MAGIC   )
# MAGIC where
# MAGIC   src.domain_id like '%Dev%'
# MAGIC   and tar.concept_id is not Null
# MAGIC   and src.valid_end_date > '20240101' --) a
# MAGIC   --where rn=1
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table if exists ${omop_catalog}.${omop_schema}.concept_modifier;
# MAGIC create
# MAGIC or replace table IDENTIFIER(
# MAGIC   concat(
# MAGIC     :omop_catalog,
# MAGIC     '.',
# MAGIC     :omop_schema,
# MAGIC     '.concept_modifier'
# MAGIC   )
# MAGIC ) as --select distinct code, vocabulary_id, src_concept_id from
# MAGIC --(
# MAGIC select
# MAGIC   distinct src.code as code,
# MAGIC   -- replace(src.vocabulary_id, 'CPT4', 'HCPCS') as vocabulary_id,
# MAGIC   src.vocabulary_id as src_vocabulary_id,
# MAGIC   src.concept_id as src_concept_id,
# MAGIC   row_number() over (
# MAGIC     partition by src.code
# MAGIC     order by
# MAGIC       src.invalid_reason nulls first,
# MAGIC       src.valid_end_date desc nulls last,
# MAGIC       src.vocabulary_id desc
# MAGIC   ) as rn
# MAGIC from
# MAGIC   ${omop_catalog}.${omop_schema}.concept src
# MAGIC where
# MAGIC   src.concept_class_id like '%Modifier%' --) a
# MAGIC   --where rn=1
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC select domain_id, vocabulary_id, count(concept_id) as concept_counts  
# MAGIC from IDENTIFIER(concat(:omop_catalog, '.', :omop_schema, '.concept')) src
# MAGIC   where src.domain_id in ('Measurement',
# MAGIC 'Observation',
# MAGIC 'Device',
# MAGIC 'Drug',
# MAGIC 'Condition',
# MAGIC 'Procedure',
# MAGIC 'Meas Value',
# MAGIC 'Condition/Meas',
# MAGIC 'Meas Value Operator',
# MAGIC 'Condition/Device')
# MAGIC and src.vocabulary_id in ('CVX',
# MAGIC 'DRG',
# MAGIC 'HCPCS',
# MAGIC 'ICD10',
# MAGIC 'ICD10CM',
# MAGIC 'ICD10CPS',
# MAGIC 'ICD9CM',
# MAGIC 'ICD9Proc',
# MAGIC '~~ICDO3~~',
# MAGIC 'LOINC',
# MAGIC 'NDC',
# MAGIC 'OMOP Extension',
# MAGIC 'SNOMED',
# MAGIC 'PCORNet',
# MAGIC 'RxNorm',
# MAGIC 'UB04 Pri Typ of Adm',
# MAGIC 'UB04 Typ bill',
# MAGIC 'VA Class')
# MAGIC   group by domain_id, vocabulary_id
# MAGIC   order by  vocabulary_id,domain_id
# MAGIC
