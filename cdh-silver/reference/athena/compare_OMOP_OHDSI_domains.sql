-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.widgets.text("source_catalog",defaultValue="HIVE_METASTORE")
-- MAGIC dbutils.widgets.text("source_schema",defaultValue="cdh_premier_v2")
-- MAGIC #mandatory parameters and names. The orchestrator will always pass these
-- MAGIC dbutils.widgets.text("omop_catalog",defaultValue="edav_dev_cdh")
-- MAGIC dbutils.widgets.text("omop_schema",defaultValue="ml")
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC omop_catalog='edav_dev_cdh'
-- MAGIC omop_schema='cdh_ml'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC CREATE WIDGET TEXT omop_catalog DEFAULT "edav_dev_cdh"
-- MAGIC CREATE WIDGET TEXT omop_schema DEFAULT "cdh_ml"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC -- create and populate the OMOP tables and default OHDSI datasets
-- MAGIC -- drop table if exists  ${omop_catalog}.${omop_schema}.concept;
-- MAGIC create table ${omop_catalog}.${omop_schema}.concept
-- MAGIC as select concept_id,
-- MAGIC concept_name,
-- MAGIC domain_id,
-- MAGIC vocabulary_id,
-- MAGIC concept_class_id,
-- MAGIC standard_concept,
-- MAGIC code,
-- MAGIC to_date(valid_start_date, 'yyyyMMdd') valid_start_date,
-- MAGIC to_date(valid_end_date, 'yyyyMMdd') valid_end_date,
-- MAGIC invalid_reason
-- MAGIC  from ${omop_catalog}.cdh_global_reference_etl.bronze_athena_concept;
-- MAGIC
-- MAGIC
-- MAGIC --drop table if exists ${omop_catalog}.${omop_schema}.concept_ancestor;
-- MAGIC create table ${omop_catalog}.${omop_schema}.concept_ancestor
-- MAGIC as select * from ${omop_catalog}.cdh_global_reference_etl.bronze_athena_concept_ancestor;
-- MAGIC
-- MAGIC --drop table if exists ${omop_catalog}.${omop_schema}.concept_class;
-- MAGIC create table ${omop_catalog}.${omop_schema}.concept_class
-- MAGIC as select * from ${omop_catalog}.cdh_global_reference_etl.bronze_athena_concept_class;
-- MAGIC
-- MAGIC --drop table if exists ${omop_catalog}.${omop_schema}.concept_relationship;
-- MAGIC create table ${omop_catalog}.${omop_schema}.concept_relationship
-- MAGIC as select 
-- MAGIC concept_id_1,
-- MAGIC concept_id_2,
-- MAGIC relationship_id,
-- MAGIC to_date(valid_start_date, 'yyyyMMdd') valid_start_date,
-- MAGIC to_date(valid_end_date, 'yyyyMMdd') valid_end_date,
-- MAGIC invalid_reason 
-- MAGIC from ${omop_catalog}.cdh_global_reference_etl.bronze_athena_concept_relationship;
-- MAGIC
-- MAGIC
-- MAGIC --drop table if exists  ${omop_catalog}.${omop_schema}.concept_synonym;
-- MAGIC create table ${omop_catalog}.${omop_schema}.concept_synonym
-- MAGIC as select * from ${omop_catalog}.cdh_global_reference_etl.bronze_athena_concept_synonym;
-- MAGIC
-- MAGIC
-- MAGIC drop  table if exists ${omop_catalog}.${omop_schema}.domain;
-- MAGIC create table ${omop_catalog}.${omop_schema}.domain
-- MAGIC as select * from ${omop_catalog}.cdh_global_reference_etl.bronze_athena_domain;
-- MAGIC
-- MAGIC drop table if exists ${omop_catalog}.${omop_schema}.drug_strength;
-- MAGIC create table ${omop_catalog}.${omop_schema}.drug_strength
-- MAGIC as select 
-- MAGIC drug_concept_id,
-- MAGIC ingredient_concept_id,
-- MAGIC amount_value,
-- MAGIC amount_unit_concept_id,
-- MAGIC numerator_value,
-- MAGIC numerator_unit_concept_id,
-- MAGIC denominator_value,
-- MAGIC denominator_unit_concept_id,
-- MAGIC box_size,
-- MAGIC to_date(valid_start_date, 'yyyyMMdd') valid_start_date,
-- MAGIC to_date(valid_end_date, 'yyyyMMdd') valid_end_date,
-- MAGIC invalid_reason
-- MAGIC  from ${omop_catalog}.cdh_global_reference_etl.bronze_athena_drug_strength;
-- MAGIC
-- MAGIC drop table if exists ${omop_catalog}.${omop_schema}.vocabulary;
-- MAGIC create table ${omop_catalog}.${omop_schema}.vocabulary
-- MAGIC as select * from ${omop_catalog}.cdh_global_reference_etl.bronze_athena_vocabulary;
-- MAGIC

-- COMMAND ----------

-- create concept source specific tables



--drop table if exists ${omop_catalog}.${omop_schema}.concept_measurement;
create table ${omop_catalog}.${omop_schema}.concept_measurement as
  from ${omop_catalog}.${omop_schema}.concept src
  left join ${omop_catalog}.${omop_schema}.concept_relationship cr on src.concept_id = cr.concept_id_1
          and cr.relationship_id = 'Maps to'
  left join ${omop_catalog}.${omop_schema}.concept tar on cr.concept_id_2 = tar.concept_id
          and tar.standard_concept = 'S'
  left join ${omop_catalog}.${omop_schema}.concept_relationship crv on src.concept_id = crv.concept_id_1
          and crv.relationship_id = 'Maps to value'
  left join ${omop_catalog}.${omop_schema}.concept val on crv.concept_id_2 = val.concept_id
          and val.standard_concept = 'S'
          and val.domain_id = 'Meas Value'
  where src.domain_id like '%Meas%'
--) a
--where rn=1
;

/*
create index idx_concept_meas_raw on ${omop_catalog}.${omop_schema}.concept_measurement( raw_code, src_vocabulary_id );
create index idx_concept_meas on ${omop_catalog}.${omop_schema}.concept_measurement( clean_code, src_vocabulary_id );
*/

---drop table if exists ${omop_catalog}.${omop_schema}.concept_observation;
create table ${omop_catalog}.${omop_schema}.concept_observation as
  from ${omop_catalog}.${omop_schema}.concept src
  left join ${omop_catalog}.${omop_schema}.concept_relationship cr on src.concept_id = cr.concept_id_1
              and cr.relationship_id = 'Maps to'
  left join ${omop_catalog}.${omop_schema}.concept tar on cr.concept_id_2 = tar.concept_id
              and tar.standard_concept = 'S'
  left join ${omop_catalog}.${omop_schema}.concept_relationship crv on src.concept_id = crv.concept_id_1
              and crv.relationship_id = 'Maps to value'
  left join ${omop_catalog}.${omop_schema}.concept val on crv.concept_id_2 = val.concept_id
              and val.standard_concept = 'S'
  where src.domain_id like '%Obs%'
--) a
--where rn = 1
;

/*
create index idx_concept_obs_raw on ${omop_catalog}.${omop_schema}.concept_observation( raw_code, src_vocabulary_id );
create index idx_concept_obs on ${omop_catalog}.${omop_schema}.concept_observation( clean_code, src_vocabulary_id );
*/
--drop table if exists ${omop_catalog}.${omop_schema}.concept_condition;
create table ${omop_catalog}.${omop_schema}.concept_condition as
  from ${omop_catalog}.${omop_schema}.concept src
  left join ${omop_catalog}.${omop_schema}.concept_relationship cr on src.concept_id = cr.concept_id_1
      and cr.relationship_id = 'Maps to'
  left join ${omop_catalog}.${omop_schema}.concept tar on cr.concept_id_2 = tar.concept_id
      and tar.standard_concept = 'S'
  where src.domain_id like '%Cond%'
--)
--a 
--where rn =1
 ;

--drop table if exists ${omop_catalog}.${omop_schema}.concept_procedure;
create table ${omop_catalog}.${omop_schema}.concept_procedure as
    from ${omop_catalog}.${omop_schema}.concept src
    left join ${omop_catalog}.${omop_schema}.concept_relationship cr on src.concept_id = cr.concept_id_1
        and cr.relationship_id = 'Maps to'
    left join ${omop_catalog}.${omop_schema}.concept tar on cr.concept_id_2 = tar.concept_id
        and tar.standard_concept = 'S'
    where src.domain_id like '%Proc%'
--) a
--where rn=1
--;

/*
create index idx_concept_proc_raw on ${omop_catalog}.${omop_schema}.concept_procedure( raw_code, src_vocabulary_id );
create index idx_concept_proc on ${omop_catalog}.${omop_schema}.concept_procedure( clean_code, src_vocabulary_id );
*/

--drop table if exists ${omop_catalog}.${omop_schema}.concept_drug;
create table ${omop_catalog}.${omop_schema}.concept_drug as
select * from 
(
  select 
    src.code as raw_code,
    replace(src.code, '.', '' ) as clean_code,
    replace(src.vocabulary_id, 'CPT4', 'HCPCS') as src_vocabulary_id,
    src.domain_id as src_domain_id,
    src.concept_id as src_concept_id,
    tar.concept_id as tar_concept_id,
    src.valid_end_date as src_end_date,
    cr.valid_end_date as cr_end_date,
    tar.valid_end_date as tar_end_date,
    src.invalid_reason as src_invalid_reason,
    cr.invalid_reason as cr_invalid_reason,
    tar.invalid_reason as tar_invalid_reason,
    coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason ) as invalid,
    row_number() over ( partition by src.code  , src.vocabulary_id, src.domain_id 
                        order by coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason ) nulls first, 
                                 tar.valid_end_date desc nulls last, cr.valid_end_date desc nulls last, 
                                 src.valid_end_date desc nulls last 
                      )  as rn
  from ${omop_catalog}.${omop_schema}.concept src
  left join ${omop_catalog}.${omop_schema}.concept_relationship cr on src.concept_id = cr.concept_id_1
      and cr.relationship_id = 'Maps to'
  left join ${omop_catalog}.${omop_schema}.concept tar on cr.concept_id_2 = tar.concept_id
      and tar.standard_concept = 'S'
  where src.domain_id like '%Drug%'
) a
where rn = 1
;

/*
create index idx_concept_drug_raw on ${omop_catalog}.${omop_schema}.concept_drug( raw_code, src_vocabulary_id );
create index idx_concept_drug on ${omop_catalog}.${omop_schema}.concept_drug( clean_code, src_vocabulary_id );
*/

--drop table if exists ${omop_catalog}.${omop_schema}.concept_device;
create table ${omop_catalog}.${omop_schema}.concept_device as
select * from 
(
  select
    src.code as raw_code,
    replace(src.code, '.', '' ) as clean_code,
    replace(src.vocabulary_id, 'CPT4', 'HCPCS') as src_vocabulary_id,
    src.domain_id as src_domain_id,
    src.concept_id as src_concept_id,
    tar.concept_id as tar_concept_id,
    src.valid_end_date as src_end_date,
    cr.valid_end_date as cr_end_date,
    tar.valid_end_date as tar_end_date,
    src.invalid_reason as src_invalid_reason,
    cr.invalid_reason as cr_invalid_reason,
    tar.invalid_reason as tar_invalid_reason,
    coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason ) as invalid,
    row_number() over ( partition by src.code  , src.vocabulary_id, src.domain_id 
                        order by coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason ) nulls first, 
                                 tar.valid_end_date desc nulls last, cr.valid_end_date desc nulls last, 
                                 src.valid_end_date desc nulls last 
                      )  as rn
  from ${omop_catalog}.${omop_schema}.concept src
  left join ${omop_catalog}.${omop_schema}.concept_relationship cr on src.concept_id = cr.concept_id_1
  and cr.relationship_id = 'Maps to'
  left join ${omop_catalog}.${omop_schema}.concept tar on cr.concept_id_2 = tar.concept_id
  and tar.standard_concept = 'S'
  where src.domain_id like '%Dev%'
) a
where rn=1
;

-- COMMAND ----------

--drop table if exists ${omop_catalog}.${omop_schema}.concept_modifier;
create table IDENTIFIER(concat(:omop_catalog,'.',:omop_schema,'.concept_modifier')) as
select distinct code, vocabulary_id, src_concept_id from 
(
  select distinct
    src.code as code,
   -- replace(src.vocabulary_id, 'CPT4', 'HCPCS') as vocabulary_id,
    src.concept_id as src_concept_id,
    row_number() over ( partition by src.code  
                        order by  src.invalid_reason  nulls first, 
                                 src.valid_end_date desc nulls last ,
                                 src.vocabulary_id desc 
                      )  as rn
    from ${omop_catalog}.${omop_schema}.concept src
    where src.concept_class_id like '%Modifier%'
) a
where rn=1
;


-- COMMAND ----------

select domain_id, vocabulary_id, count(concept_id) as concept_counts_like  
from IDENTIFIER(concat(:omop_catalog, '.', :omop_schema, '.concept')) src
  where src.domain_id like '%Dev%' or src.domain_id like '%Drug%' or src.domain_id like '%Cond%' or src.domain_id like '%Meas%' or src.domain_id like '%Obs%'  or src.domain_id like '%Proc%' 
  or src.src_domain_id like 'Type Concept'
  group by domain_id, vocabulary_id
  order by  vocabulary_id,domain_id
    

-- COMMAND ----------

select domain_id, vocabulary_id, count(concept_id) as concept_counts  
from IDENTIFIER(concat(:omop_catalog, '.', :omop_schema, '.concept')) src
  where src.domain_id in ('Measurement',
'Observation',
'Type Concept',
'Device',
'Drug',
'Condition',
'Procedure',
'Meas Value',
'Condition/Meas',
'Meas Value Operator',
'Condition/Device')
  group by domain_id, vocabulary_id
  order by  vocabulary_id,domain_id


-- COMMAND ----------

create table ${omop_catalog}.${omop_schema}.concept as
select * from 
(
  select
    src.code as raw_code,
    replace(src.code, '.', '' ) as clean_code,
    --replace(src.vocabulary_id, 'CPT4', 'HCPCS') as src_vocabulary_id,
    src.domain_id as src_domain_id,
    src.concept_id as src_concept_id,
    tar.concept_id as tar_concept_id,
    src.valid_end_date as src_end_date,
    cr.valid_end_date as cr_end_date,
    tar.valid_end_date as tar_end_date,
    src.invalid_reason as src_invalid_reason,
    cr.invalid_reason as cr_invalid_reason,
    tar.invalid_reason as tar_invalid_reason,
    coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason ) as invalid,
    row_number() over ( partition by src.code  , src.vocabulary_id, src.domain_id 
                        order by coalesce( src.invalid_reason, cr.invalid_reason, tar.invalid_reason ) nulls first, 
                                 tar.valid_end_date desc nulls last, cr.valid_end_date desc nulls last, 
                                 src.valid_end_date desc nulls last 
                      )  as rn
  from ${omop_catalog}.${omop_schema}.concept src
  left join ${omop_catalog}.${omop_schema}.concept_relationship cr on src.concept_id = cr.concept_id_1
  and cr.relationship_id = 'Maps to'
  left join ${omop_catalog}.${omop_schema}.concept tar on cr.concept_id_2 = tar.concept_id
  and tar.standard_concept = 'S'
  where src.domain_id like '%Dev%' or src.domain_id like '%Drug%' or src.domain_id like '%Cond%' or src.domain_id like '%Meas%' or src.domain_id like '%Obs%'  or src.domain_id like '%Proc%' 
) a
where rn=1
;

