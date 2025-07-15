-- Databricks notebook source
CREATE WIDGET TEXT DBCATALOG DEFAULT "edav_prd_cdh";
CREATE WIDGET TEXT SRC_SCHEMA DEFAULT "cdh_premier_v2";
CREATE WIDGET TEXT DEST_SCHEMA DEFAULT "cdh_premier_ra";
CREATE WIDGET TEXT table_name DEFAULT "fact_loinc_events";


-- COMMAND ----------


-- COMMAND ----------


create or replace table fact_loinc_genlab_events AS
select 
person_id,
collection_datetime as observation_datetime,
observation_period_number,
LAB_TEST_CODE_TYPE as vocabulary_id,
lab_test_code as concept_code,
--lab_test_RESULT_DATETIME,obser
--numeric_value,
--lab_test_result_unit as test_result_unit,
abnormal_flag as test_result
FROM ${DBCATALOG}.${SCHEMA}.genlab t
join fact_encounter_index i
on t.pat_key=i.observation_period_id
where collection_datetime is not null
order by person_id,observation_period_number,collection_datetime;

-- COMMAND ----------

CREATE OR REPLACE TABLE fact_loinc_labsens_events AS
select 
person_id,
collection_datetime as observation_datetime,
observation_period_number,
--pat_key as observation_period_id,
SUSC_TEST_METHOD_CODE_TYPE as vocabulary_id,
SUSC_TEST_METHOD_CODE as concept_code,
interpretation as test_result
from ${DBCATALOG}.${SCHEMA}.lab_sens t
join fact_encounter_index i
on t.pat_key=i.observation_period_id 
where SUSC_TEST_METHOD_CODE_TYPE="LOINC" and
collection_datetime is not null
order by person_id,observation_period_number,collection_datetime;

-- COMMAND ----------

CREATE OR REPLACE TABLE fact_loinc_vitals_events AS
SELECT 
person_id,
OBSERVATION_DATETIME as observation_datetime,
observation_period_number,
--pat_key as observation_period_id,
LAB_TEST_CODE_TYPE as vocabulary_id,
lab_test_code as concept_code,
lab_test_result as test_result
FROM ${DBCATALOG}.${SCHEMA}.vitals t
join fact_encounter_index i
on t.pat_key=i.observation_period_id
where OBSERVATION_DATETIME is not NULL
and lab_test_code_type="LOINC"
order by person_id,observation_period_number,observation_datetime;

-- COMMAND ----------

create or replace table ${DBCATALOG}.${DEST_SCHEMA}.fact_loinc_events AS
((select * from fact_premier_loinc_vitals_events) union
(select * from fact_premier_loinc_genlab_events) union
(select * from fact_premier_loinc_labsens_events)) 
--join ${dest.schema}.fact_encounter_index 
--on t.observation_period_id=i.observation_period_id
order by person_id,observation_period_number, observation_datetime;

-- COMMAND ----------

drop table fact_loinc_vitals_events;
drop table fact_loinc_labsens_events;
drop table fact_loinc_genlab_events;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC sqls=f"select distinct concept_code from {DBCATALOG}.{DEST_SCHEMA}.fact_premier_loinc_events "\
-- MAGIC     "where concept_code not in (select distinct concept_code from "\
-- MAGIC         f"{DBCATALOG}.cdh_abfm_omop_did.concept "\
-- MAGIC             "where vocabulary_id like 'LOINC' and standard_concept='S')"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC bad_loinc=spark.sql(sqls)
-- MAGIC (bad_loinc.display())
