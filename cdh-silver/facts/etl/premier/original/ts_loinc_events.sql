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
concept_set_datetime as visit_start_date,
visit_occurrence_number,
LAB_TEST_CODE_TYPE as vocabulary_id,
lab_test_code as code,
--lab_test_RESULT_DATETIME,obser
--numeric_value,
--lab_test_result_unit as test_result_unit,
abnormal_flag as test_result
FROM ${DBCATALOG}.${SCHEMA}.genlab t
join fact_encounter_index i
on t.pat_key=i.visit_occurrence_number
where concept_set_datetime is not null
order by person_id,visit_occurrence_number,concept_set_datetime;

-- COMMAND ----------

CREATE OR REPLACE TABLE fact_loinc_labsens_events AS
select 
person_id,
concept_set_datetime as visit_start_date,
visit_occurrence_number,
--pat_key as visit_occurrence_number,
SUSC_TEST_METHOD_CODE_TYPE as vocabulary_id,
SUSC_TEST_METHOD_CODE as code,
interpretation as test_result
from ${DBCATALOG}.${SCHEMA}.lab_sens t
join fact_encounter_index i
on t.pat_key=i.visit_occurrence_number 
where SUSC_TEST_METHOD_CODE_TYPE="LOINC" and
concept_set_datetime is not null
order by person_id,visit_occurrence_number,concept_set_datetime;

-- COMMAND ----------

CREATE OR REPLACE TABLE fact_loinc_vitals_events AS
SELECT 
person_id,
visit_start_date as visit_start_date,
visit_occurrence_number,
--pat_key as visit_occurrence_number,
LAB_TEST_CODE_TYPE as vocabulary_id,
lab_test_code as code,
lab_test_result as test_result
FROM ${DBCATALOG}.${SCHEMA}.vitals t
join fact_encounter_index i
on t.pat_key=i.visit_occurrence_number
where visit_start_date is not NULL
and lab_test_code_type="LOINC"
order by person_id,visit_occurrence_number,visit_start_date;

-- COMMAND ----------

create or replace table ${DBCATALOG}.${DEST_SCHEMA}.fact_loinc_events AS
((select * from fact_premier_loinc_vitals_events) union
(select * from fact_premier_loinc_genlab_events) union
(select * from fact_premier_loinc_labsens_events)) 
--join ${dest.schema}.fact_encounter_index 
--on t.visit_occurrence_number=i.visit_occurrence_number
order by person_id,visit_occurrence_number, visit_start_date;

-- COMMAND ----------

drop table fact_loinc_vitals_events;
drop table fact_loinc_labsens_events;
drop table fact_loinc_genlab_events;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC sqls=f"select distinct code from {DBCATALOG}.{DEST_SCHEMA}.fact_premier_loinc_events "\
-- MAGIC     "where code not in (select distinct code from "\
-- MAGIC         f"{DBCATALOG}.cdh_abfm_omop_did.concept "\
-- MAGIC             "where vocabulary_id like 'LOINC' and standard_concept='S')"

-- COMMAND ----------

-- MAGIC %md
-- MAGIC bad_loinc=spark.sql(sqls)
-- MAGIC (bad_loinc.display())
