-- Databricks notebook source
CREATE WIDGET TEXT DBCATALOG DEFAULT "edav_prd_cdh";
CREATE WIDGET TEXT SRC_SCHEMA DEFAULT "cdh_premier_v2";
CREATE WIDGET TEXT DEST_SCHEMA DEFAULT "cdh_premier_ra";
CREATE WIDGET TEXT table_name DEFAULT "ts_hcpcs_events"

-- COMMAND ----------


create or replace table${DBCATALOG}.${DEST_SCHEMA}.${table_name} as
SELECT 
person_id,
observation_period_number,
coalesce(proc_date,observation_period_startdate) as observation_datetime,
'HCPCS' as vocabulary_id,
--cast(cpt_pos AS INTEGER) as observation_sequence,
cpt_code as concept_code
FROM ${DBCATALOG}.${SRC_SCHEMA}.patcpt t
join ts_encounter_index i
on t.pat_key=i.observation_period_id
where observation_datetime is not NULL
order by person_id,observation_datetime,observation_period_number;


-- COMMAND ----------
