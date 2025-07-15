-- Databricks notebook source
CREATE WIDGET TEXT DBCATALOG DEFAULT "edav_prd_cdh";
CREATE WIDGET TEXT SRC_SCHEMA DEFAULT "cdh_premier_v2";
CREATE WIDGET TEXT DEST_SCHEMA DEFAULT "cdh_premier_ra";
CREATE WIDGET TEXT table_name DEFAULT "ts_hcpcs_events"

-- COMMAND ----------


create or replace table${DBCATALOG}.${DEST_SCHEMA}.${table_name} as
SELECT 
person_id,
visit_occurrence_number,
coalesce(proc_date,visit_period_startdate) as visit_start_date,
'HCPCS' as vocabulary_id,
--cast(cpt_pos AS INTEGER) as visit_sequence,
cpt_code as code
FROM ${DBCATALOG}.${SRC_SCHEMA}.patcpt t
join ts_encounter_index i
on t.pat_key=i.visit_occurrence_number
where visit_start_date is not NULL
order by person_id,visit_start_date,visit_occurrence_number;


-- COMMAND ----------
