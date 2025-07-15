-- Databricks notebook source
CREATE WIDGET TEXT DBCATALOG DEFAULT "edav_prd_cdh";
CREATE WIDGET TEXT SRC_SCHEMA DEFAULT "cdh_premier_v2";
CREATE WIDGET TEXT DEST_SCHEMA DEFAULT "cdh_premier_ra";



-- COMMAND ----------

CREATE WIDGET TEXT table_name DEFAULT "ts_premier_icd_events";
CREATE WIDGET TEXT LIMIT_value DEFAULT "LIMIT 1000";


-- COMMAND ----------

create or replace table ${DBCATALOG}.${DEST_SCHEMA}.${table_name} as 
select DISTINCT medrec_key as person_id from ${DBCATALOG}.${SRC_SCHEMA}.patdemo 
order by person_id ${LIMIT_value} 

