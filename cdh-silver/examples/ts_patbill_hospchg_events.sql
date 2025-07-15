-- Databricks notebook source
CREATE WIDGET TEXT DBCATALOG DEFAULT "edav_prd_cdh";
CREATE WIDGET TEXT DEST_SCHEMA DEFAULT "cdh_premier_ra";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC dbutils.widgets.text("DBCATALOG","edav_prd_cdh")
-- MAGIC dbutils.widgets.text("DEST_SCHEMA","cdh_premier_ra")
-- MAGIC dbutils.widgets.text("concept.name","snomed")

-- COMMAND ----------

CREATE WIDGET TEXT vocabulary DEFAULT "STDRX"

-- COMMAND ----------

create  or replace table ${DBCATALOG}.${DEST_SCHEMA}.ts_${vocabulary}_events as
SELECT 
person_id,
visit_start_date,
visit_occurrence_number,
stdchg_vocabulary_id as vocabulary_id,
if('${vocabulary}'="HOSPCHG",hosp_chg_id,std_chg_code) as code
from ts_patbill_events t
join ts_encounter_index  i
on t.visit_occurrence_number=i.visit_occurrence_number
where visit_start_date is not null 
AND
("${vocabulary}"="HOSPCHG") or (std_chg_code!='9999999'
and stdchg_vocabulary_id='${vocabulary}')


-- COMMAND ----------

show tables


