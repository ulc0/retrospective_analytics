-- Databricks notebook source
-- MAGIC %md
-- MAGIC dbutils.widgets.text("catalog",defaultValue="edav_prd_cdh")
-- MAGIC dbutils.widgets.text("schema",defaultValue="cdh_premier_v2")
-- MAGIC #mandatory parameters and names. The orchestrator will always pass these
-- MAGIC dbutils.widgets.text("DBCATALOG",defaultValue="edav_prd_cdh")
-- MAGIC dbutils.widgets.text("DEST_SCHEMA",defaultValue="cdh_sandbox")
-- MAGIC schema=dbutils.widgets.get('schema')
-- MAGIC catalog=dbutils.widgets.get('catalog')
-- MAGIC
-- MAGIC spark.conf.set('ml.schema',f"{DBCATALOG}.{SCHEMA}")
-- MAGIC
-- MAGIC DEST_SCHEMA=dbutils.widgets.get('DEST_SCHEMA')
-- MAGIC DBCATALOG=dbutils.widgets.get('DBCATALOG')
-- MAGIC spark.conf.set('dest.schema',f"{DBCATALOG}.{DEST_SCHEMA}")
-- MAGIC #experiment_id=dbutils.widgets.get('experiment_id')

-- COMMAND ----------

CREATE WIDGET TEXT DBCATALOG DEFAULT "edav_prd_cdh";
CREATE WIDGET TEXT SRC_SCHEMA DEFAULT "cdh_premier_v2";
CREATE WIDGET TEXT DEST_SCHEMA DEFAULT "cdh_premier_ra";
CREATE WIDGET TEXT table_name DEFAULT "ts_icd_events";

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Follow GTRI load specs for ICD
-- MAGIC

-- COMMAND ----------

create or replace table ts_premier_icd_preprocess as
select distinct pat_key, icd_poa,
case icd_version 
  when '10' then 'ICD10CM'
  else 'ICD9CM'
end as vocabulary_id,  
icd_pri_sec,icd_code as concept_code, null as edate from ${DBCATALOG}.${SCHEMA}.paticd_diag 
union 
select pat_key,'N' as icd_poa, 'ICD10PCS' as vocabulary_id, icd_pri_sec, icd_code as concept_code, 
proc_date as edate from ${DBCATALOG}.${SRC_SCHEMA}.paticd_proc 
order by pat_key, icd_poa


-- COMMAND ----------

CREATE OR REPLACE TABLE ${DBCATALOG}.${DEST_SCHEMA}.${table_name} AS 
SELECT distinct person_id, 
timestamp(coalesce(edate,observation_period_startdate)) as startdate ,timestamp(coalesce(edate,observation_period_end_date)) as enddate, 
case 1=1
    when (t.icd_poa=='Y') then startdate when (t.icd_pri_sec=='A') then startdate else enddate 
end observation_datetime, 
observation_period_number,vocabulary_id,concept_code 
 FROM ts_premier_icd_preprocess t join  ts_encounter_index i 
on t.pat_key=i.observation_period_id 
order by person_id,observation_period_number,observation_datetime,concept_code ;



-- COMMAND ----------


drop table ts_premier_icd_preprocess

-- COMMAND ----------



-- COMMAND ----------

-- MAGIC %md
-- MAGIC print(_sqldf.num_affected_rows)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC tbl = spark.table(f"{sschema}.{ftbl}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC tbl \
-- MAGIC     .groupby(['person_id', 'episode_datetime','vocabulary_id','concept_code']) \
-- MAGIC     .count() \
-- MAGIC     .where('count > 1') \
-- MAGIC     .sort('count', ascending=False) \
-- MAGIC     .show()

-- COMMAND ----------

-- MAGIC %md
-- MAGIC icdtbl=spark.table("edav_prd_cdh.cdh_premier_v2.paticd_diag")
-- MAGIC epitbl=spark.table(f"{SCHEMA}.fs_encounter_index")
