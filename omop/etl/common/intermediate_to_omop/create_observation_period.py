# Databricks notebook source
dbutils.widgets.text("source_catalog",defaultValue="edav_stg_cdh")
dbutils.widgets.text("source_schema",defaultValue="cdh_premier_v2")
#mandatory parameters and names. The orchestrator will always pass these
dbutils.widgets.text("etl_catalog",defaultValue="edav_stg_cdh")
dbutils.widgets.text("etl_schema",defaultValue="cdh_premier_omop_etl")
dbutils.widgets.text("omop_catalog",defaultValue="edav_stg_cdh")
dbutils.widgets.text("omop_schema",defaultValue="cdh_premier_omop")
dbutils.widgets.text("results_catalog",defaultValue="edav_stg_cdh")
dbutils.widgets.text("results_schema",defaultValue="cdh_premier_atlas_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC --HINT DISTRIBUTE ON KEY (person_id)
# MAGIC
# MAGIC CREATE TABLE if not exists ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD
# MAGIC (
# MAGIC 			observation_period_id bigint generated always as identity NOT NULL,
# MAGIC 			person_id bigint NOT NULL,
# MAGIC 			observation_period_start_date date NOT NULL,
# MAGIC 			observation_period_end_date date NOT NULL,
# MAGIC 			period_type_concept_id integer NOT NULL 
# MAGIC )
# MAGIC using delta
# MAGIC cluster by (person_id);
# MAGIC
# MAGIC
# MAGIC insert overwrite ${omop_catalog}.${omop_schema}.observation_period
# MAGIC (
# MAGIC 	person_id,
# MAGIC   observation_period_start_date,
# MAGIC   observation_period_end_date,
# MAGIC   period_type_concept_id
# MAGIC )
# MAGIC select 
# MAGIC person_id, 
# MAGIC min(visit_start_date) as observation_period_start_date,
# MAGIC max(visit_end_date) as observation_period_end_date,
# MAGIC 44814724 as period_type_concept_id
# MAGIC from ${omop_catalog}.${omop_schema}.visit_occurrence
# MAGIC group by person_id;
# MAGIC
