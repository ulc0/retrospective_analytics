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
# MAGIC -- build the DDL. must be ran prior to other dqd queries
# MAGIC
# MAGIC DROP TABLE IF EXISTS ${results_catalog}.${results_schema}.dqdashboard_results;
# MAGIC
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.dqdashboard_results
# MAGIC USING DELTA
# MAGIC  AS
# MAGIC SELECT
# MAGIC CAST(NULL AS bigint) AS num_violated_rows,
# MAGIC
# MAGIC 	CAST(NULL AS float) AS pct_violated_rows,
# MAGIC
# MAGIC 	CAST(NULL AS bigint) AS num_denominator_rows,
# MAGIC
# MAGIC 	CAST(NULL AS STRING) AS execution_time,
# MAGIC
# MAGIC 	CAST(NULL AS STRING) AS query_text,
# MAGIC
# MAGIC 	CAST(NULL AS STRING) AS check_name,
# MAGIC
# MAGIC 	CAST(NULL AS STRING) AS check_level,
# MAGIC
# MAGIC 	CAST(NULL AS STRING) AS check_description,
# MAGIC
# MAGIC 	CAST(NULL AS STRING) AS cdm_table_name,
# MAGIC
# MAGIC 	CAST(NULL AS STRING) AS cdm_field_name,
# MAGIC
# MAGIC 	CAST(NULL AS STRING) AS concept_id,
# MAGIC
# MAGIC 	CAST(NULL AS STRING) AS unit_concept_id,
# MAGIC
# MAGIC 	CAST(NULL AS STRING) AS sql_file,
# MAGIC
# MAGIC 	CAST(NULL AS STRING) AS category,
# MAGIC
# MAGIC 	CAST(NULL AS STRING) AS subcategory,
# MAGIC
# MAGIC 	CAST(NULL AS STRING) AS context,
# MAGIC
# MAGIC 	CAST(NULL AS STRING) AS warning,
# MAGIC
# MAGIC 	CAST(NULL AS STRING) AS error,
# MAGIC
# MAGIC 	CAST(NULL AS STRING) AS checkid,
# MAGIC
# MAGIC 	CAST(NULL AS integer) AS is_error,
# MAGIC
# MAGIC 	CAST(NULL AS integer) AS not_applicable,
# MAGIC
# MAGIC 	CAST(NULL AS integer) AS failed,
# MAGIC
# MAGIC 	CAST(NULL AS integer) AS passed,
# MAGIC
# MAGIC 	CAST(NULL AS STRING) AS not_applicable_reason,
# MAGIC
# MAGIC 	CAST(NULL AS integer) AS threshold_value,
# MAGIC
# MAGIC 	CAST(NULL AS STRING) AS notes_value  WHERE 1 = 0;
# MAGIC
