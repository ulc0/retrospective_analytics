# Databricks notebook source
dbutils.widgets.text("source_catalog",defaultValue="HIVE_METASTORE")
dbutils.widgets.text("source_schema",defaultValue="cdh_premier_v2")
#mandatory parameters and names. The orchestrator will always pass these
dbutils.widgets.text("etl_catalog",defaultValue="HIVE_METASTORE")
dbutils.widgets.text("etl_schema",defaultValue="cdh_premier_etl_v2")
dbutils.widgets.text("omop_catalog",defaultValue="HIVE_METASTORE")
dbutils.widgets.text("omop_schema",defaultValue="cdh_premier_omop_v2")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- load labs from vitals
# MAGIC
# MAGIC INSERT INTO ${etl_catalog}.${etl_schema}.stage_lab_temp
# MAGIC (
# MAGIC
# MAGIC     measurement_source_type,
# MAGIC     measurement_source_value,
# MAGIC     measurement_source_type_value,
# MAGIC     measurement_date,
# MAGIC     operator_source_value,
# MAGIC     unit_source_value,
# MAGIC     value_source_value,
# MAGIC     value_as_number,
# MAGIC     value_as_string,
# MAGIC     range_low,
# MAGIC     range_high,
# MAGIC     visit_source_value,
# MAGIC     person_source_value,
# MAGIC     load_id,
# MAGIC     loaded
# MAGIC )
# MAGIC SELECT
# MAGIC     replace( lab_test_code_type, 'SNOMED-CT', 'SNOMED') AS measurement_source_type,
# MAGIC     left(d.lab_test_code, 50) AS measurement_source_value,
# MAGIC     '44818702' AS measurement_source_type_value,
# MAGIC     d.observation_datetime AS measurement_date,
# MAGIC     d.numeric_value_operator AS operator_source_value,
# MAGIC     left(d.lab_test_result_unit, 50) AS unit_source_value,
# MAGIC     coalesce(left(d.lab_test_result, 50), 'no result') AS value_source_value,
# MAGIC     d.numeric_value AS value_as_number,
# MAGIC     left(d.lab_test_result, 50) AS value_as_string,
# MAGIC     NULL AS range_low,
# MAGIC     NULL AS range_high,
# MAGIC     d.pat_key AS visit_source_value,
# MAGIC     p.medrec_key AS person_source_value,
# MAGIC     1 AS load_id,
# MAGIC     0 AS loaded
# MAGIC FROM ${source_catalog}.${source_schema}.vitals d
# MAGIC join ${source_catalog}.${source_schema}.patdemo p on d.pat_key = p.pat_key
# MAGIC ;
