# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TABLE edav_stg_cdh.cdh_premier_atlas_results.dqd_master
# MAGIC (
# MAGIC id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC num_violated_rows BIGINT,
# MAGIC pct_violated_rows FLOAT,
# MAGIC num_denominator_rows BIGINT,
# MAGIC execution_time STRING,
# MAGIC query_text STRING,
# MAGIC check_name STRING,
# MAGIC check_level STRING,
# MAGIC check_description STRING,
# MAGIC cdm_table_name STRING,
# MAGIC cdm_field_name STRING,
# MAGIC concept_id STRING,
# MAGIC unit_concept_id STRING,
# MAGIC sql_file STRING,
# MAGIC category STRING,
# MAGIC subcategory STRING,
# MAGIC context STRING,
# MAGIC warning STRING,
# MAGIC error STRING,
# MAGIC checkid STRING,
# MAGIC is_error INT,
# MAGIC not_applicable INT,
# MAGIC failed INT,
# MAGIC passed INT,
# MAGIC not_applicable_reason STRING,
# MAGIC threshold_value INT,
# MAGIC notes_value STRING,
# MAGIC version BIGINT,
# MAGIC update_date TIMESTAMP
# MAGIC  ) USING DELTA
