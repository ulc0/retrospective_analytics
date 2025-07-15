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
# MAGIC -- table measure condition era completeness
# MAGIC
# MAGIC WITH cte_all  AS (SELECT cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  , CAST('' as STRING) as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'measureConditionEraCompleteness' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'The number and Percent of persons that does not have condition_era built successfully for all persons in condition_occurrence' as check_description
# MAGIC  ,'CONDITION_ERA' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_condition_era_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_measureconditioneracompleteness_condition_era' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.person_id) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  SELECT DISTINCT 
# MAGIC  co.person_id
# MAGIC  FROM ${omop_catalog}.${omop_schema}.condition_occurrence co
# MAGIC  LEFT JOIN ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable 
# MAGIC  ON co.person_id = cdmTable.person_id
# MAGIC  WHERE cdmTable.person_id IS NULL
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT person_id) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.condition_occurrence co
# MAGIC ) denominator
# MAGIC ) cte
# MAGIC )
# MAGIC INSERT INTO ${results_catalog}.${results_schema}.dqdashboard_results
# MAGIC SELECT *
# MAGIC FROM cte_all;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- table measure person completeness
# MAGIC
# MAGIC WITH cte_all  AS (SELECT cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  , CAST('' as STRING) as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'measurePersonCompleteness' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'The number and percent of persons in the CDM that do not have at least one record in the OBSERVATION_PERIOD table' as check_description
# MAGIC  ,'OBSERVATION_PERIOD' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_person_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_measurepersoncompleteness_observation_period' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.person_id) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC  LEFT JOIN ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD cdmTable2 
# MAGIC  ON cdmTable.person_id = cdmTable2.person_id
# MAGIC  WHERE cdmTable2.person_id IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'measurePersonCompleteness' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'The number and percent of persons in the CDM that do not have at least one record in the VISIT_OCCURRENCE table' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_person_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_measurepersoncompleteness_visit_occurrence' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 95 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 95 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,95 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.person_id) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC  LEFT JOIN ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable2 
# MAGIC  ON cdmTable.person_id = cdmTable2.person_id
# MAGIC  WHERE cdmTable2.person_id IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'measurePersonCompleteness' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'The number and percent of persons in the CDM that do not have at least one record in the CONDITION_OCCURRENCE table' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_person_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_measurepersoncompleteness_condition_occurrence' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 95 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 95 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,95 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.person_id) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC  LEFT JOIN ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable2 
# MAGIC  ON cdmTable.person_id = cdmTable2.person_id
# MAGIC  WHERE cdmTable2.person_id IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'measurePersonCompleteness' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'The number and percent of persons in the CDM that do not have at least one record in the DRUG_EXPOSURE table' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_person_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_measurepersoncompleteness_drug_exposure' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 95 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 95 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,95 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.person_id) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC  LEFT JOIN ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable2 
# MAGIC  ON cdmTable.person_id = cdmTable2.person_id
# MAGIC  WHERE cdmTable2.person_id IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'measurePersonCompleteness' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'The number and percent of persons in the CDM that do not have at least one record in the PROCEDURE_OCCURRENCE table' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_person_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_measurepersoncompleteness_procedure_occurrence' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 95 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 95 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,95 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.person_id) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC  LEFT JOIN ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable2 
# MAGIC  ON cdmTable.person_id = cdmTable2.person_id
# MAGIC  WHERE cdmTable2.person_id IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'measurePersonCompleteness' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'The number and percent of persons in the CDM that do not have at least one record in the DEVICE_EXPOSURE table' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_person_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_measurepersoncompleteness_device_exposure' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 100 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 100 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,100 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.person_id) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC  LEFT JOIN ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable2 
# MAGIC  ON cdmTable.person_id = cdmTable2.person_id
# MAGIC  WHERE cdmTable2.person_id IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'measurePersonCompleteness' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'The number and percent of persons in the CDM that do not have at least one record in the MEASUREMENT table' as check_description
# MAGIC  ,'MEASUREMENT' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_person_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_measurepersoncompleteness_measurement' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 95 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 95 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,95 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.person_id) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC  LEFT JOIN ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable2 
# MAGIC  ON cdmTable.person_id = cdmTable2.person_id
# MAGIC  WHERE cdmTable2.person_id IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'measurePersonCompleteness' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'The number and percent of persons in the CDM that do not have at least one record in the VISIT_DETAIL table' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_person_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_measurepersoncompleteness_visit_detail' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 100 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 100 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,100 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.person_id) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC  LEFT JOIN ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable2 
# MAGIC  ON cdmTable.person_id = cdmTable2.person_id
# MAGIC  WHERE cdmTable2.person_id IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'measurePersonCompleteness' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'The number and percent of persons in the CDM that do not have at least one record in the NOTE table' as check_description
# MAGIC  ,'NOTE' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_person_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_measurepersoncompleteness_note' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 100 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 100 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,100 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.person_id) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC  LEFT JOIN ${omop_catalog}.${omop_schema}.NOTE cdmTable2 
# MAGIC  ON cdmTable.person_id = cdmTable2.person_id
# MAGIC  WHERE cdmTable2.person_id IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'measurePersonCompleteness' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'The number and percent of persons in the CDM that do not have at least one record in the OBSERVATION table' as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_person_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_measurepersoncompleteness_observation' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 95 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 95 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,95 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.person_id) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC  LEFT JOIN ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable2 
# MAGIC  ON cdmTable.person_id = cdmTable2.person_id
# MAGIC  WHERE cdmTable2.person_id IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'measurePersonCompleteness' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'The number and percent of persons in the CDM that do not have at least one record in the SPECIMEN table' as check_description
# MAGIC  ,'SPECIMEN' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_person_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_measurepersoncompleteness_specimen' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 100 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 100 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,100 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.person_id) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC  LEFT JOIN ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable2 
# MAGIC  ON cdmTable.person_id = cdmTable2.person_id
# MAGIC  WHERE cdmTable2.person_id IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'measurePersonCompleteness' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'The number and percent of persons in the CDM that do not have at least one record in the PAYER_PLAN_PERIOD table' as check_description
# MAGIC  ,'PAYER_PLAN_PERIOD' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_person_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_measurepersoncompleteness_payer_plan_period' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 100 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 100 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,100 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.person_id) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC  LEFT JOIN ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable2 
# MAGIC  ON cdmTable.person_id = cdmTable2.person_id
# MAGIC  WHERE cdmTable2.person_id IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'measurePersonCompleteness' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'The number and percent of persons in the CDM that do not have at least one record in the DRUG_ERA table' as check_description
# MAGIC  ,'DRUG_ERA' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_person_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_measurepersoncompleteness_drug_era' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 95 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 95 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,95 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.person_id) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC  LEFT JOIN ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable2 
# MAGIC  ON cdmTable.person_id = cdmTable2.person_id
# MAGIC  WHERE cdmTable2.person_id IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'measurePersonCompleteness' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'The number and percent of persons in the CDM that do not have at least one record in the DOSE_ERA table' as check_description
# MAGIC  ,'DOSE_ERA' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_person_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_measurepersoncompleteness_dose_era' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 100 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 100 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,100 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.person_id) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC  LEFT JOIN ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable2 
# MAGIC  ON cdmTable.person_id = cdmTable2.person_id
# MAGIC  WHERE cdmTable2.person_id IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'measurePersonCompleteness' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'The number and percent of persons in the CDM that do not have at least one record in the CONDITION_ERA table' as check_description
# MAGIC  ,'CONDITION_ERA' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_person_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_measurepersoncompleteness_condition_era' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 95 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 95 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,95 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.person_id) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC  LEFT JOIN ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable2 
# MAGIC  ON cdmTable.person_id = cdmTable2.person_id
# MAGIC  WHERE cdmTable2.person_id IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'measurePersonCompleteness' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'The number and percent of persons in the CDM that do not have at least one record in the DEATH table' as check_description
# MAGIC  ,'DEATH' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_person_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_measurepersoncompleteness_death' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 100 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 100 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,100 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.person_id) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC  LEFT JOIN ${omop_catalog}.${omop_schema}.DEATH cdmTable2 
# MAGIC  ON cdmTable.person_id = cdmTable2.person_id
# MAGIC  WHERE cdmTable2.person_id IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.person cdmTable
# MAGIC ) denominator
# MAGIC ) cte
# MAGIC )
# MAGIC INSERT INTO ${results_catalog}.${results_schema}.dqdashboard_results
# MAGIC SELECT *
# MAGIC FROM cte_all;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- table cdm table
# MAGIC
# MAGIC WITH cte_all  AS (SELECT cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  , CAST('' as STRING) as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmTable' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'A yes or no value indicating if PERSON table is present as expected based on the specification.' as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_cdm_table.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_cdmtable_person' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  num_violated_rows 
# MAGIC  FROM
# MAGIC  (
# MAGIC  SELECT
# MAGIC  CASE 
# MAGIC  WHEN COUNT(*) = 0 THEN 0
# MAGIC  ELSE 0
# MAGIC  END AS num_violated_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 1 AS num_rows
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmTable' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'A yes or no value indicating if OBSERVATION_PERIOD table is present as expected based on the specification.' as check_description
# MAGIC  ,'OBSERVATION_PERIOD' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_cdm_table.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_cdmtable_observation_period' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  num_violated_rows 
# MAGIC  FROM
# MAGIC  (
# MAGIC  SELECT
# MAGIC  CASE 
# MAGIC  WHEN COUNT(*) = 0 THEN 0
# MAGIC  ELSE 0
# MAGIC  END AS num_violated_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD cdmTable
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 1 AS num_rows
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmTable' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'A yes or no value indicating if VISIT_OCCURRENCE table is present as expected based on the specification.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_cdm_table.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_cdmtable_visit_occurrence' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  num_violated_rows 
# MAGIC  FROM
# MAGIC  (
# MAGIC  SELECT
# MAGIC  CASE 
# MAGIC  WHEN COUNT(*) = 0 THEN 0
# MAGIC  ELSE 0
# MAGIC  END AS num_violated_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 1 AS num_rows
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmTable' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'A yes or no value indicating if CONDITION_OCCURRENCE table is present as expected based on the specification.' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_cdm_table.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_cdmtable_condition_occurrence' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  num_violated_rows 
# MAGIC  FROM
# MAGIC  (
# MAGIC  SELECT
# MAGIC  CASE 
# MAGIC  WHEN COUNT(*) = 0 THEN 0
# MAGIC  ELSE 0
# MAGIC  END AS num_violated_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 1 AS num_rows
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmTable' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'A yes or no value indicating if DRUG_EXPOSURE table is present as expected based on the specification.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_cdm_table.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_cdmtable_drug_exposure' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  num_violated_rows 
# MAGIC  FROM
# MAGIC  (
# MAGIC  SELECT
# MAGIC  CASE 
# MAGIC  WHEN COUNT(*) = 0 THEN 0
# MAGIC  ELSE 0
# MAGIC  END AS num_violated_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 1 AS num_rows
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmTable' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'A yes or no value indicating if PROCEDURE_OCCURRENCE table is present as expected based on the specification.' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_cdm_table.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_cdmtable_procedure_occurrence' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  num_violated_rows 
# MAGIC  FROM
# MAGIC  (
# MAGIC  SELECT
# MAGIC  CASE 
# MAGIC  WHEN COUNT(*) = 0 THEN 0
# MAGIC  ELSE 0
# MAGIC  END AS num_violated_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 1 AS num_rows
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmTable' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'A yes or no value indicating if DEVICE_EXPOSURE table is present as expected based on the specification.' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_cdm_table.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_cdmtable_device_exposure' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  num_violated_rows 
# MAGIC  FROM
# MAGIC  (
# MAGIC  SELECT
# MAGIC  CASE 
# MAGIC  WHEN COUNT(*) = 0 THEN 0
# MAGIC  ELSE 0
# MAGIC  END AS num_violated_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 1 AS num_rows
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmTable' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'A yes or no value indicating if MEASUREMENT table is present as expected based on the specification.' as check_description
# MAGIC  ,'MEASUREMENT' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_cdm_table.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_cdmtable_measurement' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  num_violated_rows 
# MAGIC  FROM
# MAGIC  (
# MAGIC  SELECT
# MAGIC  CASE 
# MAGIC  WHEN COUNT(*) = 0 THEN 0
# MAGIC  ELSE 0
# MAGIC  END AS num_violated_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 1 AS num_rows
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmTable' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'A yes or no value indicating if VISIT_DETAIL table is present as expected based on the specification.' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_cdm_table.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_cdmtable_visit_detail' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  num_violated_rows 
# MAGIC  FROM
# MAGIC  (
# MAGIC  SELECT
# MAGIC  CASE 
# MAGIC  WHEN COUNT(*) = 0 THEN 0
# MAGIC  ELSE 0
# MAGIC  END AS num_violated_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 1 AS num_rows
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmTable' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'A yes or no value indicating if NOTE table is present as expected based on the specification.' as check_description
# MAGIC  ,'NOTE' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_cdm_table.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_cdmtable_note' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  num_violated_rows 
# MAGIC  FROM
# MAGIC  (
# MAGIC  SELECT
# MAGIC  CASE 
# MAGIC  WHEN COUNT(*) = 0 THEN 0
# MAGIC  ELSE 0
# MAGIC  END AS num_violated_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 1 AS num_rows
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmTable' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'A yes or no value indicating if NOTE_NLP table is present as expected based on the specification.' as check_description
# MAGIC  ,'NOTE_NLP' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_cdm_table.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_cdmtable_note_nlp' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  num_violated_rows 
# MAGIC  FROM
# MAGIC  (
# MAGIC  SELECT
# MAGIC  CASE 
# MAGIC  WHEN COUNT(*) = 0 THEN 0
# MAGIC  ELSE 0
# MAGIC  END AS num_violated_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE_NLP cdmTable
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 1 AS num_rows
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmTable' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'A yes or no value indicating if OBSERVATION table is present as expected based on the specification.' as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_cdm_table.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_cdmtable_observation' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  num_violated_rows 
# MAGIC  FROM
# MAGIC  (
# MAGIC  SELECT
# MAGIC  CASE 
# MAGIC  WHEN COUNT(*) = 0 THEN 0
# MAGIC  ELSE 0
# MAGIC  END AS num_violated_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 1 AS num_rows
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmTable' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'A yes or no value indicating if SPECIMEN table is present as expected based on the specification.' as check_description
# MAGIC  ,'SPECIMEN' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_cdm_table.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_cdmtable_specimen' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  num_violated_rows 
# MAGIC  FROM
# MAGIC  (
# MAGIC  SELECT
# MAGIC  CASE 
# MAGIC  WHEN COUNT(*) = 0 THEN 0
# MAGIC  ELSE 0
# MAGIC  END AS num_violated_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 1 AS num_rows
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmTable' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'A yes or no value indicating if FACT_RELATIONSHIP table is present as expected based on the specification.' as check_description
# MAGIC  ,'FACT_RELATIONSHIP' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_cdm_table.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_cdmtable_fact_relationship' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  num_violated_rows 
# MAGIC  FROM
# MAGIC  (
# MAGIC  SELECT
# MAGIC  CASE 
# MAGIC  WHEN COUNT(*) = 0 THEN 0
# MAGIC  ELSE 0
# MAGIC  END AS num_violated_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.FACT_RELATIONSHIP cdmTable
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 1 AS num_rows
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmTable' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'A yes or no value indicating if LOCATION table is present as expected based on the specification.' as check_description
# MAGIC  ,'LOCATION' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_cdm_table.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_cdmtable_location' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  num_violated_rows 
# MAGIC  FROM
# MAGIC  (
# MAGIC  SELECT
# MAGIC  CASE 
# MAGIC  WHEN COUNT(*) = 0 THEN 0
# MAGIC  ELSE 0
# MAGIC  END AS num_violated_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.LOCATION cdmTable
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 1 AS num_rows
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmTable' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'A yes or no value indicating if CARE_SITE table is present as expected based on the specification.' as check_description
# MAGIC  ,'CARE_SITE' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_cdm_table.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_cdmtable_care_site' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  num_violated_rows 
# MAGIC  FROM
# MAGIC  (
# MAGIC  SELECT
# MAGIC  CASE 
# MAGIC  WHEN COUNT(*) = 0 THEN 0
# MAGIC  ELSE 0
# MAGIC  END AS num_violated_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CARE_SITE cdmTable
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 1 AS num_rows
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmTable' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'A yes or no value indicating if PROVIDER table is present as expected based on the specification.' as check_description
# MAGIC  ,'PROVIDER' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_cdm_table.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_cdmtable_provider' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  num_violated_rows 
# MAGIC  FROM
# MAGIC  (
# MAGIC  SELECT
# MAGIC  CASE 
# MAGIC  WHEN COUNT(*) = 0 THEN 0
# MAGIC  ELSE 0
# MAGIC  END AS num_violated_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROVIDER cdmTable
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 1 AS num_rows
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmTable' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'A yes or no value indicating if PAYER_PLAN_PERIOD table is present as expected based on the specification.' as check_description
# MAGIC  ,'PAYER_PLAN_PERIOD' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_cdm_table.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_cdmtable_payer_plan_period' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  num_violated_rows 
# MAGIC  FROM
# MAGIC  (
# MAGIC  SELECT
# MAGIC  CASE 
# MAGIC  WHEN COUNT(*) = 0 THEN 0
# MAGIC  ELSE 0
# MAGIC  END AS num_violated_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 1 AS num_rows
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmTable' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'A yes or no value indicating if COST table is present as expected based on the specification.' as check_description
# MAGIC  ,'COST' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_cdm_table.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_cdmtable_cost' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  num_violated_rows 
# MAGIC  FROM
# MAGIC  (
# MAGIC  SELECT
# MAGIC  CASE 
# MAGIC  WHEN COUNT(*) = 0 THEN 0
# MAGIC  ELSE 0
# MAGIC  END AS num_violated_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.COST cdmTable
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 1 AS num_rows
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmTable' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'A yes or no value indicating if DRUG_ERA table is present as expected based on the specification.' as check_description
# MAGIC  ,'DRUG_ERA' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_cdm_table.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_cdmtable_drug_era' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  num_violated_rows 
# MAGIC  FROM
# MAGIC  (
# MAGIC  SELECT
# MAGIC  CASE 
# MAGIC  WHEN COUNT(*) = 0 THEN 0
# MAGIC  ELSE 0
# MAGIC  END AS num_violated_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 1 AS num_rows
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmTable' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'A yes or no value indicating if DOSE_ERA table is present as expected based on the specification.' as check_description
# MAGIC  ,'DOSE_ERA' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_cdm_table.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_cdmtable_dose_era' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  num_violated_rows 
# MAGIC  FROM
# MAGIC  (
# MAGIC  SELECT
# MAGIC  CASE 
# MAGIC  WHEN COUNT(*) = 0 THEN 0
# MAGIC  ELSE 0
# MAGIC  END AS num_violated_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 1 AS num_rows
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmTable' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'A yes or no value indicating if CONDITION_ERA table is present as expected based on the specification.' as check_description
# MAGIC  ,'CONDITION_ERA' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_cdm_table.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_cdmtable_condition_era' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  num_violated_rows 
# MAGIC  FROM
# MAGIC  (
# MAGIC  SELECT
# MAGIC  CASE 
# MAGIC  WHEN COUNT(*) = 0 THEN 0
# MAGIC  ELSE 0
# MAGIC  END AS num_violated_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 1 AS num_rows
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmTable' as check_name
# MAGIC  ,'TABLE' as check_level
# MAGIC  ,'A yes or no value indicating if DEATH table is present as expected based on the specification.' as check_description
# MAGIC  ,'DEATH' as cdm_table_name
# MAGIC  ,'NA' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'table_cdm_table.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'table_cdmtable_death' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  num_violated_rows 
# MAGIC  FROM
# MAGIC  (
# MAGIC  SELECT
# MAGIC  CASE 
# MAGIC  WHEN COUNT(*) = 0 THEN 0
# MAGIC  ELSE 0
# MAGIC  END AS num_violated_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEATH cdmTable
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 1 AS num_rows
# MAGIC ) denominator
# MAGIC ) cte
# MAGIC )
# MAGIC INSERT INTO ${results_catalog}.${results_schema}.dqdashboard_results
# MAGIC SELECT *
# MAGIC FROM cte_all;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- field is required
# MAGIC
# MAGIC WITH cte_all  AS (SELECT cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  , CAST('' as STRING) as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the CARE_SITE_ID of the CARE_SITE that is considered not nullable.' as check_description
# MAGIC  ,'CARE_SITE' as cdm_table_name
# MAGIC  ,'CARE_SITE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_care_site_care_site_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'CARE_SITE.CARE_SITE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CARE_SITE cdmTable
# MAGIC  WHERE cdmTable.CARE_SITE_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CARE_SITE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the CONDITION_ERA_ID of the CONDITION_ERA that is considered not nullable.' as check_description
# MAGIC  ,'CONDITION_ERA' as cdm_table_name
# MAGIC  ,'CONDITION_ERA_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_condition_era_condition_era_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'CONDITION_ERA.CONDITION_ERA_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable
# MAGIC  WHERE cdmTable.CONDITION_ERA_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the PERSON_ID of the CONDITION_ERA that is considered not nullable.' as check_description
# MAGIC  ,'CONDITION_ERA' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_condition_era_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'CONDITION_ERA.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable
# MAGIC  WHERE cdmTable.PERSON_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the CONDITION_CONCEPT_ID of the CONDITION_ERA that is considered not nullable.' as check_description
# MAGIC  ,'CONDITION_ERA' as cdm_table_name
# MAGIC  ,'CONDITION_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_condition_era_condition_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'CONDITION_ERA.CONDITION_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable
# MAGIC  WHERE cdmTable.CONDITION_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the CONDITION_ERA_START_DATE of the CONDITION_ERA that is considered not nullable.' as check_description
# MAGIC  ,'CONDITION_ERA' as cdm_table_name
# MAGIC  ,'CONDITION_ERA_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_condition_era_condition_era_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'CONDITION_ERA.CONDITION_ERA_START_DATE' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable
# MAGIC  WHERE cdmTable.CONDITION_ERA_START_DATE IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the CONDITION_ERA_END_DATE of the CONDITION_ERA that is considered not nullable.' as check_description
# MAGIC  ,'CONDITION_ERA' as cdm_table_name
# MAGIC  ,'CONDITION_ERA_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_condition_era_condition_era_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'CONDITION_ERA.CONDITION_ERA_END_DATE' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable
# MAGIC  WHERE cdmTable.CONDITION_ERA_END_DATE IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the CONDITION_OCCURRENCE_ID of the CONDITION_OCCURRENCE that is considered not nullable.' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_OCCURRENCE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_condition_occurrence_condition_occurrence_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'CONDITION_OCCURRENCE.CONDITION_OCCURRENCE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.CONDITION_OCCURRENCE_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the PERSON_ID of the CONDITION_OCCURRENCE that is considered not nullable.' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_condition_occurrence_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'CONDITION_OCCURRENCE.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.PERSON_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the CONDITION_CONCEPT_ID of the CONDITION_OCCURRENCE that is considered not nullable.' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_condition_occurrence_condition_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'CONDITION_OCCURRENCE.CONDITION_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.CONDITION_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the CONDITION_START_DATE of the CONDITION_OCCURRENCE that is considered not nullable.' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_condition_occurrence_condition_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'CONDITION_OCCURRENCE.CONDITION_START_DATE' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.CONDITION_START_DATE IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the CONDITION_TYPE_CONCEPT_ID of the CONDITION_OCCURRENCE that is considered not nullable.' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_condition_occurrence_condition_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'CONDITION_OCCURRENCE.CONDITION_TYPE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.CONDITION_TYPE_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the COST_ID of the COST that is considered not nullable.' as check_description
# MAGIC  ,'COST' as cdm_table_name
# MAGIC  ,'COST_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_cost_cost_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'COST.COST_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.COST cdmTable
# MAGIC  WHERE cdmTable.COST_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.COST cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the COST_EVENT_ID of the COST that is considered not nullable.' as check_description
# MAGIC  ,'COST' as cdm_table_name
# MAGIC  ,'COST_EVENT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_cost_cost_event_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'COST.COST_EVENT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.COST cdmTable
# MAGIC  WHERE cdmTable.COST_EVENT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.COST cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the COST_DOMAIN_ID of the COST that is considered not nullable.' as check_description
# MAGIC  ,'COST' as cdm_table_name
# MAGIC  ,'COST_DOMAIN_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_cost_cost_domain_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'COST.COST_DOMAIN_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.COST cdmTable
# MAGIC  WHERE cdmTable.COST_DOMAIN_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.COST cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the COST_TYPE_CONCEPT_ID of the COST that is considered not nullable.' as check_description
# MAGIC  ,'COST' as cdm_table_name
# MAGIC  ,'COST_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_cost_cost_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'COST.COST_TYPE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.COST cdmTable
# MAGIC  WHERE cdmTable.COST_TYPE_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.COST cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the DEATH_DATE of the DEATH that is considered not nullable.' as check_description
# MAGIC  ,'DEATH' as cdm_table_name
# MAGIC  ,'DEATH_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_death_death_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DEATH.DEATH_DATE' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEATH cdmTable
# MAGIC  WHERE cdmTable.DEATH_DATE IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEATH cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the PERSON_ID of the DEATH that is considered not nullable.' as check_description
# MAGIC  ,'DEATH' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_death_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DEATH.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEATH cdmTable
# MAGIC  WHERE cdmTable.PERSON_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEATH cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the DEVICE_EXPOSURE_ID of the DEVICE_EXPOSURE that is considered not nullable.' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'DEVICE_EXPOSURE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_device_exposure_device_exposure_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DEVICE_EXPOSURE.DEVICE_EXPOSURE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE cdmTable.DEVICE_EXPOSURE_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the PERSON_ID of the DEVICE_EXPOSURE that is considered not nullable.' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_device_exposure_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DEVICE_EXPOSURE.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE cdmTable.PERSON_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the DEVICE_CONCEPT_ID of the DEVICE_EXPOSURE that is considered not nullable.' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'DEVICE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_device_exposure_device_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DEVICE_EXPOSURE.DEVICE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE cdmTable.DEVICE_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the DEVICE_EXPOSURE_START_DATE of the DEVICE_EXPOSURE that is considered not nullable.' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'DEVICE_EXPOSURE_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_device_exposure_device_exposure_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DEVICE_EXPOSURE.DEVICE_EXPOSURE_START_DATE' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE cdmTable.DEVICE_EXPOSURE_START_DATE IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the DEVICE_TYPE_CONCEPT_ID of the DEVICE_EXPOSURE that is considered not nullable.' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'DEVICE_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_device_exposure_device_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DEVICE_EXPOSURE.DEVICE_TYPE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE cdmTable.DEVICE_TYPE_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the DOSE_ERA_ID of the DOSE_ERA that is considered not nullable.' as check_description
# MAGIC  ,'DOSE_ERA' as cdm_table_name
# MAGIC  ,'DOSE_ERA_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_dose_era_dose_era_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DOSE_ERA.DOSE_ERA_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC  WHERE cdmTable.DOSE_ERA_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the PERSON_ID of the DOSE_ERA that is considered not nullable.' as check_description
# MAGIC  ,'DOSE_ERA' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_dose_era_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DOSE_ERA.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC  WHERE cdmTable.PERSON_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the DRUG_CONCEPT_ID of the DOSE_ERA that is considered not nullable.' as check_description
# MAGIC  ,'DOSE_ERA' as cdm_table_name
# MAGIC  ,'DRUG_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_dose_era_drug_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DOSE_ERA.DRUG_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC  WHERE cdmTable.DRUG_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the UNIT_CONCEPT_ID of the DOSE_ERA that is considered not nullable.' as check_description
# MAGIC  ,'DOSE_ERA' as cdm_table_name
# MAGIC  ,'UNIT_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_dose_era_unit_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DOSE_ERA.UNIT_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC  WHERE cdmTable.UNIT_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the DOSE_VALUE of the DOSE_ERA that is considered not nullable.' as check_description
# MAGIC  ,'DOSE_ERA' as cdm_table_name
# MAGIC  ,'DOSE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_dose_era_dose_value' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DOSE_ERA.DOSE_VALUE' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC  WHERE cdmTable.DOSE_VALUE IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the DOSE_ERA_START_DATE of the DOSE_ERA that is considered not nullable.' as check_description
# MAGIC  ,'DOSE_ERA' as cdm_table_name
# MAGIC  ,'DOSE_ERA_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_dose_era_dose_era_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DOSE_ERA.DOSE_ERA_START_DATE' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC  WHERE cdmTable.DOSE_ERA_START_DATE IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the DOSE_ERA_END_DATE of the DOSE_ERA that is considered not nullable.' as check_description
# MAGIC  ,'DOSE_ERA' as cdm_table_name
# MAGIC  ,'DOSE_ERA_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_dose_era_dose_era_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DOSE_ERA.DOSE_ERA_END_DATE' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC  WHERE cdmTable.DOSE_ERA_END_DATE IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the DRUG_ERA_ID of the DRUG_ERA that is considered not nullable.' as check_description
# MAGIC  ,'DRUG_ERA' as cdm_table_name
# MAGIC  ,'DRUG_ERA_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_drug_era_drug_era_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DRUG_ERA.DRUG_ERA_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC  WHERE cdmTable.DRUG_ERA_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the PERSON_ID of the DRUG_ERA that is considered not nullable.' as check_description
# MAGIC  ,'DRUG_ERA' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_drug_era_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DRUG_ERA.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC  WHERE cdmTable.PERSON_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the DRUG_CONCEPT_ID of the DRUG_ERA that is considered not nullable.' as check_description
# MAGIC  ,'DRUG_ERA' as cdm_table_name
# MAGIC  ,'DRUG_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_drug_era_drug_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DRUG_ERA.DRUG_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC  WHERE cdmTable.DRUG_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the DRUG_ERA_START_DATE of the DRUG_ERA that is considered not nullable.' as check_description
# MAGIC  ,'DRUG_ERA' as cdm_table_name
# MAGIC  ,'DRUG_ERA_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_drug_era_drug_era_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DRUG_ERA.DRUG_ERA_START_DATE' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC  WHERE cdmTable.DRUG_ERA_START_DATE IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the DRUG_ERA_END_DATE of the DRUG_ERA that is considered not nullable.' as check_description
# MAGIC  ,'DRUG_ERA' as cdm_table_name
# MAGIC  ,'DRUG_ERA_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_drug_era_drug_era_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DRUG_ERA.DRUG_ERA_END_DATE' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC  WHERE cdmTable.DRUG_ERA_END_DATE IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the DRUG_EXPOSURE_ID of the DRUG_EXPOSURE that is considered not nullable.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DRUG_EXPOSURE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_drug_exposure_drug_exposure_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DRUG_EXPOSURE.DRUG_EXPOSURE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE cdmTable.DRUG_EXPOSURE_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the PERSON_ID of the DRUG_EXPOSURE that is considered not nullable.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_drug_exposure_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DRUG_EXPOSURE.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE cdmTable.PERSON_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the DRUG_CONCEPT_ID of the DRUG_EXPOSURE that is considered not nullable.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DRUG_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_drug_exposure_drug_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DRUG_EXPOSURE.DRUG_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE cdmTable.DRUG_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the DRUG_EXPOSURE_START_DATE of the DRUG_EXPOSURE that is considered not nullable.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DRUG_EXPOSURE_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_drug_exposure_drug_exposure_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DRUG_EXPOSURE.DRUG_EXPOSURE_START_DATE' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE cdmTable.DRUG_EXPOSURE_START_DATE IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the DRUG_EXPOSURE_END_DATE of the DRUG_EXPOSURE that is considered not nullable.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DRUG_EXPOSURE_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_drug_exposure_drug_exposure_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DRUG_EXPOSURE.DRUG_EXPOSURE_END_DATE' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE cdmTable.DRUG_EXPOSURE_END_DATE IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the DRUG_TYPE_CONCEPT_ID of the DRUG_EXPOSURE that is considered not nullable.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DRUG_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_drug_exposure_drug_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DRUG_EXPOSURE.DRUG_TYPE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE cdmTable.DRUG_TYPE_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the DOMAIN_CONCEPT_ID_1 of the FACT_RELATIONSHIP that is considered not nullable.' as check_description
# MAGIC  ,'FACT_RELATIONSHIP' as cdm_table_name
# MAGIC  ,'DOMAIN_CONCEPT_ID_1' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_fact_relationship_domain_concept_id_1' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'FACT_RELATIONSHIP.DOMAIN_CONCEPT_ID_1' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.FACT_RELATIONSHIP cdmTable
# MAGIC  WHERE cdmTable.DOMAIN_CONCEPT_ID_1 IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.FACT_RELATIONSHIP cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the FACT_ID_1 of the FACT_RELATIONSHIP that is considered not nullable.' as check_description
# MAGIC  ,'FACT_RELATIONSHIP' as cdm_table_name
# MAGIC  ,'FACT_ID_1' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_fact_relationship_fact_id_1' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'FACT_RELATIONSHIP.FACT_ID_1' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.FACT_RELATIONSHIP cdmTable
# MAGIC  WHERE cdmTable.FACT_ID_1 IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.FACT_RELATIONSHIP cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the DOMAIN_CONCEPT_ID_2 of the FACT_RELATIONSHIP that is considered not nullable.' as check_description
# MAGIC  ,'FACT_RELATIONSHIP' as cdm_table_name
# MAGIC  ,'DOMAIN_CONCEPT_ID_2' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_fact_relationship_domain_concept_id_2' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'FACT_RELATIONSHIP.DOMAIN_CONCEPT_ID_2' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.FACT_RELATIONSHIP cdmTable
# MAGIC  WHERE cdmTable.DOMAIN_CONCEPT_ID_2 IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.FACT_RELATIONSHIP cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the FACT_ID_2 of the FACT_RELATIONSHIP that is considered not nullable.' as check_description
# MAGIC  ,'FACT_RELATIONSHIP' as cdm_table_name
# MAGIC  ,'FACT_ID_2' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_fact_relationship_fact_id_2' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'FACT_RELATIONSHIP.FACT_ID_2' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.FACT_RELATIONSHIP cdmTable
# MAGIC  WHERE cdmTable.FACT_ID_2 IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.FACT_RELATIONSHIP cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the RELATIONSHIP_CONCEPT_ID of the FACT_RELATIONSHIP that is considered not nullable.' as check_description
# MAGIC  ,'FACT_RELATIONSHIP' as cdm_table_name
# MAGIC  ,'RELATIONSHIP_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_fact_relationship_relationship_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'FACT_RELATIONSHIP.RELATIONSHIP_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.FACT_RELATIONSHIP cdmTable
# MAGIC  WHERE cdmTable.RELATIONSHIP_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.FACT_RELATIONSHIP cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the LOCATION_ID of the LOCATION that is considered not nullable.' as check_description
# MAGIC  ,'LOCATION' as cdm_table_name
# MAGIC  ,'LOCATION_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_location_location_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'LOCATION.LOCATION_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.LOCATION cdmTable
# MAGIC  WHERE cdmTable.LOCATION_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.LOCATION cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the MEASUREMENT_ID of the MEASUREMENT that is considered not nullable.' as check_description
# MAGIC  ,'MEASUREMENT' as cdm_table_name
# MAGIC  ,'MEASUREMENT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_measurement_measurement_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'MEASUREMENT.MEASUREMENT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC  WHERE cdmTable.MEASUREMENT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the PERSON_ID of the MEASUREMENT that is considered not nullable.' as check_description
# MAGIC  ,'MEASUREMENT' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_measurement_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'MEASUREMENT.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC  WHERE cdmTable.PERSON_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the MEASUREMENT_CONCEPT_ID of the MEASUREMENT that is considered not nullable.' as check_description
# MAGIC  ,'MEASUREMENT' as cdm_table_name
# MAGIC  ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_measurement_measurement_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'MEASUREMENT.MEASUREMENT_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC  WHERE cdmTable.MEASUREMENT_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the MEASUREMENT_DATE of the MEASUREMENT that is considered not nullable.' as check_description
# MAGIC  ,'MEASUREMENT' as cdm_table_name
# MAGIC  ,'MEASUREMENT_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_measurement_measurement_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'MEASUREMENT.MEASUREMENT_DATE' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC  WHERE cdmTable.MEASUREMENT_DATE IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the MEASUREMENT_TYPE_CONCEPT_ID of the MEASUREMENT that is considered not nullable.' as check_description
# MAGIC  ,'MEASUREMENT' as cdm_table_name
# MAGIC  ,'MEASUREMENT_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_measurement_measurement_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'MEASUREMENT.MEASUREMENT_TYPE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC  WHERE cdmTable.MEASUREMENT_TYPE_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the NOTE_ID of the NOTE that is considered not nullable.' as check_description
# MAGIC  ,'NOTE' as cdm_table_name
# MAGIC  ,'NOTE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_note_note_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'NOTE.NOTE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC  WHERE cdmTable.NOTE_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the PERSON_ID of the NOTE that is considered not nullable.' as check_description
# MAGIC  ,'NOTE' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_note_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'NOTE.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC  WHERE cdmTable.PERSON_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the NOTE_DATE of the NOTE that is considered not nullable.' as check_description
# MAGIC  ,'NOTE' as cdm_table_name
# MAGIC  ,'NOTE_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_note_note_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'NOTE.NOTE_DATE' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC  WHERE cdmTable.NOTE_DATE IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the NOTE_TYPE_CONCEPT_ID of the NOTE that is considered not nullable.' as check_description
# MAGIC  ,'NOTE' as cdm_table_name
# MAGIC  ,'NOTE_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_note_note_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'NOTE.NOTE_TYPE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC  WHERE cdmTable.NOTE_TYPE_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the NOTE_CLASS_CONCEPT_ID of the NOTE that is considered not nullable.' as check_description
# MAGIC  ,'NOTE' as cdm_table_name
# MAGIC  ,'NOTE_CLASS_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_note_note_class_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'NOTE.NOTE_CLASS_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC  WHERE cdmTable.NOTE_CLASS_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the NOTE_TEXT of the NOTE that is considered not nullable.' as check_description
# MAGIC  ,'NOTE' as cdm_table_name
# MAGIC  ,'NOTE_TEXT' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_note_note_text' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'NOTE.NOTE_TEXT' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC  WHERE cdmTable.NOTE_TEXT IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the ENCODING_CONCEPT_ID of the NOTE that is considered not nullable.' as check_description
# MAGIC  ,'NOTE' as cdm_table_name
# MAGIC  ,'ENCODING_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_note_encoding_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'NOTE.ENCODING_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC  WHERE cdmTable.ENCODING_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the LANGUAGE_CONCEPT_ID of the NOTE that is considered not nullable.' as check_description
# MAGIC  ,'NOTE' as cdm_table_name
# MAGIC  ,'LANGUAGE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_note_language_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'NOTE.LANGUAGE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC  WHERE cdmTable.LANGUAGE_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the NOTE_NLP_ID of the NOTE_NLP that is considered not nullable.' as check_description
# MAGIC  ,'NOTE_NLP' as cdm_table_name
# MAGIC  ,'NOTE_NLP_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_note_nlp_note_nlp_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'NOTE_NLP.NOTE_NLP_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE_NLP cdmTable
# MAGIC  WHERE cdmTable.NOTE_NLP_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE_NLP cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the NOTE_ID of the NOTE_NLP that is considered not nullable.' as check_description
# MAGIC  ,'NOTE_NLP' as cdm_table_name
# MAGIC  ,'NOTE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_note_nlp_note_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'NOTE_NLP.NOTE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE_NLP cdmTable
# MAGIC  WHERE cdmTable.NOTE_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE_NLP cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the LEXICAL_VARIANT of the NOTE_NLP that is considered not nullable.' as check_description
# MAGIC  ,'NOTE_NLP' as cdm_table_name
# MAGIC  ,'LEXICAL_VARIANT' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_note_nlp_lexical_variant' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'NOTE_NLP.LEXICAL_VARIANT' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE_NLP cdmTable
# MAGIC  WHERE cdmTable.LEXICAL_VARIANT IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE_NLP cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the NLP_DATE of the NOTE_NLP that is considered not nullable.' as check_description
# MAGIC  ,'NOTE_NLP' as cdm_table_name
# MAGIC  ,'NLP_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_note_nlp_nlp_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'NOTE_NLP.NLP_DATE' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE_NLP cdmTable
# MAGIC  WHERE cdmTable.NLP_DATE IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE_NLP cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the OBSERVATION_ID of the OBSERVATION that is considered not nullable.' as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'OBSERVATION_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_observation_observation_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'OBSERVATION.OBSERVATION_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  WHERE cdmTable.OBSERVATION_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the PERSON_ID of the OBSERVATION that is considered not nullable.' as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_observation_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'OBSERVATION.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  WHERE cdmTable.PERSON_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the OBSERVATION_CONCEPT_ID of the OBSERVATION that is considered not nullable.' as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'OBSERVATION_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_observation_observation_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'OBSERVATION.OBSERVATION_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  WHERE cdmTable.OBSERVATION_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the OBSERVATION_DATE of the OBSERVATION that is considered not nullable.' as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'OBSERVATION_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_observation_observation_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'OBSERVATION.OBSERVATION_DATE' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  WHERE cdmTable.OBSERVATION_DATE IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the OBSERVATION_TYPE_CONCEPT_ID of the OBSERVATION that is considered not nullable.' as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'OBSERVATION_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_observation_observation_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'OBSERVATION.OBSERVATION_TYPE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  WHERE cdmTable.OBSERVATION_TYPE_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the OBSERVATION_PERIOD_ID of the OBSERVATION_PERIOD that is considered not nullable.' as check_description
# MAGIC  ,'OBSERVATION_PERIOD' as cdm_table_name
# MAGIC  ,'OBSERVATION_PERIOD_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_observation_period_observation_period_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'OBSERVATION_PERIOD.OBSERVATION_PERIOD_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD cdmTable
# MAGIC  WHERE cdmTable.OBSERVATION_PERIOD_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the PERSON_ID of the OBSERVATION_PERIOD that is considered not nullable.' as check_description
# MAGIC  ,'OBSERVATION_PERIOD' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_observation_period_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'OBSERVATION_PERIOD.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD cdmTable
# MAGIC  WHERE cdmTable.PERSON_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the OBSERVATION_PERIOD_START_DATE of the OBSERVATION_PERIOD that is considered not nullable.' as check_description
# MAGIC  ,'OBSERVATION_PERIOD' as cdm_table_name
# MAGIC  ,'OBSERVATION_PERIOD_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_observation_period_observation_period_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'OBSERVATION_PERIOD.OBSERVATION_PERIOD_START_DATE' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD cdmTable
# MAGIC  WHERE cdmTable.OBSERVATION_PERIOD_START_DATE IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the OBSERVATION_PERIOD_END_DATE of the OBSERVATION_PERIOD that is considered not nullable.' as check_description
# MAGIC  ,'OBSERVATION_PERIOD' as cdm_table_name
# MAGIC  ,'OBSERVATION_PERIOD_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_observation_period_observation_period_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'OBSERVATION_PERIOD.OBSERVATION_PERIOD_END_DATE' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD cdmTable
# MAGIC  WHERE cdmTable.OBSERVATION_PERIOD_END_DATE IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the PERIOD_TYPE_CONCEPT_ID of the OBSERVATION_PERIOD that is considered not nullable.' as check_description
# MAGIC  ,'OBSERVATION_PERIOD' as cdm_table_name
# MAGIC  ,'PERIOD_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_observation_period_period_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'OBSERVATION_PERIOD.PERIOD_TYPE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD cdmTable
# MAGIC  WHERE cdmTable.PERIOD_TYPE_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the PAYER_PLAN_PERIOD_ID of the PAYER_PLAN_PERIOD that is considered not nullable.' as check_description
# MAGIC  ,'PAYER_PLAN_PERIOD' as cdm_table_name
# MAGIC  ,'PAYER_PLAN_PERIOD_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_payer_plan_period_payer_plan_period_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PAYER_PLAN_PERIOD.PAYER_PLAN_PERIOD_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC  WHERE cdmTable.PAYER_PLAN_PERIOD_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the PERSON_ID of the PAYER_PLAN_PERIOD that is considered not nullable.' as check_description
# MAGIC  ,'PAYER_PLAN_PERIOD' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_payer_plan_period_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PAYER_PLAN_PERIOD.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC  WHERE cdmTable.PERSON_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the PAYER_PLAN_PERIOD_START_DATE of the PAYER_PLAN_PERIOD that is considered not nullable.' as check_description
# MAGIC  ,'PAYER_PLAN_PERIOD' as cdm_table_name
# MAGIC  ,'PAYER_PLAN_PERIOD_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_payer_plan_period_payer_plan_period_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PAYER_PLAN_PERIOD.PAYER_PLAN_PERIOD_START_DATE' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC  WHERE cdmTable.PAYER_PLAN_PERIOD_START_DATE IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the PAYER_PLAN_PERIOD_END_DATE of the PAYER_PLAN_PERIOD that is considered not nullable.' as check_description
# MAGIC  ,'PAYER_PLAN_PERIOD' as cdm_table_name
# MAGIC  ,'PAYER_PLAN_PERIOD_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_payer_plan_period_payer_plan_period_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PAYER_PLAN_PERIOD.PAYER_PLAN_PERIOD_END_DATE' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC  WHERE cdmTable.PAYER_PLAN_PERIOD_END_DATE IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the PERSON_ID of the PERSON that is considered not nullable.' as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_person_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PERSON.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE cdmTable.PERSON_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the GENDER_CONCEPT_ID of the PERSON that is considered not nullable.' as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'GENDER_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_person_gender_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PERSON.GENDER_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE cdmTable.GENDER_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the YEAR_OF_BIRTH of the PERSON that is considered not nullable.' as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'YEAR_OF_BIRTH' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_person_year_of_birth' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PERSON.YEAR_OF_BIRTH' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE cdmTable.YEAR_OF_BIRTH IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the RACE_CONCEPT_ID of the PERSON that is considered not nullable.' as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'RACE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_person_race_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PERSON.RACE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE cdmTable.RACE_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the ETHNICITY_CONCEPT_ID of the PERSON that is considered not nullable.' as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'ETHNICITY_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_person_ethnicity_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PERSON.ETHNICITY_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE cdmTable.ETHNICITY_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the PROCEDURE_OCCURRENCE_ID of the PROCEDURE_OCCURRENCE that is considered not nullable.' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'PROCEDURE_OCCURRENCE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_procedure_occurrence_procedure_occurrence_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PROCEDURE_OCCURRENCE.PROCEDURE_OCCURRENCE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.PROCEDURE_OCCURRENCE_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the PERSON_ID of the PROCEDURE_OCCURRENCE that is considered not nullable.' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_procedure_occurrence_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PROCEDURE_OCCURRENCE.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.PERSON_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the PROCEDURE_CONCEPT_ID of the PROCEDURE_OCCURRENCE that is considered not nullable.' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_procedure_occurrence_procedure_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PROCEDURE_OCCURRENCE.PROCEDURE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.PROCEDURE_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the PROCEDURE_DATE of the PROCEDURE_OCCURRENCE that is considered not nullable.' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'PROCEDURE_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_procedure_occurrence_procedure_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PROCEDURE_OCCURRENCE.PROCEDURE_DATE' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.PROCEDURE_DATE IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the PROCEDURE_TYPE_CONCEPT_ID of the PROCEDURE_OCCURRENCE that is considered not nullable.' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'PROCEDURE_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_procedure_occurrence_procedure_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PROCEDURE_OCCURRENCE.PROCEDURE_TYPE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.PROCEDURE_TYPE_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the PROVIDER_ID of the PROVIDER that is considered not nullable.' as check_description
# MAGIC  ,'PROVIDER' as cdm_table_name
# MAGIC  ,'PROVIDER_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_provider_provider_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PROVIDER.PROVIDER_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROVIDER cdmTable
# MAGIC  WHERE cdmTable.PROVIDER_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROVIDER cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the SPECIMEN_ID of the SPECIMEN that is considered not nullable.' as check_description
# MAGIC  ,'SPECIMEN' as cdm_table_name
# MAGIC  ,'SPECIMEN_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_specimen_specimen_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'SPECIMEN.SPECIMEN_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC  WHERE cdmTable.SPECIMEN_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the PERSON_ID of the SPECIMEN that is considered not nullable.' as check_description
# MAGIC  ,'SPECIMEN' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_specimen_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'SPECIMEN.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC  WHERE cdmTable.PERSON_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the SPECIMEN_CONCEPT_ID of the SPECIMEN that is considered not nullable.' as check_description
# MAGIC  ,'SPECIMEN' as cdm_table_name
# MAGIC  ,'SPECIMEN_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_specimen_specimen_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'SPECIMEN.SPECIMEN_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC  WHERE cdmTable.SPECIMEN_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the SPECIMEN_TYPE_CONCEPT_ID of the SPECIMEN that is considered not nullable.' as check_description
# MAGIC  ,'SPECIMEN' as cdm_table_name
# MAGIC  ,'SPECIMEN_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_specimen_specimen_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'SPECIMEN.SPECIMEN_TYPE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC  WHERE cdmTable.SPECIMEN_TYPE_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the SPECIMEN_DATE of the SPECIMEN that is considered not nullable.' as check_description
# MAGIC  ,'SPECIMEN' as cdm_table_name
# MAGIC  ,'SPECIMEN_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_specimen_specimen_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'SPECIMEN.SPECIMEN_DATE' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC  WHERE cdmTable.SPECIMEN_DATE IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the VISIT_DETAIL_ID of the VISIT_DETAIL that is considered not nullable.' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'VISIT_DETAIL_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_visit_detail_visit_detail_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_DETAIL.VISIT_DETAIL_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE cdmTable.VISIT_DETAIL_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the PERSON_ID of the VISIT_DETAIL that is considered not nullable.' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_visit_detail_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_DETAIL.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE cdmTable.PERSON_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the VISIT_DETAIL_CONCEPT_ID of the VISIT_DETAIL that is considered not nullable.' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'VISIT_DETAIL_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_visit_detail_visit_detail_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_DETAIL.VISIT_DETAIL_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE cdmTable.VISIT_DETAIL_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the VISIT_DETAIL_START_DATE of the VISIT_DETAIL that is considered not nullable.' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'VISIT_DETAIL_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_visit_detail_visit_detail_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_DETAIL.VISIT_DETAIL_START_DATE' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE cdmTable.VISIT_DETAIL_START_DATE IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the VISIT_DETAIL_END_DATE of the VISIT_DETAIL that is considered not nullable.' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'VISIT_DETAIL_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_visit_detail_visit_detail_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_DETAIL.VISIT_DETAIL_END_DATE' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE cdmTable.VISIT_DETAIL_END_DATE IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the VISIT_DETAIL_TYPE_CONCEPT_ID of the VISIT_DETAIL that is considered not nullable.' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'VISIT_DETAIL_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_visit_detail_visit_detail_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_DETAIL.VISIT_DETAIL_TYPE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE cdmTable.VISIT_DETAIL_TYPE_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the VISIT_OCCURRENCE_ID of the VISIT_DETAIL that is considered not nullable.' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'VISIT_OCCURRENCE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_visit_detail_visit_occurrence_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_DETAIL.VISIT_OCCURRENCE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE cdmTable.VISIT_OCCURRENCE_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC ) denominator
# MAGIC ) cte
# MAGIC )
# MAGIC INSERT INTO ${results_catalog}.${results_schema}.dqdashboard_results
# MAGIC SELECT *
# MAGIC FROM cte_all;
# MAGIC WITH cte_all  AS (SELECT cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  , CAST('' as STRING) as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the VISIT_OCCURRENCE_ID of the VISIT_OCCURRENCE that is considered not nullable.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'VISIT_OCCURRENCE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_visit_occurrence_visit_occurrence_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_OCCURRENCE.VISIT_OCCURRENCE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.VISIT_OCCURRENCE_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the PERSON_ID of the VISIT_OCCURRENCE that is considered not nullable.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_visit_occurrence_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_OCCURRENCE.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.PERSON_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the VISIT_CONCEPT_ID of the VISIT_OCCURRENCE that is considered not nullable.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'VISIT_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_visit_occurrence_visit_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_OCCURRENCE.VISIT_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.VISIT_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the VISIT_START_DATE of the VISIT_OCCURRENCE that is considered not nullable.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'VISIT_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_visit_occurrence_visit_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_OCCURRENCE.VISIT_START_DATE' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.VISIT_START_DATE IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the VISIT_END_DATE of the VISIT_OCCURRENCE that is considered not nullable.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'VISIT_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_visit_occurrence_visit_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_OCCURRENCE.VISIT_END_DATE' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.VISIT_END_DATE IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isRequired' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a NULL value in the VISIT_TYPE_CONCEPT_ID of the VISIT_OCCURRENCE that is considered not nullable.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'VISIT_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_not_nullable.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Validation' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isrequired_visit_occurrence_visit_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_OCCURRENCE.VISIT_TYPE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.VISIT_TYPE_CONCEPT_ID IS NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte
# MAGIC )
# MAGIC INSERT INTO ${results_catalog}.${results_schema}.dqdashboard_results
# MAGIC SELECT *
# MAGIC FROM cte_all;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- field is primary key
# MAGIC
# MAGIC WITH cte_all  AS (SELECT cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  , CAST('' as STRING) as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isPrimaryKey' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records that have a duplicate value in the CARE_SITE_ID field of the CARE_SITE.' as check_description
# MAGIC  ,'CARE_SITE' as cdm_table_name
# MAGIC  ,'CARE_SITE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_primary_key.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isprimarykey_care_site_care_site_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'CARE_SITE.CARE_SITE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CARE_SITE cdmTable
# MAGIC  WHERE cdmTable.CARE_SITE_ID IN ( 
# MAGIC  SELECT 
# MAGIC  CARE_SITE_ID 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CARE_SITE
# MAGIC  GROUP BY CARE_SITE_ID
# MAGIC  HAVING COUNT(*) > 1 
# MAGIC  )
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CARE_SITE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isPrimaryKey' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records that have a duplicate value in the CONDITION_ERA_ID field of the CONDITION_ERA.' as check_description
# MAGIC  ,'CONDITION_ERA' as cdm_table_name
# MAGIC  ,'CONDITION_ERA_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_primary_key.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isprimarykey_condition_era_condition_era_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'CONDITION_ERA.CONDITION_ERA_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable
# MAGIC  WHERE cdmTable.CONDITION_ERA_ID IN ( 
# MAGIC  SELECT 
# MAGIC  CONDITION_ERA_ID 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA
# MAGIC  GROUP BY CONDITION_ERA_ID
# MAGIC  HAVING COUNT(*) > 1 
# MAGIC  )
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isPrimaryKey' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records that have a duplicate value in the CONDITION_OCCURRENCE_ID field of the CONDITION_OCCURRENCE.' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_OCCURRENCE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_primary_key.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isprimarykey_condition_occurrence_condition_occurrence_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'CONDITION_OCCURRENCE.CONDITION_OCCURRENCE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.CONDITION_OCCURRENCE_ID IN ( 
# MAGIC  SELECT 
# MAGIC  CONDITION_OCCURRENCE_ID 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE
# MAGIC  GROUP BY CONDITION_OCCURRENCE_ID
# MAGIC  HAVING COUNT(*) > 1 
# MAGIC  )
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isPrimaryKey' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records that have a duplicate value in the COST_ID field of the COST.' as check_description
# MAGIC  ,'COST' as cdm_table_name
# MAGIC  ,'COST_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_primary_key.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isprimarykey_cost_cost_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'COST.COST_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.COST cdmTable
# MAGIC  WHERE cdmTable.COST_ID IN ( 
# MAGIC  SELECT 
# MAGIC  COST_ID 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.COST
# MAGIC  GROUP BY COST_ID
# MAGIC  HAVING COUNT(*) > 1 
# MAGIC  )
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.COST cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isPrimaryKey' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records that have a duplicate value in the DEVICE_EXPOSURE_ID field of the DEVICE_EXPOSURE.' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'DEVICE_EXPOSURE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_primary_key.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isprimarykey_device_exposure_device_exposure_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DEVICE_EXPOSURE.DEVICE_EXPOSURE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE cdmTable.DEVICE_EXPOSURE_ID IN ( 
# MAGIC  SELECT 
# MAGIC  DEVICE_EXPOSURE_ID 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE
# MAGIC  GROUP BY DEVICE_EXPOSURE_ID
# MAGIC  HAVING COUNT(*) > 1 
# MAGIC  )
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isPrimaryKey' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records that have a duplicate value in the DOSE_ERA_ID field of the DOSE_ERA.' as check_description
# MAGIC  ,'DOSE_ERA' as cdm_table_name
# MAGIC  ,'DOSE_ERA_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_primary_key.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isprimarykey_dose_era_dose_era_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DOSE_ERA.DOSE_ERA_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC  WHERE cdmTable.DOSE_ERA_ID IN ( 
# MAGIC  SELECT 
# MAGIC  DOSE_ERA_ID 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA
# MAGIC  GROUP BY DOSE_ERA_ID
# MAGIC  HAVING COUNT(*) > 1 
# MAGIC  )
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isPrimaryKey' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records that have a duplicate value in the DRUG_ERA_ID field of the DRUG_ERA.' as check_description
# MAGIC  ,'DRUG_ERA' as cdm_table_name
# MAGIC  ,'DRUG_ERA_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_primary_key.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isprimarykey_drug_era_drug_era_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DRUG_ERA.DRUG_ERA_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC  WHERE cdmTable.DRUG_ERA_ID IN ( 
# MAGIC  SELECT 
# MAGIC  DRUG_ERA_ID 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA
# MAGIC  GROUP BY DRUG_ERA_ID
# MAGIC  HAVING COUNT(*) > 1 
# MAGIC  )
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isPrimaryKey' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records that have a duplicate value in the DRUG_EXPOSURE_ID field of the DRUG_EXPOSURE.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DRUG_EXPOSURE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_primary_key.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isprimarykey_drug_exposure_drug_exposure_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DRUG_EXPOSURE.DRUG_EXPOSURE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE cdmTable.DRUG_EXPOSURE_ID IN ( 
# MAGIC  SELECT 
# MAGIC  DRUG_EXPOSURE_ID 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE
# MAGIC  GROUP BY DRUG_EXPOSURE_ID
# MAGIC  HAVING COUNT(*) > 1 
# MAGIC  )
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isPrimaryKey' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records that have a duplicate value in the LOCATION_ID field of the LOCATION.' as check_description
# MAGIC  ,'LOCATION' as cdm_table_name
# MAGIC  ,'LOCATION_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_primary_key.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isprimarykey_location_location_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'LOCATION.LOCATION_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.LOCATION cdmTable
# MAGIC  WHERE cdmTable.LOCATION_ID IN ( 
# MAGIC  SELECT 
# MAGIC  LOCATION_ID 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.LOCATION
# MAGIC  GROUP BY LOCATION_ID
# MAGIC  HAVING COUNT(*) > 1 
# MAGIC  )
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.LOCATION cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isPrimaryKey' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records that have a duplicate value in the MEASUREMENT_ID field of the MEASUREMENT.' as check_description
# MAGIC  ,'MEASUREMENT' as cdm_table_name
# MAGIC  ,'MEASUREMENT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_primary_key.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isprimarykey_measurement_measurement_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'MEASUREMENT.MEASUREMENT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC  WHERE cdmTable.MEASUREMENT_ID IN ( 
# MAGIC  SELECT 
# MAGIC  MEASUREMENT_ID 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT
# MAGIC  GROUP BY MEASUREMENT_ID
# MAGIC  HAVING COUNT(*) > 1 
# MAGIC  )
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isPrimaryKey' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records that have a duplicate value in the NOTE_ID field of the NOTE.' as check_description
# MAGIC  ,'NOTE' as cdm_table_name
# MAGIC  ,'NOTE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_primary_key.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isprimarykey_note_note_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'NOTE.NOTE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC  WHERE cdmTable.NOTE_ID IN ( 
# MAGIC  SELECT 
# MAGIC  NOTE_ID 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE
# MAGIC  GROUP BY NOTE_ID
# MAGIC  HAVING COUNT(*) > 1 
# MAGIC  )
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isPrimaryKey' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records that have a duplicate value in the NOTE_NLP_ID field of the NOTE_NLP.' as check_description
# MAGIC  ,'NOTE_NLP' as cdm_table_name
# MAGIC  ,'NOTE_NLP_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_primary_key.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isprimarykey_note_nlp_note_nlp_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'NOTE_NLP.NOTE_NLP_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE_NLP cdmTable
# MAGIC  WHERE cdmTable.NOTE_NLP_ID IN ( 
# MAGIC  SELECT 
# MAGIC  NOTE_NLP_ID 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE_NLP
# MAGIC  GROUP BY NOTE_NLP_ID
# MAGIC  HAVING COUNT(*) > 1 
# MAGIC  )
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE_NLP cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isPrimaryKey' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records that have a duplicate value in the OBSERVATION_ID field of the OBSERVATION.' as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'OBSERVATION_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_primary_key.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isprimarykey_observation_observation_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'OBSERVATION.OBSERVATION_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  WHERE cdmTable.OBSERVATION_ID IN ( 
# MAGIC  SELECT 
# MAGIC  OBSERVATION_ID 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION
# MAGIC  GROUP BY OBSERVATION_ID
# MAGIC  HAVING COUNT(*) > 1 
# MAGIC  )
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isPrimaryKey' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records that have a duplicate value in the OBSERVATION_PERIOD_ID field of the OBSERVATION_PERIOD.' as check_description
# MAGIC  ,'OBSERVATION_PERIOD' as cdm_table_name
# MAGIC  ,'OBSERVATION_PERIOD_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_primary_key.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isprimarykey_observation_period_observation_period_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'OBSERVATION_PERIOD.OBSERVATION_PERIOD_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD cdmTable
# MAGIC  WHERE cdmTable.OBSERVATION_PERIOD_ID IN ( 
# MAGIC  SELECT 
# MAGIC  OBSERVATION_PERIOD_ID 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD
# MAGIC  GROUP BY OBSERVATION_PERIOD_ID
# MAGIC  HAVING COUNT(*) > 1 
# MAGIC  )
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isPrimaryKey' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records that have a duplicate value in the PAYER_PLAN_PERIOD_ID field of the PAYER_PLAN_PERIOD.' as check_description
# MAGIC  ,'PAYER_PLAN_PERIOD' as cdm_table_name
# MAGIC  ,'PAYER_PLAN_PERIOD_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_primary_key.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isprimarykey_payer_plan_period_payer_plan_period_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PAYER_PLAN_PERIOD.PAYER_PLAN_PERIOD_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC  WHERE cdmTable.PAYER_PLAN_PERIOD_ID IN ( 
# MAGIC  SELECT 
# MAGIC  PAYER_PLAN_PERIOD_ID 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD
# MAGIC  GROUP BY PAYER_PLAN_PERIOD_ID
# MAGIC  HAVING COUNT(*) > 1 
# MAGIC  )
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isPrimaryKey' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records that have a duplicate value in the PERSON_ID field of the PERSON.' as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_primary_key.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isprimarykey_person_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PERSON.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE cdmTable.PERSON_ID IN ( 
# MAGIC  SELECT 
# MAGIC  PERSON_ID 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON
# MAGIC  GROUP BY PERSON_ID
# MAGIC  HAVING COUNT(*) > 1 
# MAGIC  )
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isPrimaryKey' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records that have a duplicate value in the PROCEDURE_OCCURRENCE_ID field of the PROCEDURE_OCCURRENCE.' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'PROCEDURE_OCCURRENCE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_primary_key.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isprimarykey_procedure_occurrence_procedure_occurrence_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PROCEDURE_OCCURRENCE.PROCEDURE_OCCURRENCE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.PROCEDURE_OCCURRENCE_ID IN ( 
# MAGIC  SELECT 
# MAGIC  PROCEDURE_OCCURRENCE_ID 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE
# MAGIC  GROUP BY PROCEDURE_OCCURRENCE_ID
# MAGIC  HAVING COUNT(*) > 1 
# MAGIC  )
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isPrimaryKey' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records that have a duplicate value in the PROVIDER_ID field of the PROVIDER.' as check_description
# MAGIC  ,'PROVIDER' as cdm_table_name
# MAGIC  ,'PROVIDER_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_primary_key.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isprimarykey_provider_provider_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PROVIDER.PROVIDER_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROVIDER cdmTable
# MAGIC  WHERE cdmTable.PROVIDER_ID IN ( 
# MAGIC  SELECT 
# MAGIC  PROVIDER_ID 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROVIDER
# MAGIC  GROUP BY PROVIDER_ID
# MAGIC  HAVING COUNT(*) > 1 
# MAGIC  )
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROVIDER cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isPrimaryKey' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records that have a duplicate value in the SPECIMEN_ID field of the SPECIMEN.' as check_description
# MAGIC  ,'SPECIMEN' as cdm_table_name
# MAGIC  ,'SPECIMEN_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_primary_key.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isprimarykey_specimen_specimen_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'SPECIMEN.SPECIMEN_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC  WHERE cdmTable.SPECIMEN_ID IN ( 
# MAGIC  SELECT 
# MAGIC  SPECIMEN_ID 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN
# MAGIC  GROUP BY SPECIMEN_ID
# MAGIC  HAVING COUNT(*) > 1 
# MAGIC  )
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isPrimaryKey' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records that have a duplicate value in the VISIT_DETAIL_ID field of the VISIT_DETAIL.' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'VISIT_DETAIL_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_primary_key.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isprimarykey_visit_detail_visit_detail_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_DETAIL.VISIT_DETAIL_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE cdmTable.VISIT_DETAIL_ID IN ( 
# MAGIC  SELECT 
# MAGIC  VISIT_DETAIL_ID 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL
# MAGIC  GROUP BY VISIT_DETAIL_ID
# MAGIC  HAVING COUNT(*) > 1 
# MAGIC  )
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'isPrimaryKey' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records that have a duplicate value in the VISIT_OCCURRENCE_ID field of the VISIT_OCCURRENCE.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'VISIT_OCCURRENCE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_is_primary_key.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Relational' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_isprimarykey_visit_occurrence_visit_occurrence_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_OCCURRENCE.VISIT_OCCURRENCE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.VISIT_OCCURRENCE_ID IN ( 
# MAGIC  SELECT 
# MAGIC  VISIT_OCCURRENCE_ID 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE
# MAGIC  GROUP BY VISIT_OCCURRENCE_ID
# MAGIC  HAVING COUNT(*) > 1 
# MAGIC  )
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte
# MAGIC )
# MAGIC INSERT INTO ${results_catalog}.${results_schema}.dqdashboard_results
# MAGIC SELECT *
# MAGIC FROM cte_all;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- field cdm datatype
# MAGIC
# MAGIC WITH cte_all  AS (SELECT cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  , CAST('' as STRING) as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the CARE_SITE_ID in the CARE_SITE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'CARE_SITE' as cdm_table_name
# MAGIC  ,'CARE_SITE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_care_site_care_site_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'CARE_SITE.CARE_SITE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CARE_SITE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.CARE_SITE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.CARE_SITE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.CARE_SITE_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.CARE_SITE_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CARE_SITE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PLACE_OF_SERVICE_CONCEPT_ID in the CARE_SITE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'CARE_SITE' as cdm_table_name
# MAGIC  ,'PLACE_OF_SERVICE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_care_site_place_of_service_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'CARE_SITE.PLACE_OF_SERVICE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CARE_SITE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PLACE_OF_SERVICE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PLACE_OF_SERVICE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PLACE_OF_SERVICE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PLACE_OF_SERVICE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CARE_SITE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the LOCATION_ID in the CARE_SITE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'CARE_SITE' as cdm_table_name
# MAGIC  ,'LOCATION_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_care_site_location_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'CARE_SITE.LOCATION_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CARE_SITE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.LOCATION_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.LOCATION_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.LOCATION_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.LOCATION_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CARE_SITE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the CONDITION_ERA_ID in the CONDITION_ERA is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'CONDITION_ERA' as cdm_table_name
# MAGIC  ,'CONDITION_ERA_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_condition_era_condition_era_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'CONDITION_ERA.CONDITION_ERA_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.CONDITION_ERA_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.CONDITION_ERA_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.CONDITION_ERA_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.CONDITION_ERA_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PERSON_ID in the CONDITION_ERA is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'CONDITION_ERA' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_condition_era_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'CONDITION_ERA.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PERSON_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PERSON_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the CONDITION_CONCEPT_ID in the CONDITION_ERA is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'CONDITION_ERA' as cdm_table_name
# MAGIC  ,'CONDITION_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_condition_era_condition_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'CONDITION_ERA.CONDITION_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.CONDITION_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.CONDITION_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.CONDITION_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.CONDITION_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the CONDITION_OCCURRENCE_COUNT in the CONDITION_ERA is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'CONDITION_ERA' as cdm_table_name
# MAGIC  ,'CONDITION_OCCURRENCE_COUNT' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_condition_era_condition_occurrence_count' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'CONDITION_ERA.CONDITION_OCCURRENCE_COUNT' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.CONDITION_OCCURRENCE_COUNT AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.CONDITION_OCCURRENCE_COUNT AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.CONDITION_OCCURRENCE_COUNT) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.CONDITION_OCCURRENCE_COUNT IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the CONDITION_CONCEPT_ID in the CONDITION_OCCURRENCE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_condition_occurrence_condition_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'CONDITION_OCCURRENCE.CONDITION_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.CONDITION_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.CONDITION_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.CONDITION_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.CONDITION_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the CONDITION_TYPE_CONCEPT_ID in the CONDITION_OCCURRENCE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_condition_occurrence_condition_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'CONDITION_OCCURRENCE.CONDITION_TYPE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.CONDITION_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.CONDITION_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.CONDITION_TYPE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.CONDITION_TYPE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the CONDITION_STATUS_CONCEPT_ID in the CONDITION_OCCURRENCE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_STATUS_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_condition_occurrence_condition_status_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'CONDITION_OCCURRENCE.CONDITION_STATUS_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.CONDITION_STATUS_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.CONDITION_STATUS_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.CONDITION_STATUS_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.CONDITION_STATUS_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PROVIDER_ID in the CONDITION_OCCURRENCE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'PROVIDER_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_condition_occurrence_provider_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'CONDITION_OCCURRENCE.PROVIDER_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PROVIDER_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PROVIDER_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the VISIT_OCCURRENCE_ID in the CONDITION_OCCURRENCE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'VISIT_OCCURRENCE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_condition_occurrence_visit_occurrence_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'CONDITION_OCCURRENCE.VISIT_OCCURRENCE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.VISIT_OCCURRENCE_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.VISIT_OCCURRENCE_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the VISIT_DETAIL_ID in the CONDITION_OCCURRENCE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'VISIT_DETAIL_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_condition_occurrence_visit_detail_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'CONDITION_OCCURRENCE.VISIT_DETAIL_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.VISIT_DETAIL_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.VISIT_DETAIL_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the CONDITION_SOURCE_CONCEPT_ID in the CONDITION_OCCURRENCE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_SOURCE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_condition_occurrence_condition_source_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'CONDITION_OCCURRENCE.CONDITION_SOURCE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.CONDITION_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.CONDITION_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.CONDITION_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.CONDITION_SOURCE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the COST_TYPE_CONCEPT_ID in the COST is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'COST' as cdm_table_name
# MAGIC  ,'COST_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_cost_cost_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'COST.COST_TYPE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.COST cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.COST_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.COST_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.COST_TYPE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.COST_TYPE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.COST
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the CURRENCY_CONCEPT_ID in the COST is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'COST' as cdm_table_name
# MAGIC  ,'CURRENCY_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_cost_currency_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'COST.CURRENCY_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.COST cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.CURRENCY_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.CURRENCY_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.CURRENCY_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.CURRENCY_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.COST
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the REVENUE_CODE_CONCEPT_ID in the COST is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'COST' as cdm_table_name
# MAGIC  ,'REVENUE_CODE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_cost_revenue_code_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'COST.REVENUE_CODE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.COST cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.REVENUE_CODE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.REVENUE_CODE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.REVENUE_CODE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.REVENUE_CODE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.COST
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the DRG_CONCEPT_ID in the COST is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'COST' as cdm_table_name
# MAGIC  ,'DRG_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_cost_drg_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'COST.DRG_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.COST cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.DRG_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.DRG_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.DRG_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.DRG_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.COST
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the CAUSE_CONCEPT_ID in the DEATH is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'DEATH' as cdm_table_name
# MAGIC  ,'CAUSE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_death_cause_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DEATH.CAUSE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEATH cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.CAUSE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.CAUSE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.CAUSE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.CAUSE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEATH
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the CAUSE_SOURCE_CONCEPT_ID in the DEATH is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'DEATH' as cdm_table_name
# MAGIC  ,'CAUSE_SOURCE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_death_cause_source_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DEATH.CAUSE_SOURCE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEATH cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.CAUSE_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.CAUSE_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.CAUSE_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.CAUSE_SOURCE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEATH
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the DEATH_TYPE_CONCEPT_ID in the DEATH is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'DEATH' as cdm_table_name
# MAGIC  ,'DEATH_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_death_death_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DEATH.DEATH_TYPE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEATH cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.DEATH_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.DEATH_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.DEATH_TYPE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.DEATH_TYPE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEATH
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PERSON_ID in the DEATH is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'DEATH' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_death_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DEATH.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEATH cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PERSON_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PERSON_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEATH
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the DEVICE_CONCEPT_ID in the DEVICE_EXPOSURE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'DEVICE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_device_exposure_device_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DEVICE_EXPOSURE.DEVICE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.DEVICE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.DEVICE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.DEVICE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.DEVICE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the DEVICE_TYPE_CONCEPT_ID in the DEVICE_EXPOSURE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'DEVICE_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_device_exposure_device_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DEVICE_EXPOSURE.DEVICE_TYPE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.DEVICE_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.DEVICE_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.DEVICE_TYPE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.DEVICE_TYPE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the QUANTITY in the DEVICE_EXPOSURE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'QUANTITY' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_device_exposure_quantity' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DEVICE_EXPOSURE.QUANTITY' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.QUANTITY AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.QUANTITY AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.QUANTITY) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.QUANTITY IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PROVIDER_ID in the DEVICE_EXPOSURE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'PROVIDER_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_device_exposure_provider_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DEVICE_EXPOSURE.PROVIDER_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PROVIDER_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PROVIDER_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the VISIT_OCCURRENCE_ID in the DEVICE_EXPOSURE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'VISIT_OCCURRENCE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_device_exposure_visit_occurrence_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DEVICE_EXPOSURE.VISIT_OCCURRENCE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.VISIT_OCCURRENCE_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.VISIT_OCCURRENCE_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the VISIT_DETAIL_ID in the DEVICE_EXPOSURE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'VISIT_DETAIL_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_device_exposure_visit_detail_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DEVICE_EXPOSURE.VISIT_DETAIL_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.VISIT_DETAIL_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.VISIT_DETAIL_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the DEVICE_SOURCE_CONCEPT_ID in the DEVICE_EXPOSURE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'DEVICE_SOURCE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_device_exposure_device_source_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DEVICE_EXPOSURE.DEVICE_SOURCE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.DEVICE_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.DEVICE_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.DEVICE_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.DEVICE_SOURCE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the DOSE_ERA_ID in the DOSE_ERA is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'DOSE_ERA' as cdm_table_name
# MAGIC  ,'DOSE_ERA_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_dose_era_dose_era_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DOSE_ERA.DOSE_ERA_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.DOSE_ERA_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.DOSE_ERA_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.DOSE_ERA_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.DOSE_ERA_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PERSON_ID in the DOSE_ERA is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'DOSE_ERA' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_dose_era_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DOSE_ERA.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PERSON_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PERSON_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the DRUG_CONCEPT_ID in the DOSE_ERA is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'DOSE_ERA' as cdm_table_name
# MAGIC  ,'DRUG_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_dose_era_drug_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DOSE_ERA.DRUG_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.DRUG_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.DRUG_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.DRUG_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.DRUG_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the UNIT_CONCEPT_ID in the DOSE_ERA is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'DOSE_ERA' as cdm_table_name
# MAGIC  ,'UNIT_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_dose_era_unit_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DOSE_ERA.UNIT_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.UNIT_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.UNIT_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.UNIT_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.UNIT_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the DRUG_ERA_ID in the DRUG_ERA is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'DRUG_ERA' as cdm_table_name
# MAGIC  ,'DRUG_ERA_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_drug_era_drug_era_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DRUG_ERA.DRUG_ERA_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.DRUG_ERA_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.DRUG_ERA_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.DRUG_ERA_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.DRUG_ERA_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PERSON_ID in the DRUG_ERA is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'DRUG_ERA' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_drug_era_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DRUG_ERA.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PERSON_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PERSON_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the DRUG_CONCEPT_ID in the DRUG_ERA is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'DRUG_ERA' as cdm_table_name
# MAGIC  ,'DRUG_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_drug_era_drug_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DRUG_ERA.DRUG_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.DRUG_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.DRUG_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.DRUG_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.DRUG_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the DRUG_EXPOSURE_COUNT in the DRUG_ERA is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'DRUG_ERA' as cdm_table_name
# MAGIC  ,'DRUG_EXPOSURE_COUNT' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_drug_era_drug_exposure_count' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DRUG_ERA.DRUG_EXPOSURE_COUNT' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.DRUG_EXPOSURE_COUNT AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.DRUG_EXPOSURE_COUNT AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.DRUG_EXPOSURE_COUNT) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.DRUG_EXPOSURE_COUNT IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the GAP_DAYS in the DRUG_ERA is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'DRUG_ERA' as cdm_table_name
# MAGIC  ,'GAP_DAYS' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_drug_era_gap_days' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DRUG_ERA.GAP_DAYS' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.GAP_DAYS AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.GAP_DAYS AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.GAP_DAYS) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.GAP_DAYS IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the DRUG_CONCEPT_ID in the DRUG_EXPOSURE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DRUG_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_drug_exposure_drug_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DRUG_EXPOSURE.DRUG_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.DRUG_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.DRUG_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.DRUG_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.DRUG_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the DRUG_TYPE_CONCEPT_ID in the DRUG_EXPOSURE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DRUG_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_drug_exposure_drug_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DRUG_EXPOSURE.DRUG_TYPE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.DRUG_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.DRUG_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.DRUG_TYPE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.DRUG_TYPE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the REFILLS in the DRUG_EXPOSURE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'REFILLS' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_drug_exposure_refills' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DRUG_EXPOSURE.REFILLS' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.REFILLS AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.REFILLS AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.REFILLS) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.REFILLS IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the DAYS_SUPPLY in the DRUG_EXPOSURE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DAYS_SUPPLY' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_drug_exposure_days_supply' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DRUG_EXPOSURE.DAYS_SUPPLY' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.DAYS_SUPPLY AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.DAYS_SUPPLY AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.DAYS_SUPPLY) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.DAYS_SUPPLY IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the ROUTE_CONCEPT_ID in the DRUG_EXPOSURE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'ROUTE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_drug_exposure_route_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DRUG_EXPOSURE.ROUTE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.ROUTE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.ROUTE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.ROUTE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.ROUTE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PROVIDER_ID in the DRUG_EXPOSURE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'PROVIDER_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_drug_exposure_provider_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DRUG_EXPOSURE.PROVIDER_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PROVIDER_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PROVIDER_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the VISIT_OCCURRENCE_ID in the DRUG_EXPOSURE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'VISIT_OCCURRENCE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_drug_exposure_visit_occurrence_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DRUG_EXPOSURE.VISIT_OCCURRENCE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.VISIT_OCCURRENCE_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.VISIT_OCCURRENCE_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the VISIT_DETAIL_ID in the DRUG_EXPOSURE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'VISIT_DETAIL_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_drug_exposure_visit_detail_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DRUG_EXPOSURE.VISIT_DETAIL_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.VISIT_DETAIL_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.VISIT_DETAIL_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the DRUG_SOURCE_CONCEPT_ID in the DRUG_EXPOSURE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DRUG_SOURCE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_drug_exposure_drug_source_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DRUG_EXPOSURE.DRUG_SOURCE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.DRUG_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.DRUG_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.DRUG_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.DRUG_SOURCE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the DOMAIN_CONCEPT_ID_1 in the FACT_RELATIONSHIP is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'FACT_RELATIONSHIP' as cdm_table_name
# MAGIC  ,'DOMAIN_CONCEPT_ID_1' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_fact_relationship_domain_concept_id_1' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'FACT_RELATIONSHIP.DOMAIN_CONCEPT_ID_1' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.FACT_RELATIONSHIP cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.DOMAIN_CONCEPT_ID_1 AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.DOMAIN_CONCEPT_ID_1 AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.DOMAIN_CONCEPT_ID_1) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.DOMAIN_CONCEPT_ID_1 IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.FACT_RELATIONSHIP
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the FACT_ID_1 in the FACT_RELATIONSHIP is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'FACT_RELATIONSHIP' as cdm_table_name
# MAGIC  ,'FACT_ID_1' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_fact_relationship_fact_id_1' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'FACT_RELATIONSHIP.FACT_ID_1' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.FACT_RELATIONSHIP cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.FACT_ID_1 AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.FACT_ID_1 AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.FACT_ID_1) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.FACT_ID_1 IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.FACT_RELATIONSHIP
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the DOMAIN_CONCEPT_ID_2 in the FACT_RELATIONSHIP is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'FACT_RELATIONSHIP' as cdm_table_name
# MAGIC  ,'DOMAIN_CONCEPT_ID_2' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_fact_relationship_domain_concept_id_2' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'FACT_RELATIONSHIP.DOMAIN_CONCEPT_ID_2' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.FACT_RELATIONSHIP cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.DOMAIN_CONCEPT_ID_2 AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.DOMAIN_CONCEPT_ID_2 AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.DOMAIN_CONCEPT_ID_2) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.DOMAIN_CONCEPT_ID_2 IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.FACT_RELATIONSHIP
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the FACT_ID_2 in the FACT_RELATIONSHIP is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'FACT_RELATIONSHIP' as cdm_table_name
# MAGIC  ,'FACT_ID_2' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_fact_relationship_fact_id_2' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'FACT_RELATIONSHIP.FACT_ID_2' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.FACT_RELATIONSHIP cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.FACT_ID_2 AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.FACT_ID_2 AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.FACT_ID_2) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.FACT_ID_2 IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.FACT_RELATIONSHIP
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the RELATIONSHIP_CONCEPT_ID in the FACT_RELATIONSHIP is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'FACT_RELATIONSHIP' as cdm_table_name
# MAGIC  ,'RELATIONSHIP_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_fact_relationship_relationship_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'FACT_RELATIONSHIP.RELATIONSHIP_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.FACT_RELATIONSHIP cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.RELATIONSHIP_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.RELATIONSHIP_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.RELATIONSHIP_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.RELATIONSHIP_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.FACT_RELATIONSHIP
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the LOCATION_ID in the LOCATION is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'LOCATION' as cdm_table_name
# MAGIC  ,'LOCATION_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_location_location_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'LOCATION.LOCATION_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.LOCATION cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.LOCATION_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.LOCATION_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.LOCATION_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.LOCATION_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.LOCATION
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the MEASUREMENT_ID in the MEASUREMENT is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'MEASUREMENT' as cdm_table_name
# MAGIC  ,'MEASUREMENT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_measurement_measurement_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'MEASUREMENT.MEASUREMENT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.MEASUREMENT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.MEASUREMENT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.MEASUREMENT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.MEASUREMENT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PERSON_ID in the MEASUREMENT is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'MEASUREMENT' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_measurement_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'MEASUREMENT.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PERSON_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PERSON_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the MEASUREMENT_CONCEPT_ID in the MEASUREMENT is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'MEASUREMENT' as cdm_table_name
# MAGIC  ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_measurement_measurement_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'MEASUREMENT.MEASUREMENT_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.MEASUREMENT_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.MEASUREMENT_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.MEASUREMENT_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.MEASUREMENT_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the MEASUREMENT_TYPE_CONCEPT_ID in the MEASUREMENT is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'MEASUREMENT' as cdm_table_name
# MAGIC  ,'MEASUREMENT_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_measurement_measurement_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'MEASUREMENT.MEASUREMENT_TYPE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.MEASUREMENT_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.MEASUREMENT_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.MEASUREMENT_TYPE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.MEASUREMENT_TYPE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the OPERATOR_CONCEPT_ID in the MEASUREMENT is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'MEASUREMENT' as cdm_table_name
# MAGIC  ,'OPERATOR_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_measurement_operator_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'MEASUREMENT.OPERATOR_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.OPERATOR_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.OPERATOR_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.OPERATOR_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.OPERATOR_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the VALUE_AS_CONCEPT_ID in the MEASUREMENT is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'MEASUREMENT' as cdm_table_name
# MAGIC  ,'VALUE_AS_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_measurement_value_as_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'MEASUREMENT.VALUE_AS_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.VALUE_AS_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.VALUE_AS_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.VALUE_AS_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.VALUE_AS_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the UNIT_CONCEPT_ID in the MEASUREMENT is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'MEASUREMENT' as cdm_table_name
# MAGIC  ,'UNIT_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_measurement_unit_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'MEASUREMENT.UNIT_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.UNIT_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.UNIT_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.UNIT_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.UNIT_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PROVIDER_ID in the MEASUREMENT is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'MEASUREMENT' as cdm_table_name
# MAGIC  ,'PROVIDER_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_measurement_provider_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'MEASUREMENT.PROVIDER_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PROVIDER_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PROVIDER_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the VISIT_OCCURRENCE_ID in the MEASUREMENT is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'MEASUREMENT' as cdm_table_name
# MAGIC  ,'VISIT_OCCURRENCE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_measurement_visit_occurrence_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'MEASUREMENT.VISIT_OCCURRENCE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.VISIT_OCCURRENCE_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.VISIT_OCCURRENCE_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the VISIT_DETAIL_ID in the MEASUREMENT is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'MEASUREMENT' as cdm_table_name
# MAGIC  ,'VISIT_DETAIL_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_measurement_visit_detail_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'MEASUREMENT.VISIT_DETAIL_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.VISIT_DETAIL_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.VISIT_DETAIL_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the MEASUREMENT_SOURCE_CONCEPT_ID in the MEASUREMENT is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'MEASUREMENT' as cdm_table_name
# MAGIC  ,'MEASUREMENT_SOURCE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_measurement_measurement_source_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'MEASUREMENT.MEASUREMENT_SOURCE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.MEASUREMENT_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.MEASUREMENT_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.MEASUREMENT_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.MEASUREMENT_SOURCE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the NOTE_ID in the NOTE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'NOTE' as cdm_table_name
# MAGIC  ,'NOTE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_note_note_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'NOTE.NOTE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.NOTE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.NOTE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.NOTE_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.NOTE_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PERSON_ID in the NOTE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'NOTE' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_note_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'NOTE.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PERSON_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PERSON_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the NOTE_TYPE_CONCEPT_ID in the NOTE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'NOTE' as cdm_table_name
# MAGIC  ,'NOTE_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_note_note_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'NOTE.NOTE_TYPE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.NOTE_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.NOTE_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.NOTE_TYPE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.NOTE_TYPE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the NOTE_CLASS_CONCEPT_ID in the NOTE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'NOTE' as cdm_table_name
# MAGIC  ,'NOTE_CLASS_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_note_note_class_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'NOTE.NOTE_CLASS_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.NOTE_CLASS_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.NOTE_CLASS_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.NOTE_CLASS_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.NOTE_CLASS_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the ENCODING_CONCEPT_ID in the NOTE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'NOTE' as cdm_table_name
# MAGIC  ,'ENCODING_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_note_encoding_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'NOTE.ENCODING_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.ENCODING_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.ENCODING_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.ENCODING_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.ENCODING_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the LANGUAGE_CONCEPT_ID in the NOTE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'NOTE' as cdm_table_name
# MAGIC  ,'LANGUAGE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_note_language_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'NOTE.LANGUAGE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.LANGUAGE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.LANGUAGE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.LANGUAGE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.LANGUAGE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PROVIDER_ID in the NOTE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'NOTE' as cdm_table_name
# MAGIC  ,'PROVIDER_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_note_provider_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'NOTE.PROVIDER_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PROVIDER_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PROVIDER_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the VISIT_OCCURRENCE_ID in the NOTE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'NOTE' as cdm_table_name
# MAGIC  ,'VISIT_OCCURRENCE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_note_visit_occurrence_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'NOTE.VISIT_OCCURRENCE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.VISIT_OCCURRENCE_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.VISIT_OCCURRENCE_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the VISIT_DETAIL_ID in the NOTE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'NOTE' as cdm_table_name
# MAGIC  ,'VISIT_DETAIL_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_note_visit_detail_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'NOTE.VISIT_DETAIL_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.VISIT_DETAIL_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.VISIT_DETAIL_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the NOTE_NLP_ID in the NOTE_NLP is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'NOTE_NLP' as cdm_table_name
# MAGIC  ,'NOTE_NLP_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_note_nlp_note_nlp_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'NOTE_NLP.NOTE_NLP_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE_NLP cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.NOTE_NLP_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.NOTE_NLP_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.NOTE_NLP_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.NOTE_NLP_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE_NLP
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the NOTE_ID in the NOTE_NLP is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'NOTE_NLP' as cdm_table_name
# MAGIC  ,'NOTE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_note_nlp_note_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'NOTE_NLP.NOTE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE_NLP cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.NOTE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.NOTE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.NOTE_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.NOTE_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE_NLP
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the SECTION_CONCEPT_ID in the NOTE_NLP is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'NOTE_NLP' as cdm_table_name
# MAGIC  ,'SECTION_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_note_nlp_section_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'NOTE_NLP.SECTION_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE_NLP cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.SECTION_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.SECTION_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.SECTION_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.SECTION_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE_NLP
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the NOTE_NLP_CONCEPT_ID in the NOTE_NLP is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'NOTE_NLP' as cdm_table_name
# MAGIC  ,'NOTE_NLP_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_note_nlp_note_nlp_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'NOTE_NLP.NOTE_NLP_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE_NLP cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.NOTE_NLP_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.NOTE_NLP_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.NOTE_NLP_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.NOTE_NLP_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE_NLP
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the NOTE_NLP_SOURCE_CONCEPT_ID in the NOTE_NLP is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'NOTE_NLP' as cdm_table_name
# MAGIC  ,'NOTE_NLP_SOURCE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_note_nlp_note_nlp_source_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'NOTE_NLP.NOTE_NLP_SOURCE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE_NLP cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.NOTE_NLP_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.NOTE_NLP_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.NOTE_NLP_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.NOTE_NLP_SOURCE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE_NLP
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the OBSERVATION_ID in the OBSERVATION is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'OBSERVATION_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_observation_observation_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'OBSERVATION.OBSERVATION_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.OBSERVATION_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.OBSERVATION_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.OBSERVATION_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.OBSERVATION_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PERSON_ID in the OBSERVATION is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_observation_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'OBSERVATION.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PERSON_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PERSON_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the OBSERVATION_CONCEPT_ID in the OBSERVATION is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'OBSERVATION_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_observation_observation_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'OBSERVATION.OBSERVATION_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.OBSERVATION_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.OBSERVATION_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.OBSERVATION_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.OBSERVATION_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the OBSERVATION_TYPE_CONCEPT_ID in the OBSERVATION is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'OBSERVATION_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_observation_observation_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'OBSERVATION.OBSERVATION_TYPE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.OBSERVATION_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.OBSERVATION_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.OBSERVATION_TYPE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.OBSERVATION_TYPE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the QUALIFIER_CONCEPT_ID in the OBSERVATION is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'QUALIFIER_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_observation_qualifier_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'OBSERVATION.QUALIFIER_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.QUALIFIER_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.QUALIFIER_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.QUALIFIER_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.QUALIFIER_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the UNIT_CONCEPT_ID in the OBSERVATION is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'UNIT_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_observation_unit_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'OBSERVATION.UNIT_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.UNIT_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.UNIT_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.UNIT_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.UNIT_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PROVIDER_ID in the OBSERVATION is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'PROVIDER_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_observation_provider_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'OBSERVATION.PROVIDER_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PROVIDER_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PROVIDER_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the VISIT_OCCURRENCE_ID in the OBSERVATION is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'VISIT_OCCURRENCE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_observation_visit_occurrence_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'OBSERVATION.VISIT_OCCURRENCE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.VISIT_OCCURRENCE_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.VISIT_OCCURRENCE_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the VISIT_DETAIL_ID in the OBSERVATION is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'VISIT_DETAIL_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_observation_visit_detail_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'OBSERVATION.VISIT_DETAIL_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.VISIT_DETAIL_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.VISIT_DETAIL_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the OBSERVATION_SOURCE_CONCEPT_ID in the OBSERVATION is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'OBSERVATION_SOURCE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_observation_observation_source_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'OBSERVATION.OBSERVATION_SOURCE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.OBSERVATION_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.OBSERVATION_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.OBSERVATION_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.OBSERVATION_SOURCE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the OBSERVATION_PERIOD_ID in the OBSERVATION_PERIOD is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'OBSERVATION_PERIOD' as cdm_table_name
# MAGIC  ,'OBSERVATION_PERIOD_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_observation_period_observation_period_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'OBSERVATION_PERIOD.OBSERVATION_PERIOD_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.OBSERVATION_PERIOD_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.OBSERVATION_PERIOD_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.OBSERVATION_PERIOD_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.OBSERVATION_PERIOD_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PERSON_ID in the OBSERVATION_PERIOD is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'OBSERVATION_PERIOD' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_observation_period_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'OBSERVATION_PERIOD.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PERSON_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PERSON_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PAYER_PLAN_PERIOD_ID in the PAYER_PLAN_PERIOD is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PAYER_PLAN_PERIOD' as cdm_table_name
# MAGIC  ,'PAYER_PLAN_PERIOD_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_payer_plan_period_payer_plan_period_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PAYER_PLAN_PERIOD.PAYER_PLAN_PERIOD_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PAYER_PLAN_PERIOD_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PAYER_PLAN_PERIOD_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PAYER_PLAN_PERIOD_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PAYER_PLAN_PERIOD_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PERSON_ID in the PAYER_PLAN_PERIOD is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PAYER_PLAN_PERIOD' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_payer_plan_period_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PAYER_PLAN_PERIOD.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PERSON_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PERSON_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PAYER_CONCEPT_ID in the PAYER_PLAN_PERIOD is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PAYER_PLAN_PERIOD' as cdm_table_name
# MAGIC  ,'PAYER_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_payer_plan_period_payer_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PAYER_PLAN_PERIOD.PAYER_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PAYER_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PAYER_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PAYER_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PAYER_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PAYER_SOURCE_CONCEPT_ID in the PAYER_PLAN_PERIOD is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PAYER_PLAN_PERIOD' as cdm_table_name
# MAGIC  ,'PAYER_SOURCE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_payer_plan_period_payer_source_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PAYER_PLAN_PERIOD.PAYER_SOURCE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PAYER_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PAYER_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PAYER_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PAYER_SOURCE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PLAN_CONCEPT_ID in the PAYER_PLAN_PERIOD is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PAYER_PLAN_PERIOD' as cdm_table_name
# MAGIC  ,'PLAN_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_payer_plan_period_plan_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PAYER_PLAN_PERIOD.PLAN_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PLAN_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PLAN_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PLAN_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PLAN_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PLAN_SOURCE_CONCEPT_ID in the PAYER_PLAN_PERIOD is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PAYER_PLAN_PERIOD' as cdm_table_name
# MAGIC  ,'PLAN_SOURCE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_payer_plan_period_plan_source_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PAYER_PLAN_PERIOD.PLAN_SOURCE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PLAN_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PLAN_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PLAN_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PLAN_SOURCE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the SPONSOR_CONCEPT_ID in the PAYER_PLAN_PERIOD is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PAYER_PLAN_PERIOD' as cdm_table_name
# MAGIC  ,'SPONSOR_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_payer_plan_period_sponsor_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PAYER_PLAN_PERIOD.SPONSOR_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.SPONSOR_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.SPONSOR_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.SPONSOR_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.SPONSOR_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the SPONSOR_SOURCE_CONCEPT_ID in the PAYER_PLAN_PERIOD is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PAYER_PLAN_PERIOD' as cdm_table_name
# MAGIC  ,'SPONSOR_SOURCE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_payer_plan_period_sponsor_source_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PAYER_PLAN_PERIOD.SPONSOR_SOURCE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.SPONSOR_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.SPONSOR_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.SPONSOR_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.SPONSOR_SOURCE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the STOP_REASON_CONCEPT_ID in the PAYER_PLAN_PERIOD is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PAYER_PLAN_PERIOD' as cdm_table_name
# MAGIC  ,'STOP_REASON_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_payer_plan_period_stop_reason_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PAYER_PLAN_PERIOD.STOP_REASON_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.STOP_REASON_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.STOP_REASON_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.STOP_REASON_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.STOP_REASON_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the STOP_REASON_SOURCE_CONCEPT_ID in the PAYER_PLAN_PERIOD is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PAYER_PLAN_PERIOD' as cdm_table_name
# MAGIC  ,'STOP_REASON_SOURCE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_payer_plan_period_stop_reason_source_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PAYER_PLAN_PERIOD.STOP_REASON_SOURCE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.STOP_REASON_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.STOP_REASON_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.STOP_REASON_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.STOP_REASON_SOURCE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD
# MAGIC ) denominator
# MAGIC ) cte
# MAGIC )
# MAGIC INSERT INTO ${results_catalog}.${results_schema}.dqdashboard_results
# MAGIC SELECT *
# MAGIC FROM cte_all;
# MAGIC WITH cte_all  AS (SELECT cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  , CAST('' as STRING) as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PERSON_ID in the PERSON is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_person_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PERSON.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PERSON_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PERSON_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the GENDER_CONCEPT_ID in the PERSON is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'GENDER_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_person_gender_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PERSON.GENDER_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.GENDER_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.GENDER_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.GENDER_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.GENDER_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the YEAR_OF_BIRTH in the PERSON is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'YEAR_OF_BIRTH' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_person_year_of_birth' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PERSON.YEAR_OF_BIRTH' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.YEAR_OF_BIRTH AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.YEAR_OF_BIRTH AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.YEAR_OF_BIRTH) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.YEAR_OF_BIRTH IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the MONTH_OF_BIRTH in the PERSON is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'MONTH_OF_BIRTH' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_person_month_of_birth' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PERSON.MONTH_OF_BIRTH' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.MONTH_OF_BIRTH AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.MONTH_OF_BIRTH AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.MONTH_OF_BIRTH) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.MONTH_OF_BIRTH IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the DAY_OF_BIRTH in the PERSON is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'DAY_OF_BIRTH' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_person_day_of_birth' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PERSON.DAY_OF_BIRTH' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.DAY_OF_BIRTH AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.DAY_OF_BIRTH AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.DAY_OF_BIRTH) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.DAY_OF_BIRTH IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the RACE_CONCEPT_ID in the PERSON is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'RACE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_person_race_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PERSON.RACE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.RACE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.RACE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.RACE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.RACE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the ETHNICITY_CONCEPT_ID in the PERSON is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'ETHNICITY_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_person_ethnicity_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PERSON.ETHNICITY_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.ETHNICITY_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.ETHNICITY_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.ETHNICITY_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.ETHNICITY_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the LOCATION_ID in the PERSON is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'LOCATION_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_person_location_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PERSON.LOCATION_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.LOCATION_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.LOCATION_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.LOCATION_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.LOCATION_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PROVIDER_ID in the PERSON is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'PROVIDER_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_person_provider_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PERSON.PROVIDER_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PROVIDER_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PROVIDER_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the CARE_SITE_ID in the PERSON is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'CARE_SITE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_person_care_site_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PERSON.CARE_SITE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.CARE_SITE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.CARE_SITE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.CARE_SITE_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.CARE_SITE_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PROCEDURE_OCCURRENCE_ID in the PROCEDURE_OCCURRENCE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'PROCEDURE_OCCURRENCE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_procedure_occurrence_procedure_occurrence_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PROCEDURE_OCCURRENCE.PROCEDURE_OCCURRENCE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PROCEDURE_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PROCEDURE_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PROCEDURE_OCCURRENCE_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PROCEDURE_OCCURRENCE_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PERSON_ID in the PROCEDURE_OCCURRENCE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_procedure_occurrence_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PROCEDURE_OCCURRENCE.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PERSON_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PERSON_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PROCEDURE_CONCEPT_ID in the PROCEDURE_OCCURRENCE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_procedure_occurrence_procedure_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PROCEDURE_OCCURRENCE.PROCEDURE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PROCEDURE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PROCEDURE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PROCEDURE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PROCEDURE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PROCEDURE_TYPE_CONCEPT_ID in the PROCEDURE_OCCURRENCE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'PROCEDURE_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_procedure_occurrence_procedure_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PROCEDURE_OCCURRENCE.PROCEDURE_TYPE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PROCEDURE_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PROCEDURE_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PROCEDURE_TYPE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PROCEDURE_TYPE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the MODIFIER_CONCEPT_ID in the PROCEDURE_OCCURRENCE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'MODIFIER_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_procedure_occurrence_modifier_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PROCEDURE_OCCURRENCE.MODIFIER_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.MODIFIER_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.MODIFIER_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.MODIFIER_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.MODIFIER_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the QUANTITY in the PROCEDURE_OCCURRENCE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'QUANTITY' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_procedure_occurrence_quantity' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PROCEDURE_OCCURRENCE.QUANTITY' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.QUANTITY AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.QUANTITY AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.QUANTITY) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.QUANTITY IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PROVIDER_ID in the PROCEDURE_OCCURRENCE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'PROVIDER_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_procedure_occurrence_provider_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PROCEDURE_OCCURRENCE.PROVIDER_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PROVIDER_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PROVIDER_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the VISIT_OCCURRENCE_ID in the PROCEDURE_OCCURRENCE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'VISIT_OCCURRENCE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_procedure_occurrence_visit_occurrence_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PROCEDURE_OCCURRENCE.VISIT_OCCURRENCE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.VISIT_OCCURRENCE_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.VISIT_OCCURRENCE_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the VISIT_DETAIL_ID in the PROCEDURE_OCCURRENCE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'VISIT_DETAIL_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_procedure_occurrence_visit_detail_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PROCEDURE_OCCURRENCE.VISIT_DETAIL_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.VISIT_DETAIL_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.VISIT_DETAIL_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PROCEDURE_SOURCE_CONCEPT_ID in the PROCEDURE_OCCURRENCE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'PROCEDURE_SOURCE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_procedure_occurrence_procedure_source_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PROCEDURE_OCCURRENCE.PROCEDURE_SOURCE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PROCEDURE_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PROCEDURE_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PROCEDURE_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PROCEDURE_SOURCE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PROVIDER_ID in the PROVIDER is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PROVIDER' as cdm_table_name
# MAGIC  ,'PROVIDER_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_provider_provider_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PROVIDER.PROVIDER_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROVIDER cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PROVIDER_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PROVIDER_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROVIDER
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the SPECIALTY_CONCEPT_ID in the PROVIDER is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PROVIDER' as cdm_table_name
# MAGIC  ,'SPECIALTY_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_provider_specialty_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PROVIDER.SPECIALTY_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROVIDER cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.SPECIALTY_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.SPECIALTY_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.SPECIALTY_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.SPECIALTY_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROVIDER
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the CARE_SITE_ID in the PROVIDER is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PROVIDER' as cdm_table_name
# MAGIC  ,'CARE_SITE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_provider_care_site_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PROVIDER.CARE_SITE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROVIDER cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.CARE_SITE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.CARE_SITE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.CARE_SITE_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.CARE_SITE_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROVIDER
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the YEAR_OF_BIRTH in the PROVIDER is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PROVIDER' as cdm_table_name
# MAGIC  ,'YEAR_OF_BIRTH' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_provider_year_of_birth' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PROVIDER.YEAR_OF_BIRTH' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROVIDER cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.YEAR_OF_BIRTH AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.YEAR_OF_BIRTH AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.YEAR_OF_BIRTH) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.YEAR_OF_BIRTH IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROVIDER
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the GENDER_CONCEPT_ID in the PROVIDER is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PROVIDER' as cdm_table_name
# MAGIC  ,'GENDER_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_provider_gender_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PROVIDER.GENDER_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROVIDER cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.GENDER_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.GENDER_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.GENDER_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.GENDER_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROVIDER
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the SPECIALTY_SOURCE_CONCEPT_ID in the PROVIDER is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PROVIDER' as cdm_table_name
# MAGIC  ,'SPECIALTY_SOURCE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_provider_specialty_source_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PROVIDER.SPECIALTY_SOURCE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROVIDER cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.SPECIALTY_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.SPECIALTY_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.SPECIALTY_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.SPECIALTY_SOURCE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROVIDER
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the GENDER_SOURCE_CONCEPT_ID in the PROVIDER is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'PROVIDER' as cdm_table_name
# MAGIC  ,'GENDER_SOURCE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_provider_gender_source_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PROVIDER.GENDER_SOURCE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROVIDER cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.GENDER_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.GENDER_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.GENDER_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.GENDER_SOURCE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROVIDER
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the SPECIMEN_ID in the SPECIMEN is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'SPECIMEN' as cdm_table_name
# MAGIC  ,'SPECIMEN_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_specimen_specimen_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'SPECIMEN.SPECIMEN_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.SPECIMEN_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.SPECIMEN_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.SPECIMEN_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.SPECIMEN_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PERSON_ID in the SPECIMEN is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'SPECIMEN' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_specimen_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'SPECIMEN.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PERSON_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PERSON_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the SPECIMEN_CONCEPT_ID in the SPECIMEN is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'SPECIMEN' as cdm_table_name
# MAGIC  ,'SPECIMEN_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_specimen_specimen_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'SPECIMEN.SPECIMEN_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.SPECIMEN_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.SPECIMEN_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.SPECIMEN_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.SPECIMEN_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the SPECIMEN_TYPE_CONCEPT_ID in the SPECIMEN is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'SPECIMEN' as cdm_table_name
# MAGIC  ,'SPECIMEN_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_specimen_specimen_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'SPECIMEN.SPECIMEN_TYPE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.SPECIMEN_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.SPECIMEN_TYPE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.SPECIMEN_TYPE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.SPECIMEN_TYPE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the UNIT_CONCEPT_ID in the SPECIMEN is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'SPECIMEN' as cdm_table_name
# MAGIC  ,'UNIT_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_specimen_unit_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'SPECIMEN.UNIT_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.UNIT_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.UNIT_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.UNIT_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.UNIT_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the ANATOMIC_SITE_CONCEPT_ID in the SPECIMEN is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'SPECIMEN' as cdm_table_name
# MAGIC  ,'ANATOMIC_SITE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_specimen_anatomic_site_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'SPECIMEN.ANATOMIC_SITE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.ANATOMIC_SITE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.ANATOMIC_SITE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.ANATOMIC_SITE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.ANATOMIC_SITE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the DISEASE_STATUS_CONCEPT_ID in the SPECIMEN is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'SPECIMEN' as cdm_table_name
# MAGIC  ,'DISEASE_STATUS_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_specimen_disease_status_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'SPECIMEN.DISEASE_STATUS_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.DISEASE_STATUS_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.DISEASE_STATUS_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.DISEASE_STATUS_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.DISEASE_STATUS_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the VISIT_DETAIL_ID in the VISIT_DETAIL is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'VISIT_DETAIL_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_visit_detail_visit_detail_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_DETAIL.VISIT_DETAIL_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.VISIT_DETAIL_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.VISIT_DETAIL_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.VISIT_DETAIL_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PERSON_ID in the VISIT_DETAIL is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_visit_detail_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_DETAIL.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PERSON_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PERSON_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the VISIT_DETAIL_CONCEPT_ID in the VISIT_DETAIL is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'VISIT_DETAIL_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_visit_detail_visit_detail_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_DETAIL.VISIT_DETAIL_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.VISIT_DETAIL_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.VISIT_DETAIL_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.VISIT_DETAIL_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.VISIT_DETAIL_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PROVIDER_ID in the VISIT_DETAIL is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'PROVIDER_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_visit_detail_provider_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_DETAIL.PROVIDER_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PROVIDER_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PROVIDER_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the CARE_SITE_ID in the VISIT_DETAIL is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'CARE_SITE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_visit_detail_care_site_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_DETAIL.CARE_SITE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.CARE_SITE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.CARE_SITE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.CARE_SITE_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.CARE_SITE_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the VISIT_OCCURRENCE_ID in the VISIT_OCCURRENCE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'VISIT_OCCURRENCE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_visit_occurrence_visit_occurrence_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_OCCURRENCE.VISIT_OCCURRENCE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.VISIT_OCCURRENCE_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.VISIT_OCCURRENCE_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PERSON_ID in the VISIT_OCCURRENCE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'PERSON_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_visit_occurrence_person_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_OCCURRENCE.PERSON_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PERSON_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PERSON_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PERSON_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the VISIT_CONCEPT_ID in the VISIT_OCCURRENCE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'VISIT_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_visit_occurrence_visit_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_OCCURRENCE.VISIT_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.VISIT_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.VISIT_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.VISIT_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.VISIT_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PROVIDER_ID in the VISIT_OCCURRENCE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'PROVIDER_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_visit_occurrence_provider_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_OCCURRENCE.PROVIDER_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PROVIDER_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PROVIDER_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PROVIDER_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the CARE_SITE_ID in the VISIT_OCCURRENCE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CARE_SITE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_visit_occurrence_care_site_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_OCCURRENCE.CARE_SITE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.CARE_SITE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.CARE_SITE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.CARE_SITE_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.CARE_SITE_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the VISIT_SOURCE_CONCEPT_ID in the VISIT_OCCURRENCE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'VISIT_SOURCE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_visit_occurrence_visit_source_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_OCCURRENCE.VISIT_SOURCE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.VISIT_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.VISIT_SOURCE_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.VISIT_SOURCE_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.VISIT_SOURCE_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the admitted_from_concept_id in the VISIT_OCCURRENCE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'admitted_from_concept_id' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_visit_occurrence_admitted_from_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_OCCURRENCE.admitted_from_concept_id' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.admitted_from_concept_id AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.admitted_from_concept_id AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.admitted_from_concept_id) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.admitted_from_concept_id IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the DISCHARGED_TO_CONCEPT_ID in the VISIT_OCCURRENCE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'DISCHARGED_TO_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_visit_occurrence_DISCHARGED_TO_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_OCCURRENCE.DISCHARGED_TO_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.DISCHARGED_TO_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.DISCHARGED_TO_CONCEPT_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.DISCHARGED_TO_CONCEPT_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.DISCHARGED_TO_CONCEPT_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'cdmDatatype' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'A yes or no value indicating if the PRECEDING_VISIT_OCCURRENCE_ID in the VISIT_OCCURRENCE is the expected data type based on the specification. Only checks integer fields.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'PRECEDING_VISIT_OCCURRENCE_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_cdm_datatype.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'Value' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_cdmdatatype_visit_occurrence_preceding_visit_occurrence_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
# MAGIC FROM (
# MAGIC  SELECT num_violated_rows, 
# MAGIC  CASE 
# MAGIC  WHEN denominator.num_rows = 0 THEN 0 
# MAGIC  ELSE 1.0*num_violated_rows/denominator.num_rows 
# MAGIC  END AS pct_violated_rows, 
# MAGIC  denominator.num_rows AS num_denominator_rows
# MAGIC  FROM (SELECT 
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM
# MAGIC  (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_OCCURRENCE.PRECEDING_VISIT_OCCURRENCE_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE 
# MAGIC  (CASE WHEN CAST(cdmTable.PRECEDING_VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 0 
# MAGIC  OR (CASE WHEN CAST(cdmTable.PRECEDING_VISIT_OCCURRENCE_ID AS DOUBLE) IS NOT NULL THEN 1 ELSE 0 END = 1 
# MAGIC  AND INSTR(CAST(ABS(cdmTable.PRECEDING_VISIT_OCCURRENCE_ID) AS STRING),'.') != 0))
# MAGIC  AND cdmTable.PRECEDING_VISIT_OCCURRENCE_ID IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE
# MAGIC ) denominator
# MAGIC ) cte
# MAGIC )
# MAGIC INSERT INTO ${results_catalog}.${results_schema}.dqdashboard_results
# MAGIC SELECT *
# MAGIC FROM cte_all;
# MAGIC
