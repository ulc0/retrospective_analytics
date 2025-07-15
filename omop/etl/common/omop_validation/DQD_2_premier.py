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
# MAGIC -- field source value completeness
# MAGIC
# MAGIC WITH cte_all  AS (SELECT cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  , CAST('' as STRING) as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the CONDITION_SOURCE_VALUE field of the CONDITION_OCCURRENCE table mapped to 0.' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_condition_occurrence_condition_source_value' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 10 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 10 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,10 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  SELECT DISTINCT 
# MAGIC  'CONDITION_OCCURRENCE.CONDITION_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.CONDITION_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.CONDITION_CONCEPT_ID = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.CONDITION_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.CONDITION_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the CONDITION_STATUS_SOURCE_VALUE field of the CONDITION_OCCURRENCE table mapped to 0.' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_STATUS_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_condition_occurrence_condition_status_source_value' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT DISTINCT 
# MAGIC  'CONDITION_OCCURRENCE.CONDITION_STATUS_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.CONDITION_STATUS_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.CONDITION_STATUS_CONCEPT_ID = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.CONDITION_STATUS_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.CONDITION_STATUS_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the CAUSE_SOURCE_VALUE field of the DEATH table mapped to 0.' as check_description
# MAGIC  ,'DEATH' as cdm_table_name
# MAGIC  ,'CAUSE_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_death_cause_source_value' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 10 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 10 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,10 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  SELECT DISTINCT 
# MAGIC  'DEATH.CAUSE_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.CAUSE_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEATH cdmTable
# MAGIC  WHERE cdmTable.CAUSE_SOURCE_CONCEPT_ID = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.CAUSE_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.CAUSE_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEATH cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the DEVICE_SOURCE_VALUE field of the DEVICE_EXPOSURE table mapped to 0.' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'DEVICE_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_device_exposure_device_source_value' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT DISTINCT 
# MAGIC  'DEVICE_EXPOSURE.DEVICE_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.DEVICE_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE cdmTable.DEVICE_CONCEPT_ID = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.DEVICE_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.DEVICE_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the DRUG_SOURCE_VALUE field of the DRUG_EXPOSURE table mapped to 0.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DRUG_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_drug_exposure_drug_source_value' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 10 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 10 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,10 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  SELECT DISTINCT 
# MAGIC  'DRUG_EXPOSURE.DRUG_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.DRUG_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE cdmTable.DRUG_CONCEPT_ID = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.DRUG_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.DRUG_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the ROUTE_SOURCE_VALUE field of the DRUG_EXPOSURE table mapped to 0.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'ROUTE_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_drug_exposure_route_source_value' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT DISTINCT 
# MAGIC  'DRUG_EXPOSURE.ROUTE_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.ROUTE_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE cdmTable.ROUTE_CONCEPT_ID = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.ROUTE_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.ROUTE_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the MEASUREMENT_SOURCE_VALUE field of the MEASUREMENT table mapped to 0.' as check_description
# MAGIC  ,'MEASUREMENT' as cdm_table_name
# MAGIC  ,'MEASUREMENT_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_measurement_measurement_source_value' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 10 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 10 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,10 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  SELECT DISTINCT 
# MAGIC  'MEASUREMENT.MEASUREMENT_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.MEASUREMENT_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC  WHERE cdmTable.MEASUREMENT_CONCEPT_ID = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.MEASUREMENT_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.MEASUREMENT_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the UNIT_SOURCE_VALUE field of the MEASUREMENT table mapped to 0.' as check_description
# MAGIC  ,'MEASUREMENT' as cdm_table_name
# MAGIC  ,'UNIT_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_measurement_unit_source_value' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT DISTINCT 
# MAGIC  'MEASUREMENT.UNIT_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.UNIT_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC  WHERE cdmTable.UNIT_CONCEPT_ID = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.UNIT_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.UNIT_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the OBSERVATION_SOURCE_VALUE field of the OBSERVATION table mapped to 0.' as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'OBSERVATION_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_observation_observation_source_value' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT DISTINCT 
# MAGIC  'OBSERVATION.OBSERVATION_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.OBSERVATION_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  WHERE cdmTable.OBSERVATION_CONCEPT_ID = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.OBSERVATION_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.OBSERVATION_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the UNIT_SOURCE_VALUE field of the OBSERVATION table mapped to 0.' as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'UNIT_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_observation_unit_source_value' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT DISTINCT 
# MAGIC  'OBSERVATION.UNIT_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.UNIT_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  WHERE cdmTable.UNIT_CONCEPT_ID = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.UNIT_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.UNIT_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the QUALIFIER_SOURCE_VALUE field of the OBSERVATION table mapped to 0.' as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'QUALIFIER_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_observation_qualifier_source_value' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT DISTINCT 
# MAGIC  'OBSERVATION.QUALIFIER_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.QUALIFIER_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  WHERE cdmTable.QUALIFIER_CONCEPT_ID = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.QUALIFIER_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.QUALIFIER_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the PAYER_SOURCE_VALUE field of the PAYER_PLAN_PERIOD table mapped to 0.' as check_description
# MAGIC  ,'PAYER_PLAN_PERIOD' as cdm_table_name
# MAGIC  ,'PAYER_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_payer_plan_period_payer_source_value' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT DISTINCT 
# MAGIC  'PAYER_PLAN_PERIOD.PAYER_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.PAYER_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC  WHERE cdmTable.PAYER_CONCEPT_ID = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.PAYER_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.PAYER_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the PLAN_SOURCE_VALUE field of the PAYER_PLAN_PERIOD table mapped to 0.' as check_description
# MAGIC  ,'PAYER_PLAN_PERIOD' as cdm_table_name
# MAGIC  ,'PLAN_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_payer_plan_period_plan_source_value' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT DISTINCT 
# MAGIC  'PAYER_PLAN_PERIOD.PLAN_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.PLAN_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC  WHERE cdmTable.PLAN_CONCEPT_ID = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.PLAN_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.PLAN_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the SPONSOR_SOURCE_VALUE field of the PAYER_PLAN_PERIOD table mapped to 0.' as check_description
# MAGIC  ,'PAYER_PLAN_PERIOD' as cdm_table_name
# MAGIC  ,'SPONSOR_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_payer_plan_period_sponsor_source_value' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT DISTINCT 
# MAGIC  'PAYER_PLAN_PERIOD.SPONSOR_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.SPONSOR_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC  WHERE cdmTable.SPONSOR_CONCEPT_ID = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.SPONSOR_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.SPONSOR_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the FAMILY_SOURCE_VALUE field of the PAYER_PLAN_PERIOD table mapped to 0.' as check_description
# MAGIC  ,'PAYER_PLAN_PERIOD' as cdm_table_name
# MAGIC  ,'FAMILY_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_payer_plan_period_family_source_value' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT DISTINCT 
# MAGIC  'PAYER_PLAN_PERIOD.FAMILY_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.FAMILY_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC  WHERE cdmTable.FAMILY_SOURCE_VALUE = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.FAMILY_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.FAMILY_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the STOP_REASON_SOURCE_VALUE field of the PAYER_PLAN_PERIOD table mapped to 0.' as check_description
# MAGIC  ,'PAYER_PLAN_PERIOD' as cdm_table_name
# MAGIC  ,'STOP_REASON_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_payer_plan_period_stop_reason_source_value' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT DISTINCT 
# MAGIC  'PAYER_PLAN_PERIOD.STOP_REASON_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.STOP_REASON_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC  WHERE cdmTable.STOP_REASON_CONCEPT_ID = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.STOP_REASON_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.STOP_REASON_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the GENDER_SOURCE_VALUE field of the PERSON table mapped to 0.' as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'GENDER_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_person_gender_source_value' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT DISTINCT 
# MAGIC  'PERSON.GENDER_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.GENDER_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE cdmTable.GENDER_CONCEPT_ID = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.GENDER_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.GENDER_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the RACE_SOURCE_VALUE field of the PERSON table mapped to 0.' as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'RACE_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_person_race_source_value' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT DISTINCT 
# MAGIC  'PERSON.RACE_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.RACE_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE cdmTable.RACE_CONCEPT_ID = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.RACE_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.RACE_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the ETHNICITY_SOURCE_VALUE field of the PERSON table mapped to 0.' as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'ETHNICITY_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_person_ethnicity_source_value' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT DISTINCT 
# MAGIC  'PERSON.ETHNICITY_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.ETHNICITY_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE cdmTable.ETHNICITY_CONCEPT_ID = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.ETHNICITY_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.ETHNICITY_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the PROCEDURE_SOURCE_VALUE field of the PROCEDURE_OCCURRENCE table mapped to 0.' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'PROCEDURE_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_procedure_occurrence_procedure_source_value' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 10 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 10 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,10 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  SELECT DISTINCT 
# MAGIC  'PROCEDURE_OCCURRENCE.PROCEDURE_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.PROCEDURE_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.PROCEDURE_CONCEPT_ID = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.PROCEDURE_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.PROCEDURE_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the MODIFIER_SOURCE_VALUE field of the PROCEDURE_OCCURRENCE table mapped to 0.' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'MODIFIER_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_procedure_occurrence_modifier_source_value' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT DISTINCT 
# MAGIC  'PROCEDURE_OCCURRENCE.MODIFIER_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.MODIFIER_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.MODIFIER_CONCEPT_ID = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.MODIFIER_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.MODIFIER_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the SPECIALTY_SOURCE_VALUE field of the PROVIDER table mapped to 0.' as check_description
# MAGIC  ,'PROVIDER' as cdm_table_name
# MAGIC  ,'SPECIALTY_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_provider_specialty_source_value' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT DISTINCT 
# MAGIC  'PROVIDER.SPECIALTY_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.SPECIALTY_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROVIDER cdmTable
# MAGIC  WHERE cdmTable.SPECIALTY_CONCEPT_ID = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.SPECIALTY_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.SPECIALTY_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROVIDER cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the GENDER_SOURCE_VALUE field of the PROVIDER table mapped to 0.' as check_description
# MAGIC  ,'PROVIDER' as cdm_table_name
# MAGIC  ,'GENDER_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_provider_gender_source_value' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT DISTINCT 
# MAGIC  'PROVIDER.GENDER_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.GENDER_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROVIDER cdmTable
# MAGIC  WHERE cdmTable.GENDER_CONCEPT_ID = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.GENDER_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.GENDER_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROVIDER cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the SPECIMEN_SOURCE_VALUE field of the SPECIMEN table mapped to 0.' as check_description
# MAGIC  ,'SPECIMEN' as cdm_table_name
# MAGIC  ,'SPECIMEN_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_specimen_specimen_source_value' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT DISTINCT 
# MAGIC  'SPECIMEN.SPECIMEN_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.SPECIMEN_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC  WHERE cdmTable.SPECIMEN_CONCEPT_ID = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.SPECIMEN_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.SPECIMEN_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the UNIT_SOURCE_VALUE field of the SPECIMEN table mapped to 0.' as check_description
# MAGIC  ,'SPECIMEN' as cdm_table_name
# MAGIC  ,'UNIT_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_specimen_unit_source_value' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT DISTINCT 
# MAGIC  'SPECIMEN.UNIT_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.UNIT_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC  WHERE cdmTable.UNIT_CONCEPT_ID = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.UNIT_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.UNIT_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the ANATOMIC_SITE_SOURCE_VALUE field of the SPECIMEN table mapped to 0.' as check_description
# MAGIC  ,'SPECIMEN' as cdm_table_name
# MAGIC  ,'ANATOMIC_SITE_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_specimen_anatomic_site_source_value' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT DISTINCT 
# MAGIC  'SPECIMEN.ANATOMIC_SITE_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.ANATOMIC_SITE_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC  WHERE cdmTable.ANATOMIC_SITE_CONCEPT_ID = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.ANATOMIC_SITE_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.ANATOMIC_SITE_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the DISEASE_STATUS_SOURCE_VALUE field of the SPECIMEN table mapped to 0.' as check_description
# MAGIC  ,'SPECIMEN' as cdm_table_name
# MAGIC  ,'DISEASE_STATUS_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_specimen_disease_status_source_value' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT DISTINCT 
# MAGIC  'SPECIMEN.DISEASE_STATUS_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.DISEASE_STATUS_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC  WHERE cdmTable.DISEASE_STATUS_CONCEPT_ID = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.DISEASE_STATUS_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.DISEASE_STATUS_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the VISIT_DETAIL_SOURCE_VALUE field of the VISIT_DETAIL table mapped to 0.' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'VISIT_DETAIL_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_visit_detail_visit_detail_source_value' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT DISTINCT 
# MAGIC  'VISIT_DETAIL.VISIT_DETAIL_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.VISIT_DETAIL_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE cdmTable.VISIT_DETAIL_CONCEPT_ID = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.VISIT_DETAIL_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.VISIT_DETAIL_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the admitted_from_source_value field of the VISIT_DETAIL table mapped to 0.' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'admitted_from_source_value' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_visit_detail_admitted_from_source_value' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT DISTINCT 
# MAGIC  'VISIT_DETAIL.admitted_from_source_value' AS violating_field, 
# MAGIC  cdmTable.admitted_from_source_value
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE cdmTable.admitted_from_concept_id = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.admitted_from_source_value) + COUNT(DISTINCT CASE WHEN cdmTable.admitted_from_source_value IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the discharged_TO_SOURCE_VALUE field of the VISIT_DETAIL table mapped to 0.' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'discharged_TO_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_visit_detail_discharged_to_source_value' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT DISTINCT 
# MAGIC  'VISIT_DETAIL.discharged_TO_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.discharged_TO_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE cdmTable.discharged_TO_CONCEPT_ID = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.discharged_TO_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.discharged_TO_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the VISIT_SOURCE_VALUE field of the VISIT_OCCURRENCE table mapped to 0.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'VISIT_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_visit_occurrence_visit_source_value' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 10 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 10 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,10 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  SELECT DISTINCT 
# MAGIC  'VISIT_OCCURRENCE.VISIT_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.VISIT_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.VISIT_CONCEPT_ID = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.VISIT_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.VISIT_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the admitted_from_source_value field of the VISIT_OCCURRENCE table mapped to 0.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'admitted_from_source_value' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_visit_occurrence_admitted_from_source_value' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT DISTINCT 
# MAGIC  'VISIT_OCCURRENCE.admitted_from_source_value' AS violating_field, 
# MAGIC  cdmTable.admitted_from_source_value
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.admitted_from_concept_id = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.admitted_from_source_value) + COUNT(DISTINCT CASE WHEN cdmTable.admitted_from_source_value IS NULL THEN 1 END) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'sourceValueCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of distinct source values in the discharged_TO_SOURCE_VALUE field of the VISIT_OCCURRENCE table mapped to 0.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'discharged_TO_SOURCE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_source_value_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_sourcevaluecompleteness_visit_occurrence_discharged_to_source_value' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT DISTINCT 
# MAGIC  'VISIT_OCCURRENCE.discharged_TO_SOURCE_VALUE' AS violating_field, 
# MAGIC  cdmTable.discharged_TO_SOURCE_VALUE
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.discharged_TO_CONCEPT_ID = 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(DISTINCT cdmTable.discharged_TO_SOURCE_VALUE) + COUNT(DISTINCT CASE WHEN cdmTable.discharged_TO_SOURCE_VALUE IS NULL THEN 1 END) AS num_rows
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
# MAGIC -- field plausible value high
# MAGIC
# MAGIC WITH cte_all  AS (SELECT cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  , CAST('' as STRING) as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the CONDITION_ERA_START_DATE field of the CONDITION_ERA table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'CONDITION_ERA' as cdm_table_name
# MAGIC  ,'CONDITION_ERA_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_condition_era_condition_era_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'CONDITION_ERA.CONDITION_ERA_START_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable
# MAGIC  WHERE cast(cdmTable.CONDITION_ERA_START_DATE as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable
# MAGIC  WHERE CONDITION_ERA_START_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the CONDITION_ERA_END_DATE field of the CONDITION_ERA table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'CONDITION_ERA' as cdm_table_name
# MAGIC  ,'CONDITION_ERA_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_condition_era_condition_era_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'CONDITION_ERA.CONDITION_ERA_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable
# MAGIC  WHERE cast(cdmTable.CONDITION_ERA_END_DATE as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable
# MAGIC  WHERE CONDITION_ERA_END_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the CONDITION_START_DATE field of the CONDITION_OCCURRENCE table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_condition_occurrence_condition_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'CONDITION_OCCURRENCE.CONDITION_START_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE cast(cdmTable.CONDITION_START_DATE as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE CONDITION_START_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the CONDITION_START_DATETIME field of the CONDITION_OCCURRENCE table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_START_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_condition_occurrence_condition_start_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'CONDITION_OCCURRENCE.CONDITION_START_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE cast(cdmTable.CONDITION_START_DATETIME as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE CONDITION_START_DATETIME IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the CONDITION_END_DATE field of the CONDITION_OCCURRENCE table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_condition_occurrence_condition_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'CONDITION_OCCURRENCE.CONDITION_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE cast(cdmTable.CONDITION_END_DATE as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE CONDITION_END_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the CONDITION_END_DATETIME field of the CONDITION_OCCURRENCE table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_END_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_condition_occurrence_condition_end_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'CONDITION_OCCURRENCE.CONDITION_END_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE cast(cdmTable.CONDITION_END_DATETIME as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE CONDITION_END_DATETIME IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DEATH_DATE field of the DEATH table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'DEATH' as cdm_table_name
# MAGIC  ,'DEATH_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_death_death_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DEATH.DEATH_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEATH cdmTable
# MAGIC  WHERE cast(cdmTable.DEATH_DATE as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEATH cdmTable
# MAGIC  WHERE DEATH_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the death_timestamp field of the DEATH table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'DEATH' as cdm_table_name
# MAGIC  ,'death_timestamp' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_death_death_timestamp' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DEATH.death_timestamp' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEATH cdmTable
# MAGIC  WHERE cast(cdmTable.death_timestamp as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEATH cdmTable
# MAGIC  WHERE death_timestamp IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DEVICE_EXPOSURE_START_DATE field of the DEVICE_EXPOSURE table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'DEVICE_EXPOSURE_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_device_exposure_device_exposure_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DEVICE_EXPOSURE.DEVICE_EXPOSURE_START_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE cast(cdmTable.DEVICE_EXPOSURE_START_DATE as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE DEVICE_EXPOSURE_START_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DEVICE_EXPOSURE_START_DATETIME field of the DEVICE_EXPOSURE table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'DEVICE_EXPOSURE_START_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_device_exposure_device_exposure_start_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DEVICE_EXPOSURE.DEVICE_EXPOSURE_START_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE cast(cdmTable.DEVICE_EXPOSURE_START_DATETIME as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE DEVICE_EXPOSURE_START_DATETIME IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DEVICE_EXPOSURE_END_DATE field of the DEVICE_EXPOSURE table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'DEVICE_EXPOSURE_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_device_exposure_device_exposure_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DEVICE_EXPOSURE.DEVICE_EXPOSURE_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE cast(cdmTable.DEVICE_EXPOSURE_END_DATE as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE DEVICE_EXPOSURE_END_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DEVICE_EXPOSURE_END_DATETIME field of the DEVICE_EXPOSURE table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'DEVICE_EXPOSURE_END_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_device_exposure_device_exposure_end_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DEVICE_EXPOSURE.DEVICE_EXPOSURE_END_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE cast(cdmTable.DEVICE_EXPOSURE_END_DATETIME as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE DEVICE_EXPOSURE_END_DATETIME IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DOSE_ERA_START_DATE field of the DOSE_ERA table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'DOSE_ERA' as cdm_table_name
# MAGIC  ,'DOSE_ERA_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_dose_era_dose_era_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DOSE_ERA.DOSE_ERA_START_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC  WHERE cast(cdmTable.DOSE_ERA_START_DATE as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC  WHERE DOSE_ERA_START_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DOSE_ERA_END_DATE field of the DOSE_ERA table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'DOSE_ERA' as cdm_table_name
# MAGIC  ,'DOSE_ERA_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_dose_era_dose_era_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DOSE_ERA.DOSE_ERA_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC  WHERE cast(cdmTable.DOSE_ERA_END_DATE as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC  WHERE DOSE_ERA_END_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DRUG_ERA_START_DATE field of the DRUG_ERA table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'DRUG_ERA' as cdm_table_name
# MAGIC  ,'DRUG_ERA_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_drug_era_drug_era_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DRUG_ERA.DRUG_ERA_START_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC  WHERE cast(cdmTable.DRUG_ERA_START_DATE as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC  WHERE DRUG_ERA_START_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DRUG_ERA_END_DATE field of the DRUG_ERA table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'DRUG_ERA' as cdm_table_name
# MAGIC  ,'DRUG_ERA_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_drug_era_drug_era_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DRUG_ERA.DRUG_ERA_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC  WHERE cast(cdmTable.DRUG_ERA_END_DATE as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC  WHERE DRUG_ERA_END_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DRUG_EXPOSURE_START_DATE field of the DRUG_EXPOSURE table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DRUG_EXPOSURE_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_drug_exposure_drug_exposure_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DRUG_EXPOSURE.DRUG_EXPOSURE_START_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE cast(cdmTable.DRUG_EXPOSURE_START_DATE as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE DRUG_EXPOSURE_START_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DRUG_EXPOSURE_START_DATETIME field of the DRUG_EXPOSURE table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DRUG_EXPOSURE_START_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_drug_exposure_drug_exposure_start_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DRUG_EXPOSURE.DRUG_EXPOSURE_START_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE cast(cdmTable.DRUG_EXPOSURE_START_DATETIME as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE DRUG_EXPOSURE_START_DATETIME IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DRUG_EXPOSURE_END_DATE field of the DRUG_EXPOSURE table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DRUG_EXPOSURE_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_drug_exposure_drug_exposure_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DRUG_EXPOSURE.DRUG_EXPOSURE_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE cast(cdmTable.DRUG_EXPOSURE_END_DATE as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE DRUG_EXPOSURE_END_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DRUG_EXPOSURE_END_DATETIME field of the DRUG_EXPOSURE table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DRUG_EXPOSURE_END_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_drug_exposure_drug_exposure_end_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DRUG_EXPOSURE.DRUG_EXPOSURE_END_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE cast(cdmTable.DRUG_EXPOSURE_END_DATETIME as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE DRUG_EXPOSURE_END_DATETIME IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the VERBATIM_END_DATE field of the DRUG_EXPOSURE table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'VERBATIM_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_drug_exposure_verbatim_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DRUG_EXPOSURE.VERBATIM_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE cast(cdmTable.VERBATIM_END_DATE as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE VERBATIM_END_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the REFILLS field of the DRUG_EXPOSURE table greater than 24.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'REFILLS' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_drug_exposure_refills' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.REFILLS > 24
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE REFILLS IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the QUANTITY field of the DRUG_EXPOSURE table greater than 1095.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'QUANTITY' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_drug_exposure_quantity' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DRUG_EXPOSURE.QUANTITY' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE cdmTable.QUANTITY > 1095
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE QUANTITY IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DAYS_SUPPLY field of the DRUG_EXPOSURE table greater than 365.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DAYS_SUPPLY' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_drug_exposure_days_supply' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.DAYS_SUPPLY > 365
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE DAYS_SUPPLY IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the MEASUREMENT_DATE field of the MEASUREMENT table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'MEASUREMENT' as cdm_table_name
# MAGIC  ,'MEASUREMENT_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_measurement_measurement_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'MEASUREMENT.MEASUREMENT_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC  WHERE cast(cdmTable.MEASUREMENT_DATE as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC  WHERE MEASUREMENT_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the NOTE_DATE field of the NOTE table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'NOTE' as cdm_table_name
# MAGIC  ,'NOTE_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_note_note_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'NOTE.NOTE_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC  WHERE cast(cdmTable.NOTE_DATE as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC  WHERE NOTE_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the note_timestamp field of the NOTE table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'NOTE' as cdm_table_name
# MAGIC  ,'note_timestamp' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_note_note_timestamp' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'NOTE.note_timestamp' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC  WHERE cast(cdmTable.note_timestamp as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC  WHERE note_timestamp IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the nlp_timestamp field of the NOTE_NLP table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'NOTE_NLP' as cdm_table_name
# MAGIC  ,'nlp_timestamp' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_note_nlp_nlp_timestamp' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'NOTE_NLP.nlp_timestamp' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE_NLP cdmTable
# MAGIC  WHERE cast(cdmTable.nlp_timestamp as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE_NLP cdmTable
# MAGIC  WHERE nlp_timestamp IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the OBSERVATION_DATE field of the OBSERVATION table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'OBSERVATION_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_observation_observation_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'OBSERVATION.OBSERVATION_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  WHERE cast(cdmTable.OBSERVATION_DATE as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  WHERE OBSERVATION_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the OBSERVATION_DATETIME field of the OBSERVATION table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'OBSERVATION_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_observation_observation_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'OBSERVATION.OBSERVATION_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  WHERE cast(cdmTable.OBSERVATION_DATETIME as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  WHERE OBSERVATION_DATETIME IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the OBSERVATION_PERIOD_START_DATE field of the OBSERVATION_PERIOD table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'OBSERVATION_PERIOD' as cdm_table_name
# MAGIC  ,'OBSERVATION_PERIOD_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_observation_period_observation_period_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'OBSERVATION_PERIOD.OBSERVATION_PERIOD_START_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD cdmTable
# MAGIC  WHERE cast(cdmTable.OBSERVATION_PERIOD_START_DATE as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD cdmTable
# MAGIC  WHERE OBSERVATION_PERIOD_START_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the OBSERVATION_PERIOD_END_DATE field of the OBSERVATION_PERIOD table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'OBSERVATION_PERIOD' as cdm_table_name
# MAGIC  ,'OBSERVATION_PERIOD_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_observation_period_observation_period_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'OBSERVATION_PERIOD.OBSERVATION_PERIOD_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD cdmTable
# MAGIC  WHERE cast(cdmTable.OBSERVATION_PERIOD_END_DATE as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD cdmTable
# MAGIC  WHERE OBSERVATION_PERIOD_END_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the YEAR_OF_BIRTH field of the PERSON table greater than YEAR(GETDATE())+1.' as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'YEAR_OF_BIRTH' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_person_year_of_birth' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.YEAR_OF_BIRTH > YEAR(CURRENT_DATE)+1
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE YEAR_OF_BIRTH IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the MONTH_OF_BIRTH field of the PERSON table greater than 12.' as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'MONTH_OF_BIRTH' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_person_month_of_birth' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.MONTH_OF_BIRTH > 12
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE MONTH_OF_BIRTH IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DAY_OF_BIRTH field of the PERSON table greater than 31.' as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'DAY_OF_BIRTH' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_person_day_of_birth' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.DAY_OF_BIRTH > 31
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE DAY_OF_BIRTH IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the birth_timestamp field of the PERSON table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'birth_timestamp' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_person_birth_timestamp' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'PERSON.birth_timestamp' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE cast(cdmTable.birth_timestamp as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE birth_timestamp IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the PROCEDURE_DATE field of the PROCEDURE_OCCURRENCE table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'PROCEDURE_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_procedure_occurrence_procedure_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'PROCEDURE_OCCURRENCE.PROCEDURE_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  WHERE cast(cdmTable.PROCEDURE_DATE as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  WHERE PROCEDURE_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the PROCEDURE_DATETIME field of the PROCEDURE_OCCURRENCE table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'PROCEDURE_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_procedure_occurrence_procedure_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'PROCEDURE_OCCURRENCE.PROCEDURE_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  WHERE cast(cdmTable.PROCEDURE_DATETIME as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  WHERE PROCEDURE_DATETIME IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the SPECIMEN_DATE field of the SPECIMEN table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'SPECIMEN' as cdm_table_name
# MAGIC  ,'SPECIMEN_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_specimen_specimen_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'SPECIMEN.SPECIMEN_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC  WHERE cast(cdmTable.SPECIMEN_DATE as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC  WHERE SPECIMEN_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the specimen_timestamp field of the SPECIMEN table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'SPECIMEN' as cdm_table_name
# MAGIC  ,'specimen_timestamp' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_specimen_specimen_timestamp' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'SPECIMEN.specimen_timestamp' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC  WHERE cast(cdmTable.specimen_timestamp as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC  WHERE specimen_timestamp IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the VISIT_DETAIL_START_DATE field of the VISIT_DETAIL table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'VISIT_DETAIL_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_visit_detail_visit_detail_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'VISIT_DETAIL.VISIT_DETAIL_START_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE cast(cdmTable.VISIT_DETAIL_START_DATE as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE VISIT_DETAIL_START_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the visit_detail_start_timestamp field of the VISIT_DETAIL table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'visit_detail_start_timestamp' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_visit_detail_visit_detail_start_timestamp' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'VISIT_DETAIL.visit_detail_start_timestamp' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE cast(cdmTable.visit_detail_start_timestamp as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE visit_detail_start_timestamp IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the VISIT_DETAIL_END_DATE field of the VISIT_DETAIL table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'VISIT_DETAIL_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_visit_detail_visit_detail_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'VISIT_DETAIL.VISIT_DETAIL_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE cast(cdmTable.VISIT_DETAIL_END_DATE as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE VISIT_DETAIL_END_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the visit_detail_end_timestamp field of the VISIT_DETAIL table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'visit_detail_end_timestamp' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_visit_detail_visit_detail_end_timestamp' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'VISIT_DETAIL.visit_detail_end_timestamp' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE cast(cdmTable.visit_detail_end_timestamp as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE visit_detail_end_timestamp IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the VISIT_START_DATE field of the VISIT_OCCURRENCE table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'VISIT_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_visit_occurrence_visit_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'VISIT_OCCURRENCE.VISIT_START_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE cast(cdmTable.VISIT_START_DATE as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE VISIT_START_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the visit_start_timestamp field of the VISIT_OCCURRENCE table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'visit_start_timestamp' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_visit_occurrence_visit_start_timestamp' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'VISIT_OCCURRENCE.visit_start_timestamp' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE cast(cdmTable.visit_start_timestamp as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE visit_start_timestamp IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the VISIT_END_DATE field of the VISIT_OCCURRENCE table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'VISIT_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_visit_occurrence_visit_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'VISIT_OCCURRENCE.VISIT_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE cast(cdmTable.VISIT_END_DATE as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE VISIT_END_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueHigh' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the visit_end_timestamp field of the VISIT_OCCURRENCE table greater than DATEADD(DD,1,GETDATE()).' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'visit_end_timestamp' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_high.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluehigh_visit_occurrence_visit_end_timestamp' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'VISIT_OCCURRENCE.visit_end_timestamp' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE cast(cdmTable.visit_end_timestamp as date) > cast(DATE_ADD(day,1,CURRENT_DATE) as date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE visit_end_timestamp IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte
# MAGIC )
# MAGIC INSERT INTO ${results_catalog}.${results_schema}.dqdashboard_results
# MAGIC SELECT *
# MAGIC FROM cte_all;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- field plausible value low
# MAGIC
# MAGIC WITH cte_all  AS (SELECT cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  , CAST('' as STRING) as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the CONDITION_ERA_START_DATE field of the CONDITION_ERA table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'CONDITION_ERA' as cdm_table_name
# MAGIC  ,'CONDITION_ERA_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_condition_era_condition_era_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE CAST(cdmTable.CONDITION_ERA_START_DATE AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable
# MAGIC  WHERE CONDITION_ERA_START_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the CONDITION_ERA_END_DATE field of the CONDITION_ERA table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'CONDITION_ERA' as cdm_table_name
# MAGIC  ,'CONDITION_ERA_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_condition_era_condition_era_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE CAST(cdmTable.CONDITION_ERA_END_DATE AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable
# MAGIC  WHERE CONDITION_ERA_END_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the CONDITION_OCCURRENCE_COUNT field of the CONDITION_ERA table less than 1.' as check_description
# MAGIC  ,'CONDITION_ERA' as cdm_table_name
# MAGIC  ,'CONDITION_OCCURRENCE_COUNT' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_condition_era_condition_occurrence_count' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'CONDITION_ERA.CONDITION_OCCURRENCE_COUNT' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable
# MAGIC  WHERE cdmTable.CONDITION_OCCURRENCE_COUNT < 1
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable
# MAGIC  WHERE CONDITION_OCCURRENCE_COUNT IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the CONDITION_START_DATE field of the CONDITION_OCCURRENCE table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_condition_occurrence_condition_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE CAST(cdmTable.CONDITION_START_DATE AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE CONDITION_START_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the CONDITION_START_DATETIME field of the CONDITION_OCCURRENCE table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_START_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_condition_occurrence_condition_start_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'CONDITION_OCCURRENCE.CONDITION_START_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE CAST(cdmTable.CONDITION_START_DATETIME AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE CONDITION_START_DATETIME IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the CONDITION_END_DATE field of the CONDITION_OCCURRENCE table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_condition_occurrence_condition_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'CONDITION_OCCURRENCE.CONDITION_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE CAST(cdmTable.CONDITION_END_DATE AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE CONDITION_END_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the CONDITION_END_DATETIME field of the CONDITION_OCCURRENCE table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_END_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_condition_occurrence_condition_end_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'CONDITION_OCCURRENCE.CONDITION_END_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE CAST(cdmTable.CONDITION_END_DATETIME AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE CONDITION_END_DATETIME IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the DEATH_DATE field of the DEATH table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'DEATH' as cdm_table_name
# MAGIC  ,'DEATH_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_death_death_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE CAST(cdmTable.DEATH_DATE AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEATH cdmTable
# MAGIC  WHERE DEATH_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the death_timestamp field of the DEATH table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'DEATH' as cdm_table_name
# MAGIC  ,'death_timestamp' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_death_death_timestamp' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DEATH.death_timestamp' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEATH cdmTable
# MAGIC  WHERE CAST(cdmTable.death_timestamp AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEATH cdmTable
# MAGIC  WHERE death_timestamp IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the DEVICE_EXPOSURE_START_DATE field of the DEVICE_EXPOSURE table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'DEVICE_EXPOSURE_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_device_exposure_device_exposure_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE CAST(cdmTable.DEVICE_EXPOSURE_START_DATE AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE DEVICE_EXPOSURE_START_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the DEVICE_EXPOSURE_START_DATETIME field of the DEVICE_EXPOSURE table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'DEVICE_EXPOSURE_START_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_device_exposure_device_exposure_start_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DEVICE_EXPOSURE.DEVICE_EXPOSURE_START_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE CAST(cdmTable.DEVICE_EXPOSURE_START_DATETIME AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE DEVICE_EXPOSURE_START_DATETIME IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the DEVICE_EXPOSURE_END_DATE field of the DEVICE_EXPOSURE table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'DEVICE_EXPOSURE_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_device_exposure_device_exposure_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DEVICE_EXPOSURE.DEVICE_EXPOSURE_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE CAST(cdmTable.DEVICE_EXPOSURE_END_DATE AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE DEVICE_EXPOSURE_END_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the DEVICE_EXPOSURE_END_DATETIME field of the DEVICE_EXPOSURE table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'DEVICE_EXPOSURE_END_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_device_exposure_device_exposure_end_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DEVICE_EXPOSURE.DEVICE_EXPOSURE_END_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE CAST(cdmTable.DEVICE_EXPOSURE_END_DATETIME AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE DEVICE_EXPOSURE_END_DATETIME IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the QUANTITY field of the DEVICE_EXPOSURE table less than 1.' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'QUANTITY' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_device_exposure_quantity' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DEVICE_EXPOSURE.QUANTITY' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE cdmTable.QUANTITY < 1
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE QUANTITY IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DOSE_VALUE field of the DOSE_ERA table less than 0.' as check_description
# MAGIC  ,'DOSE_ERA' as cdm_table_name
# MAGIC  ,'DOSE_VALUE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_dose_era_dose_value' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.DOSE_VALUE < 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC  WHERE DOSE_VALUE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the DOSE_ERA_START_DATE field of the DOSE_ERA table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'DOSE_ERA' as cdm_table_name
# MAGIC  ,'DOSE_ERA_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_dose_era_dose_era_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE CAST(cdmTable.DOSE_ERA_START_DATE AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC  WHERE DOSE_ERA_START_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the DOSE_ERA_END_DATE field of the DOSE_ERA table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'DOSE_ERA' as cdm_table_name
# MAGIC  ,'DOSE_ERA_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_dose_era_dose_era_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE CAST(cdmTable.DOSE_ERA_END_DATE AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC  WHERE DOSE_ERA_END_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the DRUG_ERA_START_DATE field of the DRUG_ERA table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'DRUG_ERA' as cdm_table_name
# MAGIC  ,'DRUG_ERA_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_drug_era_drug_era_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE CAST(cdmTable.DRUG_ERA_START_DATE AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC  WHERE DRUG_ERA_START_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the DRUG_ERA_END_DATE field of the DRUG_ERA table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'DRUG_ERA' as cdm_table_name
# MAGIC  ,'DRUG_ERA_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_drug_era_drug_era_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE CAST(cdmTable.DRUG_ERA_END_DATE AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC  WHERE DRUG_ERA_END_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DRUG_EXPOSURE_COUNT field of the DRUG_ERA table less than 1.' as check_description
# MAGIC  ,'DRUG_ERA' as cdm_table_name
# MAGIC  ,'DRUG_EXPOSURE_COUNT' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_drug_era_drug_exposure_count' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DRUG_ERA.DRUG_EXPOSURE_COUNT' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC  WHERE cdmTable.DRUG_EXPOSURE_COUNT < 1
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC  WHERE DRUG_EXPOSURE_COUNT IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the GAP_DAYS field of the DRUG_ERA table less than 0.' as check_description
# MAGIC  ,'DRUG_ERA' as cdm_table_name
# MAGIC  ,'GAP_DAYS' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_drug_era_gap_days' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DRUG_ERA.GAP_DAYS' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC  WHERE cdmTable.GAP_DAYS < 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC  WHERE GAP_DAYS IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the DRUG_EXPOSURE_START_DATE field of the DRUG_EXPOSURE table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DRUG_EXPOSURE_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_drug_exposure_drug_exposure_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE CAST(cdmTable.DRUG_EXPOSURE_START_DATE AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE DRUG_EXPOSURE_START_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the DRUG_EXPOSURE_START_DATETIME field of the DRUG_EXPOSURE table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DRUG_EXPOSURE_START_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_drug_exposure_drug_exposure_start_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DRUG_EXPOSURE.DRUG_EXPOSURE_START_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE CAST(cdmTable.DRUG_EXPOSURE_START_DATETIME AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE DRUG_EXPOSURE_START_DATETIME IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the DRUG_EXPOSURE_END_DATE field of the DRUG_EXPOSURE table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DRUG_EXPOSURE_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_drug_exposure_drug_exposure_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE CAST(cdmTable.DRUG_EXPOSURE_END_DATE AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE DRUG_EXPOSURE_END_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the DRUG_EXPOSURE_END_DATETIME field of the DRUG_EXPOSURE table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DRUG_EXPOSURE_END_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_drug_exposure_drug_exposure_end_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DRUG_EXPOSURE.DRUG_EXPOSURE_END_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE CAST(cdmTable.DRUG_EXPOSURE_END_DATETIME AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE DRUG_EXPOSURE_END_DATETIME IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the VERBATIM_END_DATE field of the DRUG_EXPOSURE table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'VERBATIM_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_drug_exposure_verbatim_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DRUG_EXPOSURE.VERBATIM_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE CAST(cdmTable.VERBATIM_END_DATE AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE VERBATIM_END_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the REFILLS field of the DRUG_EXPOSURE table less than 0.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'REFILLS' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_drug_exposure_refills' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DRUG_EXPOSURE.REFILLS' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE cdmTable.REFILLS < 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE REFILLS IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the QUANTITY field of the DRUG_EXPOSURE table less than 0.0000001.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'QUANTITY' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_drug_exposure_quantity' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DRUG_EXPOSURE.QUANTITY' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE cdmTable.QUANTITY < 0.0000001
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE QUANTITY IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DAYS_SUPPLY field of the DRUG_EXPOSURE table less than 1.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DAYS_SUPPLY' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_drug_exposure_days_supply' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DRUG_EXPOSURE.DAYS_SUPPLY' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE cdmTable.DAYS_SUPPLY < 1
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE DAYS_SUPPLY IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the MEASUREMENT_DATE field of the MEASUREMENT table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'MEASUREMENT' as cdm_table_name
# MAGIC  ,'MEASUREMENT_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_measurement_measurement_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE CAST(cdmTable.MEASUREMENT_DATE AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC  WHERE MEASUREMENT_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the NOTE_DATE field of the NOTE table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'NOTE' as cdm_table_name
# MAGIC  ,'NOTE_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_note_note_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE CAST(cdmTable.NOTE_DATE AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC  WHERE NOTE_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the note_timestamp field of the NOTE table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'NOTE' as cdm_table_name
# MAGIC  ,'note_timestamp' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_note_note_timestamp' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'NOTE.note_timestamp' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC  WHERE CAST(cdmTable.note_timestamp AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC  WHERE note_timestamp IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the nlp_timestamp field of the NOTE_NLP table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'NOTE_NLP' as cdm_table_name
# MAGIC  ,'nlp_timestamp' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_note_nlp_nlp_timestamp' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'NOTE_NLP.nlp_timestamp' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE_NLP cdmTable
# MAGIC  WHERE CAST(cdmTable.nlp_timestamp AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE_NLP cdmTable
# MAGIC  WHERE nlp_timestamp IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the OBSERVATION_DATE field of the OBSERVATION table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'OBSERVATION_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_observation_observation_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE CAST(cdmTable.OBSERVATION_DATE AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  WHERE OBSERVATION_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the OBSERVATION_DATETIME field of the OBSERVATION table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'OBSERVATION_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_observation_observation_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'OBSERVATION.OBSERVATION_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  WHERE CAST(cdmTable.OBSERVATION_DATETIME AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  WHERE OBSERVATION_DATETIME IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the OBSERVATION_PERIOD_START_DATE field of the OBSERVATION_PERIOD table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'OBSERVATION_PERIOD' as cdm_table_name
# MAGIC  ,'OBSERVATION_PERIOD_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_observation_period_observation_period_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE CAST(cdmTable.OBSERVATION_PERIOD_START_DATE AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD cdmTable
# MAGIC  WHERE OBSERVATION_PERIOD_START_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the OBSERVATION_PERIOD_END_DATE field of the OBSERVATION_PERIOD table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'OBSERVATION_PERIOD' as cdm_table_name
# MAGIC  ,'OBSERVATION_PERIOD_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_observation_period_observation_period_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE CAST(cdmTable.OBSERVATION_PERIOD_END_DATE AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD cdmTable
# MAGIC  WHERE OBSERVATION_PERIOD_END_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the YEAR_OF_BIRTH field of the PERSON table less than 1850.' as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'YEAR_OF_BIRTH' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_person_year_of_birth' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.YEAR_OF_BIRTH < 1850
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE YEAR_OF_BIRTH IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the MONTH_OF_BIRTH field of the PERSON table less than 1.' as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'MONTH_OF_BIRTH' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_person_month_of_birth' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'PERSON.MONTH_OF_BIRTH' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE cdmTable.MONTH_OF_BIRTH < 1
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE MONTH_OF_BIRTH IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DAY_OF_BIRTH field of the PERSON table less than 1.' as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'DAY_OF_BIRTH' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_person_day_of_birth' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'PERSON.DAY_OF_BIRTH' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE cdmTable.DAY_OF_BIRTH < 1
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE DAY_OF_BIRTH IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the birth_timestamp field of the PERSON table less than ','\047','18500101','\047','.') as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'birth_timestamp' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_person_birth_timestamp' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'PERSON.birth_timestamp' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE CAST(cdmTable.birth_timestamp AS DATE) < CAST('18500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE birth_timestamp IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the PROCEDURE_DATE field of the PROCEDURE_OCCURRENCE table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'PROCEDURE_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_procedure_occurrence_procedure_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE CAST(cdmTable.PROCEDURE_DATE AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  WHERE PROCEDURE_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the PROCEDURE_DATETIME field of the PROCEDURE_OCCURRENCE table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'PROCEDURE_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_procedure_occurrence_procedure_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'PROCEDURE_OCCURRENCE.PROCEDURE_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  WHERE CAST(cdmTable.PROCEDURE_DATETIME AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  WHERE PROCEDURE_DATETIME IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the QUANTITY field of the PROCEDURE_OCCURRENCE table less than 1.' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'QUANTITY' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_procedure_occurrence_quantity' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'PROCEDURE_OCCURRENCE.QUANTITY' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.QUANTITY < 1
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  WHERE QUANTITY IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the SPECIMEN_DATE field of the SPECIMEN table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'SPECIMEN' as cdm_table_name
# MAGIC  ,'SPECIMEN_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_specimen_specimen_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE CAST(cdmTable.SPECIMEN_DATE AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC  WHERE SPECIMEN_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the specimen_timestamp field of the SPECIMEN table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'SPECIMEN' as cdm_table_name
# MAGIC  ,'specimen_timestamp' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_specimen_specimen_timestamp' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'SPECIMEN.specimen_timestamp' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC  WHERE CAST(cdmTable.specimen_timestamp AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC  WHERE specimen_timestamp IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the QUANTITY field of the SPECIMEN table less than 0.' as check_description
# MAGIC  ,'SPECIMEN' as cdm_table_name
# MAGIC  ,'QUANTITY' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_specimen_quantity' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'SPECIMEN.QUANTITY' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC  WHERE cdmTable.QUANTITY < 0
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC  WHERE QUANTITY IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the VISIT_DETAIL_START_DATE field of the VISIT_DETAIL table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'VISIT_DETAIL_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_visit_detail_visit_detail_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE CAST(cdmTable.VISIT_DETAIL_START_DATE AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE VISIT_DETAIL_START_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the visit_detail_start_timestamp field of the VISIT_DETAIL table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'visit_detail_start_timestamp' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_visit_detail_visit_detail_start_timestamp' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'VISIT_DETAIL.visit_detail_start_timestamp' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE CAST(cdmTable.visit_detail_start_timestamp AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE visit_detail_start_timestamp IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the VISIT_DETAIL_END_DATE field of the VISIT_DETAIL table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'VISIT_DETAIL_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_visit_detail_visit_detail_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE CAST(cdmTable.VISIT_DETAIL_END_DATE AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE VISIT_DETAIL_END_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the visit_detail_end_timestamp field of the VISIT_DETAIL table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'visit_detail_end_timestamp' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_visit_detail_visit_detail_end_timestamp' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'VISIT_DETAIL.visit_detail_end_timestamp' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE CAST(cdmTable.visit_detail_end_timestamp AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE visit_detail_end_timestamp IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the VISIT_START_DATE field of the VISIT_OCCURRENCE table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'VISIT_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_visit_occurrence_visit_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE CAST(cdmTable.VISIT_START_DATE AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE VISIT_START_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the visit_start_timestamp field of the VISIT_OCCURRENCE table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'visit_start_timestamp' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_visit_occurrence_visit_start_timestamp' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'VISIT_OCCURRENCE.visit_start_timestamp' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE CAST(cdmTable.visit_start_timestamp AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE visit_start_timestamp IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the VISIT_END_DATE field of the VISIT_OCCURRENCE table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'VISIT_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_visit_occurrence_visit_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE CAST(cdmTable.VISIT_END_DATE AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE VISIT_END_DATE IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleValueLow' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,CONCAT('The number and percent of records with a value in the visit_end_timestamp field of the VISIT_OCCURRENCE table less than ','\047','19500101','\047','.') as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'visit_end_timestamp' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_value_low.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Atemporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausiblevaluelow_visit_occurrence_visit_end_timestamp' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'VISIT_OCCURRENCE.visit_end_timestamp' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE CAST(cdmTable.visit_end_timestamp AS DATE) < CAST('19500101' AS DATE)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE visit_end_timestamp IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte
# MAGIC )
# MAGIC INSERT INTO ${results_catalog}.${results_schema}.dqdashboard_results
# MAGIC SELECT *
# MAGIC FROM cte_all;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- field plausible temporal after
# MAGIC
# MAGIC WITH cte_all  AS (SELECT cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  , CAST('' as STRING) as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the CONDITION_ERA_START_DATE field of the CONDITION_ERA that occurs prior to the date in the birth_timestamp field of the PERSON table.' as check_description
# MAGIC  ,'CONDITION_ERA' as cdm_table_name
# MAGIC  ,'CONDITION_ERA_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_condition_era_condition_era_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'CONDITION_ERA.CONDITION_ERA_START_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.PERSON plausibleTable 
# MAGIC  ON cdmTable.person_id = plausibleTable.person_id
# MAGIC  WHERE 
# MAGIC  COALESCE(
# MAGIC  CAST(plausibleTable.birth_timestamp AS DATE),
# MAGIC  CAST(CONCAT(plausibleTable.year_of_birth,'0601') AS DATE)
# MAGIC  ) 
# MAGIC  > CAST(cdmTable.CONDITION_ERA_START_DATE AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the CONDITION_ERA_END_DATE field of the CONDITION_ERA that occurs prior to the date in the birth_timestamp field of the PERSON table.' as check_description
# MAGIC  ,'CONDITION_ERA' as cdm_table_name
# MAGIC  ,'CONDITION_ERA_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_condition_era_condition_era_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'CONDITION_ERA.CONDITION_ERA_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.PERSON plausibleTable 
# MAGIC  ON cdmTable.person_id = plausibleTable.person_id
# MAGIC  WHERE 
# MAGIC  COALESCE(
# MAGIC  CAST(plausibleTable.birth_timestamp AS DATE),
# MAGIC  CAST(CONCAT(plausibleTable.year_of_birth,'0601') AS DATE)
# MAGIC  ) 
# MAGIC  > CAST(cdmTable.CONDITION_ERA_END_DATE AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the CONDITION_START_DATE field of the CONDITION_OCCURRENCE that occurs prior to the date in the birth_timestamp field of the PERSON table.' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_condition_occurrence_condition_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'CONDITION_OCCURRENCE.CONDITION_START_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.PERSON plausibleTable 
# MAGIC  ON cdmTable.person_id = plausibleTable.person_id
# MAGIC  WHERE 
# MAGIC  COALESCE(
# MAGIC  CAST(plausibleTable.birth_timestamp AS DATE),
# MAGIC  CAST(CONCAT(plausibleTable.year_of_birth,'0601') AS DATE)
# MAGIC  ) 
# MAGIC  > CAST(cdmTable.CONDITION_START_DATE AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the CONDITION_START_DATETIME field of the CONDITION_OCCURRENCE that occurs prior to the date in the birth_timestamp field of the PERSON table.' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_START_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_condition_occurrence_condition_start_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'CONDITION_OCCURRENCE.CONDITION_START_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.PERSON plausibleTable 
# MAGIC  ON cdmTable.person_id = plausibleTable.person_id
# MAGIC  WHERE 
# MAGIC  COALESCE(
# MAGIC  CAST(plausibleTable.birth_timestamp AS DATE),
# MAGIC  CAST(CONCAT(plausibleTable.year_of_birth,'0601') AS DATE)
# MAGIC  ) 
# MAGIC  > CAST(cdmTable.CONDITION_START_DATETIME AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the CONDITION_END_DATE field of the CONDITION_OCCURRENCE that occurs prior to the date in the CONDITION_START_DATE field of the CONDITION_OCCURRENCE table.' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_condition_occurrence_condition_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'CONDITION_OCCURRENCE.CONDITION_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE 
# MAGIC  CAST(cdmTable.CONDITION_START_DATE AS DATE)
# MAGIC  > CAST(cdmTable.CONDITION_END_DATE AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the CONDITION_END_DATETIME field of the CONDITION_OCCURRENCE that occurs prior to the date in the CONDITION_START_DATETIME field of the CONDITION_OCCURRENCE table.' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_END_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_condition_occurrence_condition_end_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'CONDITION_OCCURRENCE.CONDITION_END_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE 
# MAGIC  CAST(cdmTable.CONDITION_START_DATETIME AS DATE)
# MAGIC  > CAST(cdmTable.CONDITION_END_DATETIME AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DEATH_DATE field of the DEATH that occurs prior to the date in the birth_timestamp field of the PERSON table.' as check_description
# MAGIC  ,'DEATH' as cdm_table_name
# MAGIC  ,'DEATH_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_death_death_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DEATH.DEATH_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEATH cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.PERSON plausibleTable 
# MAGIC  ON cdmTable.person_id = plausibleTable.person_id
# MAGIC  WHERE 
# MAGIC  COALESCE(
# MAGIC  CAST(plausibleTable.birth_timestamp AS DATE),
# MAGIC  CAST(CONCAT(plausibleTable.year_of_birth,'0601') AS DATE)
# MAGIC  ) 
# MAGIC  > CAST(cdmTable.DEATH_DATE AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the death_timestamp field of the DEATH that occurs prior to the date in the birth_timestamp field of the PERSON table.' as check_description
# MAGIC  ,'DEATH' as cdm_table_name
# MAGIC  ,'death_timestamp' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_death_death_timestamp' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DEATH.death_timestamp' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEATH cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.PERSON plausibleTable 
# MAGIC  ON cdmTable.person_id = plausibleTable.person_id
# MAGIC  WHERE 
# MAGIC  COALESCE(
# MAGIC  CAST(plausibleTable.birth_timestamp AS DATE),
# MAGIC  CAST(CONCAT(plausibleTable.year_of_birth,'0601') AS DATE)
# MAGIC  ) 
# MAGIC  > CAST(cdmTable.death_timestamp AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DEVICE_EXPOSURE_START_DATE field of the DEVICE_EXPOSURE that occurs prior to the date in the birth_timestamp field of the PERSON table.' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'DEVICE_EXPOSURE_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_device_exposure_device_exposure_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DEVICE_EXPOSURE.DEVICE_EXPOSURE_START_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.PERSON plausibleTable 
# MAGIC  ON cdmTable.person_id = plausibleTable.person_id
# MAGIC  WHERE 
# MAGIC  COALESCE(
# MAGIC  CAST(plausibleTable.birth_timestamp AS DATE),
# MAGIC  CAST(CONCAT(plausibleTable.year_of_birth,'0601') AS DATE)
# MAGIC  ) 
# MAGIC  > CAST(cdmTable.DEVICE_EXPOSURE_START_DATE AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DEVICE_EXPOSURE_START_DATETIME field of the DEVICE_EXPOSURE that occurs prior to the date in the birth_timestamp field of the PERSON table.' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'DEVICE_EXPOSURE_START_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_device_exposure_device_exposure_start_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DEVICE_EXPOSURE.DEVICE_EXPOSURE_START_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.PERSON plausibleTable 
# MAGIC  ON cdmTable.person_id = plausibleTable.person_id
# MAGIC  WHERE 
# MAGIC  COALESCE(
# MAGIC  CAST(plausibleTable.birth_timestamp AS DATE),
# MAGIC  CAST(CONCAT(plausibleTable.year_of_birth,'0601') AS DATE)
# MAGIC  ) 
# MAGIC  > CAST(cdmTable.DEVICE_EXPOSURE_START_DATETIME AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DEVICE_EXPOSURE_END_DATE field of the DEVICE_EXPOSURE that occurs prior to the date in the DEVICE_EXPOSURE_START_DATE field of the DEVICE_EXPOSURE table.' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'DEVICE_EXPOSURE_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_device_exposure_device_exposure_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DEVICE_EXPOSURE.DEVICE_EXPOSURE_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE 
# MAGIC  CAST(cdmTable.DEVICE_EXPOSURE_START_DATE AS DATE)
# MAGIC  > CAST(cdmTable.DEVICE_EXPOSURE_END_DATE AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DEVICE_EXPOSURE_END_DATETIME field of the DEVICE_EXPOSURE that occurs prior to the date in the DEVICE_EXPOSURE_START_DATETIME field of the DEVICE_EXPOSURE table.' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'DEVICE_EXPOSURE_END_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_device_exposure_device_exposure_end_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DEVICE_EXPOSURE.DEVICE_EXPOSURE_END_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE 
# MAGIC  CAST(cdmTable.DEVICE_EXPOSURE_START_DATETIME AS DATE)
# MAGIC  > CAST(cdmTable.DEVICE_EXPOSURE_END_DATETIME AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DOSE_ERA_START_DATE field of the DOSE_ERA that occurs prior to the date in the birth_timestamp field of the PERSON table.' as check_description
# MAGIC  ,'DOSE_ERA' as cdm_table_name
# MAGIC  ,'DOSE_ERA_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_dose_era_dose_era_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DOSE_ERA.DOSE_ERA_START_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.PERSON plausibleTable 
# MAGIC  ON cdmTable.person_id = plausibleTable.person_id
# MAGIC  WHERE 
# MAGIC  COALESCE(
# MAGIC  CAST(plausibleTable.birth_timestamp AS DATE),
# MAGIC  CAST(CONCAT(plausibleTable.year_of_birth,'0601') AS DATE)
# MAGIC  ) 
# MAGIC  > CAST(cdmTable.DOSE_ERA_START_DATE AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DOSE_ERA_END_DATE field of the DOSE_ERA that occurs prior to the date in the birth_timestamp field of the PERSON table.' as check_description
# MAGIC  ,'DOSE_ERA' as cdm_table_name
# MAGIC  ,'DOSE_ERA_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_dose_era_dose_era_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DOSE_ERA.DOSE_ERA_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.PERSON plausibleTable 
# MAGIC  ON cdmTable.person_id = plausibleTable.person_id
# MAGIC  WHERE 
# MAGIC  COALESCE(
# MAGIC  CAST(plausibleTable.birth_timestamp AS DATE),
# MAGIC  CAST(CONCAT(plausibleTable.year_of_birth,'0601') AS DATE)
# MAGIC  ) 
# MAGIC  > CAST(cdmTable.DOSE_ERA_END_DATE AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DRUG_ERA_START_DATE field of the DRUG_ERA that occurs prior to the date in the birth_timestamp field of the PERSON table.' as check_description
# MAGIC  ,'DRUG_ERA' as cdm_table_name
# MAGIC  ,'DRUG_ERA_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_drug_era_drug_era_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DRUG_ERA.DRUG_ERA_START_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.PERSON plausibleTable 
# MAGIC  ON cdmTable.person_id = plausibleTable.person_id
# MAGIC  WHERE 
# MAGIC  COALESCE(
# MAGIC  CAST(plausibleTable.birth_timestamp AS DATE),
# MAGIC  CAST(CONCAT(plausibleTable.year_of_birth,'0601') AS DATE)
# MAGIC  ) 
# MAGIC  > CAST(cdmTable.DRUG_ERA_START_DATE AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DRUG_ERA_END_DATE field of the DRUG_ERA that occurs prior to the date in the DRUG_ERA_START_DATE field of the DRUG_ERA table.' as check_description
# MAGIC  ,'DRUG_ERA' as cdm_table_name
# MAGIC  ,'DRUG_ERA_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_drug_era_drug_era_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DRUG_ERA.DRUG_ERA_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC  WHERE 
# MAGIC  CAST(cdmTable.DRUG_ERA_START_DATE AS DATE)
# MAGIC  > CAST(cdmTable.DRUG_ERA_END_DATE AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DRUG_EXPOSURE_START_DATE field of the DRUG_EXPOSURE that occurs prior to the date in the birth_timestamp field of the PERSON table.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DRUG_EXPOSURE_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_drug_exposure_drug_exposure_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DRUG_EXPOSURE.DRUG_EXPOSURE_START_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.PERSON plausibleTable 
# MAGIC  ON cdmTable.person_id = plausibleTable.person_id
# MAGIC  WHERE 
# MAGIC  COALESCE(
# MAGIC  CAST(plausibleTable.birth_timestamp AS DATE),
# MAGIC  CAST(CONCAT(plausibleTable.year_of_birth,'0601') AS DATE)
# MAGIC  ) 
# MAGIC  > CAST(cdmTable.DRUG_EXPOSURE_START_DATE AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DRUG_EXPOSURE_START_DATETIME field of the DRUG_EXPOSURE that occurs prior to the date in the birth_timestamp field of the PERSON table.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DRUG_EXPOSURE_START_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_drug_exposure_drug_exposure_start_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DRUG_EXPOSURE.DRUG_EXPOSURE_START_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.PERSON plausibleTable 
# MAGIC  ON cdmTable.person_id = plausibleTable.person_id
# MAGIC  WHERE 
# MAGIC  COALESCE(
# MAGIC  CAST(plausibleTable.birth_timestamp AS DATE),
# MAGIC  CAST(CONCAT(plausibleTable.year_of_birth,'0601') AS DATE)
# MAGIC  ) 
# MAGIC  > CAST(cdmTable.DRUG_EXPOSURE_START_DATETIME AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DRUG_EXPOSURE_END_DATE field of the DRUG_EXPOSURE that occurs prior to the date in the DRUG_EXPOSURE_START_DATE field of the DRUG_EXPOSURE table.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DRUG_EXPOSURE_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_drug_exposure_drug_exposure_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DRUG_EXPOSURE.DRUG_EXPOSURE_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE 
# MAGIC  CAST(cdmTable.DRUG_EXPOSURE_START_DATE AS DATE)
# MAGIC  > CAST(cdmTable.DRUG_EXPOSURE_END_DATE AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the DRUG_EXPOSURE_END_DATETIME field of the DRUG_EXPOSURE that occurs prior to the date in the DRUG_EXPOSURE_START_DATETIME field of the DRUG_EXPOSURE table.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DRUG_EXPOSURE_END_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_drug_exposure_drug_exposure_end_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DRUG_EXPOSURE.DRUG_EXPOSURE_END_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE 
# MAGIC  CAST(cdmTable.DRUG_EXPOSURE_START_DATETIME AS DATE)
# MAGIC  > CAST(cdmTable.DRUG_EXPOSURE_END_DATETIME AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the VERBATIM_END_DATE field of the DRUG_EXPOSURE that occurs prior to the date in the DRUG_EXPOSURE_START_DATE field of the DRUG_EXPOSURE table.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'VERBATIM_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_drug_exposure_verbatim_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DRUG_EXPOSURE.VERBATIM_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE 
# MAGIC  CAST(cdmTable.DRUG_EXPOSURE_START_DATE AS DATE)
# MAGIC  > CAST(cdmTable.VERBATIM_END_DATE AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the MEASUREMENT_DATE field of the MEASUREMENT that occurs prior to the date in the birth_timestamp field of the PERSON table.' as check_description
# MAGIC  ,'MEASUREMENT' as cdm_table_name
# MAGIC  ,'MEASUREMENT_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_measurement_measurement_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'MEASUREMENT.MEASUREMENT_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.PERSON plausibleTable 
# MAGIC  ON cdmTable.person_id = plausibleTable.person_id
# MAGIC  WHERE 
# MAGIC  COALESCE(
# MAGIC  CAST(plausibleTable.birth_timestamp AS DATE),
# MAGIC  CAST(CONCAT(plausibleTable.year_of_birth,'0601') AS DATE)
# MAGIC  ) 
# MAGIC  > CAST(cdmTable.MEASUREMENT_DATE AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the NOTE_DATE field of the NOTE that occurs prior to the date in the birth_timestamp field of the PERSON table.' as check_description
# MAGIC  ,'NOTE' as cdm_table_name
# MAGIC  ,'NOTE_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_note_note_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'NOTE.NOTE_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.PERSON plausibleTable 
# MAGIC  ON cdmTable.person_id = plausibleTable.person_id
# MAGIC  WHERE 
# MAGIC  COALESCE(
# MAGIC  CAST(plausibleTable.birth_timestamp AS DATE),
# MAGIC  CAST(CONCAT(plausibleTable.year_of_birth,'0601') AS DATE)
# MAGIC  ) 
# MAGIC  > CAST(cdmTable.NOTE_DATE AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the note_timestamp field of the NOTE that occurs prior to the date in the birth_timestamp field of the PERSON table.' as check_description
# MAGIC  ,'NOTE' as cdm_table_name
# MAGIC  ,'note_timestamp' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_note_note_timestamp' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'NOTE.note_timestamp' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.PERSON plausibleTable 
# MAGIC  ON cdmTable.person_id = plausibleTable.person_id
# MAGIC  WHERE 
# MAGIC  COALESCE(
# MAGIC  CAST(plausibleTable.birth_timestamp AS DATE),
# MAGIC  CAST(CONCAT(plausibleTable.year_of_birth,'0601') AS DATE)
# MAGIC  ) 
# MAGIC  > CAST(cdmTable.note_timestamp AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the OBSERVATION_DATE field of the OBSERVATION that occurs prior to the date in the birth_timestamp field of the PERSON table.' as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'OBSERVATION_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_observation_observation_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'OBSERVATION.OBSERVATION_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.PERSON plausibleTable 
# MAGIC  ON cdmTable.person_id = plausibleTable.person_id
# MAGIC  WHERE 
# MAGIC  COALESCE(
# MAGIC  CAST(plausibleTable.birth_timestamp AS DATE),
# MAGIC  CAST(CONCAT(plausibleTable.year_of_birth,'0601') AS DATE)
# MAGIC  ) 
# MAGIC  > CAST(cdmTable.OBSERVATION_DATE AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the OBSERVATION_DATETIME field of the OBSERVATION that occurs prior to the date in the birth_timestamp field of the PERSON table.' as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'OBSERVATION_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_observation_observation_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'OBSERVATION.OBSERVATION_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.PERSON plausibleTable 
# MAGIC  ON cdmTable.person_id = plausibleTable.person_id
# MAGIC  WHERE 
# MAGIC  COALESCE(
# MAGIC  CAST(plausibleTable.birth_timestamp AS DATE),
# MAGIC  CAST(CONCAT(plausibleTable.year_of_birth,'0601') AS DATE)
# MAGIC  ) 
# MAGIC  > CAST(cdmTable.OBSERVATION_DATETIME AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the OBSERVATION_PERIOD_START_DATE field of the OBSERVATION_PERIOD that occurs prior to the date in the birth_timestamp field of the PERSON table.' as check_description
# MAGIC  ,'OBSERVATION_PERIOD' as cdm_table_name
# MAGIC  ,'OBSERVATION_PERIOD_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_observation_period_observation_period_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'OBSERVATION_PERIOD.OBSERVATION_PERIOD_START_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.PERSON plausibleTable 
# MAGIC  ON cdmTable.person_id = plausibleTable.person_id
# MAGIC  WHERE 
# MAGIC  COALESCE(
# MAGIC  CAST(plausibleTable.birth_timestamp AS DATE),
# MAGIC  CAST(CONCAT(plausibleTable.year_of_birth,'0601') AS DATE)
# MAGIC  ) 
# MAGIC  > CAST(cdmTable.OBSERVATION_PERIOD_START_DATE AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the OBSERVATION_PERIOD_END_DATE field of the OBSERVATION_PERIOD that occurs prior to the date in the OBSERVATION_PERIOD_START_DATE field of the OBSERVATION_PERIOD table.' as check_description
# MAGIC  ,'OBSERVATION_PERIOD' as cdm_table_name
# MAGIC  ,'OBSERVATION_PERIOD_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_observation_period_observation_period_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'OBSERVATION_PERIOD.OBSERVATION_PERIOD_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD cdmTable
# MAGIC  WHERE 
# MAGIC  CAST(cdmTable.OBSERVATION_PERIOD_START_DATE AS DATE)
# MAGIC  > CAST(cdmTable.OBSERVATION_PERIOD_END_DATE AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the PAYER_PLAN_PERIOD_END_DATE field of the PAYER_PLAN_PERIOD that occurs prior to the date in the PAYER_PLAN_PERIOD_START_DATE field of the PAYER_PLAN_PERIOD table.' as check_description
# MAGIC  ,'PAYER_PLAN_PERIOD' as cdm_table_name
# MAGIC  ,'PAYER_PLAN_PERIOD_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_payer_plan_period_payer_plan_period_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'PAYER_PLAN_PERIOD.PAYER_PLAN_PERIOD_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PAYER_PLAN_PERIOD cdmTable
# MAGIC  WHERE 
# MAGIC  CAST(cdmTable.PAYER_PLAN_PERIOD_START_DATE AS DATE)
# MAGIC  > CAST(cdmTable.PAYER_PLAN_PERIOD_END_DATE AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the PROCEDURE_DATE field of the PROCEDURE_OCCURRENCE that occurs prior to the date in the birth_timestamp field of the PERSON table.' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'PROCEDURE_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_procedure_occurrence_procedure_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'PROCEDURE_OCCURRENCE.PROCEDURE_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.PERSON plausibleTable 
# MAGIC  ON cdmTable.person_id = plausibleTable.person_id
# MAGIC  WHERE 
# MAGIC  COALESCE(
# MAGIC  CAST(plausibleTable.birth_timestamp AS DATE),
# MAGIC  CAST(CONCAT(plausibleTable.year_of_birth,'0601') AS DATE)
# MAGIC  ) 
# MAGIC  > CAST(cdmTable.PROCEDURE_DATE AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the PROCEDURE_DATETIME field of the PROCEDURE_OCCURRENCE that occurs prior to the date in the birth_timestamp field of the PERSON table.' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'PROCEDURE_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_procedure_occurrence_procedure_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'PROCEDURE_OCCURRENCE.PROCEDURE_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.PERSON plausibleTable 
# MAGIC  ON cdmTable.person_id = plausibleTable.person_id
# MAGIC  WHERE 
# MAGIC  COALESCE(
# MAGIC  CAST(plausibleTable.birth_timestamp AS DATE),
# MAGIC  CAST(CONCAT(plausibleTable.year_of_birth,'0601') AS DATE)
# MAGIC  ) 
# MAGIC  > CAST(cdmTable.PROCEDURE_DATETIME AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the SPECIMEN_DATE field of the SPECIMEN that occurs prior to the date in the birth_timestamp field of the PERSON table.' as check_description
# MAGIC  ,'SPECIMEN' as cdm_table_name
# MAGIC  ,'SPECIMEN_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_specimen_specimen_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'SPECIMEN.SPECIMEN_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.PERSON plausibleTable 
# MAGIC  ON cdmTable.person_id = plausibleTable.person_id
# MAGIC  WHERE 
# MAGIC  COALESCE(
# MAGIC  CAST(plausibleTable.birth_timestamp AS DATE),
# MAGIC  CAST(CONCAT(plausibleTable.year_of_birth,'0601') AS DATE)
# MAGIC  ) 
# MAGIC  > CAST(cdmTable.SPECIMEN_DATE AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the specimen_timestamp field of the SPECIMEN that occurs prior to the date in the birth_timestamp field of the PERSON table.' as check_description
# MAGIC  ,'SPECIMEN' as cdm_table_name
# MAGIC  ,'specimen_timestamp' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_specimen_specimen_timestamp' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'SPECIMEN.specimen_timestamp' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.PERSON plausibleTable 
# MAGIC  ON cdmTable.person_id = plausibleTable.person_id
# MAGIC  WHERE 
# MAGIC  COALESCE(
# MAGIC  CAST(plausibleTable.birth_timestamp AS DATE),
# MAGIC  CAST(CONCAT(plausibleTable.year_of_birth,'0601') AS DATE)
# MAGIC  ) 
# MAGIC  > CAST(cdmTable.specimen_timestamp AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the VISIT_DETAIL_START_DATE field of the VISIT_DETAIL that occurs prior to the date in the birth_timestamp field of the PERSON table.' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'VISIT_DETAIL_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_visit_detail_visit_detail_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'VISIT_DETAIL.VISIT_DETAIL_START_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.PERSON plausibleTable 
# MAGIC  ON cdmTable.person_id = plausibleTable.person_id
# MAGIC  WHERE 
# MAGIC  COALESCE(
# MAGIC  CAST(plausibleTable.birth_timestamp AS DATE),
# MAGIC  CAST(CONCAT(plausibleTable.year_of_birth,'0601') AS DATE)
# MAGIC  ) 
# MAGIC  > CAST(cdmTable.VISIT_DETAIL_START_DATE AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the visit_detail_start_timestamp field of the VISIT_DETAIL that occurs prior to the date in the birth_timestamp field of the PERSON table.' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'visit_detail_start_timestamp' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_visit_detail_visit_detail_start_timestamp' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'VISIT_DETAIL.visit_detail_start_timestamp' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.PERSON plausibleTable 
# MAGIC  ON cdmTable.person_id = plausibleTable.person_id
# MAGIC  WHERE 
# MAGIC  COALESCE(
# MAGIC  CAST(plausibleTable.birth_timestamp AS DATE),
# MAGIC  CAST(CONCAT(plausibleTable.year_of_birth,'0601') AS DATE)
# MAGIC  ) 
# MAGIC  > CAST(cdmTable.visit_detail_start_timestamp AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the VISIT_DETAIL_END_DATE field of the VISIT_DETAIL that occurs prior to the date in the VISIT_DETAIL_START_DATE field of the VISIT_DETAIL table.' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'VISIT_DETAIL_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_visit_detail_visit_detail_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'VISIT_DETAIL.VISIT_DETAIL_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE 
# MAGIC  CAST(cdmTable.VISIT_DETAIL_START_DATE AS DATE)
# MAGIC  > CAST(cdmTable.VISIT_DETAIL_END_DATE AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the visit_detail_end_timestamp field of the VISIT_DETAIL that occurs prior to the date in the visit_detail_start_timestamp field of the VISIT_DETAIL table.' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'visit_detail_end_timestamp' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_visit_detail_visit_detail_end_timestamp' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'VISIT_DETAIL.visit_detail_end_timestamp' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE 
# MAGIC  CAST(cdmTable.visit_detail_start_timestamp AS DATE)
# MAGIC  > CAST(cdmTable.visit_detail_end_timestamp AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the VISIT_START_DATE field of the VISIT_OCCURRENCE that occurs prior to the date in the birth_timestamp field of the PERSON table.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'VISIT_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_visit_occurrence_visit_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'VISIT_OCCURRENCE.VISIT_START_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.PERSON plausibleTable 
# MAGIC  ON cdmTable.person_id = plausibleTable.person_id
# MAGIC  WHERE 
# MAGIC  COALESCE(
# MAGIC  CAST(plausibleTable.birth_timestamp AS DATE),
# MAGIC  CAST(CONCAT(plausibleTable.year_of_birth,'0601') AS DATE)
# MAGIC  ) 
# MAGIC  > CAST(cdmTable.VISIT_START_DATE AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the visit_start_timestamp field of the VISIT_OCCURRENCE that occurs prior to the date in the birth_timestamp field of the PERSON table.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'visit_start_timestamp' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_visit_occurrence_visit_start_timestamp' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'VISIT_OCCURRENCE.visit_start_timestamp' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.PERSON plausibleTable 
# MAGIC  ON cdmTable.person_id = plausibleTable.person_id
# MAGIC  WHERE 
# MAGIC  COALESCE(
# MAGIC  CAST(plausibleTable.birth_timestamp AS DATE),
# MAGIC  CAST(CONCAT(plausibleTable.year_of_birth,'0601') AS DATE)
# MAGIC  ) 
# MAGIC  > CAST(cdmTable.visit_start_timestamp AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the VISIT_END_DATE field of the VISIT_OCCURRENCE that occurs prior to the date in the VISIT_START_DATE field of the VISIT_OCCURRENCE table.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'VISIT_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_visit_occurrence_visit_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'VISIT_OCCURRENCE.VISIT_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE 
# MAGIC  CAST(cdmTable.VISIT_START_DATE AS DATE)
# MAGIC  > CAST(cdmTable.VISIT_END_DATE AS DATE)
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
# MAGIC  ,'plausibleTemporalAfter' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value in the visit_end_timestamp field of the VISIT_OCCURRENCE that occurs prior to the date in the visit_start_timestamp field of the VISIT_OCCURRENCE table.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'visit_end_timestamp' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_temporal_after.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibletemporalafter_visit_occurrence_visit_end_timestamp' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'VISIT_OCCURRENCE.visit_end_timestamp' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE 
# MAGIC  CAST(cdmTable.visit_start_timestamp AS DATE)
# MAGIC  > CAST(cdmTable.visit_end_timestamp AS DATE)
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
# MAGIC -- field plausible during life
# MAGIC
# MAGIC WITH cte_all  AS (SELECT cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  , CAST('' as STRING) as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleDuringLife' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'If yes, the number and percent of records with a date value in the CONDITION_ERA_START_DATE field of the CONDITION_ERA table that occurs after death.' as check_description
# MAGIC  ,'CONDITION_ERA' as cdm_table_name
# MAGIC  ,'CONDITION_ERA_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_during_life.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibleduringlife_condition_era_condition_era_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'CONDITION_ERA.CONDITION_ERA_START_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.death de 
# MAGIC  ON cdmTable.person_id = de.person_id
# MAGIC  WHERE CAST(cdmTable.CONDITION_ERA_START_DATE AS DATE) > DATE_ADD(day,60,CAST(de.death_date AS DATE))
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable
# MAGIC  WHERE person_id IN
# MAGIC  (SELECT 
# MAGIC  person_id 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.death)
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleDuringLife' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'If yes, the number and percent of records with a date value in the CONDITION_START_DATE field of the CONDITION_OCCURRENCE table that occurs after death.' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_during_life.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibleduringlife_condition_occurrence_condition_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'CONDITION_OCCURRENCE.CONDITION_START_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.death de 
# MAGIC  ON cdmTable.person_id = de.person_id
# MAGIC  WHERE CAST(cdmTable.CONDITION_START_DATE AS DATE) > DATE_ADD(day,60,CAST(de.death_date AS DATE))
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE person_id IN
# MAGIC  (SELECT 
# MAGIC  person_id 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.death)
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleDuringLife' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'If yes, the number and percent of records with a date value in the CONDITION_START_DATETIME field of the CONDITION_OCCURRENCE table that occurs after death.' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_START_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_during_life.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibleduringlife_condition_occurrence_condition_start_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'CONDITION_OCCURRENCE.CONDITION_START_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.death de 
# MAGIC  ON cdmTable.person_id = de.person_id
# MAGIC  WHERE CAST(cdmTable.CONDITION_START_DATETIME AS DATE) > DATE_ADD(day,60,CAST(de.death_date AS DATE))
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE person_id IN
# MAGIC  (SELECT 
# MAGIC  person_id 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.death)
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleDuringLife' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'If yes, the number and percent of records with a date value in the CONDITION_END_DATE field of the CONDITION_OCCURRENCE table that occurs after death.' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_during_life.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibleduringlife_condition_occurrence_condition_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'CONDITION_OCCURRENCE.CONDITION_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.death de 
# MAGIC  ON cdmTable.person_id = de.person_id
# MAGIC  WHERE CAST(cdmTable.CONDITION_END_DATE AS DATE) > DATE_ADD(day,60,CAST(de.death_date AS DATE))
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE person_id IN
# MAGIC  (SELECT 
# MAGIC  person_id 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.death)
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleDuringLife' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'If yes, the number and percent of records with a date value in the CONDITION_END_DATETIME field of the CONDITION_OCCURRENCE table that occurs after death.' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_END_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_during_life.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibleduringlife_condition_occurrence_condition_end_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'CONDITION_OCCURRENCE.CONDITION_END_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.death de 
# MAGIC  ON cdmTable.person_id = de.person_id
# MAGIC  WHERE CAST(cdmTable.CONDITION_END_DATETIME AS DATE) > DATE_ADD(day,60,CAST(de.death_date AS DATE))
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE person_id IN
# MAGIC  (SELECT 
# MAGIC  person_id 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.death)
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleDuringLife' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'If yes, the number and percent of records with a date value in the DEVICE_EXPOSURE_START_DATE field of the DEVICE_EXPOSURE table that occurs after death.' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'DEVICE_EXPOSURE_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_during_life.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibleduringlife_device_exposure_device_exposure_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DEVICE_EXPOSURE.DEVICE_EXPOSURE_START_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.death de 
# MAGIC  ON cdmTable.person_id = de.person_id
# MAGIC  WHERE CAST(cdmTable.DEVICE_EXPOSURE_START_DATE AS DATE) > DATE_ADD(day,60,CAST(de.death_date AS DATE))
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE person_id IN
# MAGIC  (SELECT 
# MAGIC  person_id 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.death)
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleDuringLife' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'If yes, the number and percent of records with a date value in the DEVICE_EXPOSURE_START_DATETIME field of the DEVICE_EXPOSURE table that occurs after death.' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'DEVICE_EXPOSURE_START_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_during_life.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibleduringlife_device_exposure_device_exposure_start_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DEVICE_EXPOSURE.DEVICE_EXPOSURE_START_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.death de 
# MAGIC  ON cdmTable.person_id = de.person_id
# MAGIC  WHERE CAST(cdmTable.DEVICE_EXPOSURE_START_DATETIME AS DATE) > DATE_ADD(day,60,CAST(de.death_date AS DATE))
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE person_id IN
# MAGIC  (SELECT 
# MAGIC  person_id 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.death)
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleDuringLife' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'If yes, the number and percent of records with a date value in the DEVICE_EXPOSURE_END_DATE field of the DEVICE_EXPOSURE table that occurs after death.' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'DEVICE_EXPOSURE_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_during_life.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibleduringlife_device_exposure_device_exposure_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DEVICE_EXPOSURE.DEVICE_EXPOSURE_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.death de 
# MAGIC  ON cdmTable.person_id = de.person_id
# MAGIC  WHERE CAST(cdmTable.DEVICE_EXPOSURE_END_DATE AS DATE) > DATE_ADD(day,60,CAST(de.death_date AS DATE))
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE person_id IN
# MAGIC  (SELECT 
# MAGIC  person_id 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.death)
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleDuringLife' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'If yes, the number and percent of records with a date value in the DEVICE_EXPOSURE_END_DATETIME field of the DEVICE_EXPOSURE table that occurs after death.' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'DEVICE_EXPOSURE_END_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_during_life.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibleduringlife_device_exposure_device_exposure_end_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DEVICE_EXPOSURE.DEVICE_EXPOSURE_END_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.death de 
# MAGIC  ON cdmTable.person_id = de.person_id
# MAGIC  WHERE CAST(cdmTable.DEVICE_EXPOSURE_END_DATETIME AS DATE) > DATE_ADD(day,60,CAST(de.death_date AS DATE))
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  WHERE person_id IN
# MAGIC  (SELECT 
# MAGIC  person_id 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.death)
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleDuringLife' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'If yes, the number and percent of records with a date value in the DOSE_ERA_START_DATE field of the DOSE_ERA table that occurs after death.' as check_description
# MAGIC  ,'DOSE_ERA' as cdm_table_name
# MAGIC  ,'DOSE_ERA_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_during_life.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibleduringlife_dose_era_dose_era_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DOSE_ERA.DOSE_ERA_START_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.death de 
# MAGIC  ON cdmTable.person_id = de.person_id
# MAGIC  WHERE CAST(cdmTable.DOSE_ERA_START_DATE AS DATE) > DATE_ADD(day,60,CAST(de.death_date AS DATE))
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC  WHERE person_id IN
# MAGIC  (SELECT 
# MAGIC  person_id 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.death)
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleDuringLife' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'If yes, the number and percent of records with a date value in the DRUG_ERA_START_DATE field of the DRUG_ERA table that occurs after death.' as check_description
# MAGIC  ,'DRUG_ERA' as cdm_table_name
# MAGIC  ,'DRUG_ERA_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_during_life.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibleduringlife_drug_era_drug_era_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DRUG_ERA.DRUG_ERA_START_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.death de 
# MAGIC  ON cdmTable.person_id = de.person_id
# MAGIC  WHERE CAST(cdmTable.DRUG_ERA_START_DATE AS DATE) > DATE_ADD(day,60,CAST(de.death_date AS DATE))
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC  WHERE person_id IN
# MAGIC  (SELECT 
# MAGIC  person_id 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.death)
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleDuringLife' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'If yes, the number and percent of records with a date value in the DRUG_EXPOSURE_START_DATE field of the DRUG_EXPOSURE table that occurs after death.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DRUG_EXPOSURE_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_during_life.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibleduringlife_drug_exposure_drug_exposure_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DRUG_EXPOSURE.DRUG_EXPOSURE_START_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.death de 
# MAGIC  ON cdmTable.person_id = de.person_id
# MAGIC  WHERE CAST(cdmTable.DRUG_EXPOSURE_START_DATE AS DATE) > DATE_ADD(day,60,CAST(de.death_date AS DATE))
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE person_id IN
# MAGIC  (SELECT 
# MAGIC  person_id 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.death)
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleDuringLife' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'If yes, the number and percent of records with a date value in the DRUG_EXPOSURE_START_DATETIME field of the DRUG_EXPOSURE table that occurs after death.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DRUG_EXPOSURE_START_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_during_life.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibleduringlife_drug_exposure_drug_exposure_start_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DRUG_EXPOSURE.DRUG_EXPOSURE_START_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.death de 
# MAGIC  ON cdmTable.person_id = de.person_id
# MAGIC  WHERE CAST(cdmTable.DRUG_EXPOSURE_START_DATETIME AS DATE) > DATE_ADD(day,60,CAST(de.death_date AS DATE))
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE person_id IN
# MAGIC  (SELECT 
# MAGIC  person_id 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.death)
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleDuringLife' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'If yes, the number and percent of records with a date value in the DRUG_EXPOSURE_END_DATE field of the DRUG_EXPOSURE table that occurs after death.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DRUG_EXPOSURE_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_during_life.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibleduringlife_drug_exposure_drug_exposure_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DRUG_EXPOSURE.DRUG_EXPOSURE_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.death de 
# MAGIC  ON cdmTable.person_id = de.person_id
# MAGIC  WHERE CAST(cdmTable.DRUG_EXPOSURE_END_DATE AS DATE) > DATE_ADD(day,60,CAST(de.death_date AS DATE))
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE person_id IN
# MAGIC  (SELECT 
# MAGIC  person_id 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.death)
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleDuringLife' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'If yes, the number and percent of records with a date value in the DRUG_EXPOSURE_END_DATETIME field of the DRUG_EXPOSURE table that occurs after death.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DRUG_EXPOSURE_END_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_during_life.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibleduringlife_drug_exposure_drug_exposure_end_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DRUG_EXPOSURE.DRUG_EXPOSURE_END_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.death de 
# MAGIC  ON cdmTable.person_id = de.person_id
# MAGIC  WHERE CAST(cdmTable.DRUG_EXPOSURE_END_DATETIME AS DATE) > DATE_ADD(day,60,CAST(de.death_date AS DATE))
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE person_id IN
# MAGIC  (SELECT 
# MAGIC  person_id 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.death)
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleDuringLife' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'If yes, the number and percent of records with a date value in the VERBATIM_END_DATE field of the DRUG_EXPOSURE table that occurs after death.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'VERBATIM_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_during_life.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibleduringlife_drug_exposure_verbatim_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DRUG_EXPOSURE.VERBATIM_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.death de 
# MAGIC  ON cdmTable.person_id = de.person_id
# MAGIC  WHERE CAST(cdmTable.VERBATIM_END_DATE AS DATE) > DATE_ADD(day,60,CAST(de.death_date AS DATE))
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE person_id IN
# MAGIC  (SELECT 
# MAGIC  person_id 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.death)
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleDuringLife' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'If yes, the number and percent of records with a date value in the OBSERVATION_PERIOD_START_DATE field of the OBSERVATION_PERIOD table that occurs after death.' as check_description
# MAGIC  ,'OBSERVATION_PERIOD' as cdm_table_name
# MAGIC  ,'OBSERVATION_PERIOD_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_during_life.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibleduringlife_observation_period_observation_period_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'OBSERVATION_PERIOD.OBSERVATION_PERIOD_START_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.death de 
# MAGIC  ON cdmTable.person_id = de.person_id
# MAGIC  WHERE CAST(cdmTable.OBSERVATION_PERIOD_START_DATE AS DATE) > DATE_ADD(day,60,CAST(de.death_date AS DATE))
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD cdmTable
# MAGIC  WHERE person_id IN
# MAGIC  (SELECT 
# MAGIC  person_id 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.death)
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleDuringLife' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'If yes, the number and percent of records with a date value in the OBSERVATION_PERIOD_END_DATE field of the OBSERVATION_PERIOD table that occurs after death.' as check_description
# MAGIC  ,'OBSERVATION_PERIOD' as cdm_table_name
# MAGIC  ,'OBSERVATION_PERIOD_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_during_life.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibleduringlife_observation_period_observation_period_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'OBSERVATION_PERIOD.OBSERVATION_PERIOD_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.death de 
# MAGIC  ON cdmTable.person_id = de.person_id
# MAGIC  WHERE CAST(cdmTable.OBSERVATION_PERIOD_END_DATE AS DATE) > DATE_ADD(day,60,CAST(de.death_date AS DATE))
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD cdmTable
# MAGIC  WHERE person_id IN
# MAGIC  (SELECT 
# MAGIC  person_id 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.death)
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleDuringLife' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'If yes, the number and percent of records with a date value in the PROCEDURE_DATE field of the PROCEDURE_OCCURRENCE table that occurs after death.' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'PROCEDURE_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_during_life.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibleduringlife_procedure_occurrence_procedure_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'PROCEDURE_OCCURRENCE.PROCEDURE_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.death de 
# MAGIC  ON cdmTable.person_id = de.person_id
# MAGIC  WHERE CAST(cdmTable.PROCEDURE_DATE AS DATE) > DATE_ADD(day,60,CAST(de.death_date AS DATE))
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  WHERE person_id IN
# MAGIC  (SELECT 
# MAGIC  person_id 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.death)
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleDuringLife' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'If yes, the number and percent of records with a date value in the PROCEDURE_DATETIME field of the PROCEDURE_OCCURRENCE table that occurs after death.' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'PROCEDURE_DATETIME' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_during_life.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibleduringlife_procedure_occurrence_procedure_datetime' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'PROCEDURE_OCCURRENCE.PROCEDURE_DATETIME' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.death de 
# MAGIC  ON cdmTable.person_id = de.person_id
# MAGIC  WHERE CAST(cdmTable.PROCEDURE_DATETIME AS DATE) > DATE_ADD(day,60,CAST(de.death_date AS DATE))
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  WHERE person_id IN
# MAGIC  (SELECT 
# MAGIC  person_id 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.death)
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleDuringLife' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'If yes, the number and percent of records with a date value in the VISIT_DETAIL_START_DATE field of the VISIT_DETAIL table that occurs after death.' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'VISIT_DETAIL_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_during_life.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibleduringlife_visit_detail_visit_detail_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'VISIT_DETAIL.VISIT_DETAIL_START_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.death de 
# MAGIC  ON cdmTable.person_id = de.person_id
# MAGIC  WHERE CAST(cdmTable.VISIT_DETAIL_START_DATE AS DATE) > DATE_ADD(day,60,CAST(de.death_date AS DATE))
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE person_id IN
# MAGIC  (SELECT 
# MAGIC  person_id 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.death)
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleDuringLife' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'If yes, the number and percent of records with a date value in the visit_detail_start_timestamp field of the VISIT_DETAIL table that occurs after death.' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'visit_detail_start_timestamp' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_during_life.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibleduringlife_visit_detail_visit_detail_start_timestamp' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'VISIT_DETAIL.visit_detail_start_timestamp' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.death de 
# MAGIC  ON cdmTable.person_id = de.person_id
# MAGIC  WHERE CAST(cdmTable.visit_detail_start_timestamp AS DATE) > DATE_ADD(day,60,CAST(de.death_date AS DATE))
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE person_id IN
# MAGIC  (SELECT 
# MAGIC  person_id 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.death)
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleDuringLife' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'If yes, the number and percent of records with a date value in the VISIT_DETAIL_END_DATE field of the VISIT_DETAIL table that occurs after death.' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'VISIT_DETAIL_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_during_life.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibleduringlife_visit_detail_visit_detail_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'VISIT_DETAIL.VISIT_DETAIL_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.death de 
# MAGIC  ON cdmTable.person_id = de.person_id
# MAGIC  WHERE CAST(cdmTable.VISIT_DETAIL_END_DATE AS DATE) > DATE_ADD(day,60,CAST(de.death_date AS DATE))
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE person_id IN
# MAGIC  (SELECT 
# MAGIC  person_id 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.death)
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleDuringLife' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'If yes, the number and percent of records with a date value in the visit_detail_end_timestamp field of the VISIT_DETAIL table that occurs after death.' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'visit_detail_end_timestamp' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_during_life.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibleduringlife_visit_detail_visit_detail_end_timestamp' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'VISIT_DETAIL.visit_detail_end_timestamp' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.death de 
# MAGIC  ON cdmTable.person_id = de.person_id
# MAGIC  WHERE CAST(cdmTable.visit_detail_end_timestamp AS DATE) > DATE_ADD(day,60,CAST(de.death_date AS DATE))
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE person_id IN
# MAGIC  (SELECT 
# MAGIC  person_id 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.death)
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleDuringLife' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'If yes, the number and percent of records with a date value in the VISIT_START_DATE field of the VISIT_OCCURRENCE table that occurs after death.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'VISIT_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_during_life.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibleduringlife_visit_occurrence_visit_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'VISIT_OCCURRENCE.VISIT_START_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.death de 
# MAGIC  ON cdmTable.person_id = de.person_id
# MAGIC  WHERE CAST(cdmTable.VISIT_START_DATE AS DATE) > DATE_ADD(day,60,CAST(de.death_date AS DATE))
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE person_id IN
# MAGIC  (SELECT 
# MAGIC  person_id 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.death)
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleDuringLife' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'If yes, the number and percent of records with a date value in the visit_start_timestamp field of the VISIT_OCCURRENCE table that occurs after death.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'visit_start_timestamp' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_during_life.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibleduringlife_visit_occurrence_visit_start_timestamp' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'VISIT_OCCURRENCE.visit_start_timestamp' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.death de 
# MAGIC  ON cdmTable.person_id = de.person_id
# MAGIC  WHERE CAST(cdmTable.visit_start_timestamp AS DATE) > DATE_ADD(day,60,CAST(de.death_date AS DATE))
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE person_id IN
# MAGIC  (SELECT 
# MAGIC  person_id 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.death)
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleDuringLife' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'If yes, the number and percent of records with a date value in the VISIT_END_DATE field of the VISIT_OCCURRENCE table that occurs after death.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'VISIT_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_during_life.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibleduringlife_visit_occurrence_visit_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'VISIT_OCCURRENCE.VISIT_END_DATE' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.death de 
# MAGIC  ON cdmTable.person_id = de.person_id
# MAGIC  WHERE CAST(cdmTable.VISIT_END_DATE AS DATE) > DATE_ADD(day,60,CAST(de.death_date AS DATE))
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE person_id IN
# MAGIC  (SELECT 
# MAGIC  person_id 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.death)
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'plausibleDuringLife' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'If yes, the number and percent of records with a date value in the visit_end_timestamp field of the VISIT_OCCURRENCE table that occurs after death.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'visit_end_timestamp' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_plausible_during_life.sql' as sql_file
# MAGIC  ,'Plausibility' as category
# MAGIC  ,'Temporal' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_plausibleduringlife_visit_occurrence_visit_end_timestamp' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'VISIT_OCCURRENCE.visit_end_timestamp' AS violating_field, 
# MAGIC  cdmTable.*
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.death de 
# MAGIC  ON cdmTable.person_id = de.person_id
# MAGIC  WHERE CAST(cdmTable.visit_end_timestamp AS DATE) > DATE_ADD(day,60,CAST(de.death_date AS DATE))
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE person_id IN
# MAGIC  (SELECT 
# MAGIC  person_id 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.death)
# MAGIC ) denominator
# MAGIC ) cte
# MAGIC )
# MAGIC INSERT INTO ${results_catalog}.${results_schema}.dqdashboard_results
# MAGIC SELECT *
# MAGIC FROM cte_all;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- field within visit dates
# MAGIC
# MAGIC WITH cte_all  AS (SELECT cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  , CAST('' as STRING) as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'withinVisitDates' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records not within one week on either side of the corresponding visit occurrence start and end date' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_within_visit_dates.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_withinvisitdates_condition_occurrence_condition_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,5 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC  ON cdmTable.visit_occurrence_id = vo.visit_occurrence_id
# MAGIC  WHERE cdmTable.CONDITION_START_DATE < DATE_ADD(day,-7,vo.visit_start_date)
# MAGIC  OR cdmTable.CONDITION_START_DATE > DATE_ADD(day,7,vo.visit_end_date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC  ON cdmTable.visit_occurrence_id = vo.visit_occurrence_id
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'withinVisitDates' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records not within one week on either side of the corresponding visit occurrence start and end date' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'DEVICE_EXPOSURE_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_within_visit_dates.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_withinvisitdates_device_exposure_device_exposure_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,5 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC  ON cdmTable.visit_occurrence_id = vo.visit_occurrence_id
# MAGIC  WHERE cdmTable.DEVICE_EXPOSURE_START_DATE < DATE_ADD(day,-7,vo.visit_start_date)
# MAGIC  OR cdmTable.DEVICE_EXPOSURE_START_DATE > DATE_ADD(day,7,vo.visit_end_date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC  ON cdmTable.visit_occurrence_id = vo.visit_occurrence_id
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'withinVisitDates' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records not within one week on either side of the corresponding visit occurrence start and end date' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DRUG_EXPOSURE_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_within_visit_dates.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_withinvisitdates_drug_exposure_drug_exposure_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,5 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC  ON cdmTable.visit_occurrence_id = vo.visit_occurrence_id
# MAGIC  WHERE cdmTable.DRUG_EXPOSURE_START_DATE < DATE_ADD(day,-7,vo.visit_start_date)
# MAGIC  OR cdmTable.DRUG_EXPOSURE_START_DATE > DATE_ADD(day,7,vo.visit_end_date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC  ON cdmTable.visit_occurrence_id = vo.visit_occurrence_id
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'withinVisitDates' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records not within one week on either side of the corresponding visit occurrence start and end date' as check_description
# MAGIC  ,'MEASUREMENT' as cdm_table_name
# MAGIC  ,'MEASUREMENT_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_within_visit_dates.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_withinvisitdates_measurement_measurement_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,5 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC  ON cdmTable.visit_occurrence_id = vo.visit_occurrence_id
# MAGIC  WHERE cdmTable.MEASUREMENT_DATE < DATE_ADD(day,-7,vo.visit_start_date)
# MAGIC  OR cdmTable.MEASUREMENT_DATE > DATE_ADD(day,7,vo.visit_end_date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC  ON cdmTable.visit_occurrence_id = vo.visit_occurrence_id
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'withinVisitDates' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records not within one week on either side of the corresponding visit occurrence start and end date' as check_description
# MAGIC  ,'NOTE' as cdm_table_name
# MAGIC  ,'NOTE_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_within_visit_dates.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_withinvisitdates_note_note_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,5 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC  ON cdmTable.visit_occurrence_id = vo.visit_occurrence_id
# MAGIC  WHERE cdmTable.NOTE_DATE < DATE_ADD(day,-7,vo.visit_start_date)
# MAGIC  OR cdmTable.NOTE_DATE > DATE_ADD(day,7,vo.visit_end_date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.NOTE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC  ON cdmTable.visit_occurrence_id = vo.visit_occurrence_id
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'withinVisitDates' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records not within one week on either side of the corresponding visit occurrence start and end date' as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'OBSERVATION_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_within_visit_dates.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_withinvisitdates_observation_observation_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,5 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC  ON cdmTable.visit_occurrence_id = vo.visit_occurrence_id
# MAGIC  WHERE cdmTable.OBSERVATION_DATE < DATE_ADD(day,-7,vo.visit_start_date)
# MAGIC  OR cdmTable.OBSERVATION_DATE > DATE_ADD(day,7,vo.visit_end_date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC  ON cdmTable.visit_occurrence_id = vo.visit_occurrence_id
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'withinVisitDates' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records not within one week on either side of the corresponding visit occurrence start and end date' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'PROCEDURE_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_within_visit_dates.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_withinvisitdates_procedure_occurrence_procedure_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,5 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC  ON cdmTable.visit_occurrence_id = vo.visit_occurrence_id
# MAGIC  WHERE cdmTable.PROCEDURE_DATE < DATE_ADD(day,-7,vo.visit_start_date)
# MAGIC  OR cdmTable.PROCEDURE_DATE > DATE_ADD(day,7,vo.visit_end_date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC  ON cdmTable.visit_occurrence_id = vo.visit_occurrence_id
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'withinVisitDates' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records not within one week on either side of the corresponding visit occurrence start and end date' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'VISIT_DETAIL_START_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_within_visit_dates.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_withinvisitdates_visit_detail_visit_detail_start_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC  ON cdmTable.visit_occurrence_id = vo.visit_occurrence_id
# MAGIC  WHERE cdmTable.VISIT_DETAIL_START_DATE < DATE_ADD(day,-7,vo.visit_start_date)
# MAGIC  OR cdmTable.VISIT_DETAIL_START_DATE > DATE_ADD(day,7,vo.visit_end_date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC  ON cdmTable.visit_occurrence_id = vo.visit_occurrence_id
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'withinVisitDates' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records not within one week on either side of the corresponding visit occurrence start and end date' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'VISIT_DETAIL_END_DATE' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_within_visit_dates.sql' as sql_file
# MAGIC  ,'Conformance' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_withinvisitdates_visit_detail_visit_detail_end_date' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 1 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 1 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,1 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC  ON cdmTable.visit_occurrence_id = vo.visit_occurrence_id
# MAGIC  WHERE cdmTable.VISIT_DETAIL_END_DATE < DATE_ADD(day,-7,vo.visit_start_date)
# MAGIC  OR cdmTable.VISIT_DETAIL_END_DATE > DATE_ADD(day,7,vo.visit_end_date)
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT 
# MAGIC  COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  JOIN ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC  ON cdmTable.visit_occurrence_id = vo.visit_occurrence_id
# MAGIC ) denominator
# MAGIC ) cte
# MAGIC )
# MAGIC INSERT INTO ${results_catalog}.${results_schema}.dqdashboard_results
# MAGIC SELECT *
# MAGIC FROM cte_all;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- field standard concept record completeness
# MAGIC
# MAGIC WITH cte_all  AS (SELECT cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  , CAST('' as STRING) as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field PLACE_OF_SERVICE_CONCEPT_ID in the CARE_SITE table.' as check_description
# MAGIC  ,'CARE_SITE' as cdm_table_name
# MAGIC  ,'PLACE_OF_SERVICE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_care_site_place_of_service_concept_id' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'CARE_SITE.PLACE_OF_SERVICE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CARE_SITE cdmTable
# MAGIC  WHERE cdmTable.PLACE_OF_SERVICE_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CARE_SITE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field CONDITION_CONCEPT_ID in the CONDITION_ERA table.' as check_description
# MAGIC  ,'CONDITION_ERA' as cdm_table_name
# MAGIC  ,'CONDITION_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_condition_era_condition_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.CONDITION_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_ERA cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field CONDITION_CONCEPT_ID in the CONDITION_OCCURRENCE table.' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_condition_occurrence_condition_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,5 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.CONDITION_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field CONDITION_TYPE_CONCEPT_ID in the CONDITION_OCCURRENCE table.' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_condition_occurrence_condition_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.CONDITION_TYPE_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field CONDITION_STATUS_CONCEPT_ID in the CONDITION_OCCURRENCE table.' as check_description
# MAGIC  ,'CONDITION_OCCURRENCE' as cdm_table_name
# MAGIC  ,'CONDITION_STATUS_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_condition_occurrence_condition_status_concept_id' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'CONDITION_OCCURRENCE.CONDITION_STATUS_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.CONDITION_STATUS_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.CONDITION_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field COST_TYPE_CONCEPT_ID in the COST table.' as check_description
# MAGIC  ,'COST' as cdm_table_name
# MAGIC  ,'COST_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_cost_cost_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.COST_TYPE_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.COST cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field CAUSE_CONCEPT_ID in the DEATH table.' as check_description
# MAGIC  ,'DEATH' as cdm_table_name
# MAGIC  ,'CAUSE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_death_cause_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,5 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DEATH.CAUSE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEATH cdmTable
# MAGIC  WHERE cdmTable.CAUSE_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEATH cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field DEATH_TYPE_CONCEPT_ID in the DEATH table.' as check_description
# MAGIC  ,'DEATH' as cdm_table_name
# MAGIC  ,'DEATH_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_death_death_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'DEATH.DEATH_TYPE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEATH cdmTable
# MAGIC  WHERE cdmTable.DEATH_TYPE_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEATH cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field DEVICE_CONCEPT_ID in the DEVICE_EXPOSURE table.' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'DEVICE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_device_exposure_device_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,5 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.DEVICE_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field DEVICE_TYPE_CONCEPT_ID in the DEVICE_EXPOSURE table.' as check_description
# MAGIC  ,'DEVICE_EXPOSURE' as cdm_table_name
# MAGIC  ,'DEVICE_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_device_exposure_device_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.DEVICE_TYPE_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DEVICE_EXPOSURE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field DRUG_CONCEPT_ID in the DOSE_ERA table.' as check_description
# MAGIC  ,'DOSE_ERA' as cdm_table_name
# MAGIC  ,'DRUG_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_dose_era_drug_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.DRUG_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field UNIT_CONCEPT_ID in the DOSE_ERA table.' as check_description
# MAGIC  ,'DOSE_ERA' as cdm_table_name
# MAGIC  ,'UNIT_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_dose_era_unit_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.UNIT_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DOSE_ERA cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field DRUG_CONCEPT_ID in the DRUG_ERA table.' as check_description
# MAGIC  ,'DRUG_ERA' as cdm_table_name
# MAGIC  ,'DRUG_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_drug_era_drug_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.DRUG_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_ERA cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field DRUG_CONCEPT_ID in the DRUG_EXPOSURE table.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DRUG_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_drug_exposure_drug_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,5 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.DRUG_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field DRUG_TYPE_CONCEPT_ID in the DRUG_EXPOSURE table.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'DRUG_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_drug_exposure_drug_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.DRUG_TYPE_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field ROUTE_CONCEPT_ID in the DRUG_EXPOSURE table.' as check_description
# MAGIC  ,'DRUG_EXPOSURE' as cdm_table_name
# MAGIC  ,'ROUTE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_drug_exposure_route_concept_id' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'DRUG_EXPOSURE.ROUTE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC  WHERE cdmTable.ROUTE_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.DRUG_EXPOSURE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field DOMAIN_CONCEPT_ID_1 in the FACT_RELATIONSHIP table.' as check_description
# MAGIC  ,'FACT_RELATIONSHIP' as cdm_table_name
# MAGIC  ,'DOMAIN_CONCEPT_ID_1' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_fact_relationship_domain_concept_id_1' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'FACT_RELATIONSHIP.DOMAIN_CONCEPT_ID_1' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.FACT_RELATIONSHIP cdmTable
# MAGIC  WHERE cdmTable.DOMAIN_CONCEPT_ID_1 = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.FACT_RELATIONSHIP cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field DOMAIN_CONCEPT_ID_2 in the FACT_RELATIONSHIP table.' as check_description
# MAGIC  ,'FACT_RELATIONSHIP' as cdm_table_name
# MAGIC  ,'DOMAIN_CONCEPT_ID_2' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_fact_relationship_domain_concept_id_2' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'FACT_RELATIONSHIP.DOMAIN_CONCEPT_ID_2' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.FACT_RELATIONSHIP cdmTable
# MAGIC  WHERE cdmTable.DOMAIN_CONCEPT_ID_2 = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.FACT_RELATIONSHIP cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field RELATIONSHIP_CONCEPT_ID in the FACT_RELATIONSHIP table.' as check_description
# MAGIC  ,'FACT_RELATIONSHIP' as cdm_table_name
# MAGIC  ,'RELATIONSHIP_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_fact_relationship_relationship_concept_id' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'FACT_RELATIONSHIP.RELATIONSHIP_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.FACT_RELATIONSHIP cdmTable
# MAGIC  WHERE cdmTable.RELATIONSHIP_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.FACT_RELATIONSHIP cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field MEASUREMENT_CONCEPT_ID in the MEASUREMENT table.' as check_description
# MAGIC  ,'MEASUREMENT' as cdm_table_name
# MAGIC  ,'MEASUREMENT_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_measurement_measurement_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,5 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.MEASUREMENT_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field MEASUREMENT_TYPE_CONCEPT_ID in the MEASUREMENT table.' as check_description
# MAGIC  ,'MEASUREMENT' as cdm_table_name
# MAGIC  ,'MEASUREMENT_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_measurement_measurement_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.MEASUREMENT_TYPE_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field UNIT_CONCEPT_ID in the MEASUREMENT table.' as check_description
# MAGIC  ,'MEASUREMENT' as cdm_table_name
# MAGIC  ,'UNIT_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_measurement_unit_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,5 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'MEASUREMENT.UNIT_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC  WHERE cdmTable.UNIT_CONCEPT_ID = 0 AND cdmTable.value_as_number IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.MEASUREMENT cdmTable
# MAGIC  WHERE cdmTable.value_as_number IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field OBSERVATION_CONCEPT_ID in the OBSERVATION table.' as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'OBSERVATION_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_observation_observation_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,5 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.OBSERVATION_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field OBSERVATION_TYPE_CONCEPT_ID in the OBSERVATION table.' as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'OBSERVATION_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_observation_observation_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.OBSERVATION_TYPE_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field UNIT_CONCEPT_ID in the OBSERVATION table.' as check_description
# MAGIC  ,'OBSERVATION' as cdm_table_name
# MAGIC  ,'UNIT_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_observation_unit_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,5 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'OBSERVATION.UNIT_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  WHERE cdmTable.UNIT_CONCEPT_ID = 0 AND cdmTable.value_as_number IS NOT NULL
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION cdmTable
# MAGIC  WHERE cdmTable.value_as_number IS NOT NULL
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field PERIOD_TYPE_CONCEPT_ID in the OBSERVATION_PERIOD table.' as check_description
# MAGIC  ,'OBSERVATION_PERIOD' as cdm_table_name
# MAGIC  ,'PERIOD_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_observation_period_period_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.PERIOD_TYPE_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.OBSERVATION_PERIOD cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field GENDER_CONCEPT_ID in the PERSON table.' as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'GENDER_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_person_gender_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,5 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.GENDER_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field RACE_CONCEPT_ID in the PERSON table.' as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'RACE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_person_race_concept_id' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PERSON.RACE_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE cdmTable.RACE_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field ETHNICITY_CONCEPT_ID in the PERSON table.' as check_description
# MAGIC  ,'PERSON' as cdm_table_name
# MAGIC  ,'ETHNICITY_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_person_ethnicity_concept_id' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PERSON.ETHNICITY_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC  WHERE cdmTable.ETHNICITY_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PERSON cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field PROCEDURE_CONCEPT_ID in the PROCEDURE_OCCURRENCE table.' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'PROCEDURE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_procedure_occurrence_procedure_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,5 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.PROCEDURE_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field PROCEDURE_TYPE_CONCEPT_ID in the PROCEDURE_OCCURRENCE table.' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'PROCEDURE_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_procedure_occurrence_procedure_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.PROCEDURE_TYPE_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field MODIFIER_CONCEPT_ID in the PROCEDURE_OCCURRENCE table.' as check_description
# MAGIC  ,'PROCEDURE_OCCURRENCE' as cdm_table_name
# MAGIC  ,'MODIFIER_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_procedure_occurrence_modifier_concept_id' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PROCEDURE_OCCURRENCE.MODIFIER_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.MODIFIER_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROCEDURE_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field SPECIALTY_CONCEPT_ID in the PROVIDER table.' as check_description
# MAGIC  ,'PROVIDER' as cdm_table_name
# MAGIC  ,'SPECIALTY_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_provider_specialty_concept_id' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PROVIDER.SPECIALTY_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROVIDER cdmTable
# MAGIC  WHERE cdmTable.SPECIALTY_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROVIDER cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field GENDER_CONCEPT_ID in the PROVIDER table.' as check_description
# MAGIC  ,'PROVIDER' as cdm_table_name
# MAGIC  ,'GENDER_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_provider_gender_concept_id' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'PROVIDER.GENDER_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROVIDER cdmTable
# MAGIC  WHERE cdmTable.GENDER_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.PROVIDER cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field SPECIMEN_CONCEPT_ID in the SPECIMEN table.' as check_description
# MAGIC  ,'SPECIMEN' as cdm_table_name
# MAGIC  ,'SPECIMEN_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_specimen_specimen_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,5 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.SPECIMEN_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field SPECIMEN_TYPE_CONCEPT_ID in the SPECIMEN table.' as check_description
# MAGIC  ,'SPECIMEN' as cdm_table_name
# MAGIC  ,'SPECIMEN_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_specimen_specimen_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.SPECIMEN_TYPE_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field UNIT_CONCEPT_ID in the SPECIMEN table.' as check_description
# MAGIC  ,'SPECIMEN' as cdm_table_name
# MAGIC  ,'UNIT_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_specimen_unit_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 5 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 5 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,5 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  'SPECIMEN.UNIT_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC  WHERE cdmTable.UNIT_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.SPECIMEN cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field VISIT_DETAIL_CONCEPT_ID in the VISIT_DETAIL table.' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'VISIT_DETAIL_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_visit_detail_visit_detail_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.VISIT_DETAIL_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field VISIT_DETAIL_TYPE_CONCEPT_ID in the VISIT_DETAIL table.' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'VISIT_DETAIL_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_visit_detail_visit_detail_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.VISIT_DETAIL_TYPE_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field admitted_from_concept_id in the VISIT_DETAIL table.' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'admitted_from_concept_id' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_visit_detail_admitted_from_concept_id' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_DETAIL.admitted_from_concept_id' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE cdmTable.admitted_from_concept_id = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field discharged_TO_CONCEPT_ID in the VISIT_DETAIL table.' as check_description
# MAGIC  ,'VISIT_DETAIL' as cdm_table_name
# MAGIC  ,'discharged_TO_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_visit_detail_discharged_to_concept_id' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_DETAIL.discharged_TO_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC  WHERE cdmTable.discharged_TO_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_DETAIL cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field VISIT_CONCEPT_ID in the VISIT_OCCURRENCE table.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'VISIT_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_visit_occurrence_visit_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.VISIT_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field VISIT_TYPE_CONCEPT_ID in the VISIT_OCCURRENCE table.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'VISIT_TYPE_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_visit_occurrence_visit_type_concept_id' as checkid
# MAGIC  ,0 as is_error
# MAGIC  ,0 as not_applicable
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) > 0 THEN 1 ELSE 0 END as failed
# MAGIC  ,CASE WHEN (cte.pct_violated_rows * 100) <= 0 THEN 1 ELSE 0 END as passed
# MAGIC  ,NULL as not_applicable_reason
# MAGIC  ,0 as threshold_value
# MAGIC  ,NULL as notes_value
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
# MAGIC  WHERE cdmTable.VISIT_TYPE_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field admitted_from_concept_id in the VISIT_OCCURRENCE table.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'admitted_from_concept_id' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_visit_occurrence_admitted_from_concept_id' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_OCCURRENCE.admitted_from_concept_id' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.admitted_from_concept_id = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte UNION ALL SELECT 
# MAGIC  cte.num_violated_rows
# MAGIC  ,cte.pct_violated_rows
# MAGIC  ,cte.num_denominator_rows
# MAGIC  ,'' as execution_time
# MAGIC  ,'' as query_text
# MAGIC  ,'standardConceptRecordCompleteness' as check_name
# MAGIC  ,'FIELD' as check_level
# MAGIC  ,'The number and percent of records with a value of 0 in the standard concept field discharged_TO_CONCEPT_ID in the VISIT_OCCURRENCE table.' as check_description
# MAGIC  ,'VISIT_OCCURRENCE' as cdm_table_name
# MAGIC  ,'discharged_TO_CONCEPT_ID' as cdm_field_name
# MAGIC  ,'NA' as concept_id
# MAGIC  ,'NA' as unit_concept_id
# MAGIC  ,'field_concept_record_completeness.sql' as sql_file
# MAGIC  ,'Completeness' as category
# MAGIC  ,'NA' as subcategory
# MAGIC  ,'Verification' as context
# MAGIC  ,'' as warning
# MAGIC  ,'' as error
# MAGIC  ,'field_standardconceptrecordcompleteness_visit_occurrence_discharged_to_concept_id' as checkid
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
# MAGIC  COUNT(violated_rows.violating_field) AS num_violated_rows
# MAGIC  FROM (
# MAGIC  /*violatedRowsBegin*/
# MAGIC  SELECT 
# MAGIC  'VISIT_OCCURRENCE.discharged_TO_CONCEPT_ID' AS violating_field, 
# MAGIC  cdmTable.* 
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC  WHERE cdmTable.discharged_TO_CONCEPT_ID = 0 
# MAGIC  /*violatedRowsEnd*/
# MAGIC  ) violated_rows
# MAGIC ) violated_row_count cross join (SELECT COUNT(*) AS num_rows
# MAGIC  FROM ${omop_catalog}.${omop_schema}.VISIT_OCCURRENCE cdmTable
# MAGIC ) denominator
# MAGIC ) cte
# MAGIC )
# MAGIC INSERT INTO ${results_catalog}.${results_schema}.dqdashboard_results
# MAGIC SELECT *
# MAGIC FROM cte_all;
# MAGIC
