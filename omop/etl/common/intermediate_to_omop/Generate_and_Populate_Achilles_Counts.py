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
# MAGIC -- create achilles counts
# MAGIC
# MAGIC -- DROP TABLE IF EXISTS ${results_catalog}.${results_schema}.achilles_result_concept_count;
# MAGIC
# MAGIC CREATE TABLE if not exists ${results_catalog}.${results_schema}.achilles_result_concept_count
# MAGIC USING DELTA
# MAGIC  AS
# MAGIC SELECT
# MAGIC CAST(NULL AS int) AS concept_id,
# MAGIC 	CAST(NULL AS bigint) AS record_count,
# MAGIC 	CAST(NULL AS bigint) AS descendant_record_count,
# MAGIC 	CAST(NULL AS bigint) AS person_count,
# MAGIC 	CAST(NULL AS bigint) AS descendant_person_count  WHERE 1 = 0;
# MAGIC
# MAGIC DROP TABLE IF EXISTS qcmv0h6ktmp_counts;
# MAGIC
# MAGIC DROP VIEW IF EXISTS counts ;
# MAGIC  CREATE TEMPORARY VIEW counts  AS (SELECT stratum_1 AS concept_id, MAX (count_value) AS agg_count_value
# MAGIC  FROM ${results_catalog}.${results_schema}.achilles_results
# MAGIC  WHERE analysis_id IN (2, 4, 5, 201, 225, 301, 325, 401, 425, 501, 505, 525, 601, 625, 701, 725, 801, 825,
# MAGIC  826, 827, 901, 1001, 1201, 1203, 1425, 1801, 1825, 1826, 1827, 2101, 2125, 2301)
# MAGIC  /* analyses:
# MAGIC  Number of persons by gender
# MAGIC  Number of persons by race
# MAGIC  Number of persons by ethnicity
# MAGIC  Number of visit occurrence records, by visit_concept_id
# MAGIC  Number of visit_occurrence records, by visit_source_concept_id
# MAGIC  Number of providers by specialty concept_id
# MAGIC  Number of provider records, by specialty_source_concept_id
# MAGIC  Number of condition occurrence records, by condition_concept_id
# MAGIC  Number of condition_occurrence records, by condition_source_concept_id
# MAGIC  Number of records of death, by cause_concept_id
# MAGIC  Number of death records, by death_type_concept_id
# MAGIC  Number of death records, by cause_source_concept_id
# MAGIC  Number of procedure occurrence records, by procedure_concept_id
# MAGIC  Number of procedure_occurrence records, by procedure_source_concept_id
# MAGIC  Number of drug exposure records, by drug_concept_id
# MAGIC  Number of drug_exposure records, by drug_source_concept_id
# MAGIC  Number of observation occurrence records, by observation_concept_id
# MAGIC  Number of observation records, by observation_source_concept_id
# MAGIC  Number of observation records, by value_as_concept_id
# MAGIC  Number of observation records, by unit_concept_id
# MAGIC  Number of drug era records, by drug_concept_id
# MAGIC  Number of condition era records, by condition_concept_id
# MAGIC  Number of visits by place of service
# MAGIC  Number of visit_occurrence records, by discharge_to_concept_id
# MAGIC  Number of payer_plan_period records, by payer_source_concept_id
# MAGIC  Number of measurement occurrence records, by observation_concept_id
# MAGIC  Number of measurement records, by measurement_source_concept_id
# MAGIC  Number of measurement records, by value_as_concept_id
# MAGIC  Number of measurement records, by unit_concept_id
# MAGIC  Number of device exposure records, by device_concept_id
# MAGIC  Number of device_exposure records, by device_source_concept_id
# MAGIC  Number of location records, by region_concept_id
# MAGIC  */
# MAGIC  GROUP BY stratum_1
# MAGIC  UNION ALL
# MAGIC  SELECT stratum_2 AS concept_id, SUM (count_value) AS agg_count_value
# MAGIC  FROM ${results_catalog}.${results_schema}.achilles_results
# MAGIC  WHERE analysis_id IN (405, 605, 705, 805, 807, 1805, 1807, 2105)
# MAGIC  /* analyses:
# MAGIC  Number of condition occurrence records, by condition_concept_id by condition_type_concept_id
# MAGIC  Number of procedure occurrence records, by procedure_concept_id by procedure_type_concept_id
# MAGIC  Number of drug exposure records, by drug_concept_id by drug_type_concept_id
# MAGIC  Number of observation occurrence records, by observation_concept_id by observation_type_concept_id
# MAGIC  Number of observation occurrence records, by observation_concept_id and unit_concept_id
# MAGIC  Number of observation occurrence records, by measurement_concept_id by measurement_type_concept_id
# MAGIC  Number of measurement occurrence records, by measurement_concept_id and unit_concept_id
# MAGIC  Number of device exposure records, by device_concept_id by device_type_concept_id
# MAGIC  but this subquery only gets the type or unit concept_ids, i.e., stratum_2
# MAGIC  */
# MAGIC  GROUP BY stratum_2
# MAGIC );
# MAGIC
# MAGIC  CREATE TABLE qcmv0h6ktmp_counts
# MAGIC USING DELTA
# MAGIC AS
# MAGIC (SELECT
# MAGIC concept_id,
# MAGIC  agg_count_value
# MAGIC FROM
# MAGIC counts);
# MAGIC
# MAGIC DROP TABLE IF EXISTS qcmv0h6ktmp_counts_person;
# MAGIC
# MAGIC DROP VIEW IF EXISTS counts_person ;
# MAGIC  CREATE TEMPORARY VIEW counts_person  AS (SELECT stratum_1 AS concept_id, MAX (count_value) AS agg_count_value
# MAGIC  FROM ${results_catalog}.${results_schema}.achilles_results
# MAGIC  WHERE analysis_id IN (200, 240, 400, 440, 540, 600, 640, 700, 740, 800, 840, 900, 1000, 1300, 1340, 1800, 1840, 2100, 2140, 2200)
# MAGIC  /* analyses:
# MAGIC  Number of persons with at least one visit occurrence, by visit_concept_id
# MAGIC  Number of persons with at least one visit occurrence, by visit_source_concept_id
# MAGIC  Number of persons with at least one condition occurrence, by condition_concept_id
# MAGIC  Number of persons with at least one condition occurrence, by condition_source_concept_id
# MAGIC  Number of persons with death, by cause_source_concept_id
# MAGIC  Number of persons with at least one procedure occurrence, by procedure_concept_id
# MAGIC  Number of persons with at least one procedure occurrence, by procedure_source_concept_id
# MAGIC  Number of persons with at least one drug exposure, by drug_concept_id
# MAGIC  Number of persons with at least one drug exposure, by drug_source_concept_id
# MAGIC  Number of persons with at least one observation occurrence, by observation_concept_id
# MAGIC  Number of persons with at least one observation occurrence, by observation_source_concept_id
# MAGIC  Number of persons with at least one drug era, by drug_concept_id
# MAGIC  Number of persons with at least one condition era, by condition_concept_id
# MAGIC  Number of persons with at least one visit detail, by visit_detail_concept_id
# MAGIC  Number of persons with at least one visit detail, by visit_detail_source_concept_id
# MAGIC  Number of persons with at least one measurement occurrence, by measurement_concept_id
# MAGIC  Number of persons with at least one measurement occurrence, by measurement_source_concept_id
# MAGIC  Number of persons with at least one device exposure, by device_concept_id
# MAGIC  Number of persons with at least one device exposure, by device_source_concept_id
# MAGIC  Number of persons with at least one note by note_type_concept_id
# MAGIC  */
# MAGIC  GROUP BY stratum_1
# MAGIC );
# MAGIC
# MAGIC  CREATE TABLE qcmv0h6ktmp_counts_person
# MAGIC USING DELTA
# MAGIC AS
# MAGIC (SELECT
# MAGIC concept_id,
# MAGIC  agg_count_value
# MAGIC FROM
# MAGIC counts_person);
# MAGIC
# MAGIC DROP TABLE IF EXISTS qcmv0h6ktmp_concepts;
# MAGIC
# MAGIC DROP VIEW IF EXISTS concepts ;
# MAGIC  CREATE TEMPORARY VIEW concepts  AS (select concept_id as ancestor_id, coalesce(cast(ca.descendant_concept_id as STRING), concept_id) as descendant_id
# MAGIC  from (
# MAGIC  select concept_id from qcmv0h6ktmp_counts
# MAGIC  UNION
# MAGIC  -- include any ancestor concept that has a descendant in counts
# MAGIC  select distinct cast(ancestor_concept_id as STRING) concept_id
# MAGIC  from qcmv0h6ktmp_counts c
# MAGIC  join ${omop_catalog}.${omop_schema}.concept_ancestor ca on cast(ca.descendant_concept_id as STRING) = c.concept_id
# MAGIC  ) c
# MAGIC  left join ${omop_catalog}.${omop_schema}.concept_ancestor ca on c.concept_id = cast(ca.ancestor_concept_id as STRING)
# MAGIC );
# MAGIC
# MAGIC  CREATE TABLE qcmv0h6ktmp_concepts
# MAGIC USING DELTA
# MAGIC AS
# MAGIC (SELECT
# MAGIC ancestor_id,
# MAGIC  descendant_id
# MAGIC FROM
# MAGIC concepts);
# MAGIC
# MAGIC INSERT OVERWRITE ${results_catalog}.${results_schema}.achilles_result_concept_count (concept_id, record_count, descendant_record_count, person_count, descendant_person_count)
# MAGIC SELECT DISTINCT
# MAGIC  cast(concepts.ancestor_id as int) AS concept_id,
# MAGIC  coalesce(max(c1.agg_count_value), 0) AS record_count,
# MAGIC  coalesce(sum(c2.agg_count_value), 0) AS descendant_record_count,
# MAGIC  coalesce(max(c3.agg_count_value), 0) AS person_count,
# MAGIC  coalesce(sum(c4.agg_count_value), 0) AS descendant_person_count
# MAGIC FROM qcmv0h6ktmp_concepts concepts
# MAGIC  LEFT JOIN qcmv0h6ktmp_counts c1 ON concepts.ancestor_id = c1.concept_id
# MAGIC  LEFT JOIN qcmv0h6ktmp_counts c2 ON concepts.descendant_id = c2.concept_id
# MAGIC  LEFT JOIN qcmv0h6ktmp_counts_person c3 ON concepts.ancestor_id = c3.concept_id
# MAGIC  LEFT JOIN qcmv0h6ktmp_counts_person c4 ON concepts.descendant_id = c4.concept_id
# MAGIC GROUP BY concepts.ancestor_id;
# MAGIC
# MAGIC DROP TABLE IF EXISTS qcmv0h6ktmp_counts;
# MAGIC
# MAGIC DROP TABLE IF EXISTS qcmv0h6ktmp_counts_person;
# MAGIC
# MAGIC DROP TABLE IF EXISTS qcmv0h6ktmp_concepts;
# MAGIC
