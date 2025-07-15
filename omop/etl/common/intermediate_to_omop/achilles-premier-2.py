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
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_823
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 823 AS analysis_id,
# MAGIC  CAST(o.observation_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(o.qualifier_concept_id AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(*) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.observation o
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  o.person_id = op.person_id
# MAGIC AND 
# MAGIC  o.observation_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  o.observation_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  o.observation_concept_id,
# MAGIC  o.qualifier_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_823
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_825
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 825 AS analysis_id,
# MAGIC  CAST(o.observation_source_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(*) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.observation o
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  o.person_id = op.person_id
# MAGIC AND 
# MAGIC  o.observation_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  o.observation_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  o.observation_source_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_825
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_826
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 826 AS analysis_id,
# MAGIC  CAST(o.value_as_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(*) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.observation o
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  o.person_id = op.person_id
# MAGIC AND 
# MAGIC  o.observation_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  o.observation_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  o.value_as_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_826
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_827
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 827 AS analysis_id,
# MAGIC  CAST(o.unit_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(*) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.observation o
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  o.person_id = op.person_id
# MAGIC AND 
# MAGIC  o.observation_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  o.observation_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  o.unit_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_827
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_891
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 891 AS analysis_id,
# MAGIC  CAST(o.observation_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(o.obs_cnt AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  SUM(COUNT(o.person_id)) OVER (PARTITION BY o.observation_concept_id ORDER BY o.obs_cnt DESC) AS count_value
# MAGIC FROM
# MAGIC (
# MAGIC  SELECT 
# MAGIC  o.observation_concept_id,
# MAGIC  COUNT(o.observation_id) AS obs_cnt,
# MAGIC  o.person_id
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.observation o
# MAGIC  JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC  ON 
# MAGIC  o.person_id = op.person_id
# MAGIC  AND 
# MAGIC  o.observation_date >= op.observation_period_start_date
# MAGIC  AND 
# MAGIC  o.observation_date <= op.observation_period_end_date
# MAGIC  GROUP BY 
# MAGIC  o.person_id,
# MAGIC  o.observation_concept_id
# MAGIC  ) o
# MAGIC GROUP BY 
# MAGIC  o.observation_concept_id, 
# MAGIC  o.obs_cnt;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_891
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_900
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 900 AS analysis_id,
# MAGIC  CAST(de.drug_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(DISTINCT de.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.drug_era de
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  de.person_id = op.person_id
# MAGIC AND 
# MAGIC  de.drug_era_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  de.drug_era_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  de.drug_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_900
# MAGIC  ZORDER BY stratum_1;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_901
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 901 AS analysis_id,
# MAGIC  CAST(de.drug_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(de.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.drug_era de
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  de.person_id = op.person_id
# MAGIC AND 
# MAGIC  de.drug_era_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  de.drug_era_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  de.drug_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_901
# MAGIC  ZORDER BY stratum_1;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_902
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  de.drug_concept_id AS stratum_1,
# MAGIC  YEAR(de.drug_era_start_date) * 100 + MONTH(de.drug_era_start_date) AS stratum_2,
# MAGIC  COUNT(DISTINCT de.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.drug_era de
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  de.person_id = op.person_id
# MAGIC AND 
# MAGIC  de.drug_era_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  de.drug_era_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  de.drug_concept_id,
# MAGIC  YEAR(de.drug_era_start_date) * 100 + MONTH(de.drug_era_start_date)
# MAGIC )
# MAGIC  SELECT
# MAGIC 902 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(stratum_2 AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_902
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_903
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData(count_value)  AS (
# MAGIC SELECT
# MAGIC  COUNT(DISTINCT de.drug_concept_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.drug_era de
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  de.person_id = op.person_id
# MAGIC AND 
# MAGIC  de.drug_era_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  de.drug_era_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  de.person_id
# MAGIC ),
# MAGIC overallStats (avg_value, stdev_value, min_value, max_value, total) as
# MAGIC (
# MAGIC  select CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  from rawData
# MAGIC ),
# MAGIC statsView (count_value, total, rn) as
# MAGIC (
# MAGIC  select count_value, 
# MAGIC  COUNT(*) as total, 
# MAGIC  row_number() over (order by count_value) as rn
# MAGIC  FROM rawData
# MAGIC  group by count_value
# MAGIC ),
# MAGIC priorStats (count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on p.rn <= s.rn
# MAGIC  group by s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 903 as analysis_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC CROSS JOIN overallStats o
# MAGIC GROUP BY o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_903
# MAGIC  ZORDER BY count_value;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_903
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, 
# MAGIC cast(null as STRING) as stratum_1, cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_903
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_903
# MAGIC  ZORDER BY count_value;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_903;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_903;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_904
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  de.drug_concept_id AS stratum_1,
# MAGIC  YEAR(de.drug_era_start_date) AS stratum_2,
# MAGIC  p.gender_concept_id AS stratum_3,
# MAGIC  FLOOR((YEAR(de.drug_era_start_date) - p.year_of_birth) / 10) AS stratum_4,
# MAGIC  COUNT(DISTINCT p.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.person p
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.drug_era de 
# MAGIC ON 
# MAGIC  p.person_id = de.person_id
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  de.person_id = op.person_id
# MAGIC AND 
# MAGIC  de.drug_era_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  de.drug_era_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  de.drug_concept_id,
# MAGIC  YEAR(de.drug_era_start_date),
# MAGIC  p.gender_concept_id,
# MAGIC  FLOOR((YEAR(de.drug_era_start_date) - p.year_of_birth) / 10)
# MAGIC )
# MAGIC  SELECT
# MAGIC 904 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(stratum_2 AS STRING) AS stratum_2,
# MAGIC  CAST(stratum_3 AS STRING) AS stratum_3,
# MAGIC  CAST(stratum_4 AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_904
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(subject_id) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1rawData_906
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC de.drug_concept_id AS subject_id,
# MAGIC  p.gender_concept_id,
# MAGIC  de.drug_start_year - p.year_of_birth AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.person p
# MAGIC JOIN (
# MAGIC  SELECT 
# MAGIC  de.person_id,
# MAGIC  de.drug_concept_id,
# MAGIC  MIN(YEAR(de.drug_era_start_date)) AS drug_start_year
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.drug_era de
# MAGIC  JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC  ON 
# MAGIC  de.person_id = op.person_id
# MAGIC  AND 
# MAGIC  de.drug_era_start_date >= op.observation_period_start_date
# MAGIC  AND 
# MAGIC  de.drug_era_start_date <= op.observation_period_end_date 
# MAGIC  GROUP BY 
# MAGIC  de.person_id,
# MAGIC  de.drug_concept_id
# MAGIC  ) de 
# MAGIC ON 
# MAGIC  p.person_id = de.person_id
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1rawData_906
# MAGIC  ZORDER BY subject_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum1_id)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_906
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH overallStats (stratum1_id, stratum2_id, avg_value, stdev_value, min_value, max_value, total)  AS (
# MAGIC  select subject_id as stratum1_id,
# MAGIC  gender_concept_id as stratum2_id,
# MAGIC  CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  FROM ${results_catalog}.${results_schema}.xpi8onh1rawData_906
# MAGIC  group by subject_id, gender_concept_id
# MAGIC ),
# MAGIC statsView (stratum1_id, stratum2_id, count_value, total, rn) as
# MAGIC (
# MAGIC  select subject_id as stratum1_id, gender_concept_id as stratum2_id, count_value, COUNT(*) as total, row_number() over (partition by subject_id, gender_concept_id order by count_value) as rn
# MAGIC  FROM ${results_catalog}.${results_schema}.xpi8onh1rawData_906
# MAGIC  group by subject_id, gender_concept_id, count_value
# MAGIC ),
# MAGIC priorStats (stratum1_id, stratum2_id, count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.stratum1_id, s.stratum2_id, s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
# MAGIC  group by s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 906 as analysis_id,
# MAGIC  CAST(o.stratum1_id AS STRING) AS stratum1_id,
# MAGIC  CAST(o.stratum2_id AS STRING) AS stratum2_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC join overallStats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
# MAGIC GROUP BY o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_906
# MAGIC  ZORDER BY stratum1_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_906
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, stratum1_id as stratum_1, stratum2_id as stratum_2, 
# MAGIC cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_906
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_906
# MAGIC  ZORDER BY stratum_1;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1rawData_906;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1rawData_906;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_906;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_906;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_907
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData(stratum1_id, count_value)  AS (
# MAGIC SELECT 
# MAGIC  de.drug_concept_id AS stratum1_id,
# MAGIC  datediff(day,de.drug_era_start_date,de.drug_era_end_date) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.drug_era de
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  de.person_id = op.person_id
# MAGIC AND 
# MAGIC  de.drug_era_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  de.drug_era_start_date <= op.observation_period_end_date
# MAGIC ),
# MAGIC overallStats (stratum1_id, avg_value, stdev_value, min_value, max_value, total) as
# MAGIC (
# MAGIC  select stratum1_id, 
# MAGIC  CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  from rawData
# MAGIC  group by stratum1_id
# MAGIC ),
# MAGIC statsView (stratum1_id, count_value, total, rn) as
# MAGIC (
# MAGIC  select stratum1_id, 
# MAGIC  count_value, 
# MAGIC  COUNT(*) as total, 
# MAGIC  row_number() over (partition by stratum1_id order by count_value) as rn
# MAGIC  FROM rawData
# MAGIC  group by stratum1_id, count_value
# MAGIC ),
# MAGIC priorStats (stratum1_id, count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.stratum1_id, s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on s.stratum1_id = p.stratum1_id and p.rn <= s.rn
# MAGIC  group by s.stratum1_id, s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 907 as analysis_id,
# MAGIC  CAST(p.stratum1_id AS STRING) as stratum_1,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC join overallStats o on p.stratum1_id = o.stratum1_id
# MAGIC GROUP BY p.stratum1_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_907
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_907
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, stratum_1, 
# MAGIC cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_907
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_907
# MAGIC  ZORDER BY stratum_1;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_907;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_907;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_920
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  YEAR(de.drug_era_start_date) * 100 + MONTH(de.drug_era_start_date) AS stratum_1,
# MAGIC  COUNT(de.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.drug_era de
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  de.person_id = op.person_id
# MAGIC AND 
# MAGIC  de.drug_era_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  de.drug_era_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  YEAR(de.drug_era_start_date)*100 + MONTH(de.drug_era_start_date)
# MAGIC )
# MAGIC  SELECT
# MAGIC 920 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_920
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1000
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1000 AS analysis_id,
# MAGIC  CAST(ce.condition_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(DISTINCT ce.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.condition_era ce
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  ce.person_id = op.person_id
# MAGIC AND 
# MAGIC  ce.condition_era_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  ce.condition_era_start_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  ce.condition_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1000
# MAGIC  ZORDER BY stratum_1;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1001
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1001 AS analysis_id,
# MAGIC  CAST(ce.condition_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(ce.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.condition_era ce
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  ce.person_id = op.person_id
# MAGIC AND 
# MAGIC  ce.condition_era_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  ce.condition_era_start_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  ce.condition_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1001
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1002
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  ce.condition_concept_id AS stratum_1,
# MAGIC  YEAR(ce.condition_era_start_date) * 100 + MONTH(ce.condition_era_start_date) AS stratum_2,
# MAGIC  COUNT(DISTINCT ce.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.condition_era ce
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  ce.person_id = op.person_id
# MAGIC AND 
# MAGIC  ce.condition_era_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  ce.condition_era_start_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  ce.condition_concept_id,
# MAGIC  YEAR(ce.condition_era_start_date) * 100 + MONTH(ce.condition_era_start_date)
# MAGIC )
# MAGIC  SELECT
# MAGIC 1002 as analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(stratum_2 AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1002
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1003
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData(count_value)  AS (
# MAGIC SELECT 
# MAGIC  COUNT(DISTINCT ce.condition_concept_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.condition_era ce
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  ce.person_id = op.person_id
# MAGIC AND 
# MAGIC  ce.condition_era_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  ce.condition_era_start_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  ce.person_id
# MAGIC ),
# MAGIC overallStats (avg_value, stdev_value, min_value, max_value, total) as
# MAGIC (
# MAGIC  select CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  from rawData
# MAGIC ),
# MAGIC statsView (count_value, total, rn) as
# MAGIC (
# MAGIC  select count_value, 
# MAGIC  COUNT(*) as total, 
# MAGIC  row_number() over (order by count_value) as rn
# MAGIC  FROM rawData
# MAGIC  group by count_value
# MAGIC ),
# MAGIC priorStats (count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on p.rn <= s.rn
# MAGIC  group by s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 1003 as analysis_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC CROSS JOIN overallStats o
# MAGIC GROUP BY o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1003
# MAGIC  ZORDER BY count_value;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1003
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, 
# MAGIC cast(null as STRING) as stratum_1, cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_1003
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1003
# MAGIC  ZORDER BY count_value;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_1003;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_1003;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1004
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  ce.condition_concept_id AS stratum_1,
# MAGIC  YEAR(ce.condition_era_start_date) AS stratum_2,
# MAGIC  p.gender_concept_id AS stratum_3,
# MAGIC  FLOOR((YEAR(ce.condition_era_start_date) - p.year_of_birth) / 10) AS stratum_4,
# MAGIC  COUNT(DISTINCT p.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.person p
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.condition_era ce 
# MAGIC ON 
# MAGIC  p.person_id = ce.person_id
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  ce.person_id = op.person_id
# MAGIC AND 
# MAGIC  ce.condition_era_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  ce.condition_era_start_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  ce.condition_concept_id,
# MAGIC  YEAR(ce.condition_era_start_date),
# MAGIC  p.gender_concept_id,
# MAGIC  FLOOR((YEAR(ce.condition_era_start_date) - p.year_of_birth) / 10)
# MAGIC )
# MAGIC  SELECT
# MAGIC 1004 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(stratum_2 AS STRING) AS stratum_2,
# MAGIC  CAST(stratum_3 AS STRING) AS stratum_3,
# MAGIC  CAST(stratum_4 AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1004
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(subject_id) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1rawData_1006
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC ce.condition_concept_id AS subject_id,
# MAGIC  p.gender_concept_id,
# MAGIC  ce.condition_start_year - p.year_of_birth AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.person p
# MAGIC JOIN (
# MAGIC  SELECT 
# MAGIC  ce.person_id,
# MAGIC  ce.condition_concept_id,
# MAGIC  MIN(YEAR(ce.condition_era_start_date)) AS condition_start_year
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.condition_era ce
# MAGIC  JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC  ON 
# MAGIC  ce.person_id = op.person_id
# MAGIC  AND 
# MAGIC  ce.condition_era_start_date >= op.observation_period_start_date
# MAGIC  AND 
# MAGIC  ce.condition_era_start_date <= op.observation_period_end_date 
# MAGIC  GROUP BY 
# MAGIC  ce.person_id,
# MAGIC  ce.condition_concept_id
# MAGIC  ) ce 
# MAGIC ON 
# MAGIC  p.person_id = ce.person_id
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1rawData_1006
# MAGIC  ZORDER BY subject_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum1_id)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1006
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH overallStats (stratum1_id, stratum2_id, avg_value, stdev_value, min_value, max_value, total)  AS (
# MAGIC  select subject_id as stratum1_id,
# MAGIC  gender_concept_id as stratum2_id,
# MAGIC  CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  FROM ${results_catalog}.${results_schema}.xpi8onh1rawData_1006
# MAGIC  group by subject_id, gender_concept_id
# MAGIC ),
# MAGIC statsView (stratum1_id, stratum2_id, count_value, total, rn) as
# MAGIC (
# MAGIC  select subject_id as stratum1_id, gender_concept_id as stratum2_id, count_value, COUNT(*) as total, row_number() over (partition by subject_id, gender_concept_id order by count_value) as rn
# MAGIC  FROM ${results_catalog}.${results_schema}.xpi8onh1rawData_1006
# MAGIC  group by subject_id, gender_concept_id, count_value
# MAGIC ),
# MAGIC priorStats (stratum1_id, stratum2_id, count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.stratum1_id, s.stratum2_id, s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
# MAGIC  group by s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 1006 as analysis_id,
# MAGIC  CAST(o.stratum1_id AS STRING) AS stratum1_id,
# MAGIC  CAST(o.stratum2_id AS STRING) AS stratum2_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC join overallStats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
# MAGIC GROUP BY o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1006
# MAGIC  ZORDER BY stratum1_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1006
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, stratum1_id as stratum_1, stratum2_id as stratum_2, 
# MAGIC cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_1006
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1006
# MAGIC  ZORDER BY stratum_1;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1rawData_1006;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1rawData_1006;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_1006;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_1006;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1007
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData(stratum1_id, count_value)  AS (
# MAGIC SELECT 
# MAGIC  ce.condition_concept_id AS stratum1_id,
# MAGIC  datediff(day,ce.condition_era_start_date,ce.condition_era_end_date) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.condition_era ce
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  ce.person_id = op.person_id
# MAGIC AND 
# MAGIC  ce.condition_era_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  ce.condition_era_start_date <= op.observation_period_end_date 
# MAGIC ),
# MAGIC overallStats (stratum1_id, avg_value, stdev_value, min_value, max_value, total) as
# MAGIC (
# MAGIC  select stratum1_id, 
# MAGIC  CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  from rawData
# MAGIC  group by stratum1_id
# MAGIC ),
# MAGIC statsView (stratum1_id, count_value, total, rn) as
# MAGIC (
# MAGIC  select stratum1_id, 
# MAGIC  count_value, 
# MAGIC  COUNT(*) as total, 
# MAGIC  row_number() over (partition by stratum1_id order by count_value) as rn
# MAGIC  FROM rawData
# MAGIC  group by stratum1_id, count_value
# MAGIC ),
# MAGIC priorStats (stratum1_id, count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.stratum1_id, s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on s.stratum1_id = p.stratum1_id and p.rn <= s.rn
# MAGIC  group by s.stratum1_id, s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 1007 as analysis_id,
# MAGIC  CAST(p.stratum1_id AS STRING) as stratum_1,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC join overallStats o on p.stratum1_id = o.stratum1_id
# MAGIC GROUP BY p.stratum1_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1007
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1007
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, stratum_1, 
# MAGIC cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_1007
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1007
# MAGIC  ZORDER BY stratum_1;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_1007;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_1007;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1020
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  YEAR(ce.condition_era_start_date) * 100 + MONTH(ce.condition_era_start_date) AS stratum_1,
# MAGIC  COUNT(ce.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.condition_era ce
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  ce.person_id = op.person_id
# MAGIC AND 
# MAGIC  ce.condition_era_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  ce.condition_era_start_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  YEAR(ce.condition_era_start_date)*100 + MONTH(ce.condition_era_start_date)
# MAGIC )
# MAGIC  SELECT
# MAGIC 1020 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1020
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1100
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC  select
# MAGIC  left(l1.zip,3) as stratum_1,
# MAGIC  COUNT(distinct person_id) as count_value
# MAGIC  from ${omop_catalog}.${omop_schema}.person p1
# MAGIC  inner join ${omop_catalog}.${omop_schema}.location l1
# MAGIC  on p1.location_id = l1.location_id
# MAGIC  where p1.location_id is not null
# MAGIC  and l1.zip is not null
# MAGIC  group by left(l1.zip,3)
# MAGIC )
# MAGIC  SELECT
# MAGIC 1100 as analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) as stratum_1,
# MAGIC  cast(null as STRING) as stratum_2,
# MAGIC  cast(null as STRING) as stratum_3,
# MAGIC  cast(null as STRING) as stratum_4,
# MAGIC  cast(null as STRING) as stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1100
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1101
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1101 as analysis_id, 
# MAGIC  CAST(l1.state AS STRING) as stratum_1, 
# MAGIC  cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC  COUNT(distinct person_id) as count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.person p1
# MAGIC  inner join ${omop_catalog}.${omop_schema}.location l1
# MAGIC  on p1.location_id = l1.location_id
# MAGIC where p1.location_id is not null
# MAGIC  and l1.state is not null
# MAGIC group by l1.state;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1101
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1102
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC  select
# MAGIC  left(l1.zip,3) as stratum_1,
# MAGIC  COUNT(distinct care_site_id) as count_value
# MAGIC  from ${omop_catalog}.${omop_schema}.care_site cs1
# MAGIC  inner join ${omop_catalog}.${omop_schema}.location l1
# MAGIC  on cs1.location_id = l1.location_id
# MAGIC  where cs1.location_id is not null
# MAGIC  and l1.zip is not null
# MAGIC  group by left(l1.zip,3)
# MAGIC )
# MAGIC  SELECT
# MAGIC 1102 as analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) as stratum_1,
# MAGIC  cast(null as STRING) as stratum_2,
# MAGIC  cast(null as STRING) as stratum_3,
# MAGIC  cast(null as STRING) as stratum_4,
# MAGIC  cast(null as STRING) as stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1102
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1103
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1103 as analysis_id, 
# MAGIC  CAST(l1.state AS STRING) as stratum_1, 
# MAGIC  cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC  COUNT(distinct care_site_id) as count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.care_site cs1
# MAGIC  inner join ${omop_catalog}.${omop_schema}.location l1
# MAGIC  on cs1.location_id = l1.location_id
# MAGIC where cs1.location_id is not null
# MAGIC  and l1.state is not null
# MAGIC group by l1.state;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1103
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1200
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1200 as analysis_id, 
# MAGIC  CAST(cs1.place_of_service_concept_id AS STRING) as stratum_1, 
# MAGIC  cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC  COUNT(person_id) as count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.person p1
# MAGIC  inner join ${omop_catalog}.${omop_schema}.care_site cs1
# MAGIC  on p1.care_site_id = cs1.care_site_id
# MAGIC where p1.care_site_id is not null
# MAGIC  and cs1.place_of_service_concept_id is not null
# MAGIC group by cs1.place_of_service_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1200
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1201
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1201 as analysis_id, 
# MAGIC  CAST(cs1.place_of_service_concept_id AS STRING) as stratum_1, 
# MAGIC  cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC  COUNT(visit_occurrence_id) as count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.visit_occurrence vo1
# MAGIC  inner join ${omop_catalog}.${omop_schema}.care_site cs1
# MAGIC  on vo1.care_site_id = cs1.care_site_id
# MAGIC where vo1.care_site_id is not null
# MAGIC  and cs1.place_of_service_concept_id is not null
# MAGIC group by cs1.place_of_service_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1201
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1202
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1202 as analysis_id, 
# MAGIC  CAST(cs1.place_of_service_concept_id AS STRING) as stratum_1,
# MAGIC  cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC  COUNT(care_site_id) as count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.care_site cs1
# MAGIC where cs1.place_of_service_concept_id is not null
# MAGIC group by cs1.place_of_service_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1202
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1203
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1203 AS analysis_id,
# MAGIC  CAST(vo.discharged_to_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(*) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  vo.person_id = op.person_id
# MAGIC AND 
# MAGIC  vo.visit_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  vo.visit_start_date <= op.observation_period_end_date
# MAGIC WHERE 
# MAGIC  vo.discharged_to_concept_id != 0
# MAGIC GROUP BY 
# MAGIC  vo.discharged_to_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1203
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1300
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1300 AS analysis_id,
# MAGIC  CAST(vd.visit_detail_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(DISTINCT vd.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.visit_detail vd
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  vd.person_id = op.person_id
# MAGIC AND 
# MAGIC  vd.visit_detail_start_date >= op.observation_period_start_date 
# MAGIC AND 
# MAGIC  vd.visit_detail_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  vd.visit_detail_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1300
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1301
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1301 AS analysis_id,
# MAGIC  CAST(vd.visit_detail_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(vd.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.visit_detail vd
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  vd.person_id = op.person_id
# MAGIC AND 
# MAGIC  vd.visit_detail_start_date >= op.observation_period_start_date 
# MAGIC AND 
# MAGIC  vd.visit_detail_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  vd.visit_detail_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1301
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1302
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT
# MAGIC  vd.visit_detail_concept_id AS stratum_1,
# MAGIC  YEAR(vd.visit_detail_start_date)*100 + MONTH(vd.visit_detail_start_date) AS stratum_2,
# MAGIC  COUNT(DISTINCT vd.person_id) AS count_value
# MAGIC FROM
# MAGIC  ${omop_catalog}.${omop_schema}.visit_detail vd 
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  vd.person_id = op.person_id
# MAGIC AND 
# MAGIC  vd.visit_detail_start_date >= op.observation_period_start_date 
# MAGIC AND 
# MAGIC  vd.visit_detail_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  vd.visit_detail_concept_id,
# MAGIC  YEAR(vd.visit_detail_start_date)*100 + MONTH(vd.visit_detail_start_date)
# MAGIC )
# MAGIC  SELECT
# MAGIC 1302 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(stratum_2 AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1302
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1303
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData(person_id, count_value)  AS (
# MAGIC SELECT 
# MAGIC  vd.person_id,
# MAGIC  COUNT(DISTINCT vd.visit_detail_concept_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.visit_detail vd
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  vd.person_id = op.person_id
# MAGIC AND 
# MAGIC  vd.visit_detail_start_date >= op.observation_period_start_date 
# MAGIC AND 
# MAGIC  vd.visit_detail_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  vd.person_id
# MAGIC ),
# MAGIC overallStats (avg_value, stdev_value, min_value, max_value, total) AS
# MAGIC (
# MAGIC SELECT 
# MAGIC  CAST(AVG(1.0 * count_value) AS FLOAT) AS avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) AS stdev_value,
# MAGIC  MIN(count_value) AS min_value,
# MAGIC  MAX(count_value) AS max_value,
# MAGIC  COUNT(*) AS total
# MAGIC FROM 
# MAGIC  rawData
# MAGIC ),
# MAGIC statsView (count_value, total, rn) AS
# MAGIC (
# MAGIC SELECT 
# MAGIC  count_value,
# MAGIC  COUNT(*) AS total,
# MAGIC  ROW_NUMBER() OVER (ORDER BY count_value) AS rn
# MAGIC FROM 
# MAGIC  rawData
# MAGIC GROUP BY 
# MAGIC  count_value
# MAGIC ),
# MAGIC priorStats (count_value, total, accumulated) AS
# MAGIC (
# MAGIC SELECT 
# MAGIC  s.count_value,
# MAGIC  s.total,
# MAGIC  SUM(p.total) AS accumulated
# MAGIC FROM 
# MAGIC  statsView s
# MAGIC JOIN 
# MAGIC  statsView p ON p.rn <= s.rn
# MAGIC GROUP BY 
# MAGIC  s.count_value,
# MAGIC  s.total,
# MAGIC  s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 1303 AS analysis_id,
# MAGIC  o.total AS count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(CASE WHEN p.accumulated >= .50 * o.total THEN count_value ELSE o.max_value END) AS median_value,
# MAGIC  MIN(CASE WHEN p.accumulated >= .10 * o.total THEN count_value ELSE o.max_value END) AS p10_value,
# MAGIC  MIN(CASE WHEN p.accumulated >= .25 * o.total THEN count_value ELSE o.max_value END) AS p25_value,
# MAGIC  MIN(CASE WHEN p.accumulated >= .75 * o.total THEN count_value ELSE o.max_value END) AS p75_value,
# MAGIC  MIN(CASE WHEN p.accumulated >= .90 * o.total THEN count_value ELSE o.max_value END) AS p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC CROSS JOIN 
# MAGIC  overallStats o
# MAGIC GROUP BY 
# MAGIC  o.total,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1303
# MAGIC  ZORDER BY count_value;

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1303
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id,
# MAGIC  CAST(NULL AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value,
# MAGIC  min_value,
# MAGIC  max_value,
# MAGIC  avg_value,
# MAGIC  stdev_value,
# MAGIC  median_value,
# MAGIC  p10_value,
# MAGIC  p25_value,
# MAGIC  p75_value,
# MAGIC  p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_1303
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1303
# MAGIC  ZORDER BY count_value;
# MAGIC TRUNCATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1303;
# MAGIC DROP TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1303;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1304
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  vd.visit_detail_concept_id AS stratum_1,
# MAGIC  YEAR(vd.visit_detail_start_date) AS stratum_2,
# MAGIC  p.gender_concept_id AS stratum_3,
# MAGIC  FLOOR((YEAR(vd.visit_detail_start_date) - p.year_of_birth) / 10) AS stratum_4,
# MAGIC  COUNT(DISTINCT p.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.person p
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.visit_detail vd 
# MAGIC ON 
# MAGIC  p.person_id = vd.person_id 
# MAGIC JOIN
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  vd.person_id = op.person_id
# MAGIC AND 
# MAGIC  vd.visit_detail_start_date >= op.observation_period_start_date 
# MAGIC AND 
# MAGIC  vd.visit_detail_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  vd.visit_detail_concept_id,
# MAGIC  YEAR(vd.visit_detail_start_date),
# MAGIC  p.gender_concept_id,
# MAGIC  FLOOR((YEAR(vd.visit_detail_start_date) - p.year_of_birth)/10)
# MAGIC )
# MAGIC  SELECT
# MAGIC 1304 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(stratum_2 AS STRING) AS stratum_2,
# MAGIC  CAST(stratum_3 AS STRING) AS stratum_3,
# MAGIC  CAST(stratum_4 AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1304
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum1_id)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1306
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData (stratum1_id, stratum2_id, count_value)  AS (
# MAGIC SELECT 
# MAGIC  vd.visit_detail_concept_id AS stratum1_id,
# MAGIC  p.gender_concept_id AS stratum2_id,
# MAGIC  vd.visit_detail_start_year - p.year_of_birth AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.person p
# MAGIC JOIN (
# MAGIC  SELECT 
# MAGIC  vd.person_id,
# MAGIC  vd.visit_detail_concept_id,
# MAGIC  MIN(YEAR(vd.visit_detail_start_date)) AS visit_detail_start_year
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.visit_detail vd
# MAGIC  JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC  ON 
# MAGIC  vd.person_id = op.person_id
# MAGIC  AND 
# MAGIC  vd.visit_detail_start_date >= op.observation_period_start_date 
# MAGIC  AND 
# MAGIC  vd.visit_detail_start_date <= op.observation_period_end_date
# MAGIC  GROUP BY 
# MAGIC  vd.person_id,
# MAGIC  vd.visit_detail_concept_id
# MAGIC  ) vd 
# MAGIC ON 
# MAGIC  p.person_id = vd.person_id
# MAGIC ),
# MAGIC overallStats (stratum1_id, stratum2_id, avg_value, stdev_value, min_value, max_value, total) AS
# MAGIC (
# MAGIC SELECT 
# MAGIC  stratum1_id,
# MAGIC  stratum2_id,
# MAGIC  CAST(AVG(1.0 * count_value) AS FLOAT) AS avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) AS stdev_value,
# MAGIC  MIN(count_value) AS min_value,
# MAGIC  MAX(count_value) AS max_value,
# MAGIC  COUNT(*) AS total
# MAGIC FROM 
# MAGIC  rawData
# MAGIC GROUP BY 
# MAGIC  stratum1_id,
# MAGIC  stratum2_id
# MAGIC ),
# MAGIC statsView (stratum1_id, stratum2_id, count_value, total, rn) AS
# MAGIC (
# MAGIC SELECT 
# MAGIC  stratum1_id,
# MAGIC  stratum2_id,
# MAGIC  count_value,
# MAGIC  COUNT(*) AS total,
# MAGIC  ROW_NUMBER() OVER (PARTITION BY stratum1_id,stratum2_id ORDER BY count_value) AS rn
# MAGIC FROM 
# MAGIC  rawData
# MAGIC GROUP BY 
# MAGIC  stratum1_id,
# MAGIC  stratum2_id,
# MAGIC  count_value
# MAGIC ),
# MAGIC priorStats (stratum1_id, stratum2_id, count_value, total, accumulated) AS
# MAGIC (
# MAGIC SELECT 
# MAGIC  s.stratum1_id,
# MAGIC  s.stratum2_id,
# MAGIC  s.count_value,
# MAGIC  s.total,
# MAGIC  SUM(p.total) AS accumulated
# MAGIC FROM 
# MAGIC  statsView s
# MAGIC JOIN 
# MAGIC  statsView p ON s.stratum1_id = p.stratum1_id 
# MAGIC  AND s.stratum2_id = p.stratum2_id 
# MAGIC  AND p.rn <= s.rn
# MAGIC GROUP BY 
# MAGIC  s.stratum1_id,
# MAGIC  s.stratum2_id,
# MAGIC  s.count_value,
# MAGIC  s.total,
# MAGIC  s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 1306 AS analysis_id,
# MAGIC  CAST(o.stratum1_id AS STRING) AS stratum1_id,
# MAGIC  CAST(o.stratum2_id AS STRING) AS stratum2_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(CASE WHEN p.accumulated >= .50 * o.total THEN count_value ELSE o.max_value END) AS median_value,
# MAGIC  MIN(CASE WHEN p.accumulated >= .10 * o.total THEN count_value ELSE o.max_value END) AS p10_value,
# MAGIC  MIN(CASE WHEN p.accumulated >= .25 * o.total THEN count_value ELSE o.max_value END) AS p25_value,
# MAGIC  MIN(CASE WHEN p.accumulated >= .75 * o.total THEN count_value ELSE o.max_value END) AS p75_value,
# MAGIC  MIN(CASE WHEN p.accumulated >= .90 * o.total THEN count_value ELSE o.max_value END) AS p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC JOIN 
# MAGIC  overallStats o ON p.stratum1_id = o.stratum1_id AND p.stratum2_id = o.stratum2_id 
# MAGIC GROUP BY 
# MAGIC  o.stratum1_id, 
# MAGIC  o.stratum2_id, 
# MAGIC  o.total, 
# MAGIC  o.min_value, 
# MAGIC  o.max_value, 
# MAGIC  o.avg_value, 
# MAGIC  o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1306
# MAGIC  ZORDER BY stratum1_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1306
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id,
# MAGIC  stratum1_id AS stratum_1,
# MAGIC  stratum2_id AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value,
# MAGIC  min_value,
# MAGIC  max_value,
# MAGIC  avg_value,
# MAGIC  stdev_value,
# MAGIC  median_value,
# MAGIC  p10_value,
# MAGIC  p25_value,
# MAGIC  p75_value,
# MAGIC  p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_1306
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1306
# MAGIC  ZORDER BY stratum_1;
# MAGIC TRUNCATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1306;
# MAGIC DROP TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1306;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1312
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  YEAR(vd.visit_detail_start_date) AS stratum_1,
# MAGIC  p.gender_concept_id AS stratum_2,
# MAGIC  FLOOR((YEAR(vd.visit_detail_start_date) - p.year_of_birth) / 10) AS stratum_3,
# MAGIC  COUNT(DISTINCT vd.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.person p
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.visit_detail vd 
# MAGIC ON 
# MAGIC  vd.person_id = p.person_id
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  vd.person_id = op.person_id
# MAGIC AND 
# MAGIC  vd.visit_detail_start_date >= op.observation_period_start_date 
# MAGIC AND 
# MAGIC  vd.visit_detail_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  YEAR(vd.visit_detail_start_date),
# MAGIC  p.gender_concept_id,
# MAGIC  FLOOR((YEAR(vd.visit_detail_start_date) - p.year_of_birth) / 10)
# MAGIC )
# MAGIC  SELECT
# MAGIC 1312 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(stratum_2 AS STRING) AS stratum_2,
# MAGIC  CAST(stratum_3 AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1312
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_id)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1313
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData(stratum_id, count_value)  AS (
# MAGIC SELECT 
# MAGIC  vd.visit_detail_concept_id AS stratum_id,
# MAGIC  datediff(day,vd.visit_detail_start_date,vd.visit_detail_END_date) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.visit_detail vd
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  vd.person_id = op.person_id
# MAGIC AND 
# MAGIC  vd.visit_detail_start_date >= op.observation_period_start_date 
# MAGIC AND 
# MAGIC  vd.visit_detail_start_date <= op.observation_period_end_date
# MAGIC ),
# MAGIC overallStats (stratum_id, avg_value, stdev_value, min_value, max_value, total) AS
# MAGIC (
# MAGIC SELECT 
# MAGIC  stratum_id,
# MAGIC  CAST(AVG(1.0 * count_value) AS FLOAT) AS avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) AS stdev_value,
# MAGIC  MIN(count_value) AS min_value,
# MAGIC  MAX(count_value) AS max_value,
# MAGIC  COUNT(*) AS total
# MAGIC FROM 
# MAGIC  rawData
# MAGIC GROUP BY 
# MAGIC  stratum_id
# MAGIC ),
# MAGIC statsView (stratum_id, count_value, total, rn) AS
# MAGIC (
# MAGIC SELECT 
# MAGIC  stratum_id,
# MAGIC  count_value,
# MAGIC  COUNT(*) AS total,
# MAGIC  ROW_NUMBER() OVER (ORDER BY count_value) AS rn
# MAGIC FROM 
# MAGIC  rawData
# MAGIC GROUP BY 
# MAGIC  stratum_id,
# MAGIC  count_value
# MAGIC ),
# MAGIC priorStats (stratum_id, count_value, total, accumulated) AS
# MAGIC (
# MAGIC SELECT 
# MAGIC  s.stratum_id,
# MAGIC  s.count_value,
# MAGIC  s.total,
# MAGIC  SUM(p.total) AS accumulated
# MAGIC FROM 
# MAGIC  statsView s
# MAGIC JOIN 
# MAGIC  statsView p 
# MAGIC ON 
# MAGIC  s.stratum_id = p.stratum_id
# MAGIC AND 
# MAGIC  p.rn <= s.rn
# MAGIC GROUP BY 
# MAGIC  s.stratum_id,
# MAGIC  s.count_value,
# MAGIC  s.total,
# MAGIC  s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 1313 AS analysis_id,
# MAGIC  CAST(o.stratum_id AS STRING) AS stratum_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(CASE WHEN p.accumulated >= .50 * o.total THEN count_value ELSE o.max_value END) AS median_value,
# MAGIC  MIN(CASE WHEN p.accumulated >= .10 * o.total THEN count_value ELSE o.max_value END) AS p10_value,
# MAGIC  MIN(CASE WHEN p.accumulated >= .25 * o.total THEN count_value ELSE o.max_value END) AS p25_value,
# MAGIC  MIN(CASE WHEN p.accumulated >= .75 * o.total THEN count_value ELSE o.max_value END) AS p75_value,
# MAGIC  MIN(CASE WHEN p.accumulated >= .90 * o.total THEN count_value ELSE o.max_value END) AS p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC JOIN 
# MAGIC  overallStats o ON p.stratum_id = o.stratum_id
# MAGIC GROUP BY 
# MAGIC  o.stratum_id, 
# MAGIC  o.total, 
# MAGIC  o.min_value, 
# MAGIC  o.max_value, 
# MAGIC  o.avg_value, 
# MAGIC  o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1313
# MAGIC  ZORDER BY stratum_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1313
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id,
# MAGIC  stratum_id AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value,
# MAGIC  min_value,
# MAGIC  max_value,
# MAGIC  avg_value,
# MAGIC  stdev_value,
# MAGIC  median_value,
# MAGIC  p10_value,
# MAGIC  p25_value,
# MAGIC  p75_value,
# MAGIC  p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_1313;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1313
# MAGIC  ZORDER BY stratum_1;
# MAGIC TRUNCATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1313;
# MAGIC DROP TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1313;
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1320
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  YEAR(vd.visit_detail_start_date) * 100 + MONTH(vd.visit_detail_start_date) AS stratum_1,
# MAGIC  COUNT(vd.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.visit_detail vd
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  vd.person_id = op.person_id
# MAGIC AND 
# MAGIC  vd.visit_detail_start_date >= op.observation_period_start_date 
# MAGIC AND 
# MAGIC  vd.visit_detail_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  YEAR(vd.visit_detail_start_date) * 100 + MONTH(vd.visit_detail_start_date)
# MAGIC )
# MAGIC  SELECT
# MAGIC 1320 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1320
# MAGIC  ZORDER BY stratum_1;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1321
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  YEAR(vd.visit_detail_start_date) AS stratum_1,
# MAGIC  COUNT(DISTINCT vd.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.visit_detail vd
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  vd.person_id = op.person_id
# MAGIC AND 
# MAGIC  vd.visit_detail_start_date >= op.observation_period_start_date 
# MAGIC AND 
# MAGIC  vd.visit_detail_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  YEAR(vd.visit_detail_start_date)
# MAGIC )
# MAGIC  SELECT
# MAGIC 1321 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1321
# MAGIC  ZORDER BY stratum_1;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1325
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1325 AS analysis_id,
# MAGIC  CAST(vd.visit_detail_source_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(*) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.visit_detail vd
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  vd.person_id = op.person_id
# MAGIC AND 
# MAGIC  vd.visit_detail_start_date >= op.observation_period_start_date 
# MAGIC AND 
# MAGIC  vd.visit_detail_start_date <= op.observation_period_end_date
# MAGIC GROUP BY 
# MAGIC  visit_detail_source_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1325
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1326
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1326 AS analysis_id,
# MAGIC  CAST(v.visit_detail_concept_id AS STRING) AS stratum_1,
# MAGIC  v.cdm_table AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  v.record_count AS count_value
# MAGIC FROM
# MAGIC (
# MAGIC  SELECT 'drug_exposure' cdm_table,
# MAGIC  COALESCE(vd.visit_detail_concept_id, 0) visit_detail_concept_id,
# MAGIC  COUNT(*) record_count
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.drug_exposure de
# MAGIC  LEFT JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.visit_detail vd 
# MAGIC  ON 
# MAGIC  de.visit_occurrence_id = vd.visit_occurrence_id
# MAGIC  GROUP BY 
# MAGIC  vd.visit_detail_concept_id
# MAGIC  UNION
# MAGIC  SELECT 
# MAGIC  'condition_occurrence' cdm_table,
# MAGIC  COALESCE(vd.visit_detail_concept_id, 0) visit_detail_concept_id,
# MAGIC  COUNT(*) record_count
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.condition_occurrence co
# MAGIC  LEFT JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.visit_detail vd 
# MAGIC  ON 
# MAGIC  co.visit_occurrence_id = vd.visit_occurrence_id
# MAGIC  GROUP BY 
# MAGIC  vd.visit_detail_concept_id
# MAGIC  UNION
# MAGIC  SELECT 
# MAGIC  'device_exposure' cdm_table,
# MAGIC  COALESCE(visit_detail_concept_id, 0) visit_detail_concept_id,
# MAGIC  COUNT(*) record_count
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.device_exposure de
# MAGIC  LEFT JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.visit_detail vd 
# MAGIC  ON 
# MAGIC  de.visit_occurrence_id = vd.visit_occurrence_id
# MAGIC  GROUP BY 
# MAGIC  vd.visit_detail_concept_id
# MAGIC  UNION
# MAGIC  SELECT 
# MAGIC  'procedure_occurrence' cdm_table,
# MAGIC  COALESCE(vd.visit_detail_concept_id, 0) visit_detail_concept_id,
# MAGIC  COUNT(*) record_count
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.procedure_occurrence po
# MAGIC  LEFT JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.visit_detail vd 
# MAGIC  ON 
# MAGIC  po.visit_occurrence_id = vd.visit_occurrence_id
# MAGIC  GROUP BY 
# MAGIC  vd.visit_detail_concept_id
# MAGIC  UNION
# MAGIC  SELECT 
# MAGIC  'measurement' cdm_table,
# MAGIC  COALESCE(vd.visit_detail_concept_id, 0) visit_detail_concept_id,
# MAGIC  COUNT(*) record_count
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.measurement m
# MAGIC  LEFT JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.visit_detail vd 
# MAGIC  ON 
# MAGIC  m.visit_occurrence_id = vd.visit_occurrence_id
# MAGIC  GROUP BY 
# MAGIC  vd.visit_detail_concept_id
# MAGIC  UNION
# MAGIC  SELECT 
# MAGIC  'observation' cdm_table,
# MAGIC  COALESCE(vd.visit_detail_concept_id, 0) visit_detail_concept_id,
# MAGIC  COUNT(*) record_count
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.observation o
# MAGIC  LEFT JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.visit_detail vd 
# MAGIC  ON 
# MAGIC  o.visit_occurrence_id = vd.visit_occurrence_id
# MAGIC  GROUP BY 
# MAGIC  vd.visit_detail_concept_id
# MAGIC  ) v;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1406
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData(stratum1_id, count_value)  AS (
# MAGIC  select p1.gender_concept_id as stratum1_id,
# MAGIC  datediff(day,ppp1.payer_plan_period_start_date,ppp1.payer_plan_period_end_date) as count_value
# MAGIC  from ${omop_catalog}.${omop_schema}.person p1
# MAGIC  inner join 
# MAGIC  (select person_id, 
# MAGIC  payer_plan_period_START_DATE, 
# MAGIC  payer_plan_period_END_DATE, 
# MAGIC  ROW_NUMBER() over (PARTITION by person_id order by payer_plan_period_start_date asc) as rn1
# MAGIC  from ${omop_catalog}.${omop_schema}.payer_plan_period
# MAGIC  ) ppp1
# MAGIC  on p1.PERSON_ID = ppp1.PERSON_ID
# MAGIC  where ppp1.rn1 = 1
# MAGIC ),
# MAGIC overallStats (stratum1_id, avg_value, stdev_value, min_value, max_value, total) as
# MAGIC (
# MAGIC  select stratum1_id, 
# MAGIC  CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  from rawData
# MAGIC  group by stratum1_id
# MAGIC ),
# MAGIC statsView (stratum1_id, count_value, total, rn) as
# MAGIC (
# MAGIC  select stratum1_id, 
# MAGIC  count_value, 
# MAGIC  COUNT(*) as total, 
# MAGIC  row_number() over (partition by stratum1_id order by count_value) as rn
# MAGIC  FROM rawData
# MAGIC  group by stratum1_id, count_value
# MAGIC ),
# MAGIC priorStats (stratum1_id, count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.stratum1_id, s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on s.stratum1_id = p.stratum1_id and p.rn <= s.rn
# MAGIC  group by s.stratum1_id, s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 1406 as analysis_id,
# MAGIC  CAST(p.stratum1_id AS STRING) as stratum_1,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC join overallStats o on p.stratum1_id = o.stratum1_id
# MAGIC GROUP BY p.stratum1_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1406
# MAGIC  ZORDER BY stratum_1;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1406
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, stratum_1, 
# MAGIC cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_1406
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1406
# MAGIC  ZORDER BY stratum_1;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_1406;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_1406;
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_id)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1407
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData(stratum_id, count_value)  AS (
# MAGIC  select floor((year(ppp1.payer_plan_period_START_DATE) - p1.YEAR_OF_BIRTH)/10) as stratum_id,
# MAGIC  datediff(day,ppp1.payer_plan_period_start_date,ppp1.payer_plan_period_end_date) as count_value
# MAGIC  from ${omop_catalog}.${omop_schema}.person p1
# MAGIC  inner join 
# MAGIC  (select person_id, 
# MAGIC  payer_plan_period_START_DATE, 
# MAGIC  payer_plan_period_END_DATE, 
# MAGIC  ROW_NUMBER() over (PARTITION by person_id order by payer_plan_period_start_date asc) as rn1
# MAGIC  from ${omop_catalog}.${omop_schema}.payer_plan_period
# MAGIC  ) ppp1
# MAGIC  on p1.PERSON_ID = ppp1.PERSON_ID
# MAGIC  where ppp1.rn1 = 1
# MAGIC ),
# MAGIC overallStats (stratum_id, avg_value, stdev_value, min_value, max_value, total) as
# MAGIC (
# MAGIC  select stratum_id,
# MAGIC  CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  FROM rawData
# MAGIC  group by stratum_id
# MAGIC ),
# MAGIC statsView (stratum_id, count_value, total, rn) as
# MAGIC (
# MAGIC  select stratum_id, count_value, COUNT(*) as total, row_number() over (order by count_value) as rn
# MAGIC  FROM rawData
# MAGIC  group by stratum_id, count_value
# MAGIC ),
# MAGIC priorStats (stratum_id, count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.stratum_id, s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on s.stratum_id = p.stratum_id and p.rn <= s.rn
# MAGIC  group by s.stratum_id, s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 1407 as analysis_id,
# MAGIC  CAST(o.stratum_id AS STRING) AS stratum_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC join overallStats o on p.stratum_id = o.stratum_id
# MAGIC GROUP BY o.stratum_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1407
# MAGIC  ZORDER BY stratum_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1407
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, stratum_id as stratum_1, 
# MAGIC cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_1407
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1407
# MAGIC  ZORDER BY stratum_1;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_1407;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_1407;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1408
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1408 as analysis_id, 
# MAGIC  CAST(floor(datediff(day,ppp1.payer_plan_period_start_date,ppp1.payer_plan_period_end_date)/30) AS STRING) as stratum_1,
# MAGIC  cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC  COUNT(distinct p1.person_id) as count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.person p1
# MAGIC  inner join 
# MAGIC  (select person_id, 
# MAGIC  payer_plan_period_START_DATE, 
# MAGIC  payer_plan_period_END_DATE, 
# MAGIC  ROW_NUMBER() over (PARTITION by person_id order by payer_plan_period_start_date asc) as rn1
# MAGIC  from ${omop_catalog}.${omop_schema}.payer_plan_period
# MAGIC  ) ppp1
# MAGIC  on p1.PERSON_ID = ppp1.PERSON_ID
# MAGIC  where ppp1.rn1 = 1
# MAGIC group by CAST(floor(datediff(day,ppp1.payer_plan_period_start_date,ppp1.payer_plan_period_end_date)/30) AS STRING);
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1408
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1temp_dates_1409
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC distinct 
# MAGIC  YEAR(payer_plan_period_start_date) as obs_year 
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.payer_plan_period
# MAGIC ;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1409
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1409 as analysis_id, 
# MAGIC  CAST(t1.obs_year AS STRING) as stratum_1, 
# MAGIC  cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC  COUNT(distinct p1.PERSON_ID) as count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.person p1
# MAGIC  inner join 
# MAGIC  ${omop_catalog}.${omop_schema}.payer_plan_period ppp1
# MAGIC  on p1.person_id = ppp1.person_id
# MAGIC  ,
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1temp_dates_1409 t1 
# MAGIC where year(ppp1.payer_plan_period_START_DATE) <= t1.obs_year
# MAGIC  and year(ppp1.payer_plan_period_END_DATE) >= t1.obs_year
# MAGIC group by t1.obs_year
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1409
# MAGIC  ZORDER BY stratum_1;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1temp_dates_1409;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1temp_dates_1409;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(obs_month) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1temp_dates_1410
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC DISTINCT 
# MAGIC  YEAR(payer_plan_period_start_date)*100 + MONTH(payer_plan_period_start_date) AS obs_month,
# MAGIC  to_date(cast(YEAR(payer_plan_period_start_date) as string) || '-' || cast(MONTH(payer_plan_period_start_date) as string) || '-' || cast(1 as string)) as obs_month_start,
# MAGIC  last_day(payer_plan_period_start_date) as obs_month_end
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.payer_plan_period
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1temp_dates_1410
# MAGIC  ZORDER BY obs_month;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1410
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1410 as analysis_id, 
# MAGIC  CAST(obs_month AS STRING) as stratum_1,
# MAGIC  cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC  COUNT(distinct p1.PERSON_ID) as count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.person p1
# MAGIC  inner join 
# MAGIC  ${omop_catalog}.${omop_schema}.payer_plan_period ppp1
# MAGIC  on p1.person_id = ppp1.person_id
# MAGIC  ,
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1temp_dates_1410
# MAGIC where ppp1.payer_plan_period_START_DATE <= obs_month_start
# MAGIC  and ppp1.payer_plan_period_END_DATE >= obs_month_end
# MAGIC group by obs_month
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1410
# MAGIC  ZORDER BY stratum_1;
# MAGIC TRUNCATE TABLE ${results_catalog}.${results_schema}.xpi8onh1temp_dates_1410;
# MAGIC DROP TABLE ${results_catalog}.${results_schema}.xpi8onh1temp_dates_1410;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1411
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1411 as analysis_id, 
# MAGIC  to_date(cast(YEAR(payer_plan_period_start_date) as string) || '-' || cast(MONTH(payer_plan_period_start_date) as string) || '-' || cast(1 as string)) as stratum_1,
# MAGIC  cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC  COUNT(distinct p1.PERSON_ID) as count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.person p1
# MAGIC  inner join ${omop_catalog}.${omop_schema}.payer_plan_period ppp1
# MAGIC  on p1.person_id = ppp1.person_id
# MAGIC group by to_date(cast(YEAR(payer_plan_period_start_date) as string) || '-' || cast(MONTH(payer_plan_period_start_date) as string) || '-' || cast(1 as string));
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1411
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1412
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1412 as analysis_id, 
# MAGIC  to_date(cast(YEAR(payer_plan_period_start_date) as string) || '-' || cast(MONTH(payer_plan_period_start_date) as string) || '-' || cast(1 as string)) as stratum_1,
# MAGIC  cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC  COUNT(distinct p1.PERSON_ID) as count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.person p1
# MAGIC  inner join ${omop_catalog}.${omop_schema}.payer_plan_period ppp1
# MAGIC  on p1.person_id = ppp1.person_id
# MAGIC group by to_date(cast(YEAR(payer_plan_period_start_date) as string) || '-' || cast(MONTH(payer_plan_period_start_date) as string) || '-' || cast(1 as string));
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1412
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1413
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1413 as analysis_id, 
# MAGIC  CAST(ppp1.num_periods AS STRING) as stratum_1,
# MAGIC  cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC  COUNT(distinct p1.PERSON_ID) as count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.person p1
# MAGIC  inner join (select person_id, COUNT(payer_plan_period_start_date) as num_periods from ${omop_catalog}.${omop_schema}.payer_plan_period group by PERSON_ID) ppp1
# MAGIC  on p1.person_id = ppp1.person_id
# MAGIC group by ppp1.num_periods;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1413
# MAGIC  ZORDER BY stratum_1;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1425 
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1425 as analysis_id,
# MAGIC  cast(payer_source_concept_id AS STRING) AS stratum_1,
# MAGIC  cast(null AS STRING) AS stratum_2,
# MAGIC  cast(null as STRING) as stratum_3,
# MAGIC  cast(null as STRING) as stratum_4,
# MAGIC  cast(null as STRING) as stratum_5,
# MAGIC  COUNT(*) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.payer_plan_period
# MAGIC  group by payer_source_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1425 
# MAGIC   ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1800
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1800 AS analysis_id,
# MAGIC  CAST(m.measurement_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(DISTINCT m.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.measurement m
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  m.person_id = op.person_id
# MAGIC AND 
# MAGIC  m.measurement_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  m.measurement_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  m.measurement_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1800
# MAGIC  ZORDER BY stratum_1;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1801
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1801 AS analysis_id,
# MAGIC  CAST(m.measurement_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(m.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.measurement m
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  m.person_id = op.person_id
# MAGIC AND 
# MAGIC  m.measurement_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  m.measurement_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  m.measurement_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1801
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1802
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  m.measurement_concept_id AS stratum_1,
# MAGIC  YEAR(m.measurement_date) * 100 + MONTH(m.measurement_date) AS stratum_2,
# MAGIC  COUNT(DISTINCT m.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.measurement m
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  m.person_id = op.person_id
# MAGIC AND 
# MAGIC  m.measurement_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  m.measurement_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  m.measurement_concept_id,
# MAGIC  YEAR(m.measurement_date) * 100 + MONTH(m.measurement_date)
# MAGIC )
# MAGIC  SELECT
# MAGIC 1802 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(stratum_2 AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1802
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1803
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData(count_value)  AS (
# MAGIC SELECT 
# MAGIC  COUNT(DISTINCT m.measurement_concept_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.measurement m
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  m.person_id = op.person_id
# MAGIC AND 
# MAGIC  m.measurement_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  m.measurement_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  m.person_id
# MAGIC ),
# MAGIC overallStats (avg_value, stdev_value, min_value, max_value, total) as
# MAGIC (
# MAGIC  select CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  from rawData
# MAGIC ),
# MAGIC statsView (count_value, total, rn) as
# MAGIC (
# MAGIC  select count_value, 
# MAGIC  COUNT(*) as total, 
# MAGIC  row_number() over (order by count_value) as rn
# MAGIC  FROM rawData
# MAGIC  group by count_value
# MAGIC ),
# MAGIC priorStats (count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on p.rn <= s.rn
# MAGIC  group by s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 1803 as analysis_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC CROSS JOIN overallStats o
# MAGIC GROUP BY o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1803
# MAGIC  ZORDER BY count_value;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(count_value) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1803
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, 
# MAGIC cast(null as STRING) as stratum_1, cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_1803
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1803
# MAGIC  ZORDER BY count_value;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_1803;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_1803;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1804
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  m.measurement_concept_id AS stratum_1,
# MAGIC  YEAR(m.measurement_date) AS stratum_2,
# MAGIC  p.gender_concept_id AS stratum_3,
# MAGIC  FLOOR((YEAR(m.measurement_date) - p.year_of_birth) / 10) AS stratum_4,
# MAGIC  COUNT(DISTINCT p.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.person p
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.measurement m 
# MAGIC ON 
# MAGIC  p.person_id = m.person_id
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  m.person_id = op.person_id
# MAGIC AND 
# MAGIC  m.measurement_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  m.measurement_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  m.measurement_concept_id,
# MAGIC  YEAR(m.measurement_date),
# MAGIC  p.gender_concept_id,
# MAGIC  FLOOR((YEAR(m.measurement_date) - p.year_of_birth) / 10)
# MAGIC )
# MAGIC  SELECT
# MAGIC 1804 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(stratum_2 AS STRING) AS stratum_2,
# MAGIC  CAST(stratum_3 AS STRING) AS stratum_3,
# MAGIC  CAST(stratum_4 AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1804
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1805
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1805 AS analysis_id,
# MAGIC  CAST(m.measurement_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(m.measurement_type_concept_id AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(m.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.measurement m
# MAGIC  JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC  ON 
# MAGIC  m.person_id = op.person_id
# MAGIC  AND 
# MAGIC  m.measurement_date >= op.observation_period_start_date
# MAGIC  AND 
# MAGIC  m.measurement_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  m.measurement_concept_id,
# MAGIC  m.measurement_type_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1805
# MAGIC  ZORDER BY stratum_1;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC --HINT DISTRIBUTE_ON_KEY(subject_id) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1rawData_1806
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC o.measurement_concept_id AS subject_id,
# MAGIC  p.gender_concept_id,
# MAGIC  o.measurement_start_year - p.year_of_birth AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.person p
# MAGIC JOIN (
# MAGIC  SELECT 
# MAGIC  m.person_id,
# MAGIC  m.measurement_concept_id,
# MAGIC  MIN(YEAR(m.measurement_date)) AS measurement_start_year
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.measurement m
# MAGIC  JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC  ON 
# MAGIC  m.person_id = op.person_id
# MAGIC  AND 
# MAGIC  m.measurement_date >= op.observation_period_start_date
# MAGIC  AND 
# MAGIC  m.measurement_date <= op.observation_period_end_date 
# MAGIC  GROUP BY 
# MAGIC  m.person_id,
# MAGIC  m.measurement_concept_id
# MAGIC  ) o
# MAGIC ON 
# MAGIC  p.person_id = o.person_id
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1rawData_1806
# MAGIC  ZORDER BY subject_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum1_id)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1806
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH overallStats (stratum1_id, stratum2_id, avg_value, stdev_value, min_value, max_value, total)  AS (
# MAGIC  select subject_id as stratum1_id,
# MAGIC  gender_concept_id as stratum2_id,
# MAGIC  CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  FROM ${results_catalog}.${results_schema}.xpi8onh1rawData_1806
# MAGIC  group by subject_id, gender_concept_id
# MAGIC ),
# MAGIC statsView (stratum1_id, stratum2_id, count_value, total, rn) as
# MAGIC (
# MAGIC  select subject_id as stratum1_id, gender_concept_id as stratum2_id, count_value, COUNT(*) as total, row_number() over (partition by subject_id, gender_concept_id order by count_value) as rn
# MAGIC  FROM ${results_catalog}.${results_schema}.xpi8onh1rawData_1806
# MAGIC  group by subject_id, gender_concept_id, count_value
# MAGIC ),
# MAGIC priorStats (stratum1_id, stratum2_id, count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.stratum1_id, s.stratum2_id, s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
# MAGIC  group by s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 1806 as analysis_id,
# MAGIC  CAST(o.stratum1_id AS STRING) AS stratum1_id,
# MAGIC  CAST(o.stratum2_id AS STRING) AS stratum2_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC join overallStats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
# MAGIC GROUP BY o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1806
# MAGIC  ZORDER BY stratum1_id;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1806
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, stratum1_id as stratum_1, stratum2_id as stratum_2, 
# MAGIC cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_1806
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1806
# MAGIC  ZORDER BY stratum_1;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1rawData_1806;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1rawData_1806;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_1806;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_1806;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1807
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1807 AS analysis_id,
# MAGIC  CAST(m.measurement_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(m.unit_concept_id AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(m.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.measurement m
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  m.person_id = op.person_id
# MAGIC AND 
# MAGIC  m.measurement_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  m.measurement_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  m.measurement_concept_id,
# MAGIC  m.unit_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1807
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1811
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1811 AS analysis_id,
# MAGIC  CAST(NULL AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(m.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.measurement m
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  m.person_id = op.person_id
# MAGIC AND 
# MAGIC  m.measurement_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  m.measurement_date <= op.observation_period_end_date 
# MAGIC WHERE 
# MAGIC  m.value_as_number IS NOT NULL
# MAGIC OR 
# MAGIC  m.value_as_concept_id != 0;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1814
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1814 AS analysis_id,
# MAGIC  CAST(NULL AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(m.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.measurement m
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  m.person_id = op.person_id
# MAGIC AND 
# MAGIC  m.measurement_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  m.measurement_date <= op.observation_period_end_date 
# MAGIC WHERE 
# MAGIC  m.value_as_number IS NULL
# MAGIC AND 
# MAGIC  (m.value_as_concept_id IS NULL OR m.value_as_concept_id = 0);
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum1_id) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1statsView_1815
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC m.subject_id AS stratum1_id,
# MAGIC  m.unit_concept_id AS stratum2_id,
# MAGIC  m.count_value,
# MAGIC  COUNT(*) AS total,
# MAGIC  ROW_NUMBER() OVER (PARTITION BY m.subject_id,m.unit_concept_id ORDER BY m.count_value) AS rn
# MAGIC FROM
# MAGIC (
# MAGIC  SELECT 
# MAGIC  m.measurement_concept_id AS subject_id,
# MAGIC  m.unit_concept_id,
# MAGIC  CAST(m.value_as_number AS FLOAT) AS count_value
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.measurement m
# MAGIC  JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC  ON 
# MAGIC  m.person_id = op.person_id
# MAGIC  AND 
# MAGIC  m.measurement_date >= op.observation_period_start_date
# MAGIC  AND 
# MAGIC  m.measurement_date <= op.observation_period_end_date 
# MAGIC  WHERE 
# MAGIC  m.value_as_number IS NOT NULL
# MAGIC  ) m
# MAGIC GROUP BY 
# MAGIC  m.subject_id, 
# MAGIC  m.unit_concept_id, 
# MAGIC  m.count_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1statsView_1815
# MAGIC  ZORDER BY stratum1_id;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum1_id) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1815
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1815 as analysis_id,
# MAGIC  CAST(o.stratum1_id AS STRING) AS stratum1_id,
# MAGIC  CAST(o.stratum2_id AS STRING) AS stratum2_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC (
# MAGIC  select s.stratum1_id, s.stratum2_id, s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from ${results_catalog}.${results_schema}.xpi8onh1statsView_1815 s
# MAGIC  join ${results_catalog}.${results_schema}.xpi8onh1statsView_1815 p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
# MAGIC  group by s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
# MAGIC ) p
# MAGIC join 
# MAGIC (
# MAGIC  SELECT 
# MAGIC  m.subject_id AS stratum1_id,
# MAGIC  m.unit_concept_id AS stratum2_id,
# MAGIC  CAST(AVG(1.0 * m.count_value) AS FLOAT) AS avg_value,
# MAGIC  CAST(STDDEV(m.count_value) AS FLOAT) AS stdev_value,
# MAGIC  MIN(m.count_value) AS min_value,
# MAGIC  MAX(m.count_value) AS max_value,
# MAGIC  COUNT(*) AS total
# MAGIC  FROM 
# MAGIC  (
# MAGIC  SELECT 
# MAGIC  m.measurement_concept_id AS subject_id,
# MAGIC  m.unit_concept_id,
# MAGIC  CAST(m.value_as_number AS FLOAT) AS count_value
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.measurement m
# MAGIC  JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC  ON 
# MAGIC  m.person_id = op.person_id
# MAGIC  AND 
# MAGIC  m.measurement_date >= op.observation_period_start_date
# MAGIC  AND 
# MAGIC  m.measurement_date <= op.observation_period_end_date 
# MAGIC  WHERE 
# MAGIC  m.unit_concept_id IS NOT NULL
# MAGIC  AND 
# MAGIC  m.value_as_number IS NOT NULL
# MAGIC  ) m
# MAGIC  GROUP BY 
# MAGIC  m.subject_id, 
# MAGIC  m.unit_concept_id
# MAGIC ) o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
# MAGIC GROUP BY o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1815
# MAGIC  ZORDER BY stratum1_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1815
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, stratum1_id as stratum_1, stratum2_id as stratum_2, 
# MAGIC cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_1815
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1815
# MAGIC  ZORDER BY stratum_1;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1statsView_1815;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1statsView_1815;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_1815;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_1815;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum1_id) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1overallStats_1816
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC m.subject_id AS stratum1_id,
# MAGIC  m.unit_concept_id AS stratum2_id,
# MAGIC  CAST(AVG(1.0 * m.count_value) AS FLOAT) AS avg_value,
# MAGIC  CAST(STDDEV(m.count_value) AS FLOAT) AS stdev_value,
# MAGIC  MIN(m.count_value) AS min_value,
# MAGIC  MAX(m.count_value) AS max_value,
# MAGIC  COUNT(*) AS total
# MAGIC FROM
# MAGIC (
# MAGIC  SELECT 
# MAGIC  m.measurement_concept_id AS subject_id,
# MAGIC  m.unit_concept_id,
# MAGIC  CAST(m.range_low AS FLOAT) AS count_value
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.measurement m
# MAGIC  JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC  ON 
# MAGIC  m.person_id = op.person_id
# MAGIC  AND 
# MAGIC  m.measurement_date >= op.observation_period_start_date
# MAGIC  AND 
# MAGIC  m.measurement_date <= op.observation_period_end_date 
# MAGIC  WHERE 
# MAGIC  m.unit_concept_id IS NOT NULL
# MAGIC  AND 
# MAGIC  m.value_as_number IS NOT NULL
# MAGIC  AND 
# MAGIC  m.range_low IS NOT NULL
# MAGIC  AND 
# MAGIC  m.range_high IS NOT NULL
# MAGIC  ) m
# MAGIC GROUP BY 
# MAGIC  m.subject_id, 
# MAGIC  m.unit_concept_id
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1overallStats_1816
# MAGIC  ZORDER BY stratum1_id;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum1_id) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1statsView_1816
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC m.subject_id AS stratum1_id,
# MAGIC  m.unit_concept_id AS stratum2_id,
# MAGIC  m.count_value,
# MAGIC  COUNT(*) AS total,
# MAGIC  ROW_NUMBER() OVER (PARTITION BY m.subject_id,m.unit_concept_id ORDER BY m.count_value) AS rn
# MAGIC FROM
# MAGIC (
# MAGIC  SELECT 
# MAGIC  m.measurement_concept_id AS subject_id,
# MAGIC  m.unit_concept_id,
# MAGIC  CAST(m.range_low AS FLOAT) AS count_value
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.measurement m
# MAGIC  JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC  ON 
# MAGIC  m.person_id = op.person_id
# MAGIC  AND 
# MAGIC  m.measurement_date >= op.observation_period_start_date
# MAGIC  AND 
# MAGIC  m.measurement_date <= op.observation_period_end_date 
# MAGIC  WHERE 
# MAGIC  m.unit_concept_id IS NOT NULL
# MAGIC  AND 
# MAGIC  m.value_as_number IS NOT NULL
# MAGIC  AND 
# MAGIC  m.range_low IS NOT NULL
# MAGIC  AND 
# MAGIC  m.range_high IS NOT NULL
# MAGIC  ) m
# MAGIC GROUP BY 
# MAGIC  m.subject_id, 
# MAGIC  m.unit_concept_id, 
# MAGIC  m.count_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1statsView_1816
# MAGIC  ZORDER BY stratum1_id;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum1_id) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1816
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1816 as analysis_id,
# MAGIC  CAST(o.stratum1_id AS STRING) AS stratum1_id,
# MAGIC  CAST(o.stratum2_id AS STRING) AS stratum2_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC (
# MAGIC  select s.stratum1_id, s.stratum2_id, s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from ${results_catalog}.${results_schema}.xpi8onh1statsView_1816 s
# MAGIC  join ${results_catalog}.${results_schema}.xpi8onh1statsView_1816 p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
# MAGIC  group by s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
# MAGIC ) p
# MAGIC join ${results_catalog}.${results_schema}.xpi8onh1overallStats_1816 o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
# MAGIC GROUP BY o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1816
# MAGIC  ZORDER BY stratum1_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1816
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, stratum1_id as stratum_1, stratum2_id as stratum_2, 
# MAGIC  cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC  count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_1816
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1816
# MAGIC  ZORDER BY stratum_1;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1overallStats_1816;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1overallStats_1816;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1statsView_1816;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1statsView_1816;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_1816;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_1816;
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum1_id) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1overallStats_1817
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC m.subject_id AS stratum1_id,
# MAGIC  m.unit_concept_id AS stratum2_id,
# MAGIC  CAST(AVG(1.0 * m.count_value) AS FLOAT) AS avg_value,
# MAGIC  CAST(STDDEV(m.count_value) AS FLOAT) AS stdev_value,
# MAGIC  MIN(m.count_value) AS min_value,
# MAGIC  MAX(m.count_value) AS max_value,
# MAGIC  COUNT(*) AS total
# MAGIC FROM
# MAGIC (
# MAGIC  SELECT 
# MAGIC  measurement_concept_id AS subject_id,
# MAGIC  unit_concept_id,
# MAGIC  CAST(range_high AS FLOAT) AS count_value
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.measurement m
# MAGIC  JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC  ON 
# MAGIC  m.person_id = op.person_id
# MAGIC  AND 
# MAGIC  m.measurement_date >= op.observation_period_start_date
# MAGIC  AND 
# MAGIC  m.measurement_date <= op.observation_period_end_date 
# MAGIC  WHERE 
# MAGIC  m.unit_concept_id IS NOT NULL
# MAGIC  AND 
# MAGIC  m.value_as_number IS NOT NULL
# MAGIC  AND 
# MAGIC  m.range_low IS NOT NULL
# MAGIC  AND 
# MAGIC  m.range_high IS NOT NULL
# MAGIC  ) m
# MAGIC GROUP BY 
# MAGIC  m.subject_id, 
# MAGIC  m.unit_concept_id
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1overallStats_1817
# MAGIC  ZORDER BY stratum1_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum1_id) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1statsView_1817
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC m.subject_id AS stratum1_id,
# MAGIC  m.unit_concept_id AS stratum2_id,
# MAGIC  m.count_value,
# MAGIC  COUNT(*) AS total,
# MAGIC  ROW_NUMBER() OVER (PARTITION BY m.subject_id,m.unit_concept_id ORDER BY m.count_value) AS rn
# MAGIC FROM
# MAGIC (
# MAGIC  SELECT 
# MAGIC  m.measurement_concept_id AS subject_id,
# MAGIC  m.unit_concept_id,
# MAGIC  CAST(m.range_high AS FLOAT) AS count_value
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.measurement m
# MAGIC  JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC  ON 
# MAGIC  m.person_id = op.person_id
# MAGIC  AND 
# MAGIC  m.measurement_date >= op.observation_period_start_date
# MAGIC  AND 
# MAGIC  m.measurement_date <= op.observation_period_end_date 
# MAGIC  WHERE 
# MAGIC  m.unit_concept_id IS NOT NULL
# MAGIC  AND 
# MAGIC  m.value_as_number IS NOT NULL
# MAGIC  AND 
# MAGIC  m.range_low IS NOT NULL
# MAGIC  AND 
# MAGIC  m.range_high IS NOT NULL
# MAGIC  ) m
# MAGIC GROUP BY 
# MAGIC  m.subject_id, 
# MAGIC  m.unit_concept_id, 
# MAGIC  m.count_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1statsView_1817
# MAGIC  ZORDER BY stratum1_id;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum1_id) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1817
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1817 as analysis_id,
# MAGIC  CAST(o.stratum1_id AS STRING) AS stratum1_id,
# MAGIC  CAST(o.stratum2_id AS STRING) AS stratum2_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC (
# MAGIC  select s.stratum1_id, s.stratum2_id, s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from ${results_catalog}.${results_schema}.xpi8onh1statsView_1817 s
# MAGIC  join ${results_catalog}.${results_schema}.xpi8onh1statsView_1817 p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
# MAGIC  group by s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
# MAGIC ) p
# MAGIC join ${results_catalog}.${results_schema}.xpi8onh1overallStats_1817 o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id
# MAGIC GROUP BY o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_1817
# MAGIC  ZORDER BY stratum1_id;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1817
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, stratum1_id as stratum_1, stratum2_id as stratum_2, 
# MAGIC  cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC  count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_1817
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1817
# MAGIC  ZORDER BY stratum_1;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1overallStats_1817;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1overallStats_1817;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1statsView_1817;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1statsView_1817;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_1817;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_1817;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(person_id) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1rawData_1818
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC m.person_id,
# MAGIC  m.measurement_concept_id,
# MAGIC  m.unit_concept_id,
# MAGIC  CAST(CASE 
# MAGIC  WHEN m.value_as_number < m.range_low
# MAGIC  THEN 'Below Range Low'
# MAGIC  WHEN m.value_as_number >= m.range_low AND m.value_as_number <= m.range_high
# MAGIC  THEN 'Within Range'
# MAGIC  WHEN m.value_as_number > m.range_high
# MAGIC  THEN 'Above Range High'
# MAGIC  ELSE 'Other'
# MAGIC  END AS STRING) AS stratum_3
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.measurement m
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  m.person_id = op.person_id
# MAGIC AND 
# MAGIC  m.measurement_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  m.measurement_date <= op.observation_period_end_date 
# MAGIC WHERE 
# MAGIC  m.value_as_number IS NOT NULL
# MAGIC AND 
# MAGIC  m.unit_concept_id IS NOT NULL
# MAGIC AND 
# MAGIC  m.range_low IS NOT NULL
# MAGIC AND 
# MAGIC  m.range_high IS NOT NULL;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1rawData_1818
# MAGIC  ZORDER BY person_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1818
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1818 AS analysis_id,
# MAGIC  CAST(measurement_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(unit_concept_id AS STRING) AS stratum_2,
# MAGIC  CAST(stratum_3 AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(person_id) AS count_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1rawData_1818
# MAGIC GROUP BY 
# MAGIC  measurement_concept_id,
# MAGIC  unit_concept_id,
# MAGIC  stratum_3;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1818
# MAGIC  ZORDER BY stratum_1;
# MAGIC TRUNCATE TABLE ${results_catalog}.${results_schema}.xpi8onh1rawData_1818;
# MAGIC DROP TABLE ${results_catalog}.${results_schema}.xpi8onh1rawData_1818;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1819
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1819 AS analysis_id,
# MAGIC  CAST(m.measurement_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(m.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.measurement m
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  m.person_id = op.person_id
# MAGIC AND 
# MAGIC  m.measurement_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  m.measurement_date <= op.observation_period_end_date 
# MAGIC WHERE 
# MAGIC  m.value_as_number IS NOT NULL
# MAGIC OR 
# MAGIC  m.value_as_concept_id != 0
# MAGIC GROUP BY 
# MAGIC  m.measurement_concept_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1820
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  YEAR(m.measurement_date) * 100 + MONTH(m.measurement_date) AS stratum_1,
# MAGIC  COUNT(m.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.measurement m
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  m.person_id = op.person_id
# MAGIC AND 
# MAGIC  m.measurement_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  m.measurement_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  YEAR(m.measurement_date) * 100 + MONTH(m.measurement_date)
# MAGIC )
# MAGIC  SELECT
# MAGIC 1820 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1820
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1821
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1821 as analysis_id, 
# MAGIC  cast(null as STRING) as stratum_1, cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC  COUNT(m.PERSON_ID) as count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.measurement m
# MAGIC where m.value_as_number is null;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1822
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1822 AS analysis_id,
# MAGIC  CAST(m.measurement_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(m.value_as_concept_id AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(*) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.measurement m
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  m.person_id = op.person_id
# MAGIC AND 
# MAGIC  m.measurement_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  m.measurement_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  m.measurement_concept_id,
# MAGIC  m.value_as_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1822
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1823
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1823 AS analysis_id,
# MAGIC  CAST(m.measurement_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(m.operator_concept_id AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(*) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.measurement m
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  m.person_id = op.person_id
# MAGIC AND 
# MAGIC  m.measurement_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  m.measurement_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  m.measurement_concept_id,
# MAGIC  m.operator_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1823
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1825
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1825 AS analysis_id,
# MAGIC  CAST(m.measurement_source_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(*) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.measurement m
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  m.person_id = op.person_id
# MAGIC AND 
# MAGIC  m.measurement_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  m.measurement_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  m.measurement_source_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1825
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1826
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1826 AS analysis_id,
# MAGIC  CAST(m.value_as_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(*) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.measurement m
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  m.person_id = op.person_id
# MAGIC AND 
# MAGIC  m.measurement_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  m.measurement_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  m.value_as_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1826
# MAGIC  ZORDER BY stratum_1;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1827
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1827 AS analysis_id,
# MAGIC  CAST(m.unit_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(*) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.measurement m
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  m.person_id = op.person_id
# MAGIC AND 
# MAGIC  m.measurement_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  m.measurement_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  m.unit_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1827
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1891
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1891 AS analysis_id,
# MAGIC  CAST(m.measurement_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(m.meas_cnt AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  SUM(COUNT(m.person_id)) OVER (PARTITION BY m.measurement_concept_id ORDER BY m.meas_cnt DESC) AS count_value
# MAGIC FROM
# MAGIC (
# MAGIC  SELECT 
# MAGIC  m.measurement_concept_id,
# MAGIC  COUNT(m.measurement_id) AS meas_cnt,
# MAGIC  m.person_id
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.measurement m
# MAGIC  JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC  ON 
# MAGIC  m.person_id = op.person_id
# MAGIC  AND 
# MAGIC  m.measurement_date >= op.observation_period_start_date
# MAGIC  AND 
# MAGIC  m.measurement_date <= op.observation_period_end_date 
# MAGIC  GROUP BY 
# MAGIC  m.person_id,
# MAGIC  m.measurement_concept_id
# MAGIC  ) m
# MAGIC GROUP BY 
# MAGIC  m.measurement_concept_id,
# MAGIC  m.meas_cnt;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1891
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1900
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 1900 as analysis_id, 
# MAGIC  cast(table_name as STRING) as stratum_1, 
# MAGIC  cast(column_name as STRING) as stratum_2, 
# MAGIC  source_value as stratum_3, 
# MAGIC  cast(null as STRING) as stratum_4, 
# MAGIC  cast(null as STRING) as stratum_5,
# MAGIC cnt as count_value
# MAGIC FROM
# MAGIC (
# MAGIC  select 'measurement' as table_name, 'measurement_source_value' as column_name, measurement_source_value as source_value, COUNT(*) as cnt from ${omop_catalog}.${omop_schema}.measurement where measurement_concept_id = 0 group by measurement_source_value 
# MAGIC  union
# MAGIC  select 'measurement' as table_name, 'unit_source_value' as column_name, unit_source_value as source_value, COUNT(*) as cnt from ${omop_catalog}.${omop_schema}.measurement where unit_concept_id = 0 group by unit_source_value 
# MAGIC  union
# MAGIC  select 'procedure_occurrence' as table_name,'procedure_source_value' as column_name, procedure_source_value as source_value, COUNT(*) as cnt from ${omop_catalog}.${omop_schema}.procedure_occurrence where procedure_concept_id = 0 group by procedure_source_value 
# MAGIC  union
# MAGIC  select 'procedure_occurrence' as table_name,'modifier_source_value' as column_name, modifier_source_value as source_value, COUNT(*) as cnt from ${omop_catalog}.${omop_schema}.procedure_occurrence where modifier_concept_id = 0 group by modifier_source_value 
# MAGIC  union
# MAGIC  select 'drug_exposure' as table_name, 'drug_source_value' as column_name, drug_source_value as source_value, COUNT(*) as cnt from ${omop_catalog}.${omop_schema}.drug_exposure where drug_concept_id = 0 group by drug_source_value 
# MAGIC  union
# MAGIC  select 'drug_exposure' as table_name, 'route_source_value' as column_name, route_source_value as source_value, COUNT(*) as cnt from ${omop_catalog}.${omop_schema}.drug_exposure where route_concept_id = 0 group by route_source_value 
# MAGIC  union
# MAGIC  select 'condition_occurrence' as table_name, 'condition_source_value' as column_name, condition_source_value as source_value, COUNT(*) as cnt from ${omop_catalog}.${omop_schema}.condition_occurrence where condition_concept_id = 0 group by condition_source_value 
# MAGIC  union
# MAGIC  select 'condition_occurrence' as table_name, 'condition_status_source_value' as column_name, condition_status_source_value as source_value, COUNT(*) as cnt from ${omop_catalog}.${omop_schema}.condition_occurrence where condition_status_concept_id = 0 group by condition_status_source_value 
# MAGIC  union
# MAGIC  select 'observation' as table_name, 'observation_source_value' as column_name, observation_source_value as source_value, COUNT(*) as cnt from ${omop_catalog}.${omop_schema}.observation where observation_concept_id = 0 group by observation_source_value 
# MAGIC  union
# MAGIC  select 'observation' as table_name, 'unit_source_value' as column_name, unit_source_value as source_value, COUNT(*) as cnt from ${omop_catalog}.${omop_schema}.observation where unit_concept_id = 0 group by unit_source_value 
# MAGIC  union
# MAGIC  select 'observation' as table_name, 'qualifier_source_value' as column_name, qualifier_source_value as source_value, COUNT(*) as cnt from ${omop_catalog}.${omop_schema}.observation where qualifier_concept_id = 0 group by qualifier_source_value
# MAGIC  union
# MAGIC  select 'payer_plan_period' as table_name, 'payer_source_value' as column_name, payer_source_value as source_value, COUNT(*) as cnt from ${omop_catalog}.${omop_schema}.payer_plan_period where payer_concept_id = 0 group by payer_source_value 
# MAGIC  union
# MAGIC  select 'payer_plan_period' as table_name, 'plan_source_value' as column_name, plan_source_value as source_value, COUNT(*) as cnt from ${omop_catalog}.${omop_schema}.payer_plan_period where plan_concept_id = 0 group by plan_source_value 
# MAGIC  union
# MAGIC  select 'payer_plan_period' as table_name, 'sponsor_source_value' as column_name, sponsor_source_value as source_value, COUNT(*) as cnt from ${omop_catalog}.${omop_schema}.payer_plan_period where sponsor_concept_id = 0 group by sponsor_source_value 
# MAGIC  union
# MAGIC  select 'payer_plan_period' as table_name, 'stop_reason_source_value' as column_name, stop_reason_source_value as source_value, COUNT(*) as cnt from ${omop_catalog}.${omop_schema}.payer_plan_period where stop_reason_concept_id = 0 group by stop_reason_source_value 
# MAGIC  union
# MAGIC  select 'provider' as table_name, 'specialty_source_value' as column_name, specialty_source_value as source_value, COUNT(*) as cnt from ${omop_catalog}.${omop_schema}.provider where specialty_concept_id = 0 group by specialty_source_value
# MAGIC  union 
# MAGIC  select 'provider' as table_name, 'gender_source_value' as column_name, gender_source_value as source_value, COUNT(*) as cnt from ${omop_catalog}.${omop_schema}.provider where gender_concept_id = 0 group by gender_source_value
# MAGIC  union 
# MAGIC  select 'person' as table_name, 'gender_source_value' as column_name, gender_source_value as source_value, COUNT(*) as cnt from ${omop_catalog}.${omop_schema}.person where gender_concept_id = 0 group by gender_source_value 
# MAGIC  union
# MAGIC  select 'person' as table_name, 'race_source_value' as column_name, race_source_value as source_value, COUNT(*) as cnt from ${omop_catalog}.${omop_schema}.person where race_concept_id = 0 group by race_source_value 
# MAGIC  union
# MAGIC  select 'person' as table_name, 'ethnicity_source_value' as column_name, ethnicity_source_value as source_value, COUNT(*) as cnt from ${omop_catalog}.${omop_schema}.person where ethnicity_concept_id = 0 group by ethnicity_source_value 
# MAGIC  union
# MAGIC  select 'specimen' as table_name, 'specimen_source_value' as column_name, specimen_source_value as source_value, COUNT(*) as cnt from ${omop_catalog}.${omop_schema}.specimen where specimen_concept_id = 0 group by specimen_source_value 
# MAGIC  union
# MAGIC  select 'specimen' as table_name, 'unit_source_value' as column_name, unit_source_value as source_value, COUNT(*) as cnt from ${omop_catalog}.${omop_schema}.specimen where unit_concept_id = 0 group by unit_source_value 
# MAGIC  union
# MAGIC  select 'specimen' as table_name, 'anatomic_site_source_value' as column_name, anatomic_site_source_value as source_value, COUNT(*) as cnt from ${omop_catalog}.${omop_schema}.specimen where anatomic_site_concept_id = 0 group by anatomic_site_source_value 
# MAGIC  union
# MAGIC  select 'specimen' as table_name, 'disease_status_source_value' as column_name, disease_status_source_value as source_value, COUNT(*) as cnt from ${omop_catalog}.${omop_schema}.specimen where disease_status_concept_id = 0 group by disease_status_source_value 
# MAGIC  union
# MAGIC  select 'visit_occurrence' as table_name, 'visit_source_value' as column_name, visit_source_value as source_value, COUNT(*) as cnt from ${omop_catalog}.${omop_schema}.visit_occurrence where visit_concept_id = 0 group by visit_source_value
# MAGIC  union
# MAGIC  select 'visit_occurrence' as table_name, 'admitted_from_source_value' as column_name, admitted_from_source_value as source_value, COUNT(*) as cnt from ${omop_catalog}.${omop_schema}.visit_occurrence where admitted_from_concept_id = 0 group by admitted_from_source_value
# MAGIC  union
# MAGIC  select 'visit_occurrence' as table_name, 'discharged_to_source_value' as column_name, discharged_to_source_value as source_value, COUNT(*) as cnt from ${omop_catalog}.${omop_schema}.visit_occurrence where discharged_to_concept_id = 0 group by discharged_to_source_value
# MAGIC  union
# MAGIC  select 'device_exposure' as table_name, 'device_source_value' as column_name, device_source_value as source_value, COUNT(*) as cnt from ${omop_catalog}.${omop_schema}.device_exposure where device_concept_id = 0 group by device_source_value
# MAGIC  union
# MAGIC  select 'death' as table_name, 'cause_source_value' as column_name, cause_source_value as source_value, COUNT(*) as cnt from ${omop_catalog}.${omop_schema}.death where cause_concept_id = 0 group by cause_source_value
# MAGIC ) a
# MAGIC where cnt >= 1;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1900
# MAGIC   ZORDER BY stratum_1;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2000
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 2000 AS analysis_id,
# MAGIC  CAST(NULL AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  CAST(d.cnt AS BIGINT) AS count_value
# MAGIC FROM
# MAGIC (
# MAGIC SELECT COUNT(*) cnt
# MAGIC FROM (
# MAGIC  SELECT DISTINCT person_id
# MAGIC  FROM (
# MAGIC  SELECT
# MAGIC  co.person_id
# MAGIC  FROM
# MAGIC  ${omop_catalog}.${omop_schema}.condition_occurrence co
# MAGIC  JOIN
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op
# MAGIC  ON
# MAGIC  co.person_id = op.person_id
# MAGIC  AND
# MAGIC  co.condition_start_date >= op.observation_period_start_date
# MAGIC  AND
# MAGIC  co.condition_start_date <= op.observation_period_end_date
# MAGIC  ) a
# MAGIC  INTERSECT
# MAGIC  SELECT DISTINCT person_id
# MAGIC  FROM (
# MAGIC  SELECT
# MAGIC  de.person_id
# MAGIC  FROM
# MAGIC  ${omop_catalog}.${omop_schema}.drug_exposure de
# MAGIC  JOIN
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op
# MAGIC  ON
# MAGIC  de.person_id = op.person_id
# MAGIC  AND
# MAGIC  de.drug_exposure_start_date >= op.observation_period_start_date
# MAGIC  AND
# MAGIC  de.drug_exposure_start_date <= op.observation_period_end_date
# MAGIC  ) b
# MAGIC  ) c
# MAGIC ) d;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2001
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 2001 AS analysis_id,
# MAGIC  CAST(NULL AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  CAST(d.cnt AS BIGINT) AS count_value
# MAGIC FROM
# MAGIC (
# MAGIC SELECT COUNT(*) cnt
# MAGIC FROM (
# MAGIC  SELECT DISTINCT person_id
# MAGIC  FROM (
# MAGIC  SELECT
# MAGIC  co.person_id
# MAGIC  FROM
# MAGIC  ${omop_catalog}.${omop_schema}.condition_occurrence co
# MAGIC  JOIN
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op
# MAGIC  ON
# MAGIC  co.person_id = op.person_id
# MAGIC  AND
# MAGIC  co.condition_start_date >= op.observation_period_start_date
# MAGIC  AND
# MAGIC  co.condition_start_date <= op.observation_period_end_date
# MAGIC  ) a
# MAGIC  INTERSECT
# MAGIC  SELECT DISTINCT person_id
# MAGIC  FROM (
# MAGIC  SELECT
# MAGIC  po.person_id
# MAGIC  FROM
# MAGIC  ${omop_catalog}.${omop_schema}.procedure_occurrence po
# MAGIC  JOIN
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op
# MAGIC  ON
# MAGIC  po.person_id = op.person_id
# MAGIC  AND
# MAGIC  po.procedure_date >= op.observation_period_start_date
# MAGIC  AND
# MAGIC  po.procedure_date <= op.observation_period_end_date
# MAGIC  ) b
# MAGIC  ) c
# MAGIC ) d;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2002
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 2002 AS analysis_id,
# MAGIC  CAST(NULL AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  CAST(e.cnt AS BIGINT) AS count_value
# MAGIC FROM
# MAGIC (
# MAGIC SELECT COUNT(*) cnt
# MAGIC FROM (
# MAGIC  SELECT DISTINCT person_id
# MAGIC  FROM (
# MAGIC  SELECT
# MAGIC  m.person_id
# MAGIC  FROM
# MAGIC  ${omop_catalog}.${omop_schema}.measurement m
# MAGIC  JOIN
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op
# MAGIC  ON
# MAGIC  m.person_id = op.person_id
# MAGIC  AND
# MAGIC  m.measurement_date >= op.observation_period_start_date
# MAGIC  AND
# MAGIC  m.measurement_date <= op.observation_period_end_date
# MAGIC  ) a
# MAGIC  INTERSECT
# MAGIC  SELECT DISTINCT person_id
# MAGIC  FROM (
# MAGIC  SELECT
# MAGIC  co.person_id
# MAGIC  FROM
# MAGIC  ${omop_catalog}.${omop_schema}.condition_occurrence co
# MAGIC  JOIN
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op
# MAGIC  ON
# MAGIC  co.person_id = op.person_id
# MAGIC  AND
# MAGIC  co.condition_start_date >= op.observation_period_start_date
# MAGIC  AND
# MAGIC  co.condition_start_date <= op.observation_period_end_date
# MAGIC  ) b
# MAGIC  INTERSECT
# MAGIC  SELECT DISTINCT person_id
# MAGIC  FROM (
# MAGIC  SELECT
# MAGIC  de.person_id
# MAGIC  FROM
# MAGIC  ${omop_catalog}.${omop_schema}.drug_exposure de
# MAGIC  JOIN
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op
# MAGIC  ON
# MAGIC  de.person_id = op.person_id
# MAGIC  AND
# MAGIC  de.drug_exposure_start_date >= op.observation_period_start_date
# MAGIC  AND
# MAGIC  de.drug_exposure_start_date <= op.observation_period_end_date
# MAGIC  ) c
# MAGIC  ) d
# MAGIC ) e;
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2003
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 2003 AS analysis_id,
# MAGIC  CAST(NULL AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(DISTINCT vo.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.visit_occurrence vo
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  vo.person_id = op.person_id
# MAGIC AND 
# MAGIC  vo.visit_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  vo.visit_start_date <= op.observation_period_end_date;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1conoc 
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC distinct person_id 
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.condition_occurrence;
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1drexp 
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC distinct person_id 
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.drug_exposure;
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1dvexp 
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC distinct person_id 
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.device_exposure;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1msmt 
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC distinct person_id 
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.measurement;
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1death 
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC distinct person_id 
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.death;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1prococ 
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC distinct person_id 
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.procedure_occurrence;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1obs 
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC distinct person_id 
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.observation;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC DROP VIEW IF EXISTS rawData ; 
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC CREATE TEMPORARY VIEW rawData  AS (select 2004 as analysis_id,
# MAGIC  CAST('0000001' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0000010' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0000011' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0000100' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1death) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0000101' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0000110' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0000111' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0001000' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0001001' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0001010' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0001011' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0001100' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0001101' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0001110' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0001111' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0010000' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0010001' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0010010' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0010011' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0010100' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0010101' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0010110' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0010111' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0011000' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0011001' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0011010' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0011011' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0011100' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0011101' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0011110' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0011111' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0100000' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0100001' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0100010' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0100011' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0100100' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0100101' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0100110' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0100111' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0101000' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0101001' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0101010' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0101011' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0101100' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0101101' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0101110' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0101111' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0110000' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0110001' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0110010' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0110011' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0110100' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0110101' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0110110' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0110111' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0111000' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0111001' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0111010' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0111011' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0111100' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0111101' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0111110' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('0111111' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1000000' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1000001' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1000010' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1000011' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1000100' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1000101' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1000110' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1000111' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1001000' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1001001' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1001010' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1001011' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1001100' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1001101' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1001110' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1001111' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1010000' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1010001' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1010010' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1010011' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1010100' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1010101' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1010110' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1010111' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1011000' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1011001' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1011010' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1011011' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1011100' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1011101' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1011110' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1011111' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1100000' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1100001' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1100010' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1100011' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1100100' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1100101' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1100110' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1100111' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1101000' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1101001' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1101010' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1101011' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1101100' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1101101' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1101110' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1101111' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1110000' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1110001' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1110010' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1110011' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1110100' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1110101' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1110110' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1110111' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1111000' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1111001' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1111010' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1111011' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1111100' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1111101' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1111110' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) totalPersonsDb UNION ALL
# MAGIC select 2004 as analysis_id,
# MAGIC  CAST('1111111' AS STRING) as stratum_1,
# MAGIC  cast((1.0 * personIntersection.count_value / totalPersonsDb.totalPersons) as STRING) as stratum_2,
# MAGIC  CAST(NULL AS STRING) as stratum_3,
# MAGIC  CAST(NULL AS STRING) as stratum_4,
# MAGIC  CAST(NULL AS STRING) as stratum_5,
# MAGIC  personIntersection.count_value
# MAGIC  from
# MAGIC  (select count(*) as count_value from(select person_id from ${results_catalog}.${results_schema}.xpi8onh1conoc intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1drexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1dvexp intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1msmt intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1death intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1prococ intersect select person_id from ${results_catalog}.${results_schema}.xpi8onh1obs) subquery) personIntersection,
# MAGIC  (select count(distinct(person_id)) as totalPersons from ${omop_catalog}.${omop_schema}.person) as totalPersonsDb);
# MAGIC
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2004 
# MAGIC USING DELTA
# MAGIC AS
# MAGIC (SELECT
# MAGIC * 
# MAGIC FROM
# MAGIC rawData);

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1conoc;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1drexp;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1dvexp;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1msmt;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1death;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1prococ;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1obs;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2100
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 2100 AS analysis_id,
# MAGIC  CAST(de.device_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(DISTINCT de.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.device_exposure de
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  de.person_id = op.person_id
# MAGIC AND 
# MAGIC  de.device_exposure_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  de.device_exposure_start_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  de.device_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2100
# MAGIC  ZORDER BY stratum_1;
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2101
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 2101 AS analysis_id,
# MAGIC  CAST(de.device_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(de.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.device_exposure de
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  de.person_id = op.person_id
# MAGIC AND 
# MAGIC  de.device_exposure_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  de.device_exposure_start_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  de.device_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2101
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2102
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  de.device_concept_id AS stratum_1,
# MAGIC  YEAR(de.device_exposure_start_date) * 100 + MONTH(de.device_exposure_start_date) AS stratum_2,
# MAGIC  COUNT(DISTINCT de.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.device_exposure de
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  de.person_id = op.person_id
# MAGIC AND 
# MAGIC  de.device_exposure_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  de.device_exposure_start_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  de.device_concept_id,
# MAGIC  YEAR(de.device_exposure_start_date) * 100 + MONTH(de.device_exposure_start_date)
# MAGIC )
# MAGIC  SELECT
# MAGIC 2102 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(stratum_2 AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2102
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2104
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  de.device_concept_id AS stratum_1,
# MAGIC  YEAR(de.device_exposure_start_date) AS stratum_2,
# MAGIC  p.gender_concept_id AS stratum_3,
# MAGIC  FLOOR((YEAR(de.device_exposure_start_date) - p.year_of_birth) / 10) AS stratum_4,
# MAGIC  COUNT(DISTINCT p.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.person p
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.device_exposure de 
# MAGIC ON 
# MAGIC  p.person_id = de.person_id
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  de.person_id = op.person_id
# MAGIC AND 
# MAGIC  de.device_exposure_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  de.device_exposure_start_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  de.device_concept_id,
# MAGIC  YEAR(de.device_exposure_start_date),
# MAGIC  p.gender_concept_id,
# MAGIC  FLOOR((YEAR(de.device_exposure_start_date) - p.year_of_birth) / 10)
# MAGIC )
# MAGIC  SELECT
# MAGIC 2104 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(stratum_2 AS STRING) AS stratum_2,
# MAGIC  CAST(stratum_3 AS STRING) AS stratum_3,
# MAGIC  CAST(stratum_4 AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2104
# MAGIC  ZORDER BY stratum_1;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2105
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 2105 AS analysis_id,
# MAGIC  CAST(de.device_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(de.device_type_concept_id AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(de.person_id) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.device_exposure de
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  de.person_id = op.person_id
# MAGIC AND 
# MAGIC  de.device_exposure_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  de.device_exposure_start_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  de.device_concept_id,
# MAGIC  de.device_type_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2105
# MAGIC  ZORDER BY stratum_1;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(subject_id) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1rawData_2106
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC o.device_concept_id AS subject_id,
# MAGIC  p.gender_concept_id,
# MAGIC  o.device_exposure_start_year - p.year_of_birth AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.person p
# MAGIC JOIN (
# MAGIC  SELECT 
# MAGIC  d.person_id,
# MAGIC  d.device_concept_id,
# MAGIC  MIN(YEAR(d.device_exposure_start_date)) AS device_exposure_start_year
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.device_exposure d
# MAGIC  JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC  ON 
# MAGIC  d.person_id = op.person_id
# MAGIC  AND 
# MAGIC  d.device_exposure_start_date >= op.observation_period_start_date
# MAGIC  AND 
# MAGIC  d.device_exposure_start_date <= op.observation_period_end_date 
# MAGIC  GROUP BY 
# MAGIC  d.person_id,
# MAGIC  d.device_concept_id
# MAGIC  ) o
# MAGIC ON 
# MAGIC  p.person_id = o.person_id
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1rawData_2106
# MAGIC  ZORDER BY subject_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum1_id)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1tempResults_2106
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH overallStats (stratum1_id, stratum2_id, avg_value, stdev_value, min_value, max_value, total)  AS (
# MAGIC  select subject_id as stratum1_id,
# MAGIC  gender_concept_id as stratum2_id,
# MAGIC  CAST(avg(1.0 * count_value) AS FLOAT) as avg_value,
# MAGIC  CAST(STDDEV(count_value) AS FLOAT) as stdev_value,
# MAGIC  min(count_value) as min_value,
# MAGIC  max(count_value) as max_value,
# MAGIC  COUNT(*) as total
# MAGIC  FROM ${results_catalog}.${results_schema}.xpi8onh1rawData_2106
# MAGIC  group by subject_id, gender_concept_id
# MAGIC ),
# MAGIC statsView (stratum1_id, stratum2_id, count_value, total, rn) as
# MAGIC (
# MAGIC  select subject_id as stratum1_id, gender_concept_id as stratum2_id, count_value, COUNT(*) as total, row_number() over (partition by subject_id, gender_concept_id order by count_value) as rn
# MAGIC  FROM ${results_catalog}.${results_schema}.xpi8onh1rawData_2106
# MAGIC  group by subject_id, gender_concept_id, count_value
# MAGIC ),
# MAGIC priorStats (stratum1_id, stratum2_id, count_value, total, accumulated) as
# MAGIC (
# MAGIC  select s.stratum1_id, s.stratum2_id, s.count_value, s.total, sum(p.total) as accumulated
# MAGIC  from statsView s
# MAGIC  join statsView p on s.stratum1_id = p.stratum1_id and s.stratum2_id = p.stratum2_id and p.rn <= s.rn
# MAGIC  group by s.stratum1_id, s.stratum2_id, s.count_value, s.total, s.rn
# MAGIC )
# MAGIC  SELECT
# MAGIC 2106 as analysis_id,
# MAGIC  CAST(o.stratum1_id AS STRING) AS stratum1_id,
# MAGIC  CAST(o.stratum2_id AS STRING) AS stratum2_id,
# MAGIC  o.total as count_value,
# MAGIC  o.min_value,
# MAGIC  o.max_value,
# MAGIC  o.avg_value,
# MAGIC  o.stdev_value,
# MAGIC  MIN(case when p.accumulated >= .50 * o.total then count_value else o.max_value end) as median_value,
# MAGIC  MIN(case when p.accumulated >= .10 * o.total then count_value else o.max_value end) as p10_value,
# MAGIC  MIN(case when p.accumulated >= .25 * o.total then count_value else o.max_value end) as p25_value,
# MAGIC  MIN(case when p.accumulated >= .75 * o.total then count_value else o.max_value end) as p75_value,
# MAGIC  MIN(case when p.accumulated >= .90 * o.total then count_value else o.max_value end) as p90_value
# MAGIC FROM
# MAGIC priorStats p
# MAGIC join overallStats o on p.stratum1_id = o.stratum1_id and p.stratum2_id = o.stratum2_id 
# MAGIC GROUP BY o.stratum1_id, o.stratum2_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1tempResults_2106
# MAGIC  ZORDER BY stratum1_id;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_2106
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC analysis_id, stratum1_id as stratum_1, stratum2_id as stratum_2, 
# MAGIC cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC ${results_catalog}.${results_schema}.xpi8onh1tempResults_2106
# MAGIC ;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_2106
# MAGIC  ZORDER BY stratum_1;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1rawData_2106;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1rawData_2106;
# MAGIC truncate table ${results_catalog}.${results_schema}.xpi8onh1tempResults_2106;
# MAGIC drop table ${results_catalog}.${results_schema}.xpi8onh1tempResults_2106;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1)
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2120
# MAGIC USING DELTA
# MAGIC AS
# MAGIC WITH rawData  AS (
# MAGIC SELECT 
# MAGIC  YEAR(de.device_exposure_start_date) * 100 + MONTH(de.device_exposure_start_date) AS stratum_1,
# MAGIC  COUNT(de.person_id) AS count_value
# MAGIC FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.device_exposure de
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  de.person_id = op.person_id
# MAGIC AND 
# MAGIC  de.device_exposure_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  de.device_exposure_start_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  YEAR(de.device_exposure_start_date)*100 + MONTH(de.device_exposure_start_date)
# MAGIC )
# MAGIC  SELECT
# MAGIC 2120 AS analysis_id,
# MAGIC  CAST(stratum_1 AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  count_value
# MAGIC FROM
# MAGIC rawData;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2120
# MAGIC  ZORDER BY stratum_1;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2125
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 2125 AS analysis_id,
# MAGIC  CAST(de.device_source_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(NULL AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  COUNT(*) AS count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.device_exposure de
# MAGIC JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC ON 
# MAGIC  de.person_id = op.person_id
# MAGIC AND 
# MAGIC  de.device_exposure_start_date >= op.observation_period_start_date
# MAGIC AND 
# MAGIC  de.device_exposure_start_date <= op.observation_period_end_date 
# MAGIC GROUP BY 
# MAGIC  de.device_source_concept_id;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2125
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2191
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 2191 AS analysis_id,
# MAGIC  CAST(d.device_concept_id AS STRING) AS stratum_1,
# MAGIC  CAST(d.device_count AS STRING) AS stratum_2,
# MAGIC  CAST(NULL AS STRING) AS stratum_3,
# MAGIC  CAST(NULL AS STRING) AS stratum_4,
# MAGIC  CAST(NULL AS STRING) AS stratum_5,
# MAGIC  SUM(COUNT(d.person_id)) OVER (PARTITION BY d.device_concept_id ORDER BY d.device_count DESC) AS count_value
# MAGIC FROM
# MAGIC (
# MAGIC  SELECT 
# MAGIC  d.device_concept_id,
# MAGIC  COUNT(d.device_exposure_id) AS device_count,
# MAGIC  d.person_id
# MAGIC  FROM 
# MAGIC  ${omop_catalog}.${omop_schema}.device_exposure d
# MAGIC  JOIN 
# MAGIC  ${omop_catalog}.${omop_schema}.observation_period op 
# MAGIC  ON 
# MAGIC  d.person_id = op.person_id
# MAGIC  AND 
# MAGIC  d.device_exposure_start_date >= op.observation_period_start_date
# MAGIC  AND 
# MAGIC  d.device_exposure_start_date <= op.observation_period_end_date 
# MAGIC  GROUP BY 
# MAGIC  d.person_id,
# MAGIC  d.device_concept_id
# MAGIC  ) d
# MAGIC GROUP BY 
# MAGIC  d.device_concept_id,
# MAGIC  d.device_count;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2191
# MAGIC  ZORDER BY stratum_1;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2200
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 2200 as analysis_id, 
# MAGIC  CAST(m.note_type_CONCEPT_ID AS STRING) as stratum_1,
# MAGIC  cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC  COUNT(distinct m.PERSON_ID) as count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.note m
# MAGIC group by m.note_type_CONCEPT_ID;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2200
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(stratum_1) 
# MAGIC CREATE TABLE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2201
# MAGIC USING DELTA
# MAGIC AS
# MAGIC SELECT
# MAGIC 2201 as analysis_id, 
# MAGIC  CAST(m.note_type_CONCEPT_ID AS STRING) as stratum_1,
# MAGIC  cast(null as STRING) as stratum_2, cast(null as STRING) as stratum_3, cast(null as STRING) as stratum_4, cast(null as STRING) as stratum_5,
# MAGIC  COUNT(m.PERSON_ID) as count_value
# MAGIC FROM
# MAGIC ${omop_catalog}.${omop_schema}.note m
# MAGIC group by m.note_type_CONCEPT_ID;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2201
# MAGIC  ZORDER BY stratum_1;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC -- DROP TABLE IF EXISTS ${results_catalog}.${results_schema}.achilles_results;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC --HINT DISTRIBUTE_ON_KEY(analysis_id) 
# MAGIC CREATE TABLE if not exists ${results_catalog}.${results_schema}.achilles_results
# MAGIC (
# MAGIC   analysis_id int,
# MAGIC   stratum_1 string, 
# MAGIC   stratum_2 string, 
# MAGIC   stratum_3 string, 
# MAGIC   stratum_4 string, 
# MAGIC   stratum_5 string, 
# MAGIC   count_value bigint
# MAGIC );
# MAGIC
# MAGIC insert overwrite ${results_catalog}.${results_schema}.achilles_results
# MAGIC SELECT
# MAGIC analysis_id, stratum_1, stratum_2, stratum_3, stratum_4, stratum_5, count_value
# MAGIC FROM
# MAGIC (
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_0 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_3 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_4 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_5 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_10 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_11 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_12 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_101 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_102 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_108 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_109 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_110 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_111 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_112 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_113 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_116 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_117 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_119 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_200 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_201 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_202 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_204 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_207 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_209 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_210 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_212 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_220 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_221 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_225 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_226 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_300 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_301 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_303 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_325 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_400 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_401 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_402 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_404 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_405 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_414 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_415 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_416 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_420 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_425 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_500 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_501 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_502 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_504 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_505 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_525 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_600 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_601 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_602 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_604 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_605 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_620 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_625 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_630 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_691 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_700 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_701 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_702 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_704 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_705 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_720 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_725 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_791 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_800 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_801 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_802 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_804 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_805 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_807 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_814 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_820 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_822 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_823 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_825 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_826 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_827 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_891 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_900 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_901 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_902 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_904 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_920 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1000 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1001 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1002 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1004 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1020 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1100 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1101 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1102 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1103 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1200 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1201 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1202 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1203 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1300 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1301 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1302 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1304 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1312 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1320 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1321 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1325 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1326 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1408 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1409 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1410 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1411 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1412 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1413 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1425 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1800 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1801 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1802 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1804 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1805 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1807 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1811 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1814 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1818 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1819 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1820 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1821 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1822 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1823 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1825 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1826 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1827 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1891 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_1900 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2000 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2001 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2002 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2003 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2004 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2100 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2101 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2102 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2104 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2105 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2120 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2125 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2191 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2200 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_2201
# MAGIC ) Q
# MAGIC  where count_value > 5;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.achilles_results
# MAGIC  ZORDER BY analysis_id;
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC -- DROP TABLE IF EXISTS ${results_catalog}.${results_schema}.achilles_results_dist;
# MAGIC --HINT DISTRIBUTE_ON_KEY(analysis_id) 
# MAGIC CREATE TABLE if not exists ${results_catalog}.${results_schema}.achilles_results_dist
# MAGIC ( 
# MAGIC   analysis_id int, 
# MAGIC   stratum_1 string, 
# MAGIC   stratum_2 string,
# MAGIC   stratum_3 string,
# MAGIC   stratum_4 string,
# MAGIC   stratum_5 string,
# MAGIC   count_value bigint, 
# MAGIC   min_value float, 
# MAGIC   max_value float,
# MAGIC   avg_value float,
# MAGIC   stdev_value float,
# MAGIC   median_value float,
# MAGIC   p10_value float,
# MAGIC   p25_value float,
# MAGIC   p75_value float,
# MAGIC   p90_value float
# MAGIC )
# MAGIC
# MAGIC ;
# MAGIC
# MAGIC insert overwrite ${results_catalog}.${results_schema}.achilles_results_dist
# MAGIC (analysis_id, stratum_1, stratum_2, stratum_3, stratum_4, stratum_5, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value)
# MAGIC SELECT
# MAGIC analysis_id, stratum_1, stratum_2, stratum_3, stratum_4, stratum_5, count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
# MAGIC FROM
# MAGIC (
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_0 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_103 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_104 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_105 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_106 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_107 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_203 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_206 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_213 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_403 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_406 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_506 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_511 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_512 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_513 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_514 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_515 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_603 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_606 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_703 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_706 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_715 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_716 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_717 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_803 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_806 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_815 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_903 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_906 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_907 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1003 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1006 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1007 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1303 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1306 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1313 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1406 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1407 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1803 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1806 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1815 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1816 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_1817 
# MAGIC union all
# MAGIC  select cast(analysis_id as int) as analysis_id, cast(stratum_1 as STRING) as stratum_1, cast(stratum_2 as STRING) as stratum_2, cast(stratum_3 as STRING) as stratum_3, cast(stratum_4 as STRING) as stratum_4, cast(stratum_5 as STRING) as stratum_5, cast(count_value as bigint) as count_value, cast(min_value as float) as min_value, cast(max_value as float) as max_value, cast(avg_value as float) as avg_value, cast(stdev_value as float) as stdev_value, cast(median_value as float) as median_value, cast(p10_value as float) as p10_value, cast(p25_value as float) as p25_value, cast(p75_value as float) as p75_value, cast(p90_value as float) as p90_value from
# MAGIC  ${results_catalog}.${results_schema}.xpi8onh1s_tmpach_dist_2106
# MAGIC ) Q
# MAGIC  where count_value > 5;
# MAGIC OPTIMIZE ${results_catalog}.${results_schema}.achilles_results_dist
# MAGIC  ZORDER BY analysis_id;
# MAGIC
# MAGIC
