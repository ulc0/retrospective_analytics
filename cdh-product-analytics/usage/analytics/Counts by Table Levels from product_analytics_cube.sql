-- Databricks notebook source


-- COMMAND ----------

-- DBTITLE 1,Counts by Datatable
select
  datatable,
  count(distinct email) as user_count,
  count(distinct notebookid) as notebooks,
  count(distinct commandid) as queries,
  min(event_date) as earliest_date,
  max(event_date) as latest_date
FROM
  edav_prd_cdh.cdh_engineering_etl.product_analytics_cube
where :min_date <= event_date <= :max_date
group by
  datatable
order by
  queries desc

-- COMMAND ----------

-- DBTITLE 1,Counts By Schema
select
  table_schema,
  count(distinct email) as user_count,
  count(distinct notebookid) as notebooks,
  count(distinct commandid) as queries,
  min(event_date) as earliest_date,
  max(event_date) as latest_date
FROM
  edav_prd_cdh.cdh_engineering_etl.product_analytics_cube
group by
  table_schema
order by
  queries desc

-- COMMAND ----------

-- DBTITLE 1,Counts by DataSet
select
  ds as data_set,
  count(distinct email) as user_count,
  count(distinct notebookid) as notebooks,
  count(distinct commandid) as queries,
  min(event_date) as earliest_date,
  max(event_date) as latest_date
FROM
  edav_prd_cdh.cdh_engineering_etl.product_analytics_cube
group by
  ds
order by
  queries desc
