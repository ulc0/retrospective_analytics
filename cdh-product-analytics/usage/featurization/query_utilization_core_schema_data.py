# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window, types
from functools import reduce
import itertools as it
from typing import List

import mlflow
import pandas as pd
import json
#import sqlglot as sg

# COMMAND ----------



SILVER_USAGE_TBL="edav_prd_cdh.cdh_engineering_etl.analytics_cluster_silver"
CORE_USAGE_TBL="edav_prd_cdh.cdh_engineering_etl.product_analytics_core_usage"
ANALYTICS_TBL="edav_prd_cdh.cdh_engineering_etl.product_analytics_cube"
COMMANDTEXT_TBL="edav_prd_cdh.cdh_engineering_etl.product_analytics_commandtext"
SCHEMAS_TBL="edav_prd_cdh.cdh_engineering_etl.product_analytics_schemas"
NOTEBOOK_SCHEMAS_TBL="edav_prd_cdh.cdh_engineering_etl.product_analytics_nb_schemas"
COLUMN_LINEAGE_TBL="edav_project_audit_logs.cdh.column_lineage"

EXPERIMENT_NAME="Product_Analytics_Core"
EXPERIMENT_ID="3288147180612904"
SCRATCH='/Volumes/edav_prd_cdh/cdh_ml/metadata_data'
NOTEBOOK_ID=""
NOTEBOOK_URL=f"https://adb-5189663741904016.16.azuredatabricks.net/editor/notebooks/{NOTEBOOK_ID}"
DATASETS=['truveta','premier']
GCOLS=['notebookid', 'clusterid', 'email', 'commandid','commandtext','event_date',]

#REGEX=r"edav_prd_cdh\.cdh_.*"



# COMMAND ----------

# MAGIC %md
# MAGIC **Original Query**
# MAGIC
# MAGIC ```sql
# MAGIC WITH all_tables AS (
# MAGIC   SELECT 
# MAGIC t.table_catalog,
# MAGIC t.table_schema,
# MAGIC t.table_name,
# MAGIC       t.table_type,
# MAGIC       t.table_owner,
# MAGIC       t.last_altered_by,
# MAGIC       t.last_altered
# MAGIC   FROM edav_prd_cdh.information_schema.tables t
# MAGIC   WHERE t.table_schema NOT IN ('information_schema', 'default')
# MAGIC ),
# MAGIC accessible_tables AS (
# MAGIC   SELECT DISTINCT
# MAGIC       p.table_catalog,
# MAGIC       p.table_schema,
# MAGIC       p.table_name
# MAGIC   FROM edav_prd_cdh.information_schema.table_privileges p
# MAGIC   WHERE p.privilege_type = 'SELECT'
# MAGIC   AND p.table_schema NOT IN ('information_schema', 'default')
# MAGIC )
# MAGIC SELECT
# MAGIC     at.table_catalog AS catalog,
# MAGIC     at.table_schema AS schema,
# MAGIC     case
# MAGIC       when lower(at.table_schema) like '%abfm%' then "ABFM"
# MAGIC       when lower(at.table_schema) like '%_aha%' then "AHA"
# MAGIC       when lower(at.table_schema) like '%hcup%' then "HCUP"
# MAGIC       when lower(at.table_schema) like '%iqvia%' then "Iqvia"
# MAGIC       when lower(at.table_schema) like '%_lab%' then "Quest"
# MAGIC       when lower(at.table_schema) like '%marketscan%' then "MarketScan"
# MAGIC       when lower(at.table_schema) like '%nemsis%' then "NEMSIS"
# MAGIC       when lower(at.table_schema) like '%pointclickcare%' then "PointClickCare"
# MAGIC       when lower(at.table_schema) like '%premier%' then "Premier"
# MAGIC       when lower(at.table_schema) like '%truveta%' then "Truveta"
# MAGIC       else "unknown"
# MAGIC       end as data_asset_provider,
# MAGIC     case 
# MAGIC       when lower(at.table_schema) like '%_exploratory' then "exploratory"
# MAGIC       when lower(at.table_schema) like '%_stage' then "stage"
# MAGIC       else null
# MAGIC       end as table_schema_type,
# MAGIC     at.table_name,
# MAGIC     at.table_type,
# MAGIC     at.table_owner AS owner,
# MAGIC     at.last_altered_by,
# MAGIC     at.last_altered AS last_altered_date,
# MAGIC     CASE 
# MAGIC         WHEN ac.table_name IS NOT NULL THEN 'Yes'
# MAGIC         ELSE 'No'
# MAGIC     END AS has_select_permission
# MAGIC FROM all_tables at
# MAGIC LEFT JOIN accessible_tables ac
# MAGIC     ON at.table_catalog = ac.table_catalog
# MAGIC     AND at.table_schema = ac.table_schema
# MAGIC     AND at.table_name = ac.table_name
# MAGIC ORDER BY catalog, schema, table_name;
# MAGIC ````

# COMMAND ----------

# MAGIC %md
# MAGIC | ABFM American Family Cohort | EHR                |
# MAGIC |-----------------------------|--------------------|
# MAGIC | AHA                         | Survey             |
# MAGIC | CMS                         | Claims             |
# MAGIC | HCUP                        | Chargemaster       |
# MAGIC | IQVIA Pharmetrics           | Claims             |
# MAGIC | IQVIA ARA                   | EHR + Claims       |
# MAGIC | IQVIA SMART                 | Pharmacy           |
# MAGIC | MarketScan                  | Claims             |
# MAGIC | NEMSIS                      | Emergency Services |
# MAGIC | PCORnet                     | EHR                |
# MAGIC | PointClickCare              | EHR                |
# MAGIC | Premier                     | Chargemaster       |
# MAGIC | Quest                       | Commercial Lab     |
# MAGIC | Truveta                     | EHR                |
# MAGIC | Truveta Studio              | EHR                |

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT t.table_catalog,
# MAGIC t.table_schema,
# MAGIC t.table_name,
# MAGIC t.table_type,
# MAGIC case
# MAGIC when lower(t.table_schema) like '%abfm%' then "ABFM"
# MAGIC when lower(t.table_schema) like '%_aha%' then "AHA"
# MAGIC when lower(t.table_schema) like '%hcup%' then "HCUP"
# MAGIC when lower(t.table_schema) like '%iqvia%' then "Iqvia"
# MAGIC when lower(t.table_schema) like '%_lab%' then "Quest"
# MAGIC when lower(t.table_schema) like '%marketscan%' then "MarketScan"
# MAGIC when lower(t.table_schema) like '%nemsis%' then "NEMSIS"
# MAGIC when lower(t.table_schema) like '%pointclickcare%' then "PointClickCare"
# MAGIC when lower(t.table_schema) like '%premier%' then "Premier"
# MAGIC when lower(t.table_schema) like '%truveta%' then "Truveta"
# MAGIC when lower(t.table_schema) like '%lava%' then "LAVA"
# MAGIC else "unknown"
# MAGIC end as data_asset_provider,
# MAGIC case 
# MAGIC when lower(t.table_schema) like '%_exploratory' then "exploratory"
# MAGIC when lower(t.table_schema) like '%_stage' then "stage"
# MAGIC when lower(t.table_schema) like '%_dev' then "dev"
# MAGIC else "enriched"
# MAGIC end as table_schema_type,
# MAGIC t.table_owner,
# MAGIC t.last_altered_by,
# MAGIC t.last_altered AS last_altered_date --,
# MAGIC --CASE WHEN p.privilege_type='SELECT' THEN 'Yes' ELSE 'No'
# MAGIC --END AS has_select_permission
# MAGIC FROM edav_prd_cdh.information_schema.tables t
# MAGIC LEFT JOIN edav_prd_cdh.information_schema.table_privileges p
# MAGIC ON p.table_catalog = t.table_catalog
# MAGIC AND p.table_schema =t.table_schema
# MAGIC AND p.table_name = t.table_name
# MAGIC WHERE lower(t.table_schema) NOT IN ('information_schema', 'default')
# MAGIC ORDER BY t.table_catalog, t.table_schema, t.table_name;

# COMMAND ----------

raw_schemas_df=_sqldf
raw_schemas_df.write.mode("overwrite").format("delta")\
    .option("overwriteSchema", "true").saveAsTable(SCHEMAS_TBL)

# COMMAND ----------

schemas_cols_df=spark.table("edav_prd_cdh.information_schema.columns")\
    .select(['table_catalog','table_schema','table_name','column_name','ordinal_position','data_type',])\
    .withColumn('column_full_name',F.concat(F.col('table_name'),F.lit('.'),F.col('column_name')))\
    .orderBy(['table_catalog','table_schema','table_name','ordinal_position',])
display(schemas_cols_df)

# COMMAND ----------

schemas_cols_df.write.mode("overwrite").format("delta")\
    .option("overwriteSchema", "true").saveAsTable(f"{SCHEMAS_TBL}_columns")

# COMMAND ----------

groupByCols=schemas_cols_df.columns[:3]
print(groupByCols)

# COMMAND ----------

schemas_df=schemas_cols_df.groupBy(groupByCols)\
    .agg(F.countDistinct(F.col("column_full_name").alias("column_count"))\
        ,F.collect_list(F.col("column_full_name")).alias("columns"))
display(schemas_df)

# COMMAND ----------

schemas_cols_df.write.mode("overwrite").format("delta")\
    .option("overwriteSchema", "true").saveAsTable(f"{SCHEMAS_TBL}_columns_collected")
