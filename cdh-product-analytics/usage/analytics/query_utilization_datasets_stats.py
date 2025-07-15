# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window, types
from functools import reduce
import itertools as it
from typing import List

import mlflow

# COMMAND ----------

ANALYTICS_TBL="edav_prd_cdh.cdh_engineering_etl.analytics_cluster_cube"
NOTEBOOK_ID=""
NOTEBOOK_URL=f"https://adb-5189663741904016.16.azuredatabricks.net/editor/notebooks/{NOTEBOOK_ID}"

# COMMAND ----------


bi_usage_df=spark.table(ANALYTICS_TBL)


# COMMAND ----------

display(bi_usage_df)

# COMMAND ----------

cols=bi_usage_df.columns[:-3]
print(cols)

# COMMAND ----------
tblcol='tables'
cols=['event_date', 'email', 'clusterid', 'notebookid', 'execution_time',]
ds_usage_df=bi_usage_df.select(cols+[tblcol]).withColumn('datatable',F.explode(F.col(tblcol))).drop(tblcol).dropDuplicates()
display(ds_usage_df)

# COMMAND ----------

notebook_usage_df=ds_usage_df.groupBy('notebookid').agg(F.countDistinct(F.col('email')).alias('users'),F.countDistinct(F.col('notebookid')).alias('queries'),)

# COMMAND ----------

#TODO add datepart
user_query_by_table_df=ds_usage_df.groupBy('table').agg(F.countDistinct(F.col('email')).alias('users'),F.countDistinct(F.col('notebookid')).alias('queries'),)
display(user_query_by_table_df)

