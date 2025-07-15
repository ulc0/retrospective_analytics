# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window, types
# to create empty dataframe with schema
from pyspark.sql.types import * #StructType,StructField, StringType, TimestampType, IntegerType

from functools import reduce
import itertools as it
from typing import List

#import mlflow
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
EDAV_LINEAGE_TBL="edav_project_audit_logs.cdh.column_lineage"
EDAV_AUDIT_TBL="edav_project_audit_logs.cdh.audit"

EXPERIMENT_NAME="Product_Analytics_Core"
EXPERIMENT_ID="3288147180612904"
SCRATCH='/Volumes/edav_prd_cdh/cdh_ml/metadata_data'
NOTEBOOK_ID=""
NOTEBOOK_URL=f"https://adb-5189663741904016.16.azuredatabricks.net/editor/notebooks/{NOTEBOOK_ID}"
DATASETS=['truveta','premier']
GCOLS=['notebookid', 'clusterid', 'user_identity', 'commandid','commandtext','event_time',]

#REGEX=r"edav_prd_cdh\.cdh_.*"



# COMMAND ----------

usage_df=spark.table(EDAV_AUDIT_TBL)
usage_df.printSchema()

# COMMAND ----------

mapDict = {
    "databrickssql": {
        "warehouseId": "clusterid",
        "commandId":"commandid",
#        "user_identity":"user",
        "commandText": "commandtext",
#        "request_id":"request_id" #coalesce
        #        "commandParameters":"commandparameters",
    },
    "notebook": {
        "clusterId": "clusterid",
#        "user_identity":"user",
        "commandId":"commandid",
        "commandText": "commandtext",
        "commandLanguage": "command_language",
        "notebookId": "notebookid",
        "executionTime": "execution_time",
#        "status": "status",
    },
    "jobs": {
        "clusterId": "clusterid",
#        "user_identity":"user",
        "commandId":"commandid",
        "commandText": "commandtext",
        "commandLanguage": "command_language",
        "notebookId": "notebookid",
        "executionTime": "execution_time",
#        "status": "status",
        "jobId": "jobid",
        "runId": "runid",
    },
}

# COMMAND ----------


schema = StructType([
#core columns
  StructField('service_name', StringType(), True),
  StructField('event_time', TimestampType(), True),
#  StructField('session_id', StringType(), True),  #notebookid for jobs
  StructField('request_id', IntegerType(), True),
#  StructField('user_identity', StructType(), True),
  StructField('user_identity',
              StructType([ 
                  StructField("email", StringType()),
                  StructField("subject_name",StringType())]
                  )
              , True),
#requestParams
  StructField('commandid', StringType(), True),
  StructField('commandtext', StringType(), True),
  StructField('clusterid', StringType(), True),
  StructField('notebookid', StringType(), True),
  StructField('execution_time', DoubleType(), True),
#  StructField('status', StringType(), True),
  StructField('jobid', StringType(), True),
  StructField('runid', StringType(), True),
  ])


# COMMAND ----------

sqlstringA = "select "
sqlstringB = " from edav_project_audit_logs.cdh.audit where service_name="
core_cols = [
    "service_name",
    "event_time",
    "request_id",
#    "request_id",
#    "user_identity",
    "user_identity",
 #   "action_name",
#    "event_id",
]

# COMMAND ----------

# DBTITLE 1,Map service_name to something better
#TODO make this a function
services_dict={
    "databrickssql":"SQL",
    "notebook":"NBK",
    "jobs":"JOB",
}
services_map = F.create_map([F.lit(x) for x in it.chain(*services_dict.items())])


# COMMAND ----------

usage_df = spark.createDataFrame([], schema)
struct_colnames = list(usage_df.columns)
print(struct_colnames)

for service_name, select_cols in mapDict.items():
    print(service_name)
    request_param_src_cols = [
        f"request_params.{k} as {v}" for k, v in select_cols.items()
    ]
    sqlcolumns = core_cols + request_param_src_cols
    request_param_dst_cols = [v for k, v in select_cols.items()]
    null_columns = [
        "NULL as " + colname
        for colname in struct_colnames
        if colname not in core_cols + request_param_dst_cols
    ]
    print(null_columns)
    #    print(sqlcolumns)
    sqlstring = sqlstringA + ",".join(sqlcolumns) + ","
    if null_columns:
        sqlstring = sqlstring + ",".join(null_columns)
    else:
        sqlstring = sqlstring[:-1]  # dunno final comma
    sqlstring = sqlstring + sqlstringB + f"'{service_name}'"
    if service_name not in ("databrickssql"):
        sqlstring = sqlstring + " and action_name='runCommand'"
    print(sqlstring)
    df = spark.sql(sqlstring)
    # display(df)
    # print(df.columns)
    #    for c in null_columns:
    #        print(f" adding {c}")
    #        df = df.withColumn(c, F.lit(None))
    #    print(set(df.columns) & set(sqlcolumns))
    usage_df = usage_df.union(df.select(*struct_colnames))
usage_df = (
    usage_df.withColumn("email", F.col("user_identity.email"))
    .withColumn("notebookid", F.coalesce(F.col("notebookid"), F.col("request_id")))
    .withColumn("service_name", services_map[F.col("service_name")])
    .drop(
        F.col("user_identity"),
        F.col("request_id"),
#        F.col("service_name"),
    )
)
# .withColumn("run_as",F.col("identity_metadata.run_as"))

# COMMAND ----------

# MAGIC %md
# MAGIC select
# MAGIC request_params["warehouseId"] as clusterId,
# MAGIC from edav_project_audit_logs.cdh.audit
# MAGIC where service_name="databrickssql" --or action_name="runCommand"

# COMMAND ----------

# MAGIC %md
# MAGIC request_params is a map(), not a struct()
# MAGIC sql_usage_df=usage_df.where(F.col("service_name")=="databrickssql").select(*select_cols).explode('request_params')
# MAGIC job_usage_df=usage_df.where(F.col("service_name")=="jobs").select(*select_cols).explode('request_params')
# MAGIC nbk_usage_df=usage_df.where(F.col("service_name")=="notebook").select(*select_cols).explode('request_params')
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC display(usage_df)

# COMMAND ----------


usage_df.write.mode("overwrite").format("delta")\
    .option("overwriteSchema", "true").saveAsTable(CORE_USAGE_TBL)

# COMMAND ----------


#groupByList=core_cols[:-1]+['notebookid','commandid','clusterid','email',]
#groupByList+["execution_time","commandtext"]
groupByList=['service_name', 'event_time',  'notebookid', 'commandid', 'clusterid', 'email']
#colList=['service_name', 'event_time',  'notebookid', 'commandid', 'clusterid', 'email', 'execution_time', 'commandtext']
#print(colList)
grouped_usage_df=usage_df.dropDuplicates().groupBy(groupByList)\
    .agg(F.round(F.sum( F.nanvl(F.col('execution_time'), F.lit(0))),1).alias("execution_time"),)    

"""
 F.nanvl(F.col('col1'), F.lit(0))
grouped_usage_df=usage_df.select(*colList)\
    .dropDuplicates().groupBy(groupByList)\
    .agg(F.round(F.sum(F.col("execution_time")),1).alias("execution_time"),)    
"""

# COMMAND ----------

# MAGIC %md
# MAGIC display(grouped_usage_df)

# COMMAND ----------

grouped_usage_df.write.mode("overwrite").format("delta")\
    .option("overwriteSchema", "true")\
    .saveAsTable(f"{CORE_USAGE_TBL}_grouped")
