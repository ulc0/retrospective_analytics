# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window, types
from functools import reduce
import itertools as it
from typing import List

import mlflow
import pandas as pd
import json
import tempfile,os

# COMMAND ----------

ANALYTICS_TBL="edav_prd_cdh.cdh_engineering_etl.analytics_cluster_cube"
NOTEBOOK_ID=""
NOTEBOOK_URL=f"https://adb-5189663741904016.16.azuredatabricks.net/editor/notebooks/{NOTEBOOK_ID}"
DATASETS=['truveta','premier']=

# COMMAND ----------


bi_usage_df=spark.table(ANALYTICS_TBL)


# COMMAND ----------

display(bi_usage_df)

# COMMAND ----------

#TODO build a dictionary of core dimensions and facts, key is row, each entry is for counts
dimfactDict={
             "dstable":["email","notebookid","commandid"],
             #"dataset":["dstable","email","notebookid","queryno"],
             "email":["runtime","notebookid","commandid",'table_counts'],
             }
values=['runtime']
labels={'runtime':'Avg',"notebookid":'Total',"commandid":'Total',"table_counts":'total',}

# COMMAND ----------

#Query and user counts by table, dataset, and database pd.df.to_html()
# 'sessionid',

value=values[0]
#keycols=['schema_name', 'dstable','dataset',]
from pyspark.sql.functions import sum, countDistinct,count
g={}
#for k in keycols:
for k,v in dimfactDict.items():
    g[k]={}
    for unit in v:
        print(unit)
        label=labels[unit]
        if(unit==value):
            xg=bi_usage_df.groupBy(F.col(k)).agg(F.sum(F.col(value)).alias(f"Total {value}"),F.round(F.avg(F.col(value))).alias(f"{label} {value}"))
        else:
            xg=bi_usage_df.groupBy(F.col(k)).agg(F.countDistinct(F.col(unit)).alias(f"Distinct {unit}"),F.count(F.col(unit)).alias(f"{label} {unit}"))
        if(unit in values):
            vw=Window.orderBy(F.desc(f"{label} {unit}"))
            rg=xg.withColumn(f"Ranked {label} {unit}",F.dense_rank().over(vw))
        else:
            qw=Window.orderBy(F.desc(f"{label} {unit}"))
            rg=xg.withColumn(f"Ranked {label} {unit}",F.dense_rank().over(qw))
        g[k][unit]=rg

# COMMAND ----------

display(rg)

# COMMAND ----------

# DBTITLE 1,o
#excel output is here for now, can be integrated into the other loop at some point
xldir=tempfile.gettempdir()
print(xldir)
os.environ["TEMPDIR"]=xldir
with pd.ExcelWriter(f"{xldir}/utilization.xlsx") as writer:
    for k,v in dimfactDict.items():
        xlarray=list(g[k].values()) # list of data frames to be merged on column k
        xls=reduce(lambda left, right: left.join(right,k), xlarray).toPandas()
        xls.to_excel(writer,sheet_name=f"{k} Utilization",index=False)
    #

# COMMAND ----------

# MAGIC %md
# MAGIC cp $TEMPDIR/*.xlsx /Volumes/edav_prd_cdh/cdh_ml/metadata_data/
# MAGIC ls -ll /Volumes/edav_prd_cdh/cdh_ml/metadata_data/*.xlsx
# MAGIC
