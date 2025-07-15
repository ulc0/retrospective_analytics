# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import DataFrame, Window, types
from functools import reduce
#import itertools as it
from typing import List
#import string, re

import mlflow
import pandas as pd
import json
#import sqlglot as sg

# COMMAND ----------

SILVER_USAGE_TBL="edav_prd_cdh.cdh_engineering_etl.analytics_cluster_silver"
CORE_USAGE_TBL="edav_prd_cdh.cdh_engineering_etl.product_analytics_core_usage"
ANALYTICS_TBL="edav_prd_cdh.cdh_engineering_etl.product_analytics_cube"
EDAV_LINEAGE_TBL="edav_project_audit_logs.cdh.column_lineage"

SCHEMAS_TBL="edav_prd_cdh.cdh_engineering_etl.product_analytics_schemas"
SCHEMAS_LINEAGE_TBL="edav_prd_cdh.cdh_engineering_etl.product_analytics_schemas_lineage"
EXPERIMENT_NAME="Product_Analytics_Core"
EXPERIMENT_ID="3288147180612904"
SCRATCH='/Volumes/edav_prd_cdh/cdh_ml/metadata_data'
NOTEBOOK_ID=""
NOTEBOOK_URL=f"https://adb-5189663741904016.16.azuredatabricks.net/editor/notebooks/{NOTEBOOK_ID}"
DATASETS=['truveta','premier']
GCOLS=['notebookid', 'clusterid', 'email', 'commandid','commandtext','event_time',]

#REGEX=r"edav_prd_cdh\.cdh_.*"

# COMMAND ----------

usage_df=spark.table(CORE_USAGE_TBL)\
    .orderBy("notebookid","commandid")\
    .withColumn("lc_commandtext",F.lower(F.col("commandtext")))
display(usage_df)

# COMMAND ----------


distinct_counts={}
for col in GCOLS:
    dc=usage_df.select(col).distinct().count()
    distinct_counts[col]=dc
count_summary=pd.DataFrame.from_dict(distinct_counts,orient='index',columns=['distinct_counts']).reset_index().rename(columns={'index': 'item'})
display(count_summary)

# COMMAND ----------

lineageDict={
"source_table_full_name":"datatable",
"source_table_schema":"table_schema",
"source_table_name":"table_name",
"source_column_name":"column_name",
#"source_table_full_name":"table_full_name",
"source_type":"lineage_table_type",
"entity_id":"notebookid",
}
#"event_time":"event_time",
#lineage includes service_name=databrickssql, usage does not
lineageCols=[k for k in lineageDict.keys()]
print(lineageCols)
lineage_df=spark.table(EDAV_LINEAGE_TBL)\
    .filter(~F.col("entity_type").isin( ['EXTERNAL']))\
    .select(*lineageCols)\
    .withColumnsRenamed(lineageDict)\
    .withColumn('column_full_name',F.concat(F.col("table_name"),F.lit('.'),F.col("column_name"),))\
#    .withColumn("lc_column_name",F.lower(F.col("column_name")))
#    .withColumn("datatable",F.concat_ws('.',F.col("table_schema"),F.col("table_name")))\
 #   .withColumn('ds',F.split(F.col('table_schema'), '_').getItem(1))\
#    .orderBy("notebookid")
#    .select('datatable','table_schema','table_name','ds','table_type','column_name','column_full_name') 
#.withColumn("event_time",F.col("event_time").cast("date")).dropDuplicates()
#TODO move to after aggregation
display(lineage_df)
#schemas_df=lineage.orderBy('notebookid','column_name')


# COMMAND ----------

schemas_lineage_df=spark.table(SCHEMAS_TBL).join(lineage_df,['table_schema','table_name'],'outer')
display(schemas_lineage_df)
schemas_lineage_df.write.mode("overwrite").format("delta")\
    .option("overwriteSchema", "true")\
        .saveAsTable(SCHEMAS_LINEAGE_TBL)

# COMMAND ----------

jcol='column_name'
usage_lineage_df=schemas_lineage_df.join(usage_df,'notebookid','left')\
    .dropDuplicates()\
    .filter(F.contains(F.col('lc_commandtext'),F.col(jcol)))\
    .drop(jcol,'lc_commandtext')
display(usage_lineage_df)


# COMMAND ----------

usage_lineage_df.write.mode("overwrite").format("delta").option(
    "overwriteSchema", "true"
).saveAsTable(ANALYTICS_TBL)

# COMMAND ----------

# MAGIC %md
# MAGIC notebookid:string  
# MAGIC clusterid:string  
# MAGIC email:string  
# MAGIC commandid:string  
# MAGIC commandtext:string  
# MAGIC event_time:timestamp  
# MAGIC execution_time:double  
# MAGIC datatable:string  
# MAGIC table_schema:string  
# MAGIC table_name:string  
# MAGIC column_name:string  
# MAGIC table_type:string  
# MAGIC column_full_name:string  
# MAGIC ds:string  

# COMMAND ----------

# MAGIC %md
# MAGIC jcol='column_name'
# MAGIC usage_lineage_df=usage_df.join(lineage_df,'notebookid','left')\
# MAGIC     .dropDuplicates()\
# MAGIC     .filter(F.contains(F.col('lc_commandtext'),F.col(jcol)))\
# MAGIC     .drop(jcol,'lc_commandtext')
# MAGIC display(usage_lineage_df)
# MAGIC
# COMMAND ----------
"""
gList=["notebookid","commandid","clusterid","email","event_date","commandtext","ds"]
usage_lineage_collected_df=usage_lineage_df.groupBy(*gList)\
    .agg(F.round(F.avg(F.col("runtime")),1).alias("execution_time"),\
    F.countDistinct(F.col("datatable")).alias("table_count"),\
    F.countDistinct(F.col("full_column_name")).alias("column_count"),\
    F.array_sort(F.collect_set(F.col("datatable"))).alias("tables"),\
     F.array_sort(F.collect_set(F.col("full_column_name"))).alias("columns"),\
    )

#schemas_df=lineage.orderBy('notebookid','column_name')
"""
# COMMAND ----------
"""

jcol='lc_column_name'
usage_lineage_df=usage_df.join(lineage_df,'notebookid','left')\
    .dropDuplicates()\
    .filter(F.contains(F.col('lc_commandtext'),F.col(jcol)))\
    .drop(jcol,'lc_commandtext')
display(usage_lineage_df)
"""


# COMMAND ----------

usage_lineage_df.write.mode("overwrite").format("delta").option(
    "overwriteSchema", "true"
).saveAsTable(ANALYTICS_TBL)

# COMMAND ----------

# MAGIC %md
# MAGIC notebookid:string  
# MAGIC clusterid:string  
# MAGIC email:string  
# MAGIC commandid:string  
# MAGIC commandtext:string  
# MAGIC event_time:timestamp  
# MAGIC execution_time:double  
# MAGIC datatable:string  
# MAGIC table_schema:string  
# MAGIC table_name:string  
# MAGIC column_name:string  
# MAGIC table_type:string  
# MAGIC column_full_name:string  
# MAGIC ds:string  

# COMMAND ----------

# MAGIC %md
# MAGIC gList=["notebookid","commandid","clusterid","email","event_time","commandtext","data_asset_provider","table_schema_type"]
# MAGIC usage_lineage_collected_df=usage_lineage_df.groupBy(*gList)\
# MAGIC     .agg(F.round(F.avg(F.col("execution_time")),1).alias("execution_time"),\
# MAGIC     F.countDistinct(F.col("datatable")).alias("table_count"),\
# MAGIC     F.countDistinct(F.col("column_full_name")).alias("column_count"),\
# MAGIC     F.array_sort(F.collect_set(F.col("datatable"))).alias("tables"),\
# MAGIC      F.array_sort(F.collect_set(F.col("column_full_name"))).alias("columns"),\
# MAGIC     )
# MAGIC #####MASTER=======
# MAGIC
# MAGIC #schemas_df=lineage.orderBy('notebookid','column_name')

# COMMAND ----------

extimeWindow = Window.orderBy(F.desc(F.col("execution_time")))
tablesWindow = Window.orderBy(F.desc(F.col("table_count")))
columnsWindow = Window.orderBy(F.desc(F.col("column_count")))
# need more summaries, next collected cube commandWindow = Window.orderBy("commandid")

# pattern df.withColumn("drank", dense_rank().over(w))

# COMMAND ----------

#TODO will we need commandid with the inclusion of sql queries?
gList=["notebookid","commandid","clusterid","email","event_time","commandtext"]
gList=["notebookid","commandid","service_name","event_time","email","clusterid","commandtext",]
ncile=20
usage_lineage_collected_df=usage_lineage_df.groupBy(*gList)\
    .agg(F.round(F.avg(F.nanvl(F.col('execution_time'), F.lit(0))),1)\
        .alias("execution_time"),\
        F.countDistinct(F.col("datatable")).alias("table_count"),\
        F.countDistinct(F.col("table_schema")).alias("schema_count"),\
        F.countDistinct(F.col("column_full_name")).alias("column_count"),\
    #    F.collect_set(F.col("commandtext")).alias("commandtext"),\
        F.array_sort(F.collect_set(F.col("datatable"))).alias("tables"),\
        F.array_sort(F.collect_set(F.col("table_schema"))).alias("schemas"),\
        F.array_sort(F.collect_set(F.col("column_full_name"))).alias("columns"),)\
        .withColumn(f"execution_{ncile}cile",F.ntile(ncile).over(extimeWindow))\
        .withColumn(f"columns_{ncile}cile",F.ntile(ncile).over(columnsWindow))\
        .withColumn(f"tables_{ncile}cile",F.ntile(ncile).over(tablesWindow))\
        .orderBy(f"execution_{ncile}cile",f"columns_{ncile}cile",f"tables_{ncile}cile")


#    .withColumn("execution_rank",F.dense_rank().over(extimeWindow))\
#    .withColumn("tables_rank",F.dense_rank().over(tablesWindow))\
#    .withColumn("column_rank",F.dense_rank().over(columnsWindow))

#F.ntile(10).over(Window.partitionBy().orderBy(df_basket1['price'])).alias("decile_rank")

display(usage_lineage_collected_df)
#"datatable","column_full_name",
usage_lineage_collected_df.write.mode("overwrite")\
    .format("delta")\
    .option("overwriteSchema", "true")\
    .saveAsTable(f"{ANALYTICS_TBL}_collected_columns")

# COMMAND ----------


#mlflow.set_experiment(experiment_id=EXPERIMENT_ID)

#mlflow.set_experiment(experiment_id=EXPERIMENT_ID)
with mlflow.start_run(experiment_id=EXPERIMENT_ID,nested=True) as run:
    mlflow.log_dict(distinct_counts, "distinct_counts.json")
    #mlflow.log_table(data=values_dict, artifact_file="values.json")
#    mlflow.log_table(data=pd.DataFrame(qlist), artifact_file="queries.json")
#    mlflow.log_dict(mdict,'query_index.json')
    run_id = run.info.run_id
