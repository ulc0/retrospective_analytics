# Databricks notebook source
# base on jsl samples
# https://github.com/JohnSnowLabs/spark-nlp/blob/412ca982aa6cc286411c435dcd5fe1bc005659f5/examples/python/annotation/text/english/document-normalizer/document_normalizer_notebook.ipynb

# COMMAND ----------


#from pyspark.sql.types import StringType
import pyspark.sql.functions as F



spark.conf.set('spark.sql.shuffle.partitions',7200*4)
spark.conf.set("spark.databricks.io.cache.enabled", True)

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

dbutils.widgets.text("basetable","edav_prd_cdh.cdh_abfm_phi_exploratory.ulc0_notes",)

stbl=dbutils.widgets.get("basetable")
#RUN_ID=dbutils.jobs.taskValues.get("fs_abfm_notes_partition","run_id",debugValue="test",)
dbutils.widgets.text("experiment_id",defaultValue="3389456715448166")
EXPERIMENT_ID=dbutils.widgets.get("experiment_id")

# COMMAND ----------

import mlflow
import mlflow.data
import os
os.environ['PYSPARK_PIN_THREAD']='False'
#mlflow.autolog()
groupby=['person_id','provider_id','encoding']
notes=spark.table(stbl)
textcols=notes.schema.names
print(textcols)
#store all column names that are categorical in a list
numberCols = [item[0] for item in notes.dtypes if not item[1].startswith('string')]
print(numberCols)

# COMMAND ----------

# MAGIC %md
# MAGIC         notes.groupBy(g).agg(max(gstat), sum(gstat), 
# MAGIC                         min(gstat), avg(gstat),  
# MAGIC                         count(gstat)).show() 
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
gstat=['note_len']
groupby=['provider_id','encoding']
#sumtbl=notes.groupBy("encoding") \
#    .agg(count("*").alias("count")
#     ) \
#    .show(truncate=False)
#with mlflow.start_run(experiment_id=EXPERIMENT_ID) as run:

#    dataset=mlflow.data.from_spark(notes)
#sumtbl=notes.summary()
#display(sumtbl)
    #mlflow.log_table(sumtbl,artifact_file="summary.json")
for g in groupby:
    print(g)
#        notes.groupBy(g).agg(count(gstat)).show(truncate=False)
# Groupby with DEPT  with sum() , min() , max() 
#        notes.groupBy(g).avg(gstat)
#    sumtbl=notes.groupBy(g).summary().toPandas().to_dict()
    sumtbl=notes.groupBy(g).agg(count("*")).show(truncate=False)
    print(sumtbl)
#    mlflow.log_table(sumtbl,artifact_file=f"{g}_summary.json")

