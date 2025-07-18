# Databricks notebook source
# MAGIC %pip freeze

# COMMAND ----------

dbutils.widgets.text('EXPERIMENT_ID',defaultValue='4090675059920416')
EXPERIMENT_ID=dbutils.widgets.get('EXPERIMENT_ID')
#https://mlflow.org/docs/latest/llms/transformers/tutorials/text-generation/text-generation.html
import mlflow
from pyspark.sql.functions import struct, col
mlflow.__version__
mlflow.autolog()
#mlflow_run_id=mlflow.start_run(experiment_id=EXPERIMENT_ID)

# COMMAND ----------


dbutils.widgets.text('input_table_name' , defaultValue="edav_dev_cdh.cdh_mimic_exploratory.note_sample_sentences_biobert_diseases_ner")
input_table_name = dbutils.widgets.get("input_table_name")
dbutils.widgets.text('output_table', defaultValue=input_table_name+"_nlp" )
output_table = dbutils.widgets.get("output_table")
print(output_table)
dbutils.widgets.text('nlp_cats' , defaultValue="DISEASE")
nlp_cats_str = dbutils.widgets.get("nlp_cats")
nlp_cats=nlp_cats_str.split(",")

# COMMAND ----------

import pyspark.sql.window as W
import pyspark.sql.functions as F 


# COMMAND ----------


keylist=['note_id','note_sent',]
#["note_id","note_sent",f"{explode_col}_exploded.*", ]

# COMMAND ----------

token_window = W.Window.partitionBy(['note_id','note_sent']).orderBy(['offset']) 
dflag=df.withColumn("lag_end",F.lag('offset_end').over(token_window)) #.dropDuplicates()
display(dflag)
# load table as a Spark DataFrame this should already be done in practice
print(f"nrecs: {dflag.count()}")
# optionally, perform additional data processing (may be necessary to conform the schema)


# COMMAND ----------

# MAGIC %md
# MAGIC #filter
# MAGIC nlp_cats=df.select("nlp_category").distinct().collect()
# MAGIC print(nlp_cats)
# MAGIC nlp_cats=['DISEASE',]

# COMMAND ----------

note_nlp=dflag.withColumn('original_token',F.when(F.col('lag_end')==F.col('offset'),False).otherwise(True)).filter(F.col('original_token')).drop('original_token','lag_end')#.filter(F.col("nlp_category").isin(nlp_cats))
  #withColumn('Updated Subject', lag(data_frame['subject'], 1, 'Same Subject').over(Windowspec))
display(note_nlp)

# COMMAND ----------

# testing how tokenizer compares with auxiliary call
note_nlp.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(output_table)

