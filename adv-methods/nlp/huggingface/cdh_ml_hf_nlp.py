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

dbutils.widgets.text('model_name' , defaultValue= "d4data/biomedical-ner-all",label="Huggingface Hub Name")

dbutils.widgets.text('output_table_name' , defaultValue="edav_dev_cdh.cdh_mimic_exploratory.note_sample_sentences_biobert_diseases_ner")
input_table_name = dbutils.widgets.get("output_table_name")
print(input_table_name)
dbutils.widgets.text('nlp_table_name', defaultValue=input_table_name+"_nlp" )
output_table = dbutils.widgets.get("nlp_table_name")
print(output_table)
dbutils.widgets.text('nlp_cats' , defaultValue="DISEASE")
nlp_cats_str = dbutils.widgets.get("nlp_cats")
nlp_cats=nlp_cats_str.split(",")
# COMMAND ----------

architecture = dbutils.widgets.get("model_name")
print(architecture)
suffix=architecture.split('/')[1].replace('-',"_")
print(suffix)


# COMMAND ----------

import pyspark.sql.window as W
import pyspark.sql.functions as F 
output_df=spark.table(input_table_name+'_raw')

# COMMAND ----------

# MAGIC %md
# MAGIC https://stackoverflow.com/questions/72400105/how-to-flatten-array-of-struct   
# MAGIC
# MAGIC
# MAGIC """
# MAGIC How to change schema in PySpark from this
# MAGIC
# MAGIC |-- id: string (nullable = true)
# MAGIC |-- device: array (nullable = true)
# MAGIC |    |-- element: struct (containsNull = true)
# MAGIC |    |    |-- device_vendor: string (nullable = true)
# MAGIC |    |    |-- device_name: string (nullable = true)
# MAGIC |    |    |-- device_manufacturer: string (nullable = true)
# MAGIC to this
# MAGIC
# MAGIC |-- id: string (nullable = true)
# MAGIC |-- device_vendor: string (nullable = true)
# MAGIC |-- device_name: string (nullable = true)
# MAGIC |-- device_manufacturer: string (nullable = true)
# MAGIC """
# MAGIC ```
# MAGIC df_flat = df.withColumn('device_exploded', F.explode('device'))
# MAGIC             .select('id', 'device_exploded.*')
# MAGIC
# MAGIC df_flat.printSchema()
# MAGIC ```

# COMMAND ----------

keylist=['note_id','note_sent',]
#["note_id","note_sent",f"{explode_col}_exploded.*", ]

outlist=keylist+['results_exploded.*']

renames={"word":"snippet",
         "entity_group":"nlp_category",
         "score":"score",
         "start":"offset",
         "end":"offset_end",         }
keylist=['note_id','note_sent',]
name="results"
df=output_df.withColumn(f"{name}_exploded",F.explode(name)).select("note_id","note_sent",f"{name}_exploded.*").withColumnsRenamed(renames).withColumn("nlp_system",F.lit(architecture))
display(df)


# COMMAND ----------

# testing how tokenizer compares with auxiliary call
#note_nlp.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(output_table)

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

note_nlp=dflag.withColumn('original_token',F.when(F.col('lag_end')==F.col('offset'),False).otherwise(True)).filter(F.col('original_token')).drop('original_token','lag_end').filter(F.col("nlp_category").isin(nlp_cats))
  #withColumn('Updated Subject', lag(data_frame['subject'], 1, 'Same Subject').over(Windowspec))
display(note_nlp)

# COMMAND ----------

# testing how tokenizer compares with auxiliary call
note_nlp.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(output_table)

