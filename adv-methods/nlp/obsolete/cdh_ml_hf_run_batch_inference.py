# Databricks notebook source
# MAGIC %pip freeze

# COMMAND ----------

import mlflow
from pyspark.sql.functions import struct, col
mlflow.__version__

# COMMAND ----------

# MAGIC %md
# MAGIC This is an auto-generated notebook to perform batch inference on a Spark DataFrame using a selected model from the model registry. This feature is in preview, and we would greatly appreciate any feedback through this form: https://databricks.sjc1.qualtrics.com/jfe/form/SV_1H6Ovx38zgCKAR0.
# MAGIC
# MAGIC ## Instructions:
# MAGIC 1. Run the notebook against a cluster with Databricks ML Runtime version 14.3.x-cpu, to best re-create the training environment.
# MAGIC 2. Add additional data processing on your loaded table to match the model schema if necessary (see the "Define input and output" section below).
# MAGIC 3. "Run All" the notebook.
# MAGIC 4. Note: If the `%pip` does not work for your model (i.e. it does not have a `requirements.txt` file logged), modify to use `%conda` if possible.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC Transformer models from HuggingFace (NER) 
# MAGIC
# MAGIC alvaroalon2/biobert_diseases_ner · Hugging Face 
# MAGIC
# MAGIC bioformers/bioformer-8L-ncbi-disease · Hugging Face 
# MAGIC
# MAGIC sarahmiller137/distilbert-base-uncased-ft-ncbi-disease · Hugging Face 
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.widgets.text('run_id'  , defaultValue="3f0c998a450542bd8e64132b2da2fbf3")
dbutils.widgets.text('model_name' , defaultValue= "hf-bioformer-8L-ncbi-disease")
dbutils.widgets.text('input_table_name' , defaultValue="edav_prd_cdh.cdh_abfm_phi_exploratory.ml_fs_abfm_test_notes_text")


# COMMAND ----------

run_id = dbutils.widgets.get("run_id")
model_name = dbutils.widgets.get("model_name")
suffix=model_name.replace('-',"_")
input_table_name = dbutils.widgets.get("input_table_name")

dbutils.widgets.text('output_table' , defaultValue= f"{input_table_name}_{suffix}")
output_table = dbutils.widgets.get("output_table")

# COMMAND ----------

# load table as a Spark DataFrame
table = spark.table(input_table_name).limit(10000).select(['note_id','clean_text'])
#table = spark.table(input_table_name).limit(100).select(['clean_text'])

# optionally, perform additional data processing (may be necessary to conform the schema)


# COMMAND ----------

# MAGIC %md ## Load model and run inference
# MAGIC **Note**: If the model does not return double values, override `result_type` to the desired type.

# COMMAND ----------

import mlflow
from pyspark.sql.functions import struct
logged_model = f"runs:/{run_id}/{model_name}"
print(logged_model)



# COMMAND ----------



logged_model = 'runs:/71e70c0f53284fbf9f529521911acdc5/distilbert-base-uncased-ft-ncbi-disease'

# Load model as a Spark UDF.
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=logged_model)

# Predict on a Spark DataFrame.
##table.withColumn('predictions', loaded_model(struct(*map(col, table.columns))))

# COMMAND ----------

#print(hfmodel.predict(data)) #
output_df = table.withColumn("prediction", loaded_model(struct(*table.columns)))

# COMMAND ----------

output_df.display()

# COMMAND ----------

output_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(output_table)

