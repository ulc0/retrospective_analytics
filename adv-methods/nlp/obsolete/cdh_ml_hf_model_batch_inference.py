# Databricks notebook source
# MAGIC %pip freeze

# COMMAND ----------

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

from mlflow import MlflowClient

# Initialize an MLflow Client
client = MlflowClient()


def assign_alias_to_stage(model_name, stage, alias):
    """
    Assign an alias to the latest version of a registered model within a specified stage.

    :param model_name: The name of the registered model.
    :param stage: The stage of the model version for which the alias is to be assigned. Can be
                "Production", "Staging", "Archived", or "None".
    :param alias: The alias to assign to the model version.
    :return: None
    """
    latest_mv = client.get_latest_versions(model_name, stages=[stage])[0]
    client.set_registered_model_alias(model_name, alias, latest_mv.version)


# COMMAND ----------

dbutils.widgets.text('model_name' , defaultValue= "hf-bioformer-8L-ncbi-disease")
model_name = dbutils.widgets.get("model_name")

dbutils.widgets.text('model_version'  , defaultValue="5")
model_version = dbutils.widgets.get("model_version")


dbutils.widgets.text('input_table_name' , defaultValue="edav_prd_cdh.cdh_abfm_phi_exploratory.ml_fs_abfm_test_notes_text")
input_table_name = dbutils.widgets.get("input_table_name")

suffix=model_name.replace('-',"_")
dbutils.widgets.text('output_table' , defaultValue= f"edav_prd_cdh.cdh_abfm_phi_exploratory.ml_fs_abfm_test_notes_{suffix}")
output_table = dbutils.widgets.get("output_table")

# COMMAND ----------

# load table as a Spark DataFrame
table = spark.table(input_table_name).limit(10).select(['person_id',
'note_type',
'provider_id',
'note_datetime',
'note_section',
'note_string'])
table = spark.table(input_table_name).limit(10000).select(['note_string'])

# optionally, perform additional data processing (may be necessary to conform the schema)


# COMMAND ----------

# MAGIC %md ## Load model and run inference
# MAGIC **Note**: If the model does not return double values, override `result_type` to the desired type.

# COMMAND ----------

import mlflow
from pyspark.sql.functions import struct
model_uri = f"models:/{model_name}/{model_version}"
print(model_uri)
model_info = mlflow.models.get_model_info(model_uri)
model_info.flavors

# COMMAND ----------


# create spark user-defined function for model prediction
predict = mlflow.pyfunc.spark_udf(spark, model_uri)
#hfmodel = mlflow.pyfunc.load_model(model_name)


# COMMAND ----------

#print(hfmodel.predict(data)) #
output_df = table.withColumn("prediction", predict(struct(*table.columns)))

# COMMAND ----------

output_df.display()

# COMMAND ----------

output_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(output_table)

