# Databricks notebook source
# MAGIC %md
# MAGIC # Use a Hugging Face Transformers model for batch NLP
# MAGIC This notebook shows how to use a pre-trained [🤗 Transformers](https://huggingface.co/docs/transformers/index) model to perform NLP tasks easily on Spark. It also highlights best practices on getting and understanding performance. 
# MAGIC
# MAGIC This example shows using a pre-trained summarization [pipeline](https://huggingface.co/docs/transformers/main/en/main_classes/pipelines) directly as a UDF and  an MLflow model to summarize Wikipedia articles.
# MAGIC
# MAGIC ## Cluster setup
# MAGIC For this notebook, Databricks recommends a multi-machine, multi-GPU cluster, such as an 8 worker `p3.2xlarge` cluster on AWS or `NC6s_v3` on Azure using Databricks Runtime ML with GPU versions 10.4 or greater. 
# MAGIC
# MAGIC The recommended cluster configuration in this notebook takes about 10 minutes to run. GPU auto-assignment does not work for single-node clusters, so Databricks recommends performing GPU inference on clusters with separate drivers and workers.

# COMMAND ----------

import mlflow
from mlflow.models import infer_signature
from mlflow.transformers import generate_signature_output
import transformers
from transformers import pipeline
from transformers import AutoTokenizer, AutoModelForTokenClassification

task = "ner" #"text-classification" is a different one
architecture = "emilyalsentzer/Bio_ClinicalBERT"
modelname=architecture.split('/')[-1].lower().replace("_","-")
tokenizer = AutoTokenizer.from_pretrained(architecture)
model = AutoModelForTokenClassification.from_pretrained(architecture)
components = {
    "model": model,
    "tokenizer": tokenizer,
}
entity_detector = pipeline(
    task=task,
    model=model,
    tokenizer=tokenizer,
# template
    aggregation_strategy="simple", 
    #device = device,
    )


data = """The patient reported no recurrence of palpitations at follow-up 6 months after the ablation."""
output = generate_signature_output(entity_detector, data)
signature = infer_signature(data, output)

artifact_path = f"{modelname}/ner"

model_config = {"max_length": 1024, "truncation": True}

with mlflow.start_run() as run:
    model_info = mlflow.transformers.log_model(
        transformers_model=entity_detector,
        artifact_path=artifact_path,
        registered_model_name=f"hf-{modelname}",
        input_example=data,
        signature=signature,
#        model_config=model_config,
#        model_card=architecture,
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## MLflow scoring
# MAGIC MLflow provides an easy interface to load any logged or registered model into a spark UDF. You can look up a model URI from the Model Registry or logged experiment run UI. The following shows how to use `pyfunc.spark_udf` to apply inference transformation to the Spark DataFrame.

# COMMAND ----------

model_uri = model_info.model_uri

# Load model as a Spark UDF. Override result_type if the model does not return double values.
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri, result_type='string')

#summaries = sample.select(sample.title, sample.text, loaded_model(sample.text).alias("summary"))
#summaries.write.saveAsTable(f"{output_schema}.{output_table}", mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cleanup
# MAGIC Remove the output table this notebook writes results to.

# COMMAND ----------

# MAGIC %md
# MAGIC spark.sql(f"DROP TABLE {output_schema}.{output_table}")
