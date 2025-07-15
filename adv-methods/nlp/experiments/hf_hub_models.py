# Databricks notebook source
# MAGIC %md
# MAGIC install mlflow-skinny==2.11.3
# MAGIC %pip install mlflow==2.11.3
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

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

# MAGIC %md
# MAGIC alvaroalon2/biobert_diseases_ner · Hugging Face 
# MAGIC
# MAGIC bioformers/bioformer-8L-ncbi-disease · Hugging Face 
# MAGIC
# MAGIC sarahmiller137/distilbert-base-uncased-ft-ncbi-disease · Hugging Face 

# COMMAND ----------

# MAGIC %pip freeze

# COMMAND ----------

import mlflow
from mlflow.models import infer_signature
from mlflow.transformers import generate_signature_output


# COMMAND ----------

print(mlflow.__version__)

# COMMAND ----------

dbutils.widgets.text("architecture",defaultValue="alvaroalon2/biobert_diseases_ner")
dbutils.widgets.text("hftask",defaultValue="token-classification")
dbutils.widgets.text("compute_framework",defaultValue="pt") #pt==pytorch, tf=tensorflow
dbutils.widgets.text("EXPERIMENT_ID",defaultValue="762465570046872")

# COMMAND ----------

EXPERIMENT_ID=dbutils.widgets.get("EXPERIMENT_ID")

# COMMAND ----------

# parameterized for huggingface hub. 
#TODO parameterized based on hf hub architecture name and pull options
#hftask="ner" # (alias of "token-classification"): will return a :class:`~transformers.TokenClassificationPipeline.
#architecture = "alvaroalon2/biobert_diseases_ner"
hftask=dbutils.widgets.get("hftask") # (alias of "token-classification"): will return a :class:`~transformers.TokenClassificationPipeline.
print(hftask)
architecture = dbutils.widgets.get("architecture")
print(architecture)
model=architecture.split("/")[-1]

# COMMAND ----------

# compute specs
compute_framework=dbutils.widgets.get("compute_framework") #pt==pytorch, tf=tensorflow
print(compute_framework)
#TODO configure GPU
# configure experiment

# COMMAND ----------

# Test case expected value from hf hub transformer single record test
testcase=[ "For", "both", "sexes", "combined", ",", "the", "penetrances", "at", "age", "60", "years", "for", "all", "cancers", "and", "for", "colorectal", "cancer", "were", "0", "." ]
expectedValues=[ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1, 2, 0, 0, 0 ]
data = " ".join(testcase)
print(data)

# COMMAND ----------

import transformers
from transformers import pipeline
import torch
device = 0 if torch.cuda.is_available() else -1
print(device)


# COMMAND ----------

# MAGIC %md
# MAGIC # Use a pipeline as a high-level helper
# MAGIC from transformers import pipeline
# MAGIC
# MAGIC pipe = pipeline("token-classification", model="bioformers/bioformer-8L-ncbi-disease")
# MAGIC # Load model directly
# MAGIC from transformers import AutoTokenizer, AutoModelForTokenClassification
# MAGIC
# MAGIC tokenizer = AutoTokenizer.from_pretrained("bioformers/bioformer-8L-ncbi-disease")
# MAGIC model = AutoModelForTokenClassification.from_pretrained("bioformers/bioformer-8L-ncbi-disease")

# COMMAND ----------

# MAGIC %md
# MAGIC # Use a pipeline as a high-level helper
# MAGIC from transformers import pipeline
# MAGIC
# MAGIC pipe = pipeline("token-classification", model="alvaroalon2/biobert_diseases_ner")
# MAGIC # Load model directly
# MAGIC from transformers import AutoTokenizer, AutoModelForTokenClassification
# MAGIC
# MAGIC tokenizer = AutoTokenizer.from_pretrained("alvaroalon2/biobert_diseases_ner")
# MAGIC model = AutoModelForTokenClassification.from_pretrained("alvaroalon2/biobert_diseases_ner")

# COMMAND ----------

## Use a pipeline as a high-level helper
from transformers import pipeline

##pipe = pipeline("token-classification", model="alvaroalon2/biobert_diseases_ner")
hf_pipe = pipeline(task=hftask, model=architecture,aggregation_strategy="none",framework=compute_framework,device=device) 

print(data)
print(hf_pipe(data))
signature = infer_signature(data, output)
print(signature)

output=generate_signature_output(hf_pipe, data)
print(output)
#override to pt=PyTorch
print(compute_framework)
artifact_path = f"{model}"

##mlflow.transformers.autolog(log_input_examples=False, log_model_signatures=False, log_models=False, log_datasets=False, disable=False, exclusive=False, disable_for_unsupported_versions=False, silent=False, extra_tags=None)
##model_config = {"max_length": 1024, "truncation": True}


# COMMAND ----------

# MAGIC %md
# MAGIC hfpipe = pipeline("ner", model=model, tokenizer=tokenizer, aggregation_strategy="none",framework="pt",device=0) # pass device=0 if using gpu
# MAGIC

# COMMAND ----------

requirements=[
"mlflow-skinny==2.9.2",
"mlflow==2.9.2",]
"""
"accelerate==0.25.0",
"astunparse==1.6.3",
"cffi==1.15.1",
"defusedxml==0.7.1",
"dill==0.3.6",
"ipython==8.14.0",
"opt-einsum==3.3.0",
"pandas==1.5.3",
"pydot==2.0.0",
"pyparsing==3.0.9",
"pytesseract==0.3.10",
"scikit-learn==1.1.2",
"sentencepiece==0.1.99",
"torch==2.0.1",
"torchvision==0.15.2",
"transformers==4.36.1",]
"""

# COMMAND ----------



# COMMAND ----------



mlflow.set_experiment(experiment_id=EXPERIMENT_ID)
with mlflow.start_run(run_name=model) as run:
# try custom pyfunc
    model_info = mlflow.transformers.log_model(
        transformers_model=hf_pipe,
        artifact_path=artifact_path,
        registered_model_name=f"hf-{model}",
        input_example=data,
        extra_pip_requirements=requirements,
        task=hftask,
        signature=signature,
      ##  model_config=model_config,
      ##  model_card=architecture,
    )


# COMMAND ----------

import pprint
runinfo=run.to_dictionary()
pprint.pprint(runinfo, sort_dicts=False)

# COMMAND ----------

## https://mlflow.org/docs/latest/model-registry.html
model_uri = f"runs:/{run.info.run_id}/{artifact_path}"
mv = mlflow.register_model(
    model_uri, "Production",
   # alias=["Production","Latest"]
)

print(f"Name: {mv.name}")
print(f"Version: {mv.version}")

alias="lastest"

from mlflow import MlflowClient
##client = MlflowClient()
###client.set_registered_model_alias(mv.name, alias, mv.version)

# get a model version by alias
####latest=client.get_model_version_by_alias(mv.name, alias)




# COMMAND ----------

print(mv)

# COMMAND ----------

# MAGIC %md
# MAGIC from mlflow import MlflowClient
# MAGIC
# MAGIC # Initialize an MLflow Client
# MAGIC client = MlflowClient()
# MAGIC
# MAGIC
# MAGIC def assign_alias_to_stage(model_name, stage, alias):
# MAGIC     """
# MAGIC     Assign an alias to the latest version of a registered model within a specified stage.
# MAGIC
# MAGIC     :param model_name: The name of the registered model.
# MAGIC     :param stage: The stage of the model version for which the alias is to be assigned. Can be
# MAGIC                 "Production", "Staging", "Archived", or "None".
# MAGIC     :param alias: The alias to assign to the model version.
# MAGIC     :return: None
# MAGIC     """
# MAGIC     latest_mv = client.get_latest_versions(model_name, stages=[stage])[0]
# MAGIC     client.set_registered_model_alias(model_name, alias, latest_mv.version)
# MAGIC

# COMMAND ----------


loaded = mlflow.pyfunc.load_model(model_uri)
testoutput=loaded.predict(data) #[0].split(',')
print(testoutput)
