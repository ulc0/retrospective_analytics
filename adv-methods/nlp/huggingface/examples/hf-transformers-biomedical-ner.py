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

#import os
#os.environ['TRANSFORMERS_CACHE'] = '/dbfs/hugging_face_transformers_cache/'

# COMMAND ----------




# COMMAND ----------

# MAGIC %md
# MAGIC #MLflow wrapping
# MAGIC Storing a pre-trained model as an MLflow model makes it even easier to deploy a model for batch or real-time inference. This also allows model versioning through the Model Registry, and simplifies model loading code for your inference workloads. 
# MAGIC
# MAGIC The first step is to create a custom model for your pipeline, which encapsulates loading the model, initializing the GPU usage, and inference function. 
# MAGIC

# COMMAND ----------

import mlflow
import torch
from tqdm.auto import tqdm
from transformers import pipeline
from transformers import AutoTokenizer, AutoModelForTokenClassification
import pandas as pd


class HfTokenizerPipelineModel(mlflow.pyfunc.PythonModel):
  def load_context(self, context):
    device = 0 if torch.cuda.is_available() else -1
    tokenizer = AutoTokenizer.from_pretrained("d4data/biomedical-ner-all")
    model = AutoModelForTokenClassification.from_pretrained("d4data/biomedical-ner-all")
#    self.pipeline = pipeline("summarization", context.artifacts["pipeline"], device=device)
    self.pipeline = pipeline("ner", model=model, tokenizer=tokenizer, aggregation_strategy="simple",device=device)
    
  def predict(self, context, model_input): 
    texts = model_input.iloc[:,0].to_list() # get the first column
    pipe = tqdm(self.pipeline(texts, truncation=True, batch_size=8), total=len(texts), miniters=10)
    #summaries = [summary['summary_text'] for summary in pipe]
    tokens=[]
    return pd.Series(tokens)

# COMMAND ----------

# MAGIC %md
# MAGIC The code closely parallels the code for creating and using a Pandas UDF demonstrated above. One difference is that the pipeline is loaded from a file made available to the MLflow model’s context. This is provided to MLflow when logging a model. 🤗 Transformers pipelines make it easy to save the model to a local file on the driver, which is then passed into the `log_model` function for the MLflow `pyfunc` interfaces. 
# MAGIC
# MAGIC #### what do we do here?? pipe("""The patient reported no recurrence of palpitations at follow-up 6 months after the ablation.""")


# COMMAND ----------

pipeline_path = "pipeline"

summarizer.save_pretrained(pipeline_path)
with mlflow.start_run() as run:        
  mlflow.pyfunc.log_model(artifacts={'pipeline': pipeline_path}, artifact_path="summarization_model", python_model=SummarizationPipelineModel())

# COMMAND ----------

# MAGIC %md
# MAGIC # MLflow scoring
# MAGIC MLflow provides an easy interface to load any logged or registered model into a spark UDF. You can look up a model URI from the Model Registry or logged experiment run UI. 

# COMMAND ----------

import mlflow
logged_model_uri = f"runs:/{run.info.run_id}/summarization_model"

# Load model as a Spark UDF. Override result_type if the model does not return double values.
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=logged_model_uri, result_type='string')

