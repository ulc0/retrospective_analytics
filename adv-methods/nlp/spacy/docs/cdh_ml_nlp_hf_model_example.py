# Databricks notebook source

import mlflow
import mlflow.spacy
from mlflow import log_metric, log_param, log_params, log_artifacts, log_dict
from mlflow.models import infer_signature
#############################################################
experiment_id=dbutils.jobs.taskValues.get('cdh-ml-model','experiment_id',debugValue='1441353104968016')
dbutils.widgets.text('task_key','cdh-ml-nlp-hf-model-en_core_sci_scibert')
dbutils.widgets.text('run_id','gpu_test')

task_name=dbutils.widgets.get("task_key")   ##{{task_key}}
run_id=dbutils.widgets.get("run_id") #{{run_id}}
model_name=task_name.split('-')[-1]
lnk_name="umls"

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks recommends using [Pandas UDFs](https://www.databricks.com/blog/2020/05/20/new-pandas-udfs-and-python-type-hints-in-the-upcoming-release-of-apache-spark-3-0.html) to apply the pipeline. Spark sends batches of data to Pandas UDFs and uses `arrow` for data conversion. Receiving a batch in the UDF allows you to batch operations in the pipeline. Note that the `batch_size` in the pipeline is unlikely to be performant with the default batch size that Spark sends the UDF. Databricks recommends trying various batch sizes for the pipeline on your cluster to find the best performance. Read more about pipeline batching in [Hugging Face documentation](https://huggingface.co/docs/transformers/main_classes/pipelines#pipeline-batching).
# MAGIC
# MAGIC You can view GPU utilization on the cluster by navigating to the [live cluster metrics](https://docs.databricks.com/clusters/clusters-manage.html#metrics), clicking into a particular worker, and viewing the GPU metrics section for that worker. 
# MAGIC
# MAGIC Wrapping the pipeline with [tqdm](https://tqdm.github.io/) allows you to view progress of a particular task. Navigate into the [task details page](https://docs.databricks.com/clusters/debugging-spark-ui.html#task-details-page) and view the `stderr` logs.

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf
from tqdm.auto import tqdm

@pandas_udf('')
def summarize_batch_udf(texts: pd.Series) -> pd.Series:
  pipe = tqdm(summarizer(texts.to_list(), truncation=True, batch_size=8), total=len(texts), miniters=10)
  summaries = [summary['summary_text'] for summary in pipe]
  return pd.Series(summaries)

# COMMAND ----------

# MAGIC %md
# MAGIC Using the UDF is identical to using other UDFs on Spark.  For example, you can use it in a `select` statement to create a column with the results of the model inference.

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}")
summaries = sample.select(sample.title, sample.text, summarize_batch_udf(sample.text).alias("summary"))
summaries.write.saveAsTable(f"{output_schema}.{output_table}", mode="overwrite")

# COMMAND ----------

display(spark.sql(f"SELECT * FROM {output_schema}.{output_table} LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC #MLflow wrapping
# MAGIC Storing a pre-trained model as an MLflow model makes it even easier to deploy a model for batch or real-time inference. This also allows model versioning through the Model Registry, and simplifies model loading code for your inference workloads. 
# MAGIC
# MAGIC The first step is to create a custom model for your pipeline, which encapsulates loading the model, initializing the GPU usage, and inference function. 
# MAGIC

# COMMAND ----------

import mlflow

class SummarizationPipelineModel(mlflow.pyfunc.PythonModel):
  def load_context(self, context):
    device = 0 if torch.cuda.is_available() else -1
    self.pipeline = pipeline("summarization", context.artifacts["pipeline"], device=device)
    
  def predict(self, context, model_input): 
    texts = model_input.iloc[:,0].to_list() # get the first column
    pipe = tqdm(self.pipeline(texts, truncation=True, batch_size=8), total=len(texts), miniters=10)
    summaries = [summary['summary_text'] for summary in pipe]
    return pd.Series(summaries)

# COMMAND ----------

# MAGIC %md
# MAGIC The code closely parallels the code for creating and using a Pandas UDF demonstrated above. One difference is that the pipeline is loaded from a file made available to the MLflow model’s context. This is provided to MLflow when logging a model. 🤗 Transformers pipelines make it easy to save the model to a local file on the driver, which is then passed into the `log_model` function for the MLflow `pyfunc` interfaces. 

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

summaries = sample.select(sample.title, sample.text, loaded_model(sample.text).alias("summary"))
summaries.write.saveAsTable(f"{output_schema}.{output_table}", mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC # Cleanup
# MAGIC Remove the output table this notebook writes results to.

# COMMAND ----------

spark.sql(f"DROP TABLE {output_schema}.{output_table}")
