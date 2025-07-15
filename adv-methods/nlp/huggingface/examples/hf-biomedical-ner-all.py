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

# MAGIC %md
# MAGIC The following sections use the `transformers` pipeline for summarization using the `sshleifer/distilbart-cnn-12-6` model. The pipeline is used within the `pandas_udf` applied to the Spark DataFrame. 
# MAGIC
# MAGIC [Pipelines](https://huggingface.co/docs/transformers/main/en/main_classes/pipelines) conveniently wrap best practices for certain tasks, bundling together tokenizers and models. They can also help with batching data sent to the GPU, so that you can perform inference on multiple items at a time. Setting the `device` to 0 causes the pipeline to use the GPU for processing. You can use this setting reliably even if you have multiple GPUs on each machine in your Spark cluster. Spark automatically reassigns GPUs to the workers.
# MAGIC
# MAGIC You can also directly load tokenizers and models if needed; you would just need to reference and invoke them directly in the UDF.

# COMMAND ----------

# MAGIC %md
# MAGIC from transformers import pipeline
# MAGIC import torch
# MAGIC device = 0 if torch.cuda.is_available() else -1
# MAGIC summarizer = pipeline("summarization", model="facebook/bart-large-cnn", device=device)

# COMMAND ----------

# MAGIC %md
# MAGIC Databricks recommends using [Pandas UDFs](https://www.databricks.com/blog/2020/05/20/new-pandas-udfs-and-python-type-hints-in-the-upcoming-release-of-apache-spark-3-0.html) to apply the pipeline. Spark sends batches of data to Pandas UDFs and uses `arrow` for data conversion. Receiving a batch in the UDF allows you to batch operations in the pipeline. Note that the `batch_size` in the pipeline is unlikely to be performant with the default batch size that Spark sends the UDF. Databricks recommends trying various batch sizes for the pipeline on your cluster to find the best performance. Read more about pipeline batching in [Hugging Face documentation](https://huggingface.co/docs/transformers/main_classes/pipelines#pipeline-batching).
# MAGIC
# MAGIC You can view GPU utilization on the cluster by navigating to the [live cluster metrics](https://docs.databricks.com/clusters/clusters-manage.html#metrics), clicking into a particular worker, and viewing the GPU metrics section for that worker. 
# MAGIC
# MAGIC Wrapping the pipeline with [tqdm](https://tqdm.github.io/) allows you to view progress of a particular task. Navigate into the [task details page](https://docs.databricks.com/clusters/debugging-spark-ui.html#task-details-page) and view the `stderr` logs.

# COMMAND ----------

# MAGIC %md
# MAGIC import pandas as pd
# MAGIC from pyspark.sql.functions import pandas_udf
# MAGIC from tqdm.auto import tqdm
# MAGIC
# MAGIC @pandas_udf('string')
# MAGIC def summarize_batch_udf(texts: pd.Series) -> pd.Series:
# MAGIC   pipe = tqdm(summarizer(texts.to_list(), truncation=True, batch_size=8), total=len(texts), miniters=10)
# MAGIC   summaries = [summary['summary_text'] for summary in pipe]
# MAGIC   return pd.Series(summaries)

# COMMAND ----------

# MAGIC %md
# MAGIC Using the UDF is identical to using other UDFs on Spark.  For example, you can use it in a `select` statement to create a column with the results of the model inference.

# COMMAND ----------

# MAGIC %md
# MAGIC spark.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}")
# MAGIC summaries = sample.select(sample.title, sample.text, summarize_batch_udf(sample.text).alias("summary"))
# MAGIC summaries.write.saveAsTable(f"{output_schema}.{output_table}", mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC display(spark.sql(f"SELECT * FROM {output_schema}.{output_table} LIMIT 10"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Use the pipeline on a Pandas DataFrame
# MAGIC
# MAGIC Alternatively, you can use the pipeline summarizer on a `pandas_udf` if you don't want to use Spark.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC import pandas as pd
# MAGIC import transformers
# MAGIC
# MAGIC model_architecture = "sshleifer/distilbart-cnn-12-6"
# MAGIC
# MAGIC summarizer = transformers.pipeline(
# MAGIC     task="summarization", 
# MAGIC     model=transformers.BartForConditionalGeneration.from_pretrained(model_architecture), 
# MAGIC     tokenizer=transformers.BartTokenizerFast.from_pretrained(model_architecture),
# MAGIC     max_length=1024,
# MAGIC     truncation=True
# MAGIC )
# MAGIC
# MAGIC def summarize(text):
# MAGIC     summary = summarizer(text)[0]['summary_text']
# MAGIC     return summary
# MAGIC
# MAGIC sample_pd = sample.toPandas()
# MAGIC sample_pd["summary"] = sample_pd["text"].apply(summarize)
# MAGIC
# MAGIC display(sample_pd)

# COMMAND ----------

# MAGIC %md
# MAGIC ## MLflow wrapping
# MAGIC Storing a pre-trained model as an MLflow model makes it even easier to deploy a model for batch or real-time inference. This also allows model versioning through the Model Registry, and simplifies model loading code for your inference workloads. 
# MAGIC
# MAGIC The first step is to create a custom model for your pipeline, which encapsulates loading the model, initializing the GPU usage, and inference function. 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC from transformers import pipeline  
# MAGIC from transformers import AutoTokenizer, AutoModelForTokenClassification  
# MAGIC
# MAGIC tokenizer = AutoTokenizer.from_pretrained("d4data/biomedical-ner-all")  
# MAGIC model = AutoModelForTokenClassification.from_pretrained("d4data/biomedical-ner-all")  
# MAGIC
# MAGIC pipe = pipeline("ner", model=model, tokenizer=tokenizer, aggregation_strategy="simple") # pass device=0 if using gpu  
# MAGIC pipe("""The patient reported no recurrence of palpitations at follow-up 6 months after the ablation.""")  
# MAGIC

# COMMAND ----------

import mlflow
from mlflow.models import infer_signature
from mlflow.transformers import generate_signature_output
import transformers
from transformers import pipeline
from transformers import AutoTokenizer, AutoModelForTokenClassification

task = "ner"
architecture = "emilyalsentzer/Bio_ClinicalBERT
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

artifact_path = f"{architecture}/ner"

model_config = {"max_length": 1024, "truncation": True}

with mlflow.start_run() as run:
    model_info = mlflow.transformers.log_model(
        transformers_model=entity_detector,
        artifact_path=artifact_path,
        registered_model_name="hf-biomedical-ner-all",
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
