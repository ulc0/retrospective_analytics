# Databricks notebook source
import transformers
print(transformers.__version__)
import mlflow
print(mlflow.__version__)
 
 
task = "summarization"
architecture = "philschmid/distilbart-cnn-12-6-samsum"
model = transformers.BartForConditionalGeneration.from_pretrained(architecture)
tokenizer = transformers.AutoTokenizer.from_pretrained(architecture)
summarizer = transformers.pipeline(
    task=task,
    tokenizer=tokenizer,
    model=model,
)
 
artifact_path = "summarizer"
 
inference_config = {"max_length": 1024, "truncation": True}
 
with mlflow.start_run() as run:
    model_info = mlflow.transformers.log_model(
        transformers_model=summarizer,
        artifact_path=artifact_path,
        registered_model_name="wikipedia-summarizer",
        input_example="Hi there!",
        inference_config=inference_config,
    )

# COMMAND ----------

output_schema = "edav_dev_cdh.cdh_ml"
output_table = "wikipedia_summaries"
number_articles = 1024

# COMMAND ----------

df = spark.read.parquet("/databricks-datasets/wikipedia-datasets/data-001/en_wikipedia/articles-only-parquet").select("title", "text")
display(df)
sample_imbalanced = df.limit(number_articles)
sample = sample_imbalanced.repartition(32).persist()
sample.count()

# COMMAND ----------

from transformers import pipeline
import torch
device = 0 if torch.cuda.is_available() else -1


# COMMAND ----------

model_uri = model_info.model_uri
 
# Load model as a Spark UDF. Override result_type if the model does not return double values.
loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri=model_uri, result_type='string')
 
summaries = sample.select(sample.title, sample.text, loaded_model(sample.text).alias("summary"))
summaries.write.saveAsTable(f"{output_schema}.{output_table}", mode="overwrite")

