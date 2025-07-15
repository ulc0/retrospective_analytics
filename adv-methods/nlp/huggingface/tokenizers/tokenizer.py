# Databricks notebook source
# MAGIC %pip freeze

# COMMAND ----------

dbutils.widgets.text('EXPERIMENT_ID',defaultValue='4090675059920416')
EXPERIMENT_ID=dbutils.widgets.get('EXPERIMENT_ID')
#https://mlflow.org/docs/latest/llms/transformers/tutorials/text-generation/text-generation.html
import mlflow
import mlflow.transformers
from pyspark.sql.functions import struct, col
mlflow.__version__
mlflow.autolog()
#mlflow_run_id=mlflow.start_run(experiment_id=EXPERIMENT_ID)

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

dbutils.widgets.text('model_name' , defaultValue= "d4data/biomedical-ner-all",label="Huggingface Hub Name")
architecture = dbutils.widgets.get("model_name")
print(architecture)

print(architecture)
suffix=architecture.split('/')[1].replace('-',"_")
print(suffix)


# COMMAND ----------

"""
https://stackoverflow.com/questions/60937617/how-to-reconstruct-text-entities-with-hugging-faces-transformers-pipelines-with
aggregation_strategy	(Default: simple). There are several aggregation strategies:
none: Every token gets classified without further aggregation.
simple: Entities are grouped according to the default schema (B-, I- tags get merged when the tag is similar).
# These work only on word based models (https://huggingface.co/transformers/v4.10.1/_modules/transformers/pipelines/token_classification.html)
first: Same as the simple strategy except words cannot end up with different tags. Words will use the tag of the first token when there is ambiguity.
average: Same as the simple strategy except words cannot end up with different tags. Scores are averaged across tokens and then the maximum label is applied.
max: Same as the simple strategy except words cannot end up with different tags. Word entity will be the token with the maximum score.
"""


# COMMAND ----------


# Load model directly
from transformers import AutoTokenizer #, AutoModelForTokenClassification
#tokenizer = AutoTokenizer.from_pretrained(architecture)
#https://huggingface.co/docs/transformers/v4.46.0/en/internal/tokenization_utils#transformers.PreTrainedTokenizerBase.__call__
#older docs https://huggingface.co/transformers/v2.11.0/model_doc/auto.html?highlight=autotokenizer#autotokenizer

tokenizer = AutoTokenizer.from_pretrained(architecture,
    use_fast=True,
# model_max_length, return_overflowing_tokens and stride work together
#    model_max_length=512, 
#    stride =50, 
#    return_overflowing_tokens=True, 
#https://huggingface.co/docs/transformers/v4.46.0/en/internal/tokenization_utils#transformers.PreTrainedTokenizerBase.encode_plus.truncation
    truncation=False, 
#is_split_into_words (bool, optional, defaults to False) — 
# Whether or not the input is already pre-tokenized (e.g., split into words). 
# If set to True, the tokenizer assumes the input is already split into words (for instance, by splitting it on whitespace) 
# which it will tokenize. This is useful for NER or token classification.
    is_split_into_words=False)
#model = AutoModelForTokenClassification.from_pretrained(architecture)






# COMMAND ----------

tokens=tokenizer.get_vocab()
print(tokens)


# COMMAND ----------

values = list(tokens.items())
print(values)

# COMMAND ----------

vocab=spark.createDataFrame(data=values, schema = ["token","token_id"])
#display(vocab)

# COMMAND ----------

# testing how tokenizer compares with auxiliary call
vocab.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f"edav_prd_cdh.cdh_ml.vocab_{suffix}")

