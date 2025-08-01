# Databricks notebook source
# MAGIC %md
# MAGIC [Using complex return types transformer ner on spark](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/train-model/huggingface/model-inference-nlp)  
# MAGIC [Tune GPU Performance](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/train-model/huggingface/model-inference-nlp)  

# COMMAND ----------

# MAGIC %pip freeze

# COMMAND ----------

dbutils.widgets.text('EXPERIMENT_ID',defaultValue='1904172122826432')
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

dbutils.widgets.text('bsize',defaultValue='1',label="Training Batch Size")
bsize=int(dbutils.widgets.get('bsize'))

# COMMAND ----------

dbutils.widgets.text('model_name' , defaultValue= "d4data/biomedical-ner-all",label="Huggingface Hub Name")
architecture = dbutils.widgets.get("model_name")
print(architecture)

# COMMAND ----------


dbutils.widgets.text('hftask'  , defaultValue="ner")
hftask = dbutils.widgets.get("hftask")

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

dbutils.widgets.dropdown('aggregation'  , defaultValue="simple",choices=['none','simple','first','average','max'],label="Aggregation Strategy")
aggregation = dbutils.widgets.get("aggregation")

dbutils.widgets.text('stride', defaultValue="")
param_model_stride = dbutils.widgets.get("stride")
return_overflowing=False
model_stride=None
if param_model_stride:
    model_stride=int(param_model_stride)
    return_overflowing=True

# max_length (int, optional) — Controls the maximum length to use by 
# one of the truncation/padding parameters.
# If left unset or set to None, this will use 
# the predefined model maximum length if a maximum length is 
# required by one of the truncation/padding parameters. 
# If the model has no specific maximum input length (like XLNet) 
# truncation/padding to a maximum length will be deactivated.
max_length=None
dbutils.widgets.text('max_length'  , defaultValue="")
# revisit the falsy syntax for this
set_max_length = dbutils.widgets.get("max_length")
if set_max_length:
    max_length=int(set_max_length)


# COMMAND ----------


dbutils.widgets.dropdown('compute_framework'  , defaultValue="pt", choices=['pt','tf'], label="Compute Framework")
compute_framework = dbutils.widgets.get("compute_framework")

# COMMAND ----------


dbutils.widgets.text('input_table_name' , defaultValue="edav_prd_cdh.cdh_abfm_phi_exploratory.note_sentences")
input_table_name = dbutils.widgets.get("input_table_name")
dbutils.widgets.text('text_col' , defaultValue="clean_text")
#text_col="note_text"
text_col = dbutils.widgets.get("text_col")


# COMMAND ----------

print(architecture)
suffix=architecture.split('/')[1].replace('-',"_")
print(suffix)

# COMMAND ----------



#odef=f"{input_table_name}_{suffix}"
dbutils.widgets.text('output_table_name', defaultValue=input_table_name+f"_{suffix}")
output_table = dbutils.widgets.get("output_table_name")
print(output_table)


# COMMAND ----------

keylist=['note_id','note_sent']

# COMMAND ----------

# load table as a Spark DataFrame this should already be done in practice
df = spark.table(input_table_name).select(keylist+[text_col])
print(f"nrecs: {df.count()}")
# optionally, perform additional data processing (may be necessary to conform the schema)


# COMMAND ----------

# MAGIC %md ## Load model and run inference
# MAGIC **Note**: If the model does not return double values, override `result_type` to the desired type.

# COMMAND ----------

# MAGIC %md
# MAGIC # Load model directly
# MAGIC from transformers import AutoTokenizer, AutoModelForTokenClassification
# MAGIC
# MAGIC tokenizer = AutoTokenizer.from_pretrained("bioformers/bioformer-8L-ncbi-disease")
# MAGIC model = AutoModelForTokenClassification.from_pretrained("bioformers/bioformer-8L-ncbi-disease")

# COMMAND ----------

import transformers
#from transformers import pipeline
#import torch
#device = 0 if torch.cuda.is_available() else -1
#if (device==-1):
#    bsize=1
#print(bsize)
#print(device)
#print(compute_framework)
# Load model directly
#from transformers import AutoTokenizer, AutoModelForTokenClassification
#tokenizer = AutoTokenizer.from_pretrained(architecture)
#https://huggingface.co/docs/transformers/v4.46.0/en/internal/tokenization_utils#transformers.PreTrainedTokenizerBase.__call__
#older docs https://huggingface.co/transformers/v2.11.0/model_doc/auto.html?highlight=autotokenizer#autotokenizer
tokenizer = transformers.AutoTokenizer.from_pretrained(architecture,
    use_fast=True,
# stride (int, optional, defaults to 0) — 
# If set to a number along with max_length, 
# the overflowing tokens returned when return_overflowing_tokens=True 
# will contain some tokens from the end of the truncated sequence 
# returned to provide some overlap between truncated and 
# overflowing sequences. The value of this argument defines the 
# number of overlapping tokens
#https://huggingface.co/alvaroalon2/biobert_diseases_ner/discussions/2
# model_max_length, return_overflowing_tokens and stride work together
##    model_max_length=max_length, 
##    stride =model_stride,  
##    return_overflowing_tokens=return_overflowing, 
#https://huggingface.co/docs/transformers/v4.46.0/en/internal/tokenization_utils#transformers.PreTrainedTokenizerBase.encode_plus.truncation
    truncation=False, 
#is_split_into_words (bool, optional, defaults to False) — 
# Whether or not the input is already pre-tokenized (e.g., split into words). 
# If set to True, the tokenizer assumes the input is already split into words (for instance, by splitting it on whitespace) 
# which it will tokenize. This is useful for NER or token classification.
    is_split_into_words=False)
#model = AutoModelForTokenClassification.from_pretrained(architecture)
print(tokenizer)

# COMMAND ----------

tokenizer.save_vocabulary('.')
#word_ids=tokenizer.word_ids()
#print(word_ids)

