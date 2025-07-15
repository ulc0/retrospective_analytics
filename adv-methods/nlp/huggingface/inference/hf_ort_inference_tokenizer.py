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
mlflow_run_id=mlflow.start_run(experiment_id=EXPERIMENT_ID)

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


dbutils.widgets.text('input_table_name' , defaultValue="edav_dev_cdh.cdh_mimic_ra.note_sentences_sample")
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
import torch
device = 0 if torch.cuda.is_available() else -1
if (device==-1):
    bsize=1
print(bsize)
print(device)
print(compute_framework)
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

# MAGIC %md
# MAGIC # Use a pipeline as a high-level helper
# MAGIC from transformers import pipeline
# MAGIC
# MAGIC pipe = pipeline("token-classification", model="bioformers/bioformer-8L-ncbi-disease")

# COMMAND ----------


hf_pipe = (
    transformers.pipeline(task=hftask,
             model=architecture,
             aggregation_strategy=aggregation,
             framework=compute_framework,
             device=device,
# what happens when we skip the tokenizer?             
             tokenizer = tokenizer
             )
) 
"""

# Define a simple input example that will be recorded with the model in MLflow, giving
# users of the model an indication of the expected input format.
input_example = ["hypertension", "cirrhosis", "diabetes"]
# Define the parameters (and their defaults) for optional overrides at inference time.
parameters = {"max_length": 512 } #, "do_sample": False, "temperature": 0.4}

signature = mlflow.models.infer_signature(
    input_example,
    mlflow.transformers.generate_signature_output(hf_pipe, input_example),
    parameters,
)
with mlflow.start_run(experiment_id=EXPERIMENT_ID):
    model_info = mlflow.transformers.log_model(
        transformers_model=hf_pipe,
        artifact_path="model",
        input_example=input_example,
        signature=signature,
        # Transformer model does not use Pandas Dataframe as input, internal input type conversion should be skipped.
        example_no_conversion=True,
        # Uncomment the following line to save the model in 'reference-only' mode:
        save_pretrained=False,
    )
"""

# COMMAND ----------

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.functions import pandas_udf

@pandas_udf('array<struct<word string, entity_group string, score float, start integer, end integer>>')
def ner_udf(texts: pd.Series) -> pd.Series:
    return pd.Series(hf_pipe(texts.to_list(), batch_size=bsize))


# COMMAND ----------

#from pyspark.resource import TaskResourceRequests, ResourceProfileBuilder

#task_requests = TaskResourceRequests().resource("gpu", 0.5)

#builder = ResourceProfileBuilder()
#resource_profile = builder.require(task_requests).build

#rdd = df.withColumn('predictions', loaded_model(struct(*map(col, df.columns)))).rdd.withResources(resource_profile)

# COMMAND ----------

#print(hfmodel.predict(data)) #
#output_df = table.withColumn("prediction", loaded_model(struct(*table.columns)))
#display(df[text_col])import pyspark.sql.functions as F
#newdf=df.withColumn('results_exploded',F.explode('results')).select(*outlist)

output_df=df.withColumn('results', ner_udf(df[text_col],))
display(output_df)

# COMMAND ----------

output_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f"{output_table}_raw")

# COMMAND ----------

# MAGIC %md
# MAGIC https://stackoverflow.com/questions/72400105/how-to-flatten-array-of-struct   
# MAGIC
# MAGIC
# MAGIC """
# MAGIC How to change schema in PySpark from this
# MAGIC
# MAGIC |-- id: string (nullable = true)
# MAGIC |-- device: array (nullable = true)
# MAGIC |    |-- element: struct (containsNull = true)
# MAGIC |    |    |-- device_vendor: string (nullable = true)
# MAGIC |    |    |-- device_name: string (nullable = true)
# MAGIC |    |    |-- device_manufacturer: string (nullable = true)
# MAGIC to this
# MAGIC
# MAGIC |-- id: string (nullable = true)
# MAGIC |-- device_vendor: string (nullable = true)
# MAGIC |-- device_name: string (nullable = true)
# MAGIC |-- device_manufacturer: string (nullable = true)
# MAGIC """
# MAGIC ```
# MAGIC df_flat = df.withColumn('device_exploded', F.explode('device'))
# MAGIC             .select('id', 'device_exploded.*')
# MAGIC
# MAGIC df_flat.printSchema()
# MAGIC ```

# COMMAND ----------
outlist=keylist+['results_exploded.*']

renames={"word":"snippet",
         "entity_group":"nlp_category",
         "score":"score",
         "start":"offset",
         "end":"offset_end",         }
keylist=['note_id','note_sent',]
name="results"
note_nlp=output_df.withColumn(f"{name}_exploded",F.explode(name)).select("note_id","note_sent",f"{name}_exploded.*").withColumnsRenamed(renames).withColumn("nlp_system",F.lit(architecture))
display(note_nlp)


# COMMAND ----------

# testing how tokenizer compares with auxiliary call
note_nlp.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(output_table)

