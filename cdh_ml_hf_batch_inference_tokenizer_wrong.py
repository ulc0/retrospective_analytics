# Databricks notebook source
# MAGIC %md
# MAGIC [Using complex return types transformer ner on spark](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/train-model/huggingface/model-inference-nlp)  
# MAGIC [Tune GPU Performance](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/train-model/huggingface/model-inference-nlp)  

# COMMAND ----------

# MAGIC %pip freeze

# COMMAND ----------

dbutils.widgets.text('EXPERIMENT_ID',defaultValue='4090675059920416')
EXPERIMENT_ID=dbutils.widgets.get('EXPERIMENT_ID')
import mlflow
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

dbutils.widgets.text('model_name' , defaultValue= "alvaroalon2/biobert_diseases_ner",label="Huggingface Hub Name")
architecture = dbutils.widgets.get("model_name")
print(architecture)

# COMMAND ----------


dbutils.widgets.text('hftask'  , defaultValue="token-classification")
hftask = dbutils.widgets.get("hftask")

# COMMAND ----------

"""
aggregation_strategy	(Default: simple). There are several aggregation strategies:
none: Every token gets classified without further aggregation.
simple: Entities are grouped according to the default schema (B-, I- tags get merged when the tag is similar).
first: Same as the simple strategy except words cannot end up with different tags. Words will use the tag of the first token when there is ambiguity.
average: Same as the simple strategy except words cannot end up with different tags. Scores are averaged across tokens and then the maximum label is applied.
max: Same as the simple strategy except words cannot end up with different tags. Word entity will be the token with the maximum score.
"""

dbutils.widgets.dropdown('aggregation'  , defaultValue="simple",choices=['none','simple','first','average','max'],label="Aggregation Strategy")
aggregation = dbutils.widgets.get("aggregation")
print(aggregation)

# COMMAND ----------


dbutils.widgets.dropdown('compute_framework'  , defaultValue="pt", choices=['pt','tf'], label="Compute Framework")
compute_framework = dbutils.widgets.get("compute_framework")

# COMMAND ----------


dbutils.widgets.text('input_table_name' , defaultValue="edav_prd_cdh.cdh_abfm_phi_exploratory.aix_feedback_note_bronze_mpox")
input_table_name = dbutils.widgets.get("input_table_name")
dbutils.widgets.text('text_col' , defaultValue="clean_text")
#text_col="note_text"
text_col = dbutils.widgets.get("text_col")
print(input_table_name)

# COMMAND ----------

print(architecture)
suffix=architecture.split('/')[1].replace('-',"_")
print(suffix)

# COMMAND ----------



odef=f"{input_table_name}_{suffix}"
dbutils.widgets.text('output_table', defaultValue=odef )
output_table = dbutils.widgets.get("output_table")
print(output_table)



# COMMAND ----------

# load table as a Spark DataFrame this should already be done in practice
df = spark.table(input_table_name).select(['note_id',text_col,])
#df = spark.table(input_table_name).limit(100)
# optionally, perform additional data processing (may be necessary to conform the schema)


# COMMAND ----------

#display(df)

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
from transformers import AutoModelForTokenClassification, AutoTokenizer, pipeline
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
tokenizer = AutoTokenizer.from_pretrained(architecture, stride =50, return_overflowing_tokens=True, model_max_length=512, truncation=True, is_split_into_words=True)
#model = AutoModelForTokenClassification.from_pretrained(architecture)


# COMMAND ----------

# MAGIC %md
# MAGIC # Use a pipeline as a high-level helper
# MAGIC from transformers import pipeline
# MAGIC
# MAGIC pipe = pipeline("token-classification", model="bioformers/bioformer-8L-ncbi-disease")

# COMMAND ----------



hf_pipe = (
    pipeline(task=hftask,
             model=architecture,
             aggregation_strategy=aggregation,
             framework=compute_framework,
             device=device,
             stride = 128,
             tokenizer = tokenizer
             )
) 


# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf('array<struct<word string, entity_group string, score float, start integer, end integer>>')
def ner_udf(texts: pd.Series) -> pd.Series:
  return pd.Series(hf_pipe(texts.to_list(), batch_size=bsize))


# COMMAND ----------

from pyspark.resource import TaskResourceRequests, ResourceProfileBuilder

task_requests = TaskResourceRequests().resource("gpu", 0.5)

builder = ResourceProfileBuilder()
resource_profile = builder.require(task_requests).build

#rdd = df.withColumn('predictions', loaded_model(struct(*map(col, df.columns)))).rdd.withResources(resource_profile)

# COMMAND ----------

#print(hfmodel.predict(data)) #
#output_df = table.withColumn("prediction", loaded_model(struct(*table.columns)))
#display(df[text_col])
output_df=df.withColumn('results', ner_udf(df[text_col],))

# COMMAND ----------

#display(output_df)

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

import pyspark.sql.functions as F
renames={"word":"snippet",
         "entity_group":"nlp_category",
         "score":"score",
         "start":"offset"}
note_nlp= (
    output_df
    .withColumn('results_exploded',F.explode('results'))
    .select('note_id','results_exploded.*')
    .withColumnsRenamed(renames)
    .withColumn("nlp_system",F.lit(architecture)) 
    .withColumn("nlp_category", F.lower(F.col("nlp_category"))) # making it lower case for simplicity in the dictionary
)
#.filter("isNotNull(nlp_category)")

display(note_nlp)

# COMMAND ----------

"""
Oscar: for diease specific, in this case I will be filtering diseases or symtoms based on those speficic group entities and then save the file to the data lake

filter should go in QC, not GPU compute



dict_entities = {
    'biobert_diseases_ner': ['disease'],
    'bioformer_8L_ncbi_disease': ['bio'],
    'biomedical_ner_all': ['sign_symptom', 'disease_disorder'],
    'distilbert_base_uncased_ft_ncbi_disease': ['label_1', 'label_2']
}

import pyspark.sql.functions as F

for key, value in dict_entities.items():
    if key == suffix:
        note_nlp  = (
            note_nlp
            .withColumn("nlp_category", F.lower(F.col("nlp_category")))
            .filter(F.col("nlp_category").isin(value))
            )

display(note_nlp).
"""

# COMMAND ----------

# testing how tokenizer compares with auxiliary call
note_nlp.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(output_table)

