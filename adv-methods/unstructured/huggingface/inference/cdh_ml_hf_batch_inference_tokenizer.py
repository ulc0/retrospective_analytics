# Databricks notebook source
# MAGIC %md
# MAGIC [Using complex return types transformer ner on spark](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/train-model/huggingface/model-inference-nlp)

# COMMAND ----------

#%pip freeze

# COMMAND ----------

import mlflow
from pyspark.sql.functions import struct, col
mlflow.__version__

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

dbutils.widgets.text('model_name' , defaultValue= "alvaroalon2/biobert_diseases_ner",label="Huggingface Hub Name")
architecture = dbutils.widgets.get("model_name")
print(architecture)

# COMMAND ----------


dbutils.widgets.text('hftask'  , defaultValue="ner")
hftask = dbutils.widgets.get("hftask")

# COMMAND ----------



dbutils.widgets.text('aggregation'  , defaultValue="simple")
aggregation = dbutils.widgets.get("aggregation")

# COMMAND ----------


dbutils.widgets.text('compute_framework'  , defaultValue="pt")
compute_framework = dbutils.widgets.get("compute_framework")

# COMMAND ----------


dbutils.widgets.text('input_table_name' , defaultValue="edav_prd_cdh.cdh_abfm_phi_exploratory.aix_feedback_note_bronze")
input_table_name = dbutils.widgets.get("input_table_name")


# COMMAND ----------

print(architecture)
suffix=architecture.split('/')[1].replace('-',"_")
print(suffix)

# COMMAND ----------



odef=f"{input_table_name}_{suffix}"
dbutils.widgets.text('output_table', defaultValue=odef )
output_table = dbutils.widgets.get("output_table")
print(output_table)
text_col="note_text"
output_col="results_"+suffix

# COMMAND ----------

mpox_cohort = (
    spark.table("edav_prd_cdh.cdh_abfm_phi_exploratory.ml_nlp_mpox_notes") # cohort of interest, includes labels
    .withColumnRenamed("patientuid", "person_id")
    .withColumnRenamed("exposed","label")
    .select("person_id",'label')
    .dropDuplicates()
    .join(        
        spark.table("edav_prd_cdh.cdh_abfm_phi_ra.note_bronze")
        .select(
            'person_id',
            "note_id",
            "note_type",
            'note_datetime',
            "note_title",
            #'note_string'
            )
        ,['person_id']
    )
    .withColumnRenamed("note_title","note_section")
)

#display(mpox_cohort)

# COMMAND ----------

input_table_name

# COMMAND ----------

# load table as a Spark DataFrame this should already be done in practice
df = (
    spark.table(input_table_name)
    
    .withColumnRenamed("clean_text","note_string")
    .join(
        mpox_cohort.drop("person_id","note_type","note_datetime"),['note_id'])
    .select(
        ['person_id',
         text_col,
         'note_type',
         'provider_id',
         'note_datetime',
         'note_section',
         'note_string',
         'label']
        )
)
#df = spark.table(input_table_name).limit(100)
# optionally, perform additional data processing (may be necessary to conform the schema)
#display(df)

# COMMAND ----------

#bronze = spark.table("edav_prd_cdh.cdh_abfm_phi_ra.note_bronze")
#silver = spark.table("edav_prd_cdh.cdh_abfm_phi_ra.note_silver")

#b_s= bronze.join(silver.select("note_id"),['note_id'])

#display(b_s)



# COMMAND ----------

# MAGIC %md ## Load model and run inference
# MAGIC **Note**: If the model does not return double values, override `result_type` to the desired type.

# COMMAND ----------


import transformers
from transformers import pipeline
import torch
device = 0 if torch.cuda.is_available() else -1
print(device)
print(compute_framework)
# Load model directly
from transformers import AutoTokenizer, AutoModelForTokenClassification
tokenizer = AutoTokenizer.from_pretrained(str(architecture))
model = AutoModelForTokenClassification.from_pretrained(str(architecture))

hf_pipe = (
    pipeline(task=hftask,
             model=model,
             aggregation_strategy=aggregation,
             framework=compute_framework,
             device=device,
             tokenizer = tokenizer
             )
) 


# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf('array<struct<word string, entity_group string, score float, start integer, end integer>>')
def ner_udf(texts: pd.Series) -> pd.Series:
  return pd.Series(hf_pipe(texts.to_list(), batch_size=1))


# COMMAND ----------

#print(hfmodel.predict(data)) #
#output_df = table.withColumn("prediction", loaded_model(struct(*table.columns)))
display(df[text_col])
output_df=df.withColumn(output_col, ner_udf(df[text_col],))

# COMMAND ----------

# testing how tokenizer compares with auxiliary call
output_df.display()

# COMMAND ----------

#output_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(output_table)

