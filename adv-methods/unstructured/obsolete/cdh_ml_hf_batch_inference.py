# Databricks notebook source
# MAGIC %md
# MAGIC [Using complex return types transformer ner on spark](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/train-model/huggingface/model-inference-nlp)

# COMMAND ----------

#%pip freeze

# COMMAND ----------

import mlflow
from pyspark.sql.functions import struct, col, when
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
# MAGIC alvaroalon2/biobert_diseases_ner · Hugging Face · 512 tokens
# MAGIC
# MAGIC bioformers/bioformer-8L-ncbi-disease · Hugging Face · 512 tokens
# MAGIC
# MAGIC sarahmiller137/distilbert-base-uncased-ft-ncbi-disease · Hugging Face · 512 Tokens
# MAGIC
# MAGIC d4data/biomedical_ner_all · Hugging Face · 512 Tokens
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


#dbutils.widgets.text('input_table_name' , defaultValue="edav_prd_cdh.cdh_abfm_phi_exploratory.ml_fs_abfm_test_notes")
dbutils.widgets.text('input_table_name' , defaultValue="edav_prd_cdh.cdh_abfm_phi_exploratory.ft_abfm_notes_text")
input_table_name = dbutils.widgets.get("input_table_name")


# COMMAND ----------

print(architecture)
suffix=architecture.split('/')[1].replace('-',"_")
print(suffix)

# COMMAND ----------



odef=f"{input_table_name}_{suffix}"
#dbutils.widgets.text('output_table', defaultValue=f"edav_prd_cdh.cdh_abfm_phi_exploratory.ml_fs_abfm_test_notes_hf_{suffix}")
dbutils.widgets.text('output_table', defaultValue=f"edav_prd_cdh.cdh_abfm_phi_exploratory.ft_abfm_notes_text_{suffix}")
output_table = dbutils.widgets.get("output_table")
print(output_table)
#text_col="note_string"
text_col="note_text"
output_col="results_"+suffix

input_cols=['person_id',
'note_datetime',
'group',
'provider_id',
]

# COMMAND ----------

# load table as a Spark DataFrame this should already be done in practice
df = spark.table(input_table_name)

# COMMAND ----------


dfcols=list(df.columns)
res=[c for c in dfcols if 'results_' in c ]
print(res)
input_cols=['person_id',
'note_datetime',
'group',
'provider_id',
]+res


# COMMAND ----------

# MAGIC %md ## Load model and run inference
# MAGIC **Note**: If the model does not return double values, override `result_type` to the desired type.

# COMMAND ----------


import transformers
from transformers import AutoModelForTokenClassification, AutoTokenizer, pipeline
import torch
device = 0 if torch.cuda.is_available() else -1
print(device)
print(compute_framework)
# stride controls the overflow when tokens are > 512 as sliding window
# https://stackoverflow.com/questions/76342339/how-can-i-handle-overflowing-tokens-in-huggingface-transformer-model
# https://stackoverflow.com/questions/77579658/pretrained-model-with-stride-doesn-t-predict-long-text

# Modified pipe with tokenizer, pipe with stride = 128 did not work, got same output error...
t = AutoTokenizer.from_pretrained(architecture, stride =50, return_overflowing_tokens=True, model_max_length=512, truncation=True, is_split_into_words=True)
hf_pipe = pipeline(task=hftask, model=architecture,aggregation_strategy=aggregation,framework=compute_framework,device=device, stride = 128, tokenizer = t) 

#Original pipe
#hf_pipe = pipeline(task=hftask, model=architecture,aggregation_strategy=aggregation,framework=compute_framework,device=device) 

dict_entities = {
    'biobert_diseases_ner': ['disease'],
    'bioformer_8L_ncbi_disease': ['bio'],
    'biomedical_ner_all': ['sign_symptom', 'disease_disorder'],
    'distilbert_base_uncased_ft_ncbi_disease': ['label_1', 'label_2']
}



# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf
threshold=0.95
"""
[{
    "word": "Heart Rate"
    "entity_group": "0",
    "score": 0.99999905,
    "start": 1,
    "end": 688
}]
"""
@pandas_udf('array<struct<word string, entity_group string, score float, start integer, end integer>>')
def ner_udf(texts: pd.Series) -> pd.Series:
  results=hf_pipe(texts.to_list(),batch_size=1)
#  xresults=[p for p in results if p["score"]>=0.95]
  return pd.Series(results)


# COMMAND ----------

#output_df_0=df.withColumn(output_col, ner_udf(df[text_col],))
#output_df_0.display()

output_df=df.withColumn(output_col, ner_udf(df[text_col],))
output_df.display()


# COMMAND ----------

"""
from pyspark.sql.functions import explode, lower

for key, value in dict_entities.items():
    if key == suffix:
        output_df = (
            output_df_0
            .withColumn(f"exploded_ent_"+suffix, explode(str(output_col)))
            .select("*",
                    (lower((f"exploded_ent_{suffix}.entity_group"))).alias(f"entity_group_{suffix}"),
                    (lower(f"exploded_ent_{suffix}.word")).alias(f"word_{suffix}"), 
                    (lower(f"exploded_ent_{suffix}.score")).alias(f"score_{suffix}")
            )
            .withColumn(f"selected_feature_{suffix}",
                        when(
                            ((col(f"score_{suffix}")>=0.95) &
                            (col(f"entity_group_{suffix}").isin(value))), col(f"word_{suffix}")
                            )
                            .otherwise(None)
                        )            
            )

display(output_df)
"""

# COMMAND ----------

print(output_table)
output_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(output_table)


# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC select * from edav_prd_cdh.cdh_abfm_phi_exploratory.ft_abfm_notes_hf

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from edav_prd_cdh. cdh_abfm_phi_exploratory.ft_abfm_notes_hf
