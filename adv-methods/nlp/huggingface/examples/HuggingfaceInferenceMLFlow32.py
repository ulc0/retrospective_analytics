# Databricks notebook source
# MAGIC %md
# MAGIC Model Inference using Huggingface and MLFlow 3.2  [https://learn.microsoft.com/en-us/azure/databricks/machine-learning/train-model/huggingface/model-inference-nlp]

# COMMAND ----------

#import os
#os.environ['TRANSFORMERS_CACHE'] = '/dbfs/hugging_face_transformers_cache/'

# COMMAND ----------

import pandas as pd
from transformers import pipeline
import torch
from pyspark.sql.functions import pandas_udf

device = 0 if torch.cuda.is_available() else -1
translation_pipeline = pipeline(task="translation_en_to_fr", model="t5-base", device=device)

@pandas_udf('string')
def translation_udf(texts: pd.Series) -> pd.Series:
  translations = [result['translation_text'] for result in translation_pipeline(texts.to_list(), batch_size=1)]
  return pd.Series(translations)

# COMMAND ----------

texts = ["Hugging Face is a French company based in New York City.", "Databricks is based in San Francisco."]
df = spark.createDataFrame(pd.DataFrame(texts, columns=["texts"]))
display(df.select(df.texts, translation_udf(df.texts).alias('translation')))

# COMMAND ----------

from transformers import pipeline
import torch
device = 0 if torch.cuda.is_available() else -1
ner_pipeline = pipeline(task="ner", model="Davlan/bert-base-multilingual-cased-ner-hrl", aggregation_strategy="simple", device=device)
ner_pipeline(texts)

# COMMAND ----------

print(df)

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import pandas_udf

@pandas_udf('array<struct<word string, entity_group string, score float, start integer, end integer>>')
def ner_udf(texts: pd.Series) -> pd.Series:
  return pd.Series(ner_pipeline(texts.to_list(), batch_size=1))

display(df.select(df.texts, ner_udf(df.texts).alias('entities')))
