# Databricks notebook source
# MAGIC %pip install transformers
# MAGIC

# COMMAND ----------

# MAGIC %pip install tensorflow

# COMMAND ----------

# MAGIC %pip install torch

# COMMAND ----------

#%pip3 install torch torchvision torchaudio

# COMMAND ----------

# MAGIC %pip install tf-keras

# COMMAND ----------

#spark.sql("SHOW DATABASES").show(truncate=False)
import pandas as pd
from datetime import date
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import isnan, when, count, col, desc, asc, row_number, concat_ws, coalesce,to_timestamp,regexp_replace, to_date, date_format, concat
from pyspark.sql.functions import pandas_udf
import pyspark.sql.functions as F
from pyspark.sql.window import Window

from functools import reduce

#from pyspark.sql.types import StringType,  ArrayType
from pyspark.sql.functions import pandas_udf, udf, PandasUDFType

#import sparknlp
#from sparknlp.base import *
#from sparknlp.annotator import *

#from sparknlp.common import *
#from sparknlp.pretrained import ResourceDownloader

spark.conf.set('spark.sql.shuffle.partitions',7200*4)

# COMMAND ----------

cleaned_notes = (
    spark.table("edav_prd_cdh.cdh_abfm_phi_exploratory.ml_fs_abfm_notes_text").limit(10)
)

#display(cleaned_notes)

# COMMAND ----------

import pandas as pd
import torch

from transformers import pipeline
from transformers import AutoTokenizer, AutoModelForTokenClassification


device = 0 if torch.cuda.is_available() else -1

tokenizer = AutoTokenizer.from_pretrained("d4data/biomedical-ner-all")
model = AutoModelForTokenClassification.from_pretrained("d4data/biomedical-ner-all")
pipe = pipeline("ner", model=model, tokenizer=tokenizer, aggregation_strategy="average", device = device) # pass device=0 if using gpu
#model = "d4data/biomedical-ner-all"

# Use a pipeline as a high-level helper
#pipe = pipeline("ner", model="d4data/biomedical-ner-all", aggregation_strategy="average", device = 1)

@pandas_udf('array<struct<word string, entity_group string, score float, start integer, end integer>>')
def ner_udf(texts: pd.Series) -> pd.Series:
  return pd.Series(pipe(texts.to_list(), batch_size=1))


output_agg_avg = (
    cleaned_notes.select("*", ner_udf(cleaned_notes.note_string).alias('entities'))
    .withColumn("exploded_ent", F.explode(F.col("entities")))
    .select("*",(F.lower(F.col("exploded_ent.entity_group"))).alias("entity_group"), F.col("exploded_ent.word"), F.col("exploded_ent.score"))    
)

display(output_agg_avg)
