# Databricks notebook source
#%pip install spark-nlp==5.1.3
#%pip install spark-nlp==4.3.2

# COMMAND ----------

#pyspark --packages com.johnsnowlabs.nlp:spark-nlp_2.12:5.1.3

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

from pyspark.sql.types import StringType,  ArrayType
from pyspark.sql.functions import pandas_udf, udf, PandasUDFType

spark.conf.set('spark.sql.shuffle.partitions',7200*4)

# COMMAND ----------

#temporarily here the spark nlp

# COMMAND ----------



#spark.sql("SHOW DATABASES").show(truncate=False)
#import pandas as pd
from pyspark.sql.window import Window
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import isnan, when, count, col, desc, asc, row_number, concat_ws, coalesce,to_timestamp,regexp_replace, to_date, date_format, concat, lower, udf
import pyspark.sql.functions as F
from functools import reduce
from pyspark.sql import DataFrame
#   RTF from striprtf.striprtf import rtf_to_text

spark.conf.set('spark.sql.shuffle.partitions',7200*4)
spark.conf.set("spark.databricks.io.cache.enabled", True)

import pandas as pd
from pyspark.sql.window import Window
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, when, count, col, desc, asc, row_number, concat_ws, coalesce,to_timestamp,regexp_replace, to_date, date_format, concat
import pyspark.sql.functions as F
from functools import reduce
from pyspark.sql import DataFrame
#setup sparknlp https://sparknlp.org/api/python/reference/autosummary/sparknlp/annotator/sentence/sentence_detector/index.html


# COMMAND ----------

dbschema=f"cdh_abfm_phi_exploratory"
treated_notes = spark.table(f"{dbschema}.ml_nlp_mpox_notes_presentation_sentences_v2")

# COMMAND ----------

import pandas as pd
import torch

from transformers import pipeline
from transformers import AutoTokenizer, AutoModelForTokenClassification


device = 0 if torch.cuda.is_available() else -1

tokenizer = AutoTokenizer.from_pretrained("d4data/biomedical-ner-all")
model = AutoModelForTokenClassification.from_pretrained("d4data/biomedical-ner-all")
pipe = pipeline("ner", model=model, tokenizer=tokenizer, aggregation_strategy="average", device = device) # pass device=0 if using gpu



@pandas_udf('array<struct<word string, entity_group string, score float, start integer, end integer>>')
def ner_udf(texts: pd.Series) -> pd.Series:
  return pd.Series(pipe(texts.to_list(), batch_size=1))


output_agg_avg = (
    treated_notes.select("*", ner_udf(treated_notes.note_string).alias('entities'))
    .withColumn("exploded_ent", F.explode(F.col("entities")))
    .select("*",(F.lower(F.col("exploded_ent.entity_group"))).alias("entity_group"), F.col("exploded_ent.word"), F.col("exploded_ent.score"))    
)

#display(output_agg_avg)


# COMMAND ----------

dbschema=f"cdh_abfm_phi_exploratory"
#output_agg_avg.write.mode("overwrite").format("delta").saveAsTable(f"{dbschema}.ml_nlp_mpox_notes_presentation_nerall_entities_v2")

# COMMAND ----------

output_agg_avg = (
    treated_notes.select("id", 'entities')
    .withColumn("exploded_ent", F.explode(F.col("entities")))
    .select("*",(F.lower(F.col("exploded_ent.entity_group"))).alias("entity_group"), F.col("exploded_ent.word"), F.col("exploded_ent.score"))    
)

