# Databricks notebook source
# MAGIC %md
# MAGIC [Using complex return types transformer ner on spark](https://learn.microsoft.com/en-us/azure/databricks/machine-learning/train-model/huggingface/model-inference-nlp)

# COMMAND ----------

# MAGIC %pip install /Volumes/edav_dev_cdh/cdh_ml/compute/packages/wheels/en_core_sci_lg-0.5.4.tar.gz
# MAGIC %pip freeze

# COMMAND ----------

import mlflow
from pyspark.sql.functions import struct, col
mlflow.__version__

# COMMAND ----------

dbutils.widgets.text('MODEL_NAME' , defaultValue= "en_core_sci_lg")
MODEL_NAME = dbutils.widgets.get("MODEL_NAME")

dbutils.widgets.text('LINKER_NAME' , defaultValue= "umls")
LINKER_NAME = dbutils.widgets.get("LINKER_NAME")

dbutils.widgets.text('INPUT_TABLE_NAME' , defaultValue="edav_dev_cdh.cdh_mimic_ra.note_clean")
INPUT_TABLE_NAME = dbutils.widgets.get("INPUT_TABLE_NAME")

dbutils.widgets.text('OUTPUT_TABLE' , defaultValue= f"edav_dev_cdh.cdh_mimic_ra.note_clean_{MODEL_NAME}")
OUTPUT_TABLE = dbutils.widgets.get("OUTPUT_TABLE")

dbutils.widgets.text('TEXT_COL' , defaultValue= "note_clean")
TEXT_COL = dbutils.widgets.get("TEXT_COL")

#text_col="note_text"
output_col=f"results_{MODEL_NAME}_{LINKER_NAME}"

# COMMAND ----------

df = spark.table(INPUT_TABLE_NAME)

# COMMAND ----------

# load table as a Spark DataFrame this should already be done in practice

#.select(['person_id',
#'note_type',
#'provider_id','note_datetime',
#'note_section','note_text'])
#df = spark.table(input_table_name).limit(100)
# optionally, perform additional data processing (may be necessary to conform the schema)
table = df.limit(100).select(TEXT_COL)
display(table)

# COMMAND ----------

# MAGIC %md ## Load model and run inference
# MAGIC **Note**: If the model does not return double values, override `result_type` to the desired type.

# COMMAND ----------

import spacy

# COMMAND ----------

# https://sujitpal.blogspot.com/2020/08/disambiguating-scispacy-umls-entities.html
nlp = spacy.load(f"{MODEL_NAME}", exclude=["tok2vec", "tagger", "parser", "attribute_ruler", "lemmatizer"])
print(nlp.pipe_names)

# COMMAND ----------

test="pv: tzm ekg report: heart rate: 64 bpm p - r wave interval: 194 msec qt interval: 380 msec corrected qt interval: 387 msec qrs width: 88 msec p wave frontal axis: 74 ° qrs complex frontal axis: 51 ° t wave frontal axis: 72 ° interpretation: sinus rhythm - negative precordial t-waves ."
doc=nlp(test)
print(doc)

# COMMAND ----------


import pandas as pd
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import ArrayType, StringType

@pandas_udf('array<struct<snippet string, entity_group string, score float, start integer, end integer>>')
def umls_udf(texts: pd.Series) -> pd.Series:
  return pd.Series(nlp(texts.to_list(), batch_size=1))


### NOT Reliable
"""
@pandas_udf(ArrayType(StringType()))
def entities(list_of_text: pd.Series) -> pd.Series:
    doc=nlp(list_of_text)
    for entity in doc.ents:
        entity_details = []
        entity_details.append(entity)
        try:
            for linker_ent in entity._.kb_ents:
                print(linker_ent)
                Concept_Id, Score = linker_ent
                entity_details.append('Entity_Matching_Score :{}'.format(Score))
                entity_details.append(linker.kb.cui_to_entity[linker_ent[0]])
        except AttributeError:
            pass
    return pd.Series(entity_details)
"""

# COMMAND ----------


output_df=table.withColumn(output_col, umls_udf(struct(*table.columns)))

# COMMAND ----------

output_df.display()

# COMMAND ----------

output_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(OUTPUT_TABLE)

