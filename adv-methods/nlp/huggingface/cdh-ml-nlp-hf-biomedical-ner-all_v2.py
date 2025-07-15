# Databricks notebook source
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
    spark.table("edav_prd_cdh.cdh_abfm_phi_exploratory.ml_fs_abfm_notes_text")
)

#display(cleaned_notes)

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, DoubleType

import mlflow
from mlflow.models import infer_signature
from mlflow.transformers import generate_signature_output


import pandas as pd
import torch

from transformers import pipeline
from transformers import AutoTokenizer, AutoModelForTokenClassification


#device = 0 if torch.cuda.is_available() else -1

tokenizer = AutoTokenizer.from_pretrained("d4data/biomedical-ner-all")
model = AutoModelForTokenClassification.from_pretrained("d4data/biomedical-ner-all")
#pipe = pipeline("ner", model=model, tokenizer=tokenizer, aggregation_strategy="average", device = device) # pass device=0 if using gpu
hf_pipe = pipeline("ner", model=model, tokenizer=tokenizer, aggregation_strategy="average", framework = 'pt') # pass device=0 if using gpu

#output = generate_signature_output(hf_pipe, cleaned_notes)
#signature = infer_signature(cleaned_notes, output)

artifact_path = "biomedical-ner-all"

model_config = {"max_length": 1024, "truncation": True}

#mlflow.set_experiment(experiment_id='464910545782982')
#with mlflow.start_run(run_name=model) as run:
#    model_info = mlflow.transformers.log_model(
#        transformers_model=hf_pipe,
#        artifact_path=artifact_path,
#        registered_model_name="hf-biomedical-ner-all",
#        input_example=cleaned_notes
##        #signature=signature,
##        model_config=model_config,
##        model_card=architecture,
#    )

def perform_ner(text):       
    # Perform NER
    ner_output = hf_pipe(text)
    ner_array = [(item['entity_group'], item['score'], item['word']) for item in ner_output]

    # Return NER output
    return ner_array

ner_schema = ArrayType(StructType([
    StructField("entity_group", StringType(), True),
    StructField("word", StringType(), True),
    StructField("score", DoubleType(), True)
]))

ner_udf = udf(perform_ner, ner_schema)

# Apply NER function to each row
#df_with_ner = df.withColumn("ner_output", ner_udf(df["sentence"]))
# Apply UDF to DataFrame

# Sample DataFrame with sentences

output_agg_avg = (
    cleaned_notes.limit(500)
    .withColumn("entities", ner_udf(cleaned_notes["note_string"]))
    #.select("*", udf_extract_entities(cleaned_notes.note_string).alias('entities'))
)

display(output_agg_avg)

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, DoubleType

def perform_ner(text):       
    # Perform NER
    ner_output = hf_pipe(text)
    ner_array = [(item['entity_group'], item['score'], item['word']) for item in ner_output]

    # Return NER output
    return ner_array

ner_schema = ArrayType(StructType([
    StructField("entity_group", StringType(), True),
    StructField("word", StringType(), True),
    StructField("score", DoubleType(), True)
]))

ner_udf = udf(perform_ner, ner_schema)

output_agg_avg = (
    cleaned_notes.limit(500)
    .withColumn("entities", ner_udf(cleaned_notes["note_string"]))
    #.select("*", udf_extract_entities(cleaned_notes.note_string).alias('entities'))
)

#display(output_agg_avg)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

"""
from pyspark.sql.functions import udf
from pyspark.sql.types import ArrayType, StructType, StructField, StringType, FloatType

import pandas as pd
import torch

from transformers import pipeline
from transformers import AutoTokenizer, AutoModelForTokenClassification


device = 0 if torch.cuda.is_available() else -1

tokenizer = AutoTokenizer.from_pretrained("d4data/biomedical-ner-all")
model = AutoModelForTokenClassification.from_pretrained("d4data/biomedical-ner-all")
pipe = pipeline("ner", model=model, tokenizer=tokenizer, aggregation_strategy="average", device = device) # pass device=0 if using gpu


def extract_entities(text):
    entities = pipe(text)
    extracted_entities = []
    for entity in entities:
        extracted_entity = {
            'word': entity['word'],
            'score': entity['score'],  # Assuming 'score' is a key in the entity dictionary
            'entity_group': entity['entity_group']  # Assuming 'entity_group' is a key in the entity dictionary
        }
        extracted_entities.append(extracted_entity)
    return extracted_entities

# Define the schema for the StructType
entity_schema = StructType([
    StructField("word", StringType(), True),
    StructField("score", FloatType(), True),  # Adjust the type accordingly
    StructField("entity_group", StringType(), True)
])

# Register UDF
udf_extract_entities = udf(extract_entities, ArrayType(entity_schema))

# Apply UDF to DataFrame
output_agg_avg = (
    cleaned_notes.limit(500)
    .select("*", udf_extract_entities(cleaned_notes.note_string).alias('entities'))
)

display(output_agg_avg)
"""

# COMMAND ----------

"""
import pandas as pd
import torch

from transformers import pipeline
from transformers import AutoTokenizer, AutoModelForTokenClassification


device = 0 if torch.cuda.is_available() else -1

tokenizer = AutoTokenizer.from_pretrained("d4data/biomedical-ner-all")
model = AutoModelForTokenClassification.from_pretrained("d4data/biomedical-ner-all")
pipe = pipeline("ner", model=model, tokenizer=tokenizer, aggregation_strategy="average", device = device) # pass device=0 if using gpu

def extract_entities(text):
    entities = pipe(text)
    return [entity['word'] for entity in entities]

# Register UDF
udf_extract_entities = udf(extract_entities, ArrayType(StringType()))


output_agg_avg = (
    cleaned_notes.limit(500)
    .select("*", udf_extract_entities(cleaned_notes.note_string).alias('entities'))
    .withColumn("exploded_ent", F.explode(F.col("entities")))
    .select("*",(F.lower(F.col("exploded_ent.entity_group"))).alias("entity_group"), F.col("exploded_ent.word"), F.col("exploded_ent.score"))    
)

display(output_agg_avg)
"""

