# Databricks notebook source
#%pip install transformers

# COMMAND ----------

#%pip install "tensorflow>=2.0.0"

# COMMAND ----------

#%pip install torch 

# COMMAND ----------

#%pip install scispacy

# COMMAND ----------

#%pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.1/en_ner_bc5cdr_md-0.5.1.tar.gz

# COMMAND ----------

# MAGIC %md # sciSpacy model

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

#import sparknlp
#from sparknlp.base import *
#from sparknlp.annotator import *

#from sparknlp.common import *
#from sparknlp.pretrained import ResourceDownloader

spark.conf.set('spark.sql.shuffle.partitions',7200*4)

# COMMAND ----------

dbschema=f"cdh_abfm_phi_exploratory"
treated_notes = spark.table(f"{dbschema}.ml_nlp_mpox_notes_presentation_sentences_v2")

# COMMAND ----------

display(treated_notes)

# COMMAND ----------

# I cant select the GPU, installing spacu instead

# COMMAND ----------

import scispacy, spacy
sci_nlp = spacy.load("en_core_sci_scibert")

# COMMAND ----------

sci_nlp.component_names

# COMMAND ----------

c= 0 
for i in sci_nlp.get_pipe('ner').labels:
    c=c+1
    print(c,"<==>",i)

    

# COMMAND ----------

x = "The Influenza pandemic of 1918 was one of the deadliest epidemics of infectious disease the world has ever seen. In response, many cities introduced widespread interventions intended to reduce the spread. There is evidence [3] that those cities which implemented these interventions later had fewer deaths. This seemingly counter-intuitive observation suggests that those cities which were slow to respond were the most successful."

docx = sci_nlp(x)
for ent in docx.ents:
    print(ent.text,ent.label_)

# COMMAND ----------

test = [(ent.text,ent.label_) for ent in docx.ents]

# COMMAND ----------

test

# COMMAND ----------

#dbutils.notebook.exit("whatever reason to make it stop")

# COMMAND ----------

# Load model directly
from transformers import AutoTokenizer, AutoModelForTokenClassification

tokenizer = AutoTokenizer.from_pretrained("medical-ner-proj/bert-medical-ner-proj")
model = AutoModelForTokenClassification.from_pretrained("medical-ner-proj/bert-medical-ner-proj")

# COMMAND ----------

text= "John Doe has a history of hypertension, which is well-controlled with medication. He has no history of allergies or surgeries. He is not currently taking any medication except for his blood pressure medication."

# COMMAND ----------


from transformers import pipeline
ner = pipeline(task="ner",model=model, tokenizer=tokenizer)
ner(text, aggregation_strategy="first")

# COMMAND ----------

# New addutuib 10/16/2023


import pandas as pd
import torch

from transformers import pipeline
from transformers import AutoTokenizer, AutoModelForTokenClassification

tokenizer = AutoTokenizer.from_pretrained("medical-ner-proj/bert-medical-ner-proj")
model = AutoModelForTokenClassification.from_pretrained("medical-ner-proj/bert-medical-ner-proj")
pipe = pipeline("ner", model=model, tokenizer=tokenizer, aggregation_strategy="average") 

@pandas_udf('array<struct<word string, entity_group string, score float, start integer, end integer>>')
def ner_udf(texts: pd.Series) -> pd.Series:
  return pd.Series(pipe(texts.to_list(), batch_size=1))


output_agg_avg_bert_medical_ner = (
    treated_notes.select("*", ner_udf(treated_notes.note_string).alias('entities'))
    .withColumn("exploded_ent", F.explode(F.col("entities")))
    .select("*",(F.lower(F.col("exploded_ent.entity_group"))).alias("entity_group"), F.col("exploded_ent.word"), F.col("exploded_ent.score"))    
)

#display(output_agg_avg_bert_medical_ner)


# COMMAND ----------

# testing this as temporary solution spacy in transformers
"""
import pandas as pd
import torch

from transformers import pipeline
from transformers import AutoTokenizer, AutoModelForTokenClassification


device = 0 if torch.cuda.is_available() else -1

from transformers import AutoTokenizer, AutoModelForTokenClassification

tokenizer = AutoTokenizer.from_pretrained("judithrosell/biobert-ft-scispacy-ner")
model = AutoModelForTokenClassification.from_pretrained("judithrosell/biobert-ft-scispacy-ner", from_tf=True)
pipe = pipeline("ner", model=model, tokenizer=tokenizer, aggregation_strategy="average", device = device) # pass device=0 if using 


@pandas_udf('array<struct<word string, entity_group string, score float, start integer, end integer>>')
def ner_udf(texts: pd.Series) -> pd.Series:
  return pd.Series(pipe(texts.to_list(), batch_size=1))


output_agg_avg_scispacy_ner = (
    treated_notes.select("*", ner_udf(treated_notes.note_string).alias('entities'))
    .withColumn("exploded_ent", F.explode(F.col("entities")))
    .select("*",(F.lower(F.col("exploded_ent.entity_group"))).alias("entity_group"), F.col("exploded_ent.word"), F.col("exploded_ent.score"))    
)

display(output_agg_avg_scispacy_ner)
"""

# COMMAND ----------

# Testing this as another temporary solution biobert_diseases_ner
"""
import pandas as pd
import torch

from transformers import pipeline
from transformers import AutoTokenizer, AutoModelForTokenClassification

tokenizer = AutoTokenizer.from_pretrained("alvaroalon2/biobert_diseases_ner")
model = AutoModelForTokenClassification.from_pretrained("alvaroalon2/biobert_diseases_ner")
pipe = pipeline("ner", model=model, tokenizer=tokenizer, aggregation_strategy="average") 

@pandas_udf('array<struct<word string, entity_group string, score float, start integer, end integer>>')
def ner_udf(texts: pd.Series) -> pd.Series:
  return pd.Series(pipe(texts.to_list(), batch_size=1))


output_agg_avg_biobert_diseases_ner = (
    treated_notes.select("*", ner_udf(treated_notes.note_string).alias('entities'))
    .withColumn("exploded_ent", F.explode(F.col("entities")))
    .select("*",(F.lower(F.col("exploded_ent.entity_group"))).alias("entity_group"), F.col("exploded_ent.word"), F.col("exploded_ent.score"))    
)

display(output_agg_avg_biobert_diseases_ner)
"""


# COMMAND ----------


def load_spacy_model():
    print("Loading spacy model...")
    return spacy.load("en_ner_bc5cdr_md")

# decorator indicating that this function is pandas_udf
# and that it's gonna process list of string
@pandas_udf(ArrayType(StringType()))
# function receiving a pd.Series and returning a pd.Series
def entities(list_of_text: pd.Series) -> pd.Series:
    # retrieving the shared nlp object
    nlp = boardcasted_nlp.value
    # batch processing our list of text
    docs = nlp.pipe(list_of_text)
    
    # retrieving the str representation of entity label
    # as we are limited in the types of obj
    # we can return from a panda_udf
    # we couldn't return a Span obj for example
    #ents=[
    #    [(ent.label_,ent.text) for ent in doc.ents]
    #    for doc in docs
    #]
    #ents= [(ent.text,ent.label_) for ent in docx.ents]
    
    ents= {x.text: x.label_ for x in docs.ents}
    return pd.Series(ents)

# we load the spacy model and broadcast it
boardcasted_nlp = spark.sparkContext.broadcast(load_spacy_model())

df_new = treated_notes.select("*", (entities(F.col("note_string"))).alias("scibert_ner_results"))




# COMMAND ----------

#dbschema=f"cdh_abfm_phi_exploratory"
#output_agg_avg_scispacy_ner.write.mode("overwrite").format("delta").saveAsTable(f"{dbschema}.ml_nlp_mpox_notes_presentation_scispacy_entities_v2")

# COMMAND ----------

dbschema=f"cdh_abfm_phi_exploratory"

output_agg_avg_bert_medical_ner.write.mode("overwrite").format("delta").saveAsTable(f"{dbschema}.ml_nlp_mpox_notes_presentation_bert_medical_ner_v2")
