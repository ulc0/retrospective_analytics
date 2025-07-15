# Databricks notebook source
# MAGIC %pip install transformers

# COMMAND ----------

# MAGIC %pip install "tensorflow>=2.0.0"

# COMMAND ----------

# MAGIC %pip install --upgrade tensorflow-hub

# COMMAND ----------

# MAGIC %pip install torch 

# COMMAND ----------

# MAGIC %pip install Bio-Epidemiology-NER

# COMMAND ----------

# MAGIC %pip install flair

# COMMAND ----------

# MAGIC %pip install scispacy

# COMMAND ----------

pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.1/en_core_sci_sm-0.5.1.tar.gz

# COMMAND ----------

import pyspark.sql.functions as F

def cleanDate(date):
    return F.to_date(F.date_format(date, "yyyy-MM-dd"), "yyyy-MM-dd")

root = 'cdh_abfm_phi_exploratory'

cleanedNotes_transformers_mpox = (
    spark.table(f'{root}.ml_patientnote_mpox').withColumn("note_datetime", cleanDate("note_datetime"))#.where("mpox_notes= True")
)

# aggregating notes by day
selected_notes = (
    cleanedNotes_transformers_mpox.select("person_id","note_text","provider_id","mpox_notes", "note_datetime").dropDuplicates()
    .where("mpox_notes = true")
    .groupBy("person_id","note_datetime")
    .agg(
        F.collect_list("note_text").alias("daily_notes"),
        F.collect_list("provider_id").alias("providers"),
    )
)

# COMMAND ----------

# MAGIC %md # Library testing 

# COMMAND ----------

# MAGIC %md ##biomedical-ner-all

# COMMAND ----------

from transformers import pipeline
from transformers import AutoTokenizer, AutoModelForTokenClassification

tokenizer = AutoTokenizer.from_pretrained("d4data/biomedical-ner-all")
model = AutoModelForTokenClassification.from_pretrained("d4data/biomedical-ner-all")

pipe = pipeline("ner", model=model, tokenizer=tokenizer, aggregation_strategy="simple") # pass device=0 if using gpu

# COMMAND ----------

out = pipe("""Note:_<p>Willica presents today for a follow-up. She is currently working at Henderson. Her EKG was normal. She notes that she has been feeling tired lately. She notes that she is cold a lot. She notes that her heart rate is slightly elevated, and at first it was 100 beats a minute, and then when she checked it again, it was around 90 bpm. She reports that she thought she had monkeypox. She notes that bumps popped up a couple of weeks ago on her hands, face, and foot, but they went away. She notes that she still has a period, and it is heavy the first day, but the other days are normal. She denies any chance of pregnancy since her husband is getting a vasectomy. She notes that there have been no changes with her period lately. She reports that her blood pressure is normal. She is currently taking Acetazolamide. She notes that she is getting an EGD tomorrow. She notes that she has a history of cedar tumors, which are controlled. She reports that her dose of Acetazolamide has not changed recently, and she has always been on 2 twice a day. Her lipids back in January were normal, and her sugar was normal. She is not fasting right now. She had a screening test for diabetes less than a year ago, and it came back normal. She notes that her mother stresses her out before bedtime. Her dad helps take the pressure off of her that her mom puts on her. She states that her mom is not acting better. She states that she has not had her flu shot. </p>""")

out

# COMMAND ----------

selected_notes_pdf = selected_notes.toPandas()
selected_notes_pdf

# COMMAND ----------

from Bio_Epidemiology_NER.bio_recognizer import ner_prediction

doc = selected_notes_pdf.iloc[7,2]
doc_string = ' '.join([str(elem) for elem in doc])


# COMMAND ----------

temp_output = ner_prediction(corpus = doc_string, compute = 'cpu')
temp_output

# COMMAND ----------

# MAGIC %md ## Flair

# COMMAND ----------

# MAGIC %md https://github.com/flairNLP/flair/blob/master/resources/docs/HUNFLAIR.md

# COMMAND ----------

from flair.data import Sentence
from flair.nn import Classifier
from flair.tokenization import SciSpacyTokenizer

# make a sentence and tokenize with SciSpaCy
sentence = Sentence("Behavioral abnormalities in the Fmr1 KO2 Mouse Model of Fragile X Syndrome",
                    use_tokenizer=SciSpacyTokenizer())

# load biomedical tagger
tagger = Classifier.load("hunflair")

# tag sentence
tagger.predict(sentence)

# COMMAND ----------

# MAGIC %md ## Distilled bert

# COMMAND ----------

#from transformers import pipeline
from transformers import AutoTokenizer, AutoModel
tokenizer = AutoTokenizer.from_pretrained("sarahmiller137/distilbert-base-uncased-ft-ncbi-disease")
model = AutoModel.from_pretrained("sarahmiller137/distilbert-base-uncased-ft-ncbi-disease")


# COMMAND ----------

pipe2 = pipeline("ner", model=model, tokenizer=tokenizer)

# COMMAND ----------

model

# COMMAND ----------

model.__dict__

# COMMAND ----------

#! pip install johnsnowlabs


# COMMAND ----------

#from johnsnowlabs import nlp
import nlu
ner=nlu.load()
#ner.predict("Dr. John Snow is a british physician born in 1813")
