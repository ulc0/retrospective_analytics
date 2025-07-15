# Databricks notebook source


# COMMAND ----------


spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id",  dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-client-id"))
spark.conf.set("fs.azure.account.oauth2.client.secret", dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-client-secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint", dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-tenant-id-endpoint"))

  

# COMMAND ----------

<<<<<<< Updated upstream
=======
#%pip install --upgrade tensorflow-hub

# COMMAND ----------

#%pip install transformers

# COMMAND ----------

#%pip install torch 

# COMMAND ----------

#%pip install torchvision

# COMMAND ----------

#%pip install transformers datasets

# COMMAND ----------

# %pip install Bio-Epidemiology-NER

# COMMAND ----------

#%pip install johnsnowlabs

# COMMAND ----------

#%pip install markupsafe==2.0.1

# COMMAND ----------

#%pip install spacy

# COMMAND ----------

#%pip install scispacy

# COMMAND ----------

#%pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.1/en_core_sci_scibert-0.5.1.tar.gz

# COMMAND ----------

#%pip install protobuf==3.20.* # debug PythonException: 'TypeError: Descriptors cannot not be created directly by downgrading

# COMMAND ----------

#%pip install spacy-entity-linker==1.0.3

# COMMAND ----------

#%pip install spacy-transformers

# COMMAND ----------

#pip install scispacy
>>>>>>> Stashed changes

# COMMAND ----------

import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *

from sparknlp.common import *
from sparknlp.pretrained import ResourceDownloader

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

# MAGIC %md # Testing pretrained models 
# MAGIC
# MAGIC dbschema=f"cdh_abfm_phi_exploratory"

# COMMAND ----------

case_control_cohort=spark.table(f"{dbschema}.ml_nlp_mpox_notes")

# COMMAND ----------

# 9/13/2023
""" 
import spacy
import spacy_transformers
from scispacy.abbreviation import AbbreviationDetector  # type: ignore
from scispacy.linking import EntityLinker
import en_core_sci_scibert
from spacy.language import Language

# make the factory work
#from rel_pipe import make_relation_extractor, score_relations

# make the config work
#from rel_model import create_relation_model, create_classification_layer, create_instances

# printing a msg each time we load the model

@Language.factory('my_scispacy_linker')
def my_scispacy_linker(nlp, name):
    return EntityLinker()


def load_spacy_model():
    print("Loading spacy model...")
    return en_core_sci_scibert.load() #load(disable=["tagger", "ner"])

# decorator indicating that this function is pandas_udf
# and that it's gonna process list of string
@pandas_udf(ArrayType(StringType()))
# function receiving a pd.Series and returning a pd.Series
def entities(list_of_text: pd.Series) -> pd.Series:
    # retrieving the shared nlp object
    nlp = boardcasted_nlp.value
    # Add the abbreviation pipe to the spacy pipeline.
    nlp.add_pipe("my_scispacy_linker", config={"resolve_abbreviations": True, "linker_name": "umls"})
    # batch processing our list of text
    linker = nlp.get_pipe("my_scispacy_linker")
        
    # retrieving the str representation of entity label
    # as we are limited in the types of obj
    # we can return from a panda_udf
    # we couldn't return a Span obj for example
    abrevs=[
        [linker.kb_ciu_to_entity[umls_ent[0]] for link in linker for umls_ent in entity._.kb_ents]               
        
    ]
    return pd.Series(abrevs)

# we load the spacy model and broadcast it
boardcasted_nlp = spark.sparkContext.broadcast(load_spacy_model())

test_abreviations = case_control_cohort.withColumn("map_umls", entities(F.col("note_clean_misc")))
"""



# COMMAND ----------

# MAGIC %pip list

# COMMAND ----------

# 9/13/2023
 
import spacy
import scispacy
from scispacy.linking import EntityLinker
from scispacy.abbreviation import AbbreviationDetector  
from scispacy.linking import EntityLinker
import en_core_sci_scibert

from spacy.language import Language

def create_scispacy_linker(nlp, name):
    return EntityLinker()

#def load_spacy_model():
#    print("Loading spacy model...")
#    return en_core_sci_scibert.load(disable=["tagger", "ner"])


@pandas_udf(ArrayType(StringType()))

def entities(list_of_text: pd.Series) -> pd.Series:
    #EntityLinker.factory("scispacy_linker", func = create_scispacy_linker)
    Language.factory("scispacy_linker", func = create_scispacy_linker)

    nlp = boardcasted_nlp.value

    nlp.add_pipe("scispacy_linker", config={"resolve_abbreviations": True, "linker_name": "umls"})

    linker = nlp.get_pipe("scispacy_linker")

    abrevs=[            
        [linker.kb.cui_to_entity[umls_ent[0]] for umls_ent in entity._.kb_ents]                 
    ]
    return pd.Series(abrevs)


#boardcasted_nlp = spark.sparkContext.broadcast(load_spacy_model())
boardcasted_nlp = spark.sparkContext.broadcast(en_core_sci_scibert.load())

test_abreviations = case_control_cohort.withColumn("map_umls", entities(F.col("note_clean_misc")))



# COMMAND ----------



# COMMAND ----------


#display(test_abreviations)

# COMMAND ----------



# COMMAND ----------

"""# Databricks notebook source
# sample notes
# reference https://scispacy.apps.allenai.org/

note0 = "Note:Willica presents today for a follow-up. She is currently working at Henderson. Her EKG was normal. She notes that she has been feeling tired lately. She notes that she is cold a lot. She notes that her heart rate is slightly elevated, and at first it was 100 beats a minute, and then when she checked it again, it was around 90 bpm. She reports that she thought she had monkeypox. She notes that bumps popped up a couple of weeks ago on her hands, face, and foot, but they went away. She notes that she still has a period, and it is heavy the first day, but the other days are normal. She denies any chance of pregnancy since her husband is getting a vasectomy. She notes that there have been no changes with her period lately. She reports that her blood pressure is normal. She is currently taking Acetazolamide. She notes that she is getting an EGD tomorrow. She notes that she has a history of cedar tumors, which are controlled. She reports that her dose of Acetazolamide has not changed recently, and she has always been on 2 twice a day. Her lipids back in January were normal, and her sugar was normal. She is not fasting right now. She had a screening test for diabetes less than a year ago, and it came back normal. She notes that her mother stresses her out before bedtime. Her dad helps take the pressure off of her that her mom puts on her. She states that her mom is not acting better. She states that she has not had her flu shot."

note1 = "Doubt monkey pox, or zoster. Hope is not mrsa. Cover with keflex and rto 3-4 d if not responding. 9/16/22  Small infection is gone.  Lump itself is not tender now, but same size.  We will refer to surgery for their opinion"

note2 = "RESPIRATORY: normal breath sounds with no rales, rhonchi, wheezes or rubs; CARDIOVASCULAR: normal rate; rhythm is regular; no systolic murmur; GASTROINTESTINAL: nontender; normal bowel sounds; LYMPHATICS: no adenopathy in cervical, supraclavicular, axillary, or inguinal regions; BREAST/INTEGUMENT: no rashes or lesions; MUSCULOSKELETAL: normal gait pain with range of motion in Left wrist ormal tone; NEUROLOGIC: appropriate for age; Lab/Test Results: X-RAY INTERPRETATION: ORTHOPEDIC X-RAY: Left wrist(AP view): (+) fracture: of the distal radius"

reference_text="Spinal and bulbar muscular atrophy (SBMA) is an inherited motor neuron disease caused by the expansion of a polyglutamine tract within the androgen receptor (AR). SBMA can be caused by this easily."

# COMMAND ----------

## Referene https://github.com/allenai/scispacy#example-usage-1
import os
import spacy
import scispacy
from scispacy.abbreviation import AbbreviationDetector
from scispacy.linking import EntityLinker
import mlflow
import mlflow.spacy
from mlflow import log_metric, log_param, log_params, log_artifacts, log_dict
from mlflow.models import infer_signature
#############################################################
#model_name=dbutils.jobs.taskValues.get("cdh_ml_nlp_spacy","model_name")
experiment_id=dbutils.jobs.taskValues.get('cdh-ml-model','experiment_id',debugValue='1441353104968016')
dbutils.widgets.text('task_key','cdh-ml-nlp-spacy-model-en_core_sci_scibert')
dbutils.widgets.text('run_id','gpu_test')
SCISPACY_CACHE=os.getenv("SCISPACY_CACHE")

task_name=dbutils.widgets.get("task_key")   ##{{task_key}}
run_id=dbutils.widgets.get("run_id") #{{run_id}}
#model_name= 'en_core_sci_scibert'
lnk_name="umls"
################################################################
doctext = (
    "DNA is a very important part of the cellular structure of the body. "
    "John uses IL gene and interleukin-2 to treat diabetes and "
    "aspirin as proteins for arms and legs on lemurs and humans."
)

#print(type(doctext))

doctext=note0+note1+note2

#print(type(doctext))
doctext=reference_text

print(doctext)
nlp_model=spacy.load('spacy-scibert')
# Add the abbreviation pipe to the spacy pipeline.
# This line takes a while, because we have to download ~1GB of data
# and load a large JSON file (the knowledge base). Be patient!
# Thankfully it should be faster after the first time you use it, because
# the downloads are cached.
nlp_model.add_pipe("abbreviation_detector")
   # NOTE: The resolve_abbreviations parameter is optional, and requires that
# the AbbreviationDetector pipe has already been added to the pipeline. Adding
# the AbbreviationDetector pipe and setting resolve_abbreviations to True means
# that linking will only be performed on the long form of abbreviations.
nlp_model.add_pipe( "scispacy_linker",
             config={"resolve_abbreviations": True, 
                     "linker_name": lnk_name,
                      "cache_folder": SCISPACY_CACHE,},)
linker = nlp_model.get_pipe("scispacy_linker") 
#
with mlflow.start_run(experiment_id=experiment_id,run_name=model_name+'_'+run_id):
    doc = nlp_model(doctext)
    # create a dictionary to log
    mlflow.set_tag('model_flavor', 'spacy')
    mlflow.set_tag('linker_name',lnk_name)
    mlflow.set_tag('model_name',model_name)
    mlflow.log_artifacts('text',doctext)
    mlflow.spacy.log_model(spacy_model=nlp_model, artifact_path='model')
   # mlflow.log_metric(('accuracy', 0.72))
 #   doc = nlp_model(text)
    #print(doc.ents)
    results={}
    for e in doc.ents:
        ent=e.text #doc[e.start_char:e.end_char]
#        print(ent)
        concepts_list=[]
        for concept_score in ent._.kb_ents:
            print(type(concept_score))
            print(concept_score)
            #Concept_Id, Score = ontology_entity
            concepts_list.append(concept_score) #tuple
	        #print(linker.kb.cui_to_entity[concept_score[0]])
        results[ent]=concepts_list
    print(type(results))
    print(results)
    log_dict(results,f"{model_name}_{lnk_name}_results.json")   
    """ 
    

# COMMAND ----------

#test_abreviations = case_control_cohort.withColumn("abbreviation", detect_abbreviations(F.col("note_clean_misc")))

# COMMAND ----------

#display(test_abreviations)

# COMMAND ----------

dbutils.notebook.exit("whatever reason to make it stop")

# COMMAND ----------

documentAssembler = (
    DocumentAssembler()
    .setInputCol('note_clean_misc') 
    .setOutputCol('note_doc')
)

#default
cleanUpPatternsHTML = ["<[^>]*>|&[^;]+;"]
documentNormalizerHTML = (
    DocumentNormalizer() 
    .setInputCols(["note_doc"]) 
    .setOutputCol("cleaned_markup") 
    .setAction("clean") 
    .setPatterns(cleanUpPatternsHTML) 
    .setReplacement(" ") 
    .setPolicy("pretty_all") 
    .setLowercase(True)
)

sentence = (
    SentenceDetector() 
    .setInputCols(["cleaned_markup"]) 
    .setOutputCol("note_sent") 
    .setExplodeSentences(True)
) 

finisher = Finisher() \
    .setInputCols(["note_sent"]) \
    .setOutputCols(["note_string"]) \
    .setOutputAsArray(False) \
    .setCleanAnnotations(False) 


docPatternRemoverPipeline = (
    Pipeline() 
    .setStages([documentAssembler,                
                documentNormalizerHTML,
                sentence,
                finisher
                ]
               )
)

treated_notes = docPatternRemoverPipeline.fit(case_control_cohort).transform(case_control_cohort)

# COMMAND ----------

display(case_control_cohort)
treated_notes.selectExpr("explode(note_sent) as sentences").display()
treated_notes.display()

# COMMAND ----------

#DB_EXPLORATORY = 'cdh_abfm_phi_exploratory'
#DB_NAME= 'mpoxProject_data_treated_notes_set_9_9_23'
#USER = 'run9'

#(
#    treated_notes
#    .write
#    .format('parquet')
#    .mode('overwrite')
#    .saveAsTable(f"{DB_EXPLORATORY}.{DB_NAME}_{USER}")
#)

# COMMAND ----------

# MAGIC %md 
# MAGIC #### This is workflow break point

# COMMAND ----------

input_ner = treated_notes

#spark.table("cdh_abfm_phi_exploratory.mpoxProject_data_treated_notes_set_9_9_23_run9")

#display(input_ner)


# COMMAND ----------

# MAGIC %md  
# MAGIC
# MAGIC This should move to a model endpoint

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
    input_ner.select("*", ner_udf(input_ner.note_string).alias('entities'))
    .withColumn("exploded_ent", F.explode(F.col("entities")))
    .select("*",(F.lower(F.col("exploded_ent.entity_group"))).alias("entity_group"), F.col("exploded_ent.word"), F.col("exploded_ent.score"))    
)

display(output_agg_avg)


# COMMAND ----------

# MAGIC %md  
# MAGIC
# MAGIC This should move to a model endpoint
# MAGIC

# COMMAND ----------

pipe = pipeline("ner", model=model, tokenizer=tokenizer, aggregation_strategy="max", device = device) # pass device=0 if using gpu

@pandas_udf('array<struct<word string, entity_group string, score float, start integer, end integer>>')
def ner_udf(texts: pd.Series) -> pd.Series:
  return pd.Series(pipe(texts.to_list(), batch_size=1))

output_agg_max = (
    input_ner.select("*", ner_udf(input_ner.note_string).alias('entities'))
    .withColumn("exploded_ent", F.explode(F.col("entities")))
    .select("*",(F.lower(F.col("exploded_ent.entity_group"))).alias("entity_group"), F.col("exploded_ent.word"), F.col("exploded_ent.score"))    
)

display(output_agg_max)

# COMMAND ----------

#display(
#    output_agg_max
#    .groupBy("entity_group")
#    .count()
#    )

# COMMAND ----------

# MAGIC %md ## Comparing output avg and max

# COMMAND ----------

display(
    output_agg_avg
    .where("""
           entity_group in (
               'sign_symptom',
               'biological_structure',
               'diagnostic_procedure',
               'disease_disorder',
               'personal_background',
               'sex',
               'outcome',
               'color',
               'texture',
               'shape',
               'area'
               )""")
)

# COMMAND ----------


#display(
#    output_agg_max
#    .where("""
#           entity_group in (
#               'sign_symptom',
#               'biological_structure',
#               'diagnostic_procedure',
#               'disease_disorder',
#               'personal_background',
#               'sex',
#               'outcome',
#               'color',
#               'texture',
#               'shape',
#               'area'
#               )""")
#)


# COMMAND ----------

display(
    output_agg_avg
    .where("""
           entity_group in (
               'sign_symptom',               
               'diagnostic_procedure',
               'disease_disorder'              
               )"""
            )
)

# COMMAND ----------

#display(
#    output_agg_max
#    .where("""
#           entity_group in (
#               'sign_symptom',               
#               'diagnostic_procedure',
#               'disease_disorder'
#               )"""
#            )
#)

# COMMAND ----------

display(
    output_agg_avg
    .select(*output_agg_avg.columns[0:16],F.col("entity_group"),F.col("word"),F.col("score"))
    .where("""
           entity_group in (
               'sign_symptom',               
               'diagnostic_procedure',
               'disease_disorder'              
               )"""
            )
)

#testing

# COMMAND ----------

output_agg_max.printSchema()

# COMMAND ----------

#def processed_text(text, pipe):
#    processe_result = pipe(text)
#    return processe_result

#process_ner_model_udf = udf(processed_text, StringType())



# COMMAND ----------

#input_ner1 = (
#    input_ner
#    .select("*", processed_text(F.col("note_clean_misc"), pipe).alias("process_text"))
    #.where("patientuid = '7521AF8B-BF3B-4429-936D-8B7132C71AAF'")
    #.withColumn("processed_text_ner", )
    
#)

#display(input_ner1)
