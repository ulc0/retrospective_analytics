# Databricks notebook source
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
#from scispacy.linking import EntityLinker
import en_core_sci_scibert
#############################################################
#model_name=dbutils.jobs.taskValues.get("cdh_ml_nlp_spacy","model_name")
experiment_id=dbutils.jobs.taskValues.get('cdh-ml-model','experiment_id',debugValue='392518310784613')
dbutils.widgets.text('task_key','cdh-ml-nlp-spacy-model-en_core_sci_scibert')
dbutils.widgets.text('run_id','gpu_test')
SCISPACY_CACHE=os.getenv("SCISPACY_CACHE")

task_name=dbutils.widgets.get("task_key")   ##{{task_key}}
run_id=dbutils.widgets.get("run_id") #{{run_id}}
model_name=task_name.split('-')[-1]
lnk_name="umls"
################################################################
################################################################
import pandas as pd
from pyspark.sql.functions import col, pandas_udf, struct
from typing import Iterator
################################################################

doctext = (
    "DNA is a very important part of the cellular structure of the body. "
    "John uses IL gene and interleukin-2 to treat diabetes and "
    "aspirin as proteins for arms and legs on lemurs and humans."
)

doctext=note0+note1+note2

#print(type(doctext))
doctext=reference_text

print(doctext)
with mlflow.start_run(experiment_id=experiment_id,run_name=model_name+'_'+run_id):
    nlp_model=spacy.load(model_name)
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
                      #"cache_folder": SCISPACY_CACHE,
                      },)
    linker = nlp_model.get_pipe("scispacy_linker") 
#


    doc = nlp_model(doctext)
    # create a dictionary to log
    mlflow.set_tag('model_flavor', 'spacy')
    mlflow.set_tag('linker_name',lnk_name)
    mlflow.set_tag('model_name',model_name)
    mlflow.log_artifacts('text',doctext)
    mlflow.spacy.log_model(spacy_model=nlp_model, artifact_path='model_'+model_name)
   # mlflow.log_metric(('accuracy', 0.72))
 #   doc = nlp_model(text)
    #print(doc.ents)
    results={}
    for entity in doc.ents:
        ent_text=entity.text #doc[e.start_char:e.end_char]
        print(ent_text)
        concepts_list=[]
        for concept_score in entity._.kb_ents:
            #print(type(concept_score))
            #print(concept_score)
            #Concept_Id, Score = ontology_entity
            concepts_list.append(concept_score) #tuple#
            print(linker.kb.cui_to_entity[concept_score[0]])
        results[ent_text]=concepts_list
    print(type(results))
    print(results)
    log_dict(results,f"{model_name}_{lnk_name}_results.json")    
    
