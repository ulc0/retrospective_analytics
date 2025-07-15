# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id",  dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-client-id"))
spark.conf.set("fs.azure.account.oauth2.client.secret", dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-client-secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint", dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-tenant-id-endpoint"))

  

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

import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *

from sparknlp.common import *
from sparknlp.pretrained import ResourceDownloader

spark.conf.set('spark.sql.shuffle.partitions',7200*4)

# COMMAND ----------

# MAGIC %md # Testing pretrained models 
# MAGIC

# COMMAND ----------

dbschema=f"edav_prd_cdh.cdh_abfm_phi_exploratory"
cleaned_case_control_cohort=spark.table(f"{dbschema}.ml_nlp_mpox_notes")

# COMMAND ----------

display(cleaned_case_control_cohort)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Referene https://github.com/allenai/scispacy#example-usage-1
# MAGIC import os
# MAGIC import spacy
# MAGIC import scispacy
# MAGIC from scispacy.abbreviation import AbbreviationDetector
# MAGIC from scispacy.linking import EntityLinker
# MAGIC import mlflow
# MAGIC import mlflow.spacy
# MAGIC from mlflow import log_metric, log_param, log_params, log_artifacts, log_dict
# MAGIC from mlflow.models import infer_signature
# MAGIC #############################################################
# MAGIC #model_name=dbutils.jobs.taskValues.get("edav_prd_cdh.cdh_ml_nlp_spacy","model_name")
# MAGIC experiment_id=dbutils.jobs.taskValues.get('cdh-ml-model','experiment_id',debug='1441353104968016')
# MAGIC dbutils.widgets.text('task_key','cdh-ml-nlp-spacy-model-en_core_sci_scibert')
# MAGIC dbutils.widgets.text('run_id','gpu_test')
# MAGIC SCISPACY_CACHE=os.getenv("SCISPACY_CACHE")
# MAGIC
# MAGIC task_name=dbutils.widgets.get("task_key")   ##{{task_key}}
# MAGIC run_id=dbutils.widgets.get("run_id") #{{run_id}}
# MAGIC #model_name= 'en_core_sci_scibert'
# MAGIC lnk_name="umls"
# MAGIC ################################################################
# MAGIC doctext = (
# MAGIC     "DNA is a very important part of the cellular structure of the body. "
# MAGIC     "John uses IL gene and interleukin-2 to treat diabetes and "
# MAGIC     "aspirin as proteins for arms and legs on lemurs and humans."
# MAGIC )
# MAGIC
# MAGIC #print(type(doctext))
# MAGIC
# MAGIC doctext=note0+note1+note2
# MAGIC
# MAGIC #print(type(doctext))
# MAGIC doctext=reference_text
# MAGIC
# MAGIC print(doctext)
# MAGIC nlp_model=spacy.load('spacy-scibert')
# MAGIC # Add the abbreviation pipe to the spacy pipeline.
# MAGIC # This line takes a while, because we have to download ~1GB of data
# MAGIC # and load a large JSON file (the knowledge base). Be patient!
# MAGIC # Thankfully it should be faster after the first time you use it, because
# MAGIC # the downloads are cached.
# MAGIC nlp_model.add_pipe("abbreviation_detector")
# MAGIC    # NOTE: The resolve_abbreviations parameter is optional, and requires that
# MAGIC # the AbbreviationDetector pipe has already been added to the pipeline. Adding
# MAGIC # the AbbreviationDetector pipe and setting resolve_abbreviations to True means
# MAGIC # that linking will only be performed on the long form of abbreviations.
# MAGIC nlp_model.add_pipe( "scispacy_linker",
# MAGIC              config={"resolve_abbreviations": True, 
# MAGIC                      "linker_name": lnk_name,
# MAGIC                       "cache_folder": SCISPACY_CACHE,},)
# MAGIC linker = nlp_model.get_pipe("scispacy_linker") 
# MAGIC #
# MAGIC with mlflow.start_run(experiment_id=experiment_id,run_name=model_name+'_'+run_id):
# MAGIC     doc = nlp_model(doctext)
# MAGIC     # create a dictionary to log
# MAGIC     mlflow.set_tag('model_flavor', 'spacy')
# MAGIC     mlflow.set_tag('linker_name',lnk_name)
# MAGIC     mlflow.set_tag('model_name',model_name)
# MAGIC     mlflow.log_artifacts('text',doctext)
# MAGIC     mlflow.spacy.log_model(spacy_model=nlp_model, artifact_path='model')
# MAGIC    # mlflow.log_metric(('accuracy', 0.72))
# MAGIC  #   doc = nlp_model(text)
# MAGIC     #print(doc.ents)
# MAGIC     results={}
# MAGIC     for e in doc.ents:
# MAGIC         ent=e.text #doc[e.start_char:e.end_char]
# MAGIC #        print(ent)
# MAGIC         concepts_list=[]
# MAGIC         for concept_score in ent._.kb_ents:
# MAGIC             print(type(concept_score))
# MAGIC             print(concept_score)
# MAGIC             #Concept_Id, Score = ontology_entity
# MAGIC             concepts_list.append(concept_score) #tuple
# MAGIC 	        #print(linker.kb.cui_to_entity[concept_score[0]])
# MAGIC         results[ent]=concepts_list
# MAGIC     print(type(results))
# MAGIC     print(results)
# MAGIC     log_dict(results,f"{model_name}_{lnk_name}_results.json")   
# MAGIC
# MAGIC     

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

treated_notes = docPatternRemoverPipeline.fit(cleaned_case_control_cohort).transform(cleaned_case_control_cohort)

# COMMAND ----------

display(treated_notes)
treated_notes.selectExpr("explode(note_sent) as sentences").display()
treated_notes.display()

# COMMAND ----------

dbschema=f"edav_prd_cdh.cdh_abfm_phi_exploratory"
treated_notes.write.mode("overwrite").format("delta").saveAsTable(f"{dbschema}.ml_nlp_mpox_notes_sentences")

# COMMAND ----------

# MAGIC %md 
# MAGIC #### This is workflow break point

# COMMAND ----------

# MAGIC %md  
# MAGIC
# MAGIC This should move to a model endpoint
# MAGIC

# COMMAND ----------

#pipe = pipeline("ner", model=model, tokenizer=tokenizer, aggregation_strategy="max", device = device) # pass device=0 if using gpu

#@pandas_udf('array<struct<word string, entity_group string, score float, start integer, end integer>>')
#def ner_udf(texts: pd.Series) -> pd.Series:
#  return pd.Series(pipe(texts.to_list(), batch_size=1))

#output_agg_max = (
#    input_ner.select("*", ner_udf(input_ner.note_string).alias('entities'))
#    .withColumn("exploded_ent", F.explode(F.col("entities")))
#    .select("*",(F.lower(F.col("exploded_ent.entity_group"))).alias("entity_group"), F.col("exploded_ent.word"), F.col("exploded_ent.score"))    
#)

#display(output_agg_max)

# COMMAND ----------

#display(
#    output_agg_max
#    .groupBy("entity_group")
#    .count()
#    )

# COMMAND ----------

# MAGIC %md ## Comparing output avg and max

# COMMAND ----------

#display(
#    output_agg_avg
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

#display(
#    output_agg_avg
#    .where("""
#           entity_group in (
#               'sign_symptom',               
#               'diagnostic_procedure',
#               'disease_disorder'              
#               )"""
#            )
#)

# COMMAND ----------

#display(
#    output_agg_avg
#    .select(*output_agg_avg.columns[0:16],F.col("entity_group"),F.col("word"),F.col("score"))
#    .where("""
#           entity_group in (
#               'sign_symptom',               
#               'diagnostic_procedure',
#               'disease_disorder'              
#               )"""
#            )
#)

#testing

# COMMAND ----------


