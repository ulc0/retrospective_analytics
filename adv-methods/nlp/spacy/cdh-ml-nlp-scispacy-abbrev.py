# Databricks notebook source
# MAGIC %pip install /Volumes/edav_dev_cdh_test/dev_cdh_ml_test/compute/packages/wheels/en_core_sci_lg-0.5.4.tar.gz
# MAGIC %pip install /Volumes/edav_dev_cdh_test/dev_cdh_ml_test/compute/packages/wheels/en_ner_bc5cdr_md-0.5.4.tar.gz
# MAGIC

# COMMAND ----------

#https://medium.com/analytics-vidhya/scale-spacy-text-similarity-nlp-on-apache-spark-cce95bda686c
#https://nlp.johnsnowlabs.com/docs/en/benchmark

# COMMAND ----------

dbschema=f"cdh_abfm_phi_exploratory"
output_agg_avg = spark.table(f"{dbschema}.ml_nlp_mpox_notes_entities")

# COMMAND ----------

display(output_agg_avg)

# COMMAND ----------

 
import pandas as pd
from pyspark.sql.types import StringType,  ArrayType
from pyspark.sql.functions import pandas_udf, udf, PandasUDFType

import pyspark.sql.functions as F
import spacy
import scispacy
from scispacy.linking import EntityLinker
from scispacy.abbreviation import AbbreviationDetector  
#from scispacy.linking import EntityLinker

#from spacy.language import Language

#def create_scispacy_linker(nlp, name):
#    return EntityLinker()

#def load_spacy_model():
#    print("Loading spacy model...")
#    return en_core_sci_scibert.load(disable=["tagger", "ner"])

MODEL_NAME="en_core_sci_lg"
lnk_name="umls"
nlp=spacy.load(MODEL_NAME)
# Add the abbreviation pipe to the spacy pipeline.
# This line takes a while, because we have to download ~1GB of data
# and load a large JSON file (the knowledge base). Be patient!
# Thankfully it should be faster after the first time you use it, because
# the downloads are cached.
nlp.add_pipe("abbreviation_detector")   
nlp.add_pipe("scispacy_linker", 
                config={"resolve_abbreviations": True, 
                    "linker_name": lnk_name,
                #     "cache_folder": SCISPACY_CACHE,
                    },)
linker = nlp.get_pipe("scispacy_linker")


#@pandas_udf('string')
#
# def entities(list_of_text: pd.Series) -> pd.Series:
def entities(text):
    #EntityLinker.factory("scispacy_linker", func = create_scispacy_linker)
#    Language.factory("scispacy_linker", func = create_scispacy_linker)
#    nlp = boardcasted_nlp.value
    abblist=[]
#    for text in list_of_text:
    doc=nlp(text)
    docas=[]
    for abrv in doc._.abbreviations:
        #print(f"{abrv} \t ({abrv.start}, {abrv.end}) {abrv._.long_form}")
        docas.append(abrv._.long_form)
#        abrevs=pd.Series( [linker.kb.cui_to_entity[umls_ent[0]] for umls_ent in entity._.kb_ents]  )
#   abrevs=pd.Series(docas)
    return abrevs #d.Series(abrevs)


#boardcasted_nlp = spark.sparkContext.broadcast(load_spacy_model())
#boardcasted_nlp = spark.sparkContext.broadcast(en_core_sci_scibert.load())

test_abreviations = output_agg_avg.withColumn("map_umls", entities(F.col("word")))

# COMMAND ----------

display(test_abreviations)
