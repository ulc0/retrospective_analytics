# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id",  dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-client-id"))
spark.conf.set("fs.azure.account.oauth2.client.secret", dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-client-secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint", dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-tenant-id-endpoint"))


# COMMAND ----------

# MAGIC %pip install /Volumes/edav_dev_cdh_test/dev_cdh_ml_test/compute/packages/wheels/en_core_sci_lg-0.5.4.tar.gz

# COMMAND ----------

dbschema=f"cdh_abfm_phi_exploratory"
entity_table="ml_nlp_mpox_notes_entities"
word_table="ml_nlp_text_umls"
textcol="note_string"
keygroup="patientuid,STI_encounter"
# first time only, recreates the file
ssql=f"select distinct {keygroup},{textcol} from {dbschema}.{entity_table}"
print(ssql)
wtbl = spark.sql(ssql)


# COMMAND ----------

 
import pandas as pd
from pyspark.sql.types import StringType,  ArrayType
from pyspark.sql.functions import pandas_udf, udf, PandasUDFType

import pyspark.sql.functions as F
import spacy
from spacy.tokens import Doc
#from spacy_transformers import TransformerData
#from thinc.api import set_gpu_allocator, require_gpu

### not sure why this is needed Doc.set_extension('custom_attr', force=True)

import scispacy
#from scispacy.linking import EntityLinker
from scispacy.abbreviation import AbbreviationDetector  
from scispacy.umls_linking import UmlsEntityLinker

#from spacy.language import Language

# COMMAND ----------

import sklearn
print('The scikit-learn version is {}.'.format(sklearn.__version__))

# COMMAND ----------

threshold=0.95
#def create_scispacy_linker(nlp, name):
#    return EntityLinker()
SPACY_MODEL_NAMES = ["en_core_sci_sm", "en_core_sci_md", "en_core_sci_lg"]
NER_MODEL_NAMES = ["en_ner_craft_md", "en_ner_jnlpba_md", 
                   "en_ner_bc5cdr_md", "en_ner_bionlp13cg_md"]

#def load_spacy_model():
#    print("Loading spacy model...")
#    return en_core_sci_scibert.load(disable=["tagger", "ner"])

# COMMAND ----------

MODEL_NAME="en_core_sci_lg"
nlp=spacy.load(MODEL_NAME)
# Add the abbreviation pipe to the spacy pipeline.
# This line takes a while, because we have to download ~1GB of data
# and load a large JSON file (the knowledge base). Be patient!
# Thankfully it should be faster after the first time you use it, because
# the downloads are cached.
#abbreviation_pipe = AbbreviationDetector(nlp)
#nlp.add_pipe(abbreviation_pipe)
#nlp.add_pipe("abbreviation_detector")   
nlp.add_pipe("abbreviation_detector", config={"make_serializable": True})
linker = UmlsEntityLinker(resolve_abbreviations=True)
linker.threshold = threshold
nlp.add_pipe(
            "scispacy_linker",
            config={"resolve_abbreviations": True, "linker_name":lnk_name},
        )

#nlp.add_pipe("scispacy_linker", 
#                config={"resolve_abbreviations": True, 
#                    "linker_name": lnk_name,
#                #     "cache_folder": SCISPACY_CACHE,
#                    },)

#linker = nlp.get_pipe("scispacy_linker")
# skip NER RIGHT NOW ner = spacy.load(ner_model)

# COMMAND ----------

def linkumls(doc):
    data = []
    linkdoc=linker(doc).ents
    for ent in linkdoc:
        for ent_id, score in ent._.umls_ents:
            kb_entity = linker.umls.cui_to_entity[ent_id]
            tuis = ",".join(kb_entity.types)
            data.append([
                ent.text,
                kb_entity.canonical_name,
                ent_id,
                tuis,
                score,
                ent.start,
                ent.end,
            ])
    return data




# COMMAND ----------



text=("cancer is awful and ibuprofen does not help")
doc=nlp(text)
linker = nlp.get_pipe("scispacy_linker")
linker.threshold = threshold

for entity in doc.ents:
    print(entity)
    for umls_ent in entity:
        print(linker.kb.cui_to_entity[umls_ent[0]])

# COMMAND ----------

doc = nlp("Spinal and bulbar muscular atrophy (SBMA) is an \
           inherited motor neuron disease caused by the expansion \
           of a polyglutamine tract within the androgen receptor (AR). \
           SBMA can be caused by this easily.")

# Let's look at a random entity!


entity = doc.ents[1]
# Each entity is linked to UMLS with a score
# (currently just char-3gram matching).
for umls_ent in entity:
	print(linker.kb.cui_to_entity[umls_ent[0]])

# COMMAND ----------


import pandas as pd
stext="Spinal and bulbar muscular atrophy (SBMA) is an inherited motor neuron disease caused by the expansion of a polyglutamine tract within the androgen receptor (AR). SBMA can be caused by this easily."

print(ent)
colnames=['word','desc','concept_id','tui','score','start','end']
output=pd.DataFrame(ent,columns=colnames)
print(output)


# COMMAND ----------

print(wtbl[(textcol)].collect())

# COMMAND ----------

w=[]
for l in wtbl[textcol].collect():
    k=entity(l)
    if len(k)>0:
        print((k[0]))
        w.append(k)
#        for k in l:
#            w.append(k)


# COMMAND ----------

import pandas as pd
colnames=['word','desc','concept_id','tui','score','start','end']
ouput=pd.DataFrame(w,columns=colnames)

# COMMAND ----------

souput=spark.createDataFrame(ouput)
souput.write.saveAsTable(f"{dbschema}.{word_table}", mode="overwrite")
