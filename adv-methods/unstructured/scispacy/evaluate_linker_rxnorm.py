# Databricks notebook source
#%pip install /Volumes/edav_dev_cdh_test/dev_cdh_ml_test/compute/packages/wheels/en_core_sci_sm-0.5.4.tar.gz
#%pip install "nmslib@git+https://github.com/nmslib/nmslib.git/#subdirectory=python_bindings"
%pip freeze

# COMMAND ----------

import spacy
from scispacy.abbreviation import AbbreviationDetector
from scispacy.linking import EntityLinker


# COMMAND ----------

drugNames={
"DRUGNAME_COVID" : [
    #unchanged from HV
    "CASIRIVIMAB",
    "IMDEVIMAB",
    "ETESEVIMAB",
    "BAMLANIVIMAB",
    "SOTROVIMAB",
    "BEBTELOVIMAB",
    "PAXLOVID",
    "MOLNUPIRAVIR",
    "REMDESIVIR",
       "PAXLOVID", 
         "MOLNUPIR",
         "EVUSHELD",
         "TIXAGEVIMAB",
         "CILGAVIMAB",
         "BEBTELOVIMA",
         "SOTROVIMAB",
         "BAMLANIVIMAB",
         "ETESEVIMAB",
         "REGEN-COV",
         "CASIRIVIMAB",
         "IMDEVIMAB",
         "DEXAMETHASONE",
         "TOFACITINIB", 
         "TOCILIZUMAB",
         "SARILUMAB",
         "BARICITINIB",
         "REMDESIVIR",
         ],

}

# COMMAND ----------

text=' '.join(drugNames["DRUGNAME_COVID"]).lower()

#text="REMDESIVIR"
print(text)

# COMMAND ----------

import spacy
from scispacy.abbreviation import AbbreviationDetector
from scispacy.linking import EntityLinker
nlp = spacy.load("en_core_sci_lg")
nlp.add_pipe("scispacy_linker", config={"resolve_abbreviations": False, "linker_name": "rxnorm"})


# COMMAND ----------


#text="aspirin"
print(text)
doc = nlp(text)
linker = nlp.get_pipe("scispacy_linker")
linker.threshold = 0.99
# Let's look at a random entity!

for entity in doc.ents:
    print("Name: ", entity)
    for ent in entity._.kb_ents:
        print(ent)
        print(linker.kb.cui_to_entity[ent[0]])
