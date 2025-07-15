# Databricks notebook source
dbutils.widgets.text("CATALOG",defaultValue="HIVE_METASTORE")
CATALOG=dbutils.widgets.get("CATALOG")
dbutils.widgets.text("SRC_SCHEMA",defaultValue="edav_dev_edav_prd_cdh.cdh_test.dev_edav_prd_cdh.cdh_ml_test")
SRC_SCHEMA=dbutils.widgets.get("SRC_SCHEMA")

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

import pandas as pd
import pyspark as ps


import spacy

#from scispacy.umls_linking import UmlsEntityLinker
from scispacy.abbreviation import AbbreviationDetector
from scispacy.linking import EntityLinker


DEFAULT_TEXT = "Spinal and bulbar muscular atrophy (SBMA) is an inherited motor neuron disease caused by the expansion of a polyglutamine tract within the androgen receptor (AR). SBMA can be caused by this easily."

def load_model(name):
    nlp = spacy.load(name)
    # Add abbreviation detector
    abbreviation_pipe = AbbreviationDetector(nlp)
    nlp.add_pipe(abbreviation_pipe)
    return nlp


spacy_model = "en_core_sci_scibert"
nlp = load_model(spacy_model)
#linker = UmlsEntityLinker(resolve_abbreviations=True)

linker = nlp.get_pipe("scispacy_linker")
for umls_ent in entity._.kb_ents:
	print(linker.kb.cui_to_entity[umls_ent[0]])
#threshold = st.sidebar.slider("Mention Threshold", 0.0, 1.0, 0.85)
linker.threshold=0.85

ner_model="en_ner_bc5cdr_md"
ner = load_model(ner_model)

#about 17k
ddesc=list(spark.sql("select distinct std_chg_DESC as rxname from chgmstr where PROD_NAME_DESC <>'Unknown'").toPandas()['rxname'])



for drugname in ddesc:
    doc = process_text(spacy_model, drugname)
    ner_doc = process_text(ner_model, drugname)

    data = []
    for ent in linker(doc).ents:
        for ent_id, score in ent._.umls_ents:

            kb_entity = linker.umls.cui_to_entity[ent_id]
            tuis = ",".join(kb_entity.types)
            data.append([
                drugname,
                ent.text,
                kb_entity.canonical_name,
                ent_id,
                tuis,
                score,
                ent.start,
                ent.end,
            ])

df = pd.DataFrame(data)
print(df)

# COMMAND ----------

#fp_name=f"{{CATALOG}.SRC_SCHEMA}.ft_drug_table"
#fp.write.mode("overwrite").format("delta").saveAsTable(ft_name)
#display(fp)

