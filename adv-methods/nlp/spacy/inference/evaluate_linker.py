# Databricks notebook source
#%pip install /Volumes/edav_dev_cdh_test/dev_cdh_ml_test/compute/packages/wheels/en_core_sci_sm-0.5.4.tar.gz

# COMMAND ----------

LINKER="umls2024"
umls="Spinal and bulbar muscular atrophy (SBMA) is an inherited motor neuron disease caused by the expansion of a polyglutamine tract within the androgen receptor (AR). "

drug_list=[
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
         ]
rxnorm=' '.join(drug_list)
rxnorm="aspirin"
testtext={"umls":umls,"rxnorm":rxnorm}

# COMMAND ----------

#testtext="G0439  Annual wellness visit, includes a PPPS, subsequent visit  (In-House)    99497-33  Advance care planning first 30 mins  (In-House)  1090F  Presence or absence of urinary incontinence assessed (GER)  (In-House)     1101F  Pt screen for fall risk; document no falls in past year or only 1 fall w/o injury in past year (GER)  (In-House)       Fall risk screening, >65, Q yr, <1 fall w/o injury   "
text=testtext[LINKER]

# COMMAND ----------

import spacy
from scispacy.abbreviation import AbbreviationDetector
from scispacy.linking import EntityLinker
nlp = spacy.load("en_core_sci_lg")

# COMMAND ----------

## abbreviation detector is not appropriate for these applications
# Add the abbreviation pipe to the spacy pipeline.
#nlp.add_pipe("abbreviation_detector", config={"make_serializable": True})

doc = nlp(text)

# COMMAND ----------




#abblist=doc._.abbreviations
#print(abblist)


# COMMAND ----------

#abbtable=spark.createDataFrame(abblist)
#display(abbtable)

# COMMAND ----------


#print("Abbreviation", "\t", "Definition")

#for abrv in doc._.abbreviations:
	#print(f"{abrv} \t ({abrv.start}, {abrv.end}) {abrv._.long_form}")
#	print( abrv)

# COMMAND ----------

nlp.add_pipe("scispacy_linker", config={"resolve_abbreviations": False, "linker_name": LINKER})

# COMMAND ----------



doc = nlp(text)

# Let's look at a random entity!
for entity in doc.ents:
    print("Name: ", entity)



# COMMAND ----------



# Each entity is linked to UMLS with a score
# (currently just char-3gram matching).
linker = nlp.get_pipe("scispacy_linker")
linker.threshold = 0.70
for entity in doc.ents:
    for lnkd_ent in entity._.kb_ents:
        print(lnkd_ent)
        print(linker.kb.cui_to_entity[lnkd_ent[0]])
