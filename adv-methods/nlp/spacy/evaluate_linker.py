# Databricks notebook source
# MAGIC %pip install -r /Volumes/edav_prd_cdh/cdh_ml/metadata_compute/requirements_scispacy_support.txt
# MAGIC %pip freeze

# COMMAND ----------

from scispacy.candidate_generation import DEFAULT_PATHS, DEFAULT_KNOWLEDGE_BASES, CandidateGenerator, LinkerPaths
from scispacy.linking_utils import KnowledgeBase
basePath="/Volumes/edav_prd_cdh/cdh_ml/metadata_data/linker/umls"

UmlsLinkerPaths_2024AA = LinkerPaths(
ann_index=f"{basePath}/nmslib_index.bin",  # noqa
tfidf_vectorizer=f"{basePath}/tfidf_vectorizer.joblib",  # noqa
tfidf_vectors=f"{basePath}/tfidf_vectors_sparse.npz",  # noqa
concept_aliases_list=f"{basePath}/concept_aliases.json",  # noqa
)
print(UmlsLinkerPaths_2024AA)

class UMLS2024KnowledgeBase(KnowledgeBase):
    def __init__(
        self,
        file_path: str = f"/Volumes/edav_prd_cdh/cdh_ml/metadata_data/jsonl/umls_kb.jsonl",
    ):
        super().__init__(file_path)

print(UmlsLinkerPaths_2024AA)
print(UMLS2024KnowledgeBase)
# Admittedly this is a bit of a hack, because we are mutating a global object.
# However, it's just a kind of registry, so maybe it's ok.
DEFAULT_PATHS["umls2024"] = UmlsLinkerPaths_2024AA
print(DEFAULT_PATHS)
DEFAULT_KNOWLEDGE_BASES["umls2024"] = UMLS2024KnowledgeBase
print(DEFAULT_KNOWLEDGE_BASES)
LINKER = CandidateGenerator(name="umls2024")

# COMMAND ----------

CandidateGenerator(name="umls2024")

# COMMAND ----------

LINKER="umls"
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
rxnorm=' '.join([d.lower() for d in drug_list])
#rxnorm="aspirin"
testtext={"umls":umls,"rxnorm":rxnorm}

# COMMAND ----------

import spacy
#from scispacy.abbreviation import AbbreviationDetector
from scispacy.linking import EntityLinker
nlp = spacy.load("en_core_sci_lg")

# COMMAND ----------


# Add the abbreviation pipe to the spacy pipeline.
#nlp.add_pipe("abbreviation_detector", config={"make_serializable": True})


# COMMAND ----------

# MAGIC %md
# MAGIC testtext="G0439  Annual wellness visit, includes a PPPS, subsequent visit  (In-House)    99497-33  Advance care planning first 30 mins  (In-House)  1090F  Presence or absence of urinary incontinence assessed (GER)  (In-House)     1101F  Pt screen for fall risk; document no falls in past year or only 1 fall w/o injury in past year (GER)  (In-House)       Fall risk screening, >65, Q yr, <1 fall w/o injury   "
# MAGIC text=testtext
# MAGIC text="Myeloid derived suppressor cells (MDSC) are immature myeloid cells with immunosuppressive activity. They accumulate in tumor-bearing mice and humans with different types of cancer, including hepatocellular carcinoma (HCC)"
# MAGIC doc = nlp(text)
# MAGIC print(doc.ents)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC
# MAGIC
# MAGIC abblist=doc._.abbreviations
# MAGIC print(abblist)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #todo each "doc" is one note with a unique note_id, that note_id must be added to this output in the UDF
# MAGIC abbtable=spark.createDataFrame(abblist)
# MAGIC display(abbtable)

# COMMAND ----------


#print("Abbreviation", "\t", "Definition")

#for abrv in doc._.abbreviations:
	#print(f"{abrv} \t ({abrv.start}, {abrv.end}) {abrv._.long_form}")
#	print( abrv)

# COMMAND ----------

nlp.add_pipe("scispacy_linker", config={"resolve_abbreviations": False, "linker_name": LINKER})

# COMMAND ----------



doc = nlp(testtext["umls2024"])

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
