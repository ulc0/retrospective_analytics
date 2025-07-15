# Databricks notebook source
import os
ss_cache="SCISPACY_CACHE"
print(os.getenv(ss_cache))
#os.environ["SCISPACY_CACHE"]="/dbfs/mnt/ml/scispacy_cache/"
#model_dir='/dbfs/packages/sdist/'

# COMMAND ----------

import spacy



# COMMAND ----------

model_names = [
    #  "en_core_sci_sm",
   # "en_core_sci_md",
    #"en_core_sci_lg",
    "en_core_sci_scibert",
    # "en_ner_bc5cdr_md",
    # "en_ner_craft_md",
    #"en_ner_bionlp13cg_md",
    # "en_ner_jnlpba_md",
]

models = [
    spacy.load(model_name)
    for model_name in model_names ]

# COMMAND ----------

text = (
    "Spinal and bulbar muscular atrophy (SBMA) is an inherited motor neuron disease caused by the expansion of a" "polyglutamine tract within the androgen receptor (AR). SBMA can be caused by this easily."
)
text=("RESPIRATORY: normal breath sounds with no rales, rhonchi, wheezes or rubs;"
      " CARDIOVASCULAR: normal rate; " "rhythm is regular; no systolic murmur; "
      "GASTROINTESTINAL: nontender; normal bowel sounds; "
      "LYMPHATICS: no " 
      "MUSCULOSKELETAL: normal gait pain with range of motion in Left wrist ormal tone;"
      " NEUROLOGIC: appropriate for age;"
       "Lab/Test Results: X-RAY INTERPRETATION: ORTHOPEDIC X-RAY: Left wrist(AP view): (+) fracture: of the distal radius")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC for model_name, model in zip(model_names, models):
# MAGIC     print(f"Testing {model_name}")
# MAGIC     doc = model(text)
# MAGIC     for sentence in doc.sents:
# MAGIC         print([t.text for t in sentence])
# MAGIC         print([t.lemma_ for t in sentence])
# MAGIC         print([t.pos_ for t in sentence])
# MAGIC         print([t.tag_ for t in sentence])
# MAGIC         print([t.dep_ for t in sentence])
# MAGIC         print([t.ent_type_ for t in sentence])
# MAGIC         print()
# MAGIC     print()
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC from scispacy.abbreviation import AbbreviationDetector
# MAGIC print("Testing abbreviation detector...")
# MAGIC abbreviation_nlp = spacy.load("en_core_sci_scibert")
# MAGIC abbreviation_nlp.add_pipe("abbreviation_detector")
# MAGIC abbreviation_text = (
# MAGIC     "Spinal and bulbar muscular atrophy (SBMA) is an inherited "
# MAGIC     "motor neuron disease caused by the expansion of a polyglutamine "
# MAGIC     "tract within the androgen receptor (AR). SBMA can be caused by this easily."
# MAGIC )
# MAGIC abbreviation_doc = abbreviation_nlp(abbreviation_text)
# MAGIC for abbrevation in abbreviation_doc._.abbreviations:
# MAGIC     print(
# MAGIC         f"{abbrevation} \t ({abbrevation.start}, {abbrevation.end}) {abbrevation._.long_form}"
# MAGIC     )
# MAGIC

# COMMAND ----------

from scispacy.linking import EntityLinker
from scispacy.abbreviation import AbbreviationDetector
print("Testing entity linkers...")
lnk_names = ["umls", "mesh", "rxnorm", "go", "hpo"]
lnk_name="umls"
nlp = spacy.load("en_core_sci_scibert")
nlp.add_pipe( "scispacy_linker",config={"resolve_abbreviations": True, "linker_name": lnk_name},)
nlp.add_pipe("abbreviation_detector")
linking_text = "Diabetes is a disease that affects humans and is treated with aspirin via a metabolic process."
print(f"Testing {lnk_name} linker...")
# this is the decode 
umls_pipe = nlp.get_pipe("scispacy_linker")
doc = nlp(text)


# COMMAND ----------

#print(type(doc.ents))
for umls_entity in doc.ents:
        #print("Entity name: ", umls_entity)
        print(umls_entity)
        for concept_score in umls_entity._.kb_ents:
            #Concept_Id, Score = ontology_entity
            print(concept_score)
            #print(umls_pipe.kb.cui_to_entity[ontology_entity[0]])

# COMMAND ----------

# MAGIC %md
# MAGIC from scispacy.linking import EntityLinker
# MAGIC print("Testing entity linkers...")
# MAGIC ontology_names = ["umls", "mesh", "rxnorm", "go", "hpo"]
# MAGIC ontology_names=["umls"]
# MAGIC ontology_models = [spacy.load("en_core_sci_scibert") for _ in ontology_names]
# MAGIC for ontology_name, ontology_model in zip(ontology_names, ontology_models):
# MAGIC     ontology_model.add_pipe(
# MAGIC         "scispacy_linker",
# MAGIC         config={"resolve_abbreviations": True, "linker_name": ontology_name},
# MAGIC     )
# MAGIC
# MAGIC linking_text = "Diabetes is a disease that affects humans and is treated with aspirin via a metabolic process."
# MAGIC for ontology_name, ontology_model in zip(ontology_names, ontology_models):
# MAGIC     print(f"Testing {ontology_name} linker...")
# MAGIC     linker_pipe = ontology_model.get_pipe("scispacy_linker")
# MAGIC     doc = ontology_model(text)
# MAGIC     for entity in doc.ents:
# MAGIC         print("Entity name: ", entity)
# MAGIC         for ontology_entity in entity._.kb_ents[:1]:
# MAGIC             print(linker_pipe.kb.cui_to_entity[ontology_entity[0]])
