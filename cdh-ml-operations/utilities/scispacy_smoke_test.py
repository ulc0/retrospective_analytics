# Databricks notebook source
# MAGIC %pip freeze

# COMMAND ----------

import spacy
from tqdm import tqdm
#https://github.com/allenai/scispacy/issues/517
#https://stackoverflow.com/questions/52570805/how-to-identify-abbreviations-acronyms-and-expand-them-in-spacy
#https://aclanthology.org/W19-5034.pdf
#https://github.com/allenai/scispacy/issues/464
#
# after processing abbreviation, need to save note_id, start,end, expanded https://github.com/allenai/scispacy/issues/418
#
from scispacy.abbreviation import AbbreviationDetector
from scispacy.linking import EntityLinker

# COMMAND ----------

dbutils.widgets.text("PATH",defaultValue="/Volumes/edav_dev_cdh_test/dev_cdh_ml_test/compute/packages/wheels/")
def_model_names = [
    "en_core_sci_sm",
    "en_core_sci_md",
    "en_core_sci_lg",
    "en_core_sci_scibert",
    "en_ner_bc5cdr_md",
  #  "en_ner_craft_md",
  #  "en_ner_bionlp13cg_md",
  #  "en_ner_jnlpba_md",
]
dbutils.widgets.text("MODELS","en_core_sci_lg")
dbutils.widgets.text("VERSION","0.5.4")
PATH=dbutils.widgets.get("PATH")
print(PATH)
# can also specify model.path in spacy
MODELS=dbutils.widgets.get("MODELS")
VERSION=dbutils.widgets.get("VERSION")
model_names=MODELS.split(',')
print(model_names)

# COMMAND ----------

print("Testing core models...")
print()
#/Volumes/edav_dev_cdh_test/dev_cdh_ml_test/compute/packages/wheels/en_core_sci_lg-0.5.4.tar.gz
models = [
    spacy.load(f"{PATH}{model_name}-{VERSION}")
        for model_name in tqdm(model_names, desc="Loading core models")
]


# COMMAND ----------


text = (
    "DNA is a very important part of the cellular structure of the body. "
    "John uses IL gene and interleukin-2 to treat diabetes and "
    "aspirin as proteins for arms and legs on lemurs and humans."
)

for model_name, model in zip(model_names, models):
    print(f"Testing {model_name}")
    doc = model(text)
    for sentence in doc.sents:
        print([t.text for t in sentence])
        print([t.lemma_ for t in sentence])
        print([t.pos_ for t in sentence])
        print([t.tag_ for t in sentence])
        print([t.dep_ for t in sentence])
        print([t.ent_type_ for t in sentence])
        print()
    print()
    #input("Continue?")


# COMMAND ----------


print("Testing abbreviation detector...")
abbreviation_nlp = spacy.load("en_core_sci_sm")
abbreviation_nlp.add_pipe("abbreviation_detector")
abbreviation_text = (
    "Spinal and bulbar muscular atrophy (SBMA) is an inherited "
    "motor neuron disease caused by the expansion of a polyglutamine "
    "tract within the androgen receptor (AR). SBMA can be caused by this easily."
)
abbreviation_doc = abbreviation_nlp(abbreviation_text)
for abbrevation in abbreviation_doc._.abbreviations:
    print(
        f"{abbrevation} \t ({abbrevation.start}, {abbrevation.end}) {abbrevation._.long_form}"
    )
print()
#input("Continue?")

# COMMAND ----------



print("Testing entity linkers...")
print()
ontology_names = ["umls", "mesh", "rxnorm", "go", "hpo"]
ontology_models = [spacy.load("en_core_sci_sm") for _ in ontology_names]
for ontology_name, ontology_model in tqdm(
    zip(ontology_names, ontology_models), desc="Adding entity linker pipes"
):
    ontology_model.add_pipe(
        "scispacy_linker",
        config={"resolve_abbreviations": True, "linker_name": ontology_name},
    )

linking_text = "Diabetes is a disease that affects humans and is treated with aspirin via a metabolic process without paxlovid."
for ontology_name, ontology_model in zip(ontology_names, ontology_models):
    print(f"Testing {ontology_name} linker...")
    linker_pipe = ontology_model.get_pipe("scispacy_linker")
    doc = ontology_model(linking_text)
    for entity in doc.ents:
        print("Entity name: ", entity)
        for ontology_entity in entity._.kb_ents[:1]:
            print(linker_pipe.kb.cui_to_entity[ontology_entity[0]])
    print()
    #input("Continue?")

