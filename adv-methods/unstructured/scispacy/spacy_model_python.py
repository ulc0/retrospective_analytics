import os
import spacy
import mlflow
import mlflow.spacy
from mlflow import log_metric, log_param, log_params, log_artifacts

SCISPACY_CACHE=os.getenv("SCISPACY_CACHE")
model_name=dbutils.jobs.taskValues("spacy_setup","model_name")
run_id=dbutils.jobs.taskValues('spacy_setup','run_id')
experiment_id=dblutils.jobs.taskValues('cdh_ml_models','experiment_id')

doctext = (
    "DNA is a very important part of the cellular structure of the body. "
    "John uses IL gene and interleukin-2 to treat diabetes and "
    "aspirin as proteins for arms and legs on lemurs and humans."
)

#nlp_model=spacy.load(model_name)


from scispacy.linking import EntityLinker
from scispacy.abbreviation import AbbreviationDetector
print("Testing entity linkers...")
lnk_name="umls" #future parameter
nlp = spacy.load(model_name)
nlp.add_pipe( "scispacy_linker",
             config={"resolve_abbreviations": True, 
                     "linker_name": lnk_name,
                      "cache_folder": SCISPACY_CACHE,},)
nlp.add_pipe("abbreviation_detector")

#umls_pipe = nlp.get_pipe("scispacy_linker")
doc = nlp(doctext)

with mlflow.start_run(experiment_id=experiment_id,run_id=run_id):
    mlflow.set_tag('model_flavor', 'spacy')
    mlflow.set_tag('linker_name',lnk_name)
    mlflow.set_tag('model_name',model_name)
    mlflow.artifact('text',doctext)
    mlflow.spacy.log_model(spacy_model=model_name, artifact_path='model')
   # mlflow.log_metric(('accuracy', 0.72))
    doc = nlp(doctext)
    print(doc.ents)
    results={}
    for umls_entity in doc.ents:
        #print("Entity name: ", umls_entity)
        concepts=[]
        print(umls_entity)
        for concept_score in umls_entity._.kb_ents:
            #Concept_Id, Score = ontology_entity
           concepts.append(concept_score) #tuple
            #print(umls_pipe.kb.cui_to_entity[ontology_entity[0]]
        results[umls_entity]=concepts
    mlflow.log_dict(results,f"{model_name}_{linker_name}_results.json")    
    