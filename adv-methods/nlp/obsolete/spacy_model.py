import spacy
import mlflow
import mlflow.spacy
from mlflow import log_metric, log_param, log_params, log_artifacts

model_name=dbutils.jobs.taskValues("spacy_setup","model_name")
run_id=dbutils.jobs.taskValues('spacy_setup','run_id')
experiment_id=dbutils.jobs.taskValues('cdh_ml_models','experiment_id')

doctext = (
    "DNA is a very important part of the cellular structure of the body. "
    "John uses IL gene and interleukin-2 to treat diabetes and "
    "aspirin as proteins for arms and legs on lemurs and humans."
)

nlp_model=spacy.load(model_name)
with mlflow.start_run(experiment_id=experiment_id,run_id=run_id):
    mlflow.set_tag('model_flavor', 'spacy')
    mlflow.artifact('text',doctext)
    mlflow.spacy.log_model(spacy_model=nlp_model, artifact_path='model')
    mlflow.log_metric(('accuracy', 0.72))
        

    doc = nlp_model(doctext)
    for sentence in doc.sents:
        print([t.text for t in sentence])
        print([t.lemma_ for t in sentence])
        print([t.pos_ for t in sentence])
        print([t.tag_ for t in sentence])
        print([t.dep_ for t in sentence])
        print([t.ent_type_ for t in sentence])
    