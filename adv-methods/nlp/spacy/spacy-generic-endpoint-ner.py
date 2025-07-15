# Databricks notebook source
# MAGIC %pip freeze

# COMMAND ----------

import mlflow

# import mlflow.spacy
import spacy
ver=spacy.__version__
print(ver)
## NO, myst load the model import en_core_web_sm
###print(en_core_web_sm.__version__)
#import scispacy
#from scispacy.abbreviation import AbbreviationDetector
#from scispacy.linking import EntityLinker
from mlflow import log_metric, log_param, log_params, log_artifacts, log_dict
from mlflow.models import infer_signature

spacy_model = "en_core_web_sm"
version = "3.7.0"
artifact_path = f"{spacy_model}/ner"
pip_requirements = ["spacy",
        f"/Volumes/edav_dev_cdh_test/dev_cdh_ml_test/compute/packages/wheels/en_core_web_sm-{version}-py3-none-any.whl",
        f"/Volumes/edav_dev_cdh_test/dev_cdh_ml_test/compute/packages/wheels/spacy-3.7.4-cp310-cp310-manylinux_2_17_x86_64.manylinux2014_x86_64.wh",
#f"{spacy_model}",
#f"/dbfs/packages/sdist/{spacy_model}-0.{ver}.tar.gz",
]
print(pip_requirements)


# COMMAND ----------

nlp = spacy.load(spacy_model)
nlp.pipe_names
#nlpx=spacy.blank('en')
#nlp = nlps.add_pipe("textcat",last=True)
#textcat=nlpx.create_pipe( "textcat") # config={"exclusive_classes": True, "architecture": "simple_cnn"})
#nlp.add_pipe(textcat, last=True)
#nlp = nlp.add_pipe("textcat")

#


# COMMAND ----------


data = "Spinal and bulbar muscular atrophy (SBMA) is an \
           inherited motor neuron disease caused by the expansion \
           of a polyglutamine tract within the androgen receptor (AR). \
           SBMA can be caused by this easily."
# MLflow Tracking
doc = nlp(data)
for e in doc.ents:
    print(e)
#nlp.add_pipe("abbreviation_detector")
mlflow.end_run()
my_run_id = mlflow.start_run(run_name="spacy-sm")
mlflow.set_tag("model_flavor", "spacy")
output = nlp.pipe(
    data, disable=["tok2vec", "tagger", "parser", "attribute_ruler", "lemmatizer"]
)  # NER only need doc.ents
mlflow.spacy.log_model(
    spacy_model=nlp,
    artifact_path=artifact_path,
    registered_model_name="spacy-sm",
    input_example=data,
    pip_requirements=pip_requirements,
    # signature=signature, this is override
)
# my_run_id = mlflow.active_run().info.run_id

mlflow.end_run()

# MLflow Models
# model_uri = f'runs:/{my_run_id}/model'
# nlp2 = mlflow.spacy.load_model(model_uri=model_uri)
