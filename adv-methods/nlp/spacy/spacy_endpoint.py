# Databricks notebook source
import mlflow
import mlflow.spacy

# MLflow Tracking
nlp = spacy.load('my_best_model_path/output/model-best')
with mlflow.start_run(run_name='Spacy'):
    mlflow.set_tag('model_flavor', 'spacy')
    mlflow.spacy.log_model(spacy_model=nlp, artifact_path='model')
    mlflow.log_metric(('accuracy', 0.72))
    my_run_id = mlflow.active_run().info.run_id


# MLflow Models
model_uri = f'runs:/{my_run_id}/model'
nlp2 = mlflow.spacy.load_model(model_uri=model_uri)
