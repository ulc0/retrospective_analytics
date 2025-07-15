# Databricks notebook source
import mlflow, os
# proforma, lists modules, maybe sets dictionary of available models as taskValue? 
experiment_id=dbutils.widgets.get("experiment_id")
model_name=dbutils.widgets.get("model_name")

with mlflow.start_run(experiment_id=experiment_id) as run:
    run_id=run.info.run_id
    mlflow.log_value('run_id',run_id)
    mlflow.log_value('model_name',model_name)

os['MLFLOW_RUN_ID']=run_id
dbutils.jobs.taskValues.set("run_id",run_id)
dbutils.jobs.taskValues.set("model_name",model_name)

