# Databricks notebook source
import os
from shared.configs import set_secret_scope
set_secret_scope()

#os.environ["PYSPARK_PIN_THREAD"]=False
dbutils.widgets.text("schema",defaultValue="edav_prd_cdh.cdh_premier_exploratory")
dbutils.widgets.text("experiment_id",defaultValue="")
dbutils.widgets.text("begin",defaultValue="01/01/2020")
dbutils.widgets.text("end",defaultValue="12/31/2021")

schema=dbutils.widgets.get("schema")
experiment_id=dbutils.widgets.get("experiment_id")
beginDate=dbutils.widgets.get("begin")
endDate=dbutils.widgets.get("end")
#import mlflow.spark
