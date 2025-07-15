# Databricks notebook source
import pyspark

dbutils.widgets.text('gitbranch',defaultValue="exploratory")
dbutils.widgets.text('experiment_id',defaultValue='')

# COMMAND ----------

#mandatory parameters and names. The orchestrator will always pass these
# gitbranch will tell 
gitbranch=dbutils.widgets.get('gitbranch')
experiment_id=dbutils.widgets.get('experiment_id')
dbutils.jobs.taskValues.set('gitbranch',gitbranch)
dbutils.jobs.taskValues.set('experiment_id',experiment_id)

