# Databricks notebook source
# sample head node for setting task values

dbutils.widgets.text('experiment_id',defaultValue='')

# COMMAND ----------

#mandatory parameters and names. The orchestrator will always pass these
# gitbranch will tell 
dbutils.jobs.taskValues.set('experiment_id',experiment_id)

