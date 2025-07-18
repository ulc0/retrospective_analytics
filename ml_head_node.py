# Databricks notebook source
dbutils.widgets.text('begin',  defaultValue = "2018412")
dbutils.widgets.text('end', defaultValue="2022103")
dbutils.widgets.text('gitbranch',defaultValue="exploratory")
dbutils.widgets.text('experiment_id',defaultValue='')

# COMMAND ----------

#mandatory parameters and names. The orchestrator will always pass these
beginDate=dbutils.widgets.get('begin')
endDate=dbutils.widgets.get('end')
gitbranch=dbutils.widgets.get('gitbranch')
experiment_id=dbutils.widgets.get('experiment_id')

dbutils.jobs.taskValues.set('begin',beginDate)
dbutils.jobs.taskValues.set('end',endDate)
dbutils.jobs.taskValues.set('gitbranch',gitbranch)
dbutils.jobs.taskValues.set('experiment_id',experiment_id)

