# Databricks notebook source
# %sh pwd

# COMMAND ----------

import pyspark

dbutils.widgets.text('experiment_id',defaultValue='')
experiment_id=dbutils.widgets.get('experiment_id')

dbutils.widgets.text("run_name", defaultValue="test")
run_name = dbutils.widgets.get("run_name")

dbutils.widgets.text("M", defaultValue="200")
M = int(dbutils.widgets.get("M"))

dbutils.widgets.text("DRAWS", defaultValue="200")
DRAWS = int(dbutils.widgets.get("DRAWS"))


dbutils.widgets.text("TUNE", defaultValue="200")
TUNE = int(dbutils.widgets.get("TUNE"))

dbutils.widgets.text("CORES", defaultValue="4")
CORES = int(dbutils.widgets.get("CORES"))

dbutils.widgets.text("SPLIT_RULES", defaultValue="[pmb.ContinuousSplitRule(), pmb.ContinuousSplitRule(), pmb.OneHotSplitRule(), pmb.OneHotSplitRule()]")
SPLIT_RULES = dbutils.widgets.get("SPLIT_RULES")


# COMMAND ----------



dbutils.jobs.taskValues.set('experiment_id',experiment_id)
dbutils.jobs.taskValues.set("run_name", run_name)
dbutils.jobs.taskValues.set("M", M)
dbutils.jobs.taskValues.set("DRAWS", DRAWS)
dbutils.jobs.taskValues.set("TUNE", TUNE)
dbutils.jobs.taskValues.set("CORES", CORES)
dbutils.jobs.taskValues.set("SPLIT_RULES", SPLIT_RULES)




