# Databricks notebook source
# %md 
# ## Which are run parameters? Which are Job parameters?

# COMMAND ----------

import json
# Dictionary of parameter values
PARMS={
  "CONFIG1":  {
  "TUNE": "200",
  "M": "200",
  "DRAWS": "200"}
}

# COMMAND ----------


experiment_id=dbutils.widgets.get('experiment_id')
defaultConfig=str(PARMS["CONFIG1"])
print(defaultConfig)
dbutils.widgets.text('run_name',defaultValue="pymc-bart")
dbutils.widgets.text('tparms',defaultValue=defaultConfig)
#task parameters must be string, task values may be other types 
# Presume this is a run block? 
# should run name be based on parameterization? Or Run Description
RUN_NAME =dbutils.widgets.get("run_name")

CONFIG=dbutils.widgets.get('tparms')
TPARMS=PARMS[CONFIG]
print(TPARMS)
#CORES       = (TPARMS["CORES"])
TUNE        = int(TPARMS["TUNE"])

M           = int(TPARMS["M"])
DRAWS       = int(TPARMS["DRAWS"])

# COMMAND ----------

#dbutils.jobs.taskValues.set("CORES", CORES)
dbutils.jobs.taskValues.set("TUNE", TUNE)
dbutils.jobs.taskValues.set("RUN_NAME", RUN_NAME)
#dbutils.jobs.taskValues.set("SPLIT_RULES", SPLIT_RULES)
dbutils.jobs.taskValues.set("M", M)
dbutils.jobs.taskValues.set("DRAWS", DRAWS)



