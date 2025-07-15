# Databricks notebook source
# MAGIC %md
# MAGIC pwd
# MAGIC date -d @1681149639000
# MAGIC date -d @1655929734000

# COMMAND ----------

initscripts="/Workspace/CDH/init_scripts/"
wheels="/dbfs/packages/wheels/"
# abfss doesn't like shell commands, and it not recommended by databricks. Current best practice is use /Workspace
#cdhls=dbutils.fs.ls(cdh_mnt)
#ml=cdh_mnt+'/machinelearning'
#initscripts='/scripts'
#mlscripts=ml+initscripts
#dbutils.fs.mkdirs(mlscripts)
#cdhmlls=dbutils.fs.ls(mlscripts)
#print(cdhmlls)

# Out[15]: True

# COMMAND ----------

# MAGIC %sh
# MAGIC #mkdir /dbfs/packages
# MAGIC #mkdir /dbfs/packages/wheels
# MAGIC #mkdir /dbfs/packages/wheels/obs
# MAGIC cd
# MAGIC ls -ll  /dbfs/packages/wheels/
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC pwd
# MAGIC ls -ll  ../packages/wheels
# MAGIC yes | cp  /dbfs/packages/wheels/*.whl  /dbfs/packages/wheels/obs/
# MAGIC rm -f  /dbfs/packages/wheels/*.whl
# MAGIC ls -ll  /dbfs/packages/wheels/obs/*.whl

# COMMAND ----------

# MAGIC %sh
# MAGIC pwd
# MAGIC ls -ll ../packages/wheels/*.whl
# MAGIC cp ../packages/wheels/*.whl /dbfs/packages/wheels/
# MAGIC ls -ll /dbfs/packages/wheels/*.whl
