# Databricks notebook source
# MAGIC %md
# MAGIC pwd
# MAGIC date -d @1681149639000
# MAGIC date -d @1655929734000

# COMMAND ----------

initscripts="/Workspace/CDH/Config/init_scripts/"
#/Volumes/edav_prd_cdh/cdh_ml/metadata_compute
wheels="/Volumes/edav_prd_cdh/cdh_ml/metadata_compute/packages/wheels/"
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
# MAGIC mkdir /Volumes/edav_prd_cdh/cdh_ml/metadata_compute/packages/jars
# MAGIC ls /Volumes/edav_prd_cdh/cdh_ml/metadata_compute/packages
# MAGIC #mkdir packages
# MAGIC #mkdir jars
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC pwd
# MAGIC ls -ll  ../packages/wheels
# MAGIC #yes | cp  /dbfs/packages/wheels/*.whl  /dbfs/packages/wheels/obs/
# MAGIC #rm -f  /dbfs/packages/wheels/*.whl
# MAGIC #ls -ll  /dbfs/packages/wheels/obs/*.whl
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC pwd
# MAGIC #ls -ll ../packages/wheels/*.whl
# MAGIC cp ../packages/wheels/*.whl /Volumes/edav_prd_cdh/cdh_ml/metadata_compute/packages/wheels/
# MAGIC ls -ll /Volumes/edav_prd_cdh/cdh_ml/metadata_compute/packages/wheels/
