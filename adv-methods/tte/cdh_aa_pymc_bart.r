# Databricks notebook source
# MAGIC %md
# MAGIC matrix_run_id=dbutils.jobs.taskValues.get(ptask, 'matrix_run_id', 
# MAGIC                                       default='3613c92f3f784cc298ed75976beb40ea',
# MAGIC                                       debugValue='3613c92f3f784cc298ed75976beb40ea')
# MAGIC
# MAGIC print(matrix_run_id)
# MAGIC #event_data_txt=mlflow.artifacts.load_text(matrix_uri)
# MAGIC
# MAGIC mfname=dbutils.jobs.taskValues.get(ptask, 'matrix_name', 
# MAGIC                                       default='ml_kickstarter_success_days_matrix.npy',
# MAGIC                                       debugValue='ml_kickstarter_success_days_matrix.npy')
# MAGIC print(mfname)

# COMMAND ----------

matrix_run_id='3613c92f3f784cc298ed75976beb40ea'
mfname='./ml_kickstarter_success_days_matrix.npy'

# COMMAND ----------

library(mlflow)
library(reticulate)
library(BART)

# COMMAND ----------

artifacts <- mlflow_download_artifacts(path='.', run_id = matrix_run_id,)
np <- import("numpy")
print(artifacts)
print(mfname)
list.files(artifacts)
setwd(artifacts)

# COMMAND ----------

mat<-np$load(mfname, allow_pickle=TRUE)
print(mat)
