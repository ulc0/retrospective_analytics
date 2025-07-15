# Databricks notebook source
import mlflow, os


experiment_id=dbutils.jobs.taskValues.get("cdh-ml-model","experiment_id", debugValue=2256023545555400)

# COMMAND ----------

dbutils.widgets.text("run_name",default="test2")
this_run_name = dbutils.widgets.get("run_name")

# COMMAND ----------

with mlflow.start_run(experiment_id=experiment_id,run_name=this_run_name) as run:
   run_id=run.info.run_id
   mlflow.log_param('run_id_main', run_id)
   mlflow.log_param('run_name', this_run_name)

# COMMAND ----------

dbutils.jobs.taskValues.set("run_id_main",run_id)

# COMMAND ----------

# # run_info = mlflow.active_run()
# # run_id = run_info.info.run_id
# # OUTPUTS = "outputs"
# ALPHA = 3
# # ALPHA_F = "1 + (1.5 * x_mat[:,0]) + x_mat[:,1]"
# LAMBDA = "np.exp(2 + 0.4*(x_mat[:,0] + x_mat[:,1]))"
# # LAMBDA = "np.exp(1 + .2*x_mat[:,0] + .3*x_mat[:,1] + 0.8*np.sin(x_mat[:,0] * x_mat[:,1]) + np.power((x_mat[:,2] - 0.5),2))"
# TRAIN_CSV = "outputs/train.csv"
# RBART_CSV = "outputs/rbart_surv.csv"
# N = 100
# X_VARS = 2
# CENS_IND = False
# CENS_SCALE = 60
