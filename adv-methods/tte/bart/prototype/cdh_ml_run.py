# Databricks notebook source
import mlflow, os


experiment_id=dbutils.jobs.taskValues.get("cdh-ml-model","experiment_id", debugValue=2248668235047615)
run_name = dbutils.jobs.taskValues.get("cdh-ml-init", "run_name", debugValue="test2")
M = dbutils.jobs.taskValues.get("cdh-ml-init", "M", debugValue=200)
DRAWS = dbutils.jobs.taskValues.get("cdh-ml-init", "DRAWS", debugValue=200)
TUNE = dbutils.jobs.taskValues.get("cdh-ml-init", "TUNE", debugValue= 200)
CORES = dbutils.jobs.taskValues.get("cdh-ml-init", "CORES", debugValue=4)
SPLIT_RULES = dbutils.jobs.taskValues.get("cdh-ml-init", "SPLIT_RULES", debugValue="[pmb.ContinuousSplitRule(), pmb.ContinuousSplitRule(), pmb.OneHotSplitRule(), pmb.OneHotSplitRule()]")


# COMMAND ----------




with mlflow.start_run(experiment_id=experiment_id) as run:
   run_id=run.info.run_id
   # set the run name here
   #mlflow.log_param('run_id_main', run_id)
   #mlflow.log_param('run_name', run_name)
   mlflow.log_param("M", M)
   mlflow.log_param("DRAWS", DRAWS)
   mlflow.log_param("TUNE", TUNE)
   mlflow.log_param("CORES", CORES)
   mlflow.log_param("SPLIT_RULES", SPLIT_RULES)


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
