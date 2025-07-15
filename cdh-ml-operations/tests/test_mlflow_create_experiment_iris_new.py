# Databricks notebook source
# MAGIC %pip install mlflow-skinny==2.9.2
# MAGIC %pip install scikit-learn
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
print(mlflow.__version__)
import mlflow.sklearn
#optional, databricks will automatically create an experiment for a notebook, this is to override with an experiment shared among notebooks
#mlflow.set_experiment(experiment_id=EXPERIMENT_ID) <-- EXPERIMENT_ID=<experiment id from Experiments tab> set elsewhere or hardcoded here

# COMMAND ----------

# MAGIC %md
# MAGIC import os  
# MAGIC os.environ['PYSPARK_PIN_THREAD']='false'  
# MAGIC import mlflow.spark
# MAGIC #enable spark autologging, may require additional configuration
# MAGIC #mlflow.spark.autolog()   
# MAGIC #
# MAGIC #2024/01/24 13:25:31 WARNING mlflow.spark: With Pyspark >= 3.2, PYSPARK_PIN_THREAD environment variable must be set to false for Spark datasource autologging to work.  

# COMMAND ----------


import sklearn 
print(f"scikit-learn version {sklearn.__version__}")
from sklearn import datasets
from sklearn.linear_model import LogisticRegression
#mlflow has an sklearn autolog()
mlflow.sklearn.autolog()

# COMMAND ----------

iris = datasets.load_iris()

# COMMAND ----------

# specify run name or databricks will generate a name. The name may be a python variable or python string including f-string
with mlflow.start_run(run_name="iris-logistic-hv") as run:
    logistic= LogisticRegression(max_iter=1000)
    logistic.fit(iris.data, iris.target)
