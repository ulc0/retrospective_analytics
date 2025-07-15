# Databricks notebook source
install mlflow-skinny==2.10.0
%pip install scikit-learn
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC dbutils.widgets.text('EXPERIMENT_ID',defaultValue='03949a4a5cc44998bfc511b6b11e5880')
# MAGIC EXPERIMENT_ID=dbutils.widgets.get('EXPERIMENT_ID')

# COMMAND ----------

# MAGIC %md
# MAGIC display(dbutils.fs.ls('/databricks-datasets'))
# MAGIC
# MAGIC =======
# MAGIC
# MAGIC %md
# MAGIC dbutils.widgets.text('EXPERIMENT_ID',defaultValue='03949a4a5cc44998bfc511b6b11e5880')
# MAGIC EXPERIMENT_ID=dbutils.widgets.get('EXPERIMENT_ID')
# MAGIC
# MAGIC #display(dbutils.fs.ls('/databricks-datasets'))
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC This is a notebook type error. looking ipynb context like jupyter
# MAGIC %sh
# MAGIC cd /databricks/python_shell/scripts/
# MAGIC python ./db_ipykernel_launcher.py configure

# COMMAND ----------

import os
os.environ['PYSPARK_PIN_THREAD']='false'
### 2024/01/24 13:25:31 WARNING mlflow.spark: With Pyspark >= 3.2, PYSPARK_PIN_THREAD environment variable must be set to false for Spark datasource autologging to work.

# COMMAND ----------

import mlflow as ml
print(ml.__version__)
ml.set_tracking_uri('databricks')
import mlflow.sklearn

import mlflow.spark

#ml.set_experiment(experiment_id=EXPERIMENT_ID)

ml.spark.autolog()



# COMMAND ----------


import sklearn 
print(f"scikit-learn version {sklearn.__version__}")
from sklearn import datasets
from sklearn.linear_model import LogisticRegression
ml.sklearn.autolog()

# COMMAND ----------

iris = datasets.load_iris()
print(iris)

# COMMAND ----------

with ml.start_run() as run:
    logistic= LogisticRegression(max_iter=1000)
    logistic.fit(iris.data, iris.target)
