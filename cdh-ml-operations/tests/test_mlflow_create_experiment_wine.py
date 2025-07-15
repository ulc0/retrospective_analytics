# Databricks notebook source
# MAGIC %pip install mlflow-skinny==2.9.2
# MAGIC %pip install scikit-learn openpyxl
# MAGIC dbutils.library.restartPython() 

# COMMAND ----------

# MAGIC %md
# MAGIC This is a notebook type error. looking ipynb context like jupyter
# MAGIC %sh
# MAGIC cd /databricks/python_shell/scripts/
# MAGIC python ./db_ipykernel_launcher.py configure

# COMMAND ----------

dbutils.widgets.text('EXPERIMENT_ID',defaultValue='5d1f0b6f227e4dcebb550f2c9c913d8b')
EXPERIMENT_ID=dbutils.widgets.get('EXPERIMENT_ID')

dbutils.widgets.text('task_name',defaultValue='flat-many')
dbutils.widgets.text('MAX_DEPTH',defaultValue='2')
dbutils.widgets.text('MAX_LEAF_NODES',defaultValue='16')
run_origin=dbutils.widgets.get('task_name')
max_depth=int(dbutils.widgets.get('MAX_DEPTH'))
max_leaf_nodes=int(dbutils.widgets.get('MAX_LEAF_NODES'))


# COMMAND ----------

import os
os.environ['PYSPARK_PIN_THREAD']='false'
### 2024/01/24 13:25:31 WARNING mlflow.spark: With Pyspark >= 3.2, PYSPARK_PIN_THREAD environment variable must be set to false for Spark datasource autologging to work.

# COMMAND ----------

import pandas as pd
import numpy as np

import mlflow
print(mlflow.__version__)
import mlflow.sklearn
import mlflow.data
from mlflow.data.pandas_dataset import PandasDataset

mlflow.set_tracking_uri('databricks')
#mlflow.set_tracking_uri('databricks-uc')

#mlflow.set_experiment(experiment_id=EXPERIMENT_ID)
#ml.spark.autolog()


# COMMAND ----------


def eval_metrics(actual, pred):
    rmse = np.sqrt(mean_squared_error(actual, pred))
    mae = mean_absolute_error(actual, pred)
    r2 = r2_score(actual, pred)
    return rmse, mae, r2
import matplotlib.pyplot as plt

def create_plot_file(y_test_set, y_predicted, plot_file):
    global image
    fig, ax = plt.subplots()
    ax.scatter(y_test_set, y_predicted, edgecolors=(0, 0, 0))
    ax.plot([y_test_set.min(), y_test_set.max()], [y_test_set.min(), y_test_set.max()], 'k--', lw=4)
    ax.set_xlabel('Actual')
    ax.set_ylabel('Predicted')
    ax.set_title("Ground Truth vs Predicted")
    #plt.show()

    image = fig
    fig.savefig(plot_file)
    plt.close(fig)      

# COMMAND ----------


import sklearn 
print(f"scikit-learn version {sklearn.__version__}")
from sklearn import datasets
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.tree import DecisionTreeRegressor
mlflow.sklearn.autolog()

# COMMAND ----------

data_path="https://raw.githubusercontent.com/mlflow/mlflow/master/examples/sklearn_elasticnet_wine/wine-quality.csv"
wine = pd.read_csv(data_path)
print(wine)
winedata: PandasDataset = mlflow.data.from_pandas(wine, source=data_path)
mlflow.log_input(winedata, context="wine-quality")
train, test = train_test_split(wine)
#mlflow.log_input(train, context="training")
#mlflow.log_input(test, context="test")


# COMMAND ----------

# create sklearn inputs
# The predicted column is "quality" which is a scalar from [3, 9]
train_x = train.drop(["quality"], axis=1)
test_x = test.drop(["quality"], axis=1)
train_y = train[["quality"]]
test_y = test[["quality"]]

# COMMAND ----------

mlflow.end_run()
with mlflow.start_run(run_name=run_origin) as run:
    run_id = run.info.run_uuid
    experiment_id = run.info.experiment_id
    print("MLflow:")
    print("  run_id:",run_id)
    print("  experiment_id:",experiment_id)

    # Create model
    dt = DecisionTreeRegressor(max_depth=max_depth, max_leaf_nodes=max_leaf_nodes)
    print("Model:",dt)

    # Fit and predict
    dt.fit(train_x, train_y)
    predictions = dt.predict(test_x)
    mlflow.log_input

    # MLflow params
    print("Parameters:")
    print("  max_depth:",max_depth)
    print("  max_leaf_nodes:",max_leaf_nodes)
    mlflow.log_param("max_depth", max_depth)
    mlflow.log_param("max_leaf_nodes", max_leaf_nodes)

    # MLflow metrics
    rmse = np.sqrt(mean_squared_error(test_y, predictions))
    mae = mean_absolute_error(test_y, predictions)
    r2 = r2_score(test_y, predictions)
    print("Metrics:")
    print("  rmse:",rmse)
    print("  mae:",mae)
    print("  r2:",r2)
    mlflow.log_metric("rmse", rmse)
    mlflow.log_metric("r2", r2)
    mlflow.log_metric("mae", mae)
    
    # MLflow tags
    mlflow.set_tag("data_path", data_path)
    mlflow.set_tag("exp_id", experiment_id)

    # MLflow log model
    mlflow.sklearn.log_model(dt, "sklearn-model-decision-tree")

    #svc = SVC(random_state=42)
    #svc.fit(X_train, y_train)
    #svc_disp = RocCurveDisplay.from_estimator(svc, X_test, y_test)
    
    os.makedirs("results", exist_ok=True)
    # MLflow log plot file artifact
    create_plot_file(test_y, predictions, "results/plot.png")
    train.to_excel("results/train.xls",engine="openpyxl")    
    test.to_excel("results/test.xls",engine="openpyxl")    


    mlflow.log_artifacts("results", artifact_path="results")
    #mlflow.log_artifact("plot.png")


# COMMAND ----------


dbutils.listdir(".")
destVol="dbfs:/Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data"
dbutils.fs.ls("results")
#dbutils.fs.cp('results/plot.png',f'{destVol}/plot.png')
#create_plot_file(test_y, predictions, "/Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data/plot.png")
#train.to_excel("/Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data/train.xls",engine="openpyxl")    
#test.to_excel("/Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data/test.xls",engine="openpyxl")    
 

# COMMAND ----------

mlflow.end_run()
#train(4,7,"deep-few")


# COMMAND ----------


