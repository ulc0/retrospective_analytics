# Databricks notebook source
# MAGIC %md
# MAGIC # https://statusneo.com/mlflow-wrappers-adding-our-own-code-to-a-pre-trained-model/

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id",  dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-client-id"))
spark.conf.set("fs.azure.account.oauth2.client.secret", dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-client-secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint", dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-tenant-id-endpoint"))

# https://learn.microsoft.com/en-us/azure/machine-learning/how-to-log-view-metrics 

# COMMAND ----------

# this code is to convert spark data to a matrix format suitable for pytensor and write to mlflow
PYTENSOR_FLAGS='allow_gc=False,scan__allow_gc=False,int_division=floatX,base_compiledir=/dbfs/mnt/ml/pytensor/'

# COMMAND ----------

import os
print (os.environ["MLFLOW_TRACKING_URI"])

# COMMAND ----------

import mlflow
print(mlflow.__version__)
mlflow.set_registry_uri("databricks-uc")
import mlflow.spark
import mlflow.data
#import pandas as pd
#from mlflow.data.spark.dataset import SparkDataset
from mlflow.data.numpy_dataset import NumpyDataset
mlflow.spark.autolog()
tracking_uri = mlflow.get_tracking_uri()
print(f"Current tracking uri: {tracking_uri}")
import logging

logger = logging.getLogger("mlflow")

# Set log level to debugging
logger.setLevel(logging.DEBUG)

# COMMAND ----------

from fastprogress import fastprogress
fastprogress.printing = lambda: True
os.environ["CUDA_ROOT"]='/usr/local/cuda'
#os.environ["PYTENSOR_FLAGS"]='allow_gc=True' #,floatX=float64'
#os.environ["PYTENSORS_FLAGS"]=PYTENSOR_FLAGS
#import pytensor as pt

# COMMAND ----------

import pymc as pm
#import pymc_bart as pmb
import pyspark.pandas as ps
import numpy as np

# COMMAND ----------

#TODO Configure pytensor
#import matplotlib.pyplot as plt
####import pytensor #.configdefaults import config
####print(pytensor.config)
#print(pymc.__version__)
#print(pymc_bart.__version__)

# COMMAND ----------

#TODO Configure/Move exhibits
#import sklearn as skl
#import scipy.stats as sp
#TODO OBSOLETE import shared.simsurv_func as ssf

# COMMAND ----------

#TODO ML functions v BART specific functions
from shared.cdh_pymc_bart_functions import bart_expanded

# COMMAND ----------

import pyspark.sql.functions as F
import scipy.stats as sp

# COMMAND ----------

# import pyspark

dbutils.widgets.text('task_key',defaultValue='this-days')
task_name=dbutils.widgets.get("task_key")   ##{{task_key}}
TTE=task_name.split('-')[-1]

# COMMAND ----------

lungDefaultSplit=f"onehot,continuous,onehot"
lungDefaultCovcols="is_female,karno,age"
lungDefaultEvent="event"
ksDefaultSplit=f"onehot,continuous,onehot"
ksDefaultCovcols="cat2,cat5,cat10"
ksDefaultEvent="event"
ksDefaultTTE="days"
ptask=f"cdh-ml-bart-data-{TTE}"

# COMMAND ----------

# SPLIT_RULES = "[pmb.ContinuousSplitRule(), pmb.ContinuousSplitRule(), pmb.OneHotSplitRule(), pmb.OneHotSplitRule()]"

feature_set = dbutils.jobs.taskValues.get(ptask, "feature_set",
                                       default="cdh_reference_data.ml_kickstarter_success",
                                       debugValue="cdh_reference_data.ml_kickstarter_success")
COVCOLS = dbutils.jobs.taskValues.get(ptask, "COVCOLS", 
                                      default=ksDefaultCovcols,
                                      debugValue=ksDefaultCovcols)
TTE = dbutils.jobs.taskValues.get(ptask, "TTE", 
                                      default=ksDefaultTTE,
                                      debugValue=ksDefaultTTE)
EVENT  = dbutils.jobs.taskValues.get(ptask, "EVENT", 
                                      default=ksDefaultEvent,
                                      debugValue=ksDefaultEvent)

# COMMAND ----------

# M = 200 # number of trees
# DRAWS = 200
# TUNE = 1000
# CORES = 4

ptask="cdh-ml-bart-init"
RUN_NAME = dbutils.jobs.taskValues.get(ptask, "RUN_NAME", 
                                    default="lung_cancer",
                                   debugValue="lung_cancer")

# COMMAND ----------

#TODO set TaskValues here
#CORES=64
#mlflow.set_experiment(experiment_id=experiment_id)


# COMMAND ----------

experiment_id=dbutils.jobs.taskValues.get("cdh-ml-model",
                                          "experiment_id",
                                          debugValue="2248668235047615")

mlflow.set_experiment(experiment_id=experiment_id)

# COMMAND ----------

with mlflow.start_run(run_name=f"{RUN_NAME}_{TTE}_matrix") as run:  
    run_id = run.info.run_id
    #TODO write featureset in data, MLFLow Log
    mlflow.log_param('feature_set',feature_set) #.keys())
    mlflow.log_param('TTE',TTE)
    mlflow.log_param('COVCOLS',COVCOLS)
    mlflow.log_param('EVENT',EVENT)
    bart_covariates=[TTE]+COVCOLS.split(',')
  
   

    # entries in D = [1,2,1,3,4]
    # Construct a Pandas DataFrame using iris flower data from a web URL
#    dataset_source_url = "http://archive.ics.uci.edu/ml/machine-learning-databases/wine-quality/winequality-red.csv"
#    df = pd.read_csv(dataset_source_url)
    # Construct an MLflow PandasDataset from the Pandas DataFrame, and specify the web URL
    # as the source
    training=spark.table(feature_set)
    #TODO log to mlflow
    mlflow.log_metric('record count',training.count())
    mlflow.log_param('schema',training.schema) #.printSchema())
    #print(training)
 
#    print(TTE)
#    print(event)

    #TODO Log this in data? or in post-data/pre-model deteministic analytics
    tevent=training.select(EVENT).collect()
    offset = sp.norm.ppf(np.mean(tevent)) #expected value of 'event' to z
    #offset=training[event].summary("mean")
    #TODO log to mlflow
    mlflow.log_metric('mean(event)',np.mean(tevent))
    mlflow.log_metric('offset norm.ppf(mean)',offset)

    time_arr = training.select(F.collect_set(TTE)).first()[0]
    time_arr.sort()
    mlflow.log_param('ordered(time vector)',time_arr)
    xref=spark.createDataFrame(bart_expanded(time_arr=time_arr),[TTE,'n'+TTE])
    # merge drop TTE, rename nTTE to TTE, sent event to False, innter is default
    expanded=xref.join(training,TTE).drop(*(TTE,EVENT)).withColumnRenamed('n'+TTE,TTE).withColumn(EVENT,F.lit(0)).union(training) #.cast('boolean'))
        # no schema for pyspark pandas mlflow.log_param('schema',expanded.schema) #.printSchema())
    expanded_shape=expanded.count()
    expanded.createOrReplaceTempView("expanded")
    expanded=ps.read_table("expanded")
    #mlflow.log_param('record count',expanded.count())

    mlflow.log_param("expanded rows",expanded.count())
    #TODO consider going directly to *pytensor*
    #TODO consider different write
    event_data=expanded.to_numpy()
    #TODO log to mlflow
    expanded_matrix: NumpyDataset = mlflow.data.from_numpy(event_data)
    mlflow.log_input(expanded_matrix, context="expanded_matrix")

    event_data=expanded.to_numpy().tolist()
#    print(event_data)
    #TODO log to mlflow, seems to not like binary
    mfname='matrix.txt'
#    mfname='./matrix.npy'
    with open(mfname,'wb') as mfile:      
        np.save(mfile, event_data,allow_pickle=True,fix_imports=False)
    np.savetxt(mfname,event_data,header='Expanded Numpy Matrix')
    mlflow.log_artifact(mfname)
#    event_data_txt = np.loadtxt(mfname, dtype=int)
# FUTURE    expanded_matrix: NumpyDataset = mlflow.data.from_numpy(event_data)
#    mlflow.log_input(expanded_matrix, context="expanded_matrix")
############# No this crashes    mlflow.log_text(event_data,mfname) 

    # Fetch the artifact uri root directory
 #   artifact_uri = mlflow.get_artifact_uri()
 #   print(f"Artifact uri: {artifact_uri}")

    # Fetch a specific artifact uri
 #   artifact_uri = mlflow.get_artifact_uri(artifact_path="features/features.txt")
#    print(f"Artifact uri: {artifact_uri}")


    #TODO include trace? or other metrics
    #print(event_data)

    # run pymc
#TODO log to mlflow
    #mlflow.log_numpy("bartarray",expanded[bart_covariates].to_pandas().to_numpy())

# COMMAND ----------

spark.catalog.dropTempView("expanded")
