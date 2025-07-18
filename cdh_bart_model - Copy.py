# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id",  dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-client-id"))
spark.conf.set("fs.azure.account.oauth2.client.secret", dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-client-secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint", dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-tenant-id-endpoint"))


# COMMAND ----------

PYTENSOR_FLAGS='allow_gc=False,scan__allow_gc=False,int_division=floatX,base_compiledir=/dbfs/mnt/ml/pytensor/'

# COMMAND ----------

import mlflow
import mlflow.spark
mlflow.spark.autolog()

# COMMAND ----------

from fastprogress import fastprogress
fastprogress.printing = lambda: True
import os
os.environ["CUDA_ROOT"]='/usr/local/cuda'
#os.environ["PYTENSOR_FLAGS"]='allow_gc=True' #,floatX=float64'
#os.environ["PYTENSORS_FLAGS"]=PYTENSOR_FLAGS
#import pytensor as pt

# COMMAND ----------

import pymc as pm
import pymc_bart as pmb
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
from cdh_bart_functions import *

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# import pyspark

# COMMAND ----------

experiment_id=dbutils.jobs.taskValues.get("cdh-ml-model",
                                          "experiment_id",
                                          debugValue="2248668235047615")
#experiment_id=dbutils.jobs.taskValues.get('cdh-ml-model','experiment_id')

mlflow.set_experiment(experiment_id=experiment_id)

# COMMAND ----------

task_name=dbutils.widgets.get("task_key")   ##{{task_key}}
TTE=task_name.split('-')[-1]

# COMMAND ----------

lungDefaultSplit=f"onehot,continuous,onehot"
lungDefaultCovcols="is_female,karno,age"
ptask=f"cdh-ml-bart-data-{TTE}"

# COMMAND ----------

# SPLIT_RULES = "[pmb.ContinuousSplitRule(), pmb.ContinuousSplitRule(), pmb.OneHotSplitRule(), pmb.OneHotSplitRule()]"

feature_set = dbutils.jobs.taskValues.get(ptask, "feature_set",
                                       default="cdh_premier_exploratory.ml_lung_cancer_days",
                                       debugValue="cdh_premier_exploratory.ml_lung_cancer_days")
SPLIT_RULES = dbutils.jobs.taskValues.get(ptask, "SPLIT_RULES",
                                        default=lungDefaultSplit,
                                        debugValue=lungDefaultSplit)
COVCOLS = dbutils.jobs.taskValues.get(ptask, "COVCOLS", 
                                      default=lungDefaultCovcols,
                                      debugValue=lungDefaultCovcols)
event = dbutils.jobs.taskValues.get(ptask, "event", 
                                      default='event',
                                      debugValue='event')

# COMMAND ----------

# M = 200 # number of trees
# DRAWS = 200
# TUNE = 100
# CORES = 4

ptask="cdh-ml-bart-init"
M = dbutils.jobs.taskValues.get(ptask, "M",
                                    default=200,
                                    debugValue=200)
DRAWS = dbutils.jobs.taskValues.get(ptask, "DRAWS", 
                                    default=200,
                                    debugValue=200)
TUNE = dbutils.jobs.taskValues.get(ptask, "TUNE", 
                                    default=200,
                                   debugValue=200)
RUN_NAME = dbutils.jobs.taskValues.get(ptask, "RUN_NAME", 
                                    default="lung_cancer",
                                   debugValue="lung_cancer")

# COMMAND ----------

#CORES = dbutils.jobs.taskValues.get(ptask, "CORES", debugValue=4)
dbutils.widgets.text("SEED",defaultValue="2")
dbutils.widgets.text("ALPHA",defaultValue="0.95")
dbutils.widgets.text("BETA",defaultValue="2")

# COMMAND ----------

# get from task name at some point
SEED=eval(dbutils.widgets.get("SEED"))
ALPHA=eval(dbutils.widgets.get("ALPHA"))
BETA=eval(dbutils.widgets.get("BETA"))

# COMMAND ----------

#TODO set TaskValues here
CORES=64
#mlflow.set_experiment(experiment_id=experiment_id)

# COMMAND ----------

#TODO MLFLow Log
print(feature_set) #.keys())
bart_covariates=[TTE]+COVCOLS.split(',')
print(bart_covariates) 


# COMMAND ----------

#TODO BART function
import pandas as pd
bart_split_rules=["continuous"]+SPLIT_RULES.split(',')
print(bart_split_rules)
split_dict={"continuous":"pmb.ContinuousSplitRule()",
        "onehot":"pmb.OneHotSplitRule()",}
splits =[ eval(s) for s in list((pd.Series(bart_split_rules)).map(split_dict)) ]
#TODO log to mlflow
print(splits)

# entries in D = [1,2,1,3,4]

# COMMAND ----------

training=spark.table(feature_set)
#TODO log to mlflow
print(training.count())
print(training)
print(training.printSchema())
print(TTE)
print(event)

# COMMAND ----------

import scipy.stats as sp
#TODO Log this in data? or in post-data/pre-model deteministic analytics
tevent=training.select(event).collect()
offset = sp.norm.ppf(np.mean(tevent)) #expected value of 'event' to z
#offset=training[event].summary("mean")
#TODO log to mlflow
print(np.mean(tevent))
print(offset)


# COMMAND ----------

xref=spark.createDataFrame(bart_expanded(TTE,training),[TTE,'n'+TTE])
# merge drop TTE, rename nTTE to TTE, sent event to False, innter is default
expanded=xref.join(training,TTE).drop(*(TTE,event)).withColumnRenamed('n'+TTE,TTE).withColumn(event,F.lit(0)) #.cast('boolean'))
expanded.createOrReplaceTempView("expanded")
expanded=ps.read_table("expanded")
expanded_shape=expanded.count()

# COMMAND ----------

##x_data_full= expanded[bart_covariates]
##x_data=np.array(x_data_full.to_numpy()).astype(int)

# COMMAND ----------

event_data=expanded[event].to_numpy()
#TODO log to mlflow
print(event_data)


# COMMAND ----------

# BART
# M = 200 # number of trees
# DRAWS = 2000
# TUNE = 1000
# CORES = 4
# SPLIT_RULES = "[pmb.ContinuousSplitRule(), pmb.ContinuousSplitRule(), pmb.OneHotSplitRule(), pmb.OneHotSplitRule()]

 
with mlflow.start_run(run_name=f"{RUN_NAME}_{TTE}") as mlflow_run:
    # run pymc
#TODO log to mlflow
    #mlflow.log_numpy("bartarray",expanded[bart_covariates].to_pandas().to_numpy())
    with pm.Model() as bart:
        x_data = pm.MutableData("x_data",expanded[bart_covariates])
#        event_data=pm.Deterministic(event,expanded[event])
        x_shape=x_data.shape[0]
        f = pmb.BART("f", X=x_data, Y=event_data, m=M, alpha = ALPHA , split_rules=splits)
        z = pm.Deterministic("z", f + offset)
        mu = pm.Deterministic("mu", pm.math.invprobit(z))
        y_pred = pm.Bernoulli("y_pred", p=mu, observed=event_data, shape=x_shape)
        bdata = pm.sample(random_seed=SEED, draws=DRAWS, tune = TUNE, cores=CORES)
    modelviz = pm.model_to_graphviz(bart)
    mvfn="modelviz_{TTE}.png"
    artifact_uri = mlflow_run.info.artifact_uri
    mlflow.log_image(modelviz, mvfn)
    mvimage = mlflow.artifacts.load_image(artifact_uri + '/' + mvfn)
    print(mvimage)
#    pm.model_to_graphviz(bart)
    #akb add
#TODO log to mlflow
#   print(bart)

# COMMAND ----------

spark.catalog.dropTempView("expanded")
