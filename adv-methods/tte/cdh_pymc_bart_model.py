# Databricks notebook source
"""
spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id",  dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-client-id"))
spark.conf.set("fs.azure.account.oauth2.client.secret", dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-client-secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint", dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-tenant-id-endpoint"))
"""

# COMMAND ----------

PYTENSOR_FLAGS='allow_gc=False,scan__allow_gc=False,int_division=floatX,base_compiledir=/dbfs/mnt/ml/pytensor/'

# COMMAND ----------

import mlflow
import mlflow.spark
mlflow.spark.autolog()
from mlflow import MlflowClient

# COMMAND ----------

#from fastprogress import fastprogress
#fastprogress.printing = lambda: True
import os
os.environ["CUDA_ROOT"]='/usr/local/cuda'
os.environ["PYTENSOR_FLAGS"]='allow_gc=True' #,floatX=float64'
#os.environ["PYTENSORS_FLAGS"]=PYTENSOR_FLAGS
#import pytensor as pt

# COMMAND ----------
import numpy
print(numpy.__version__)
import warnings
numpy.warnings=warnings
import pytensor
# COMMAND ----------
import pymc as pm
import pymc_bart as pmb
#import pyspark.pandas as ps

### FUTURE import pygraphviz as pgv
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
##from cdh_bart_functions import *

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

# import pyspark

# COMMAND ----------

experiment_id=experiment_id=dbutils.widgets.get('experiment_id')

mlflow.set_experiment(experiment_id=experiment_id)

# COMMAND ----------

dbutils.widgets.text('task_key',defaultValue='this-days')
task_name=dbutils.widgets.get("task_key")   ##{{task_key}}
TTE=task_name.split('-')[-1]

ptask=f"cdh-ml-bart-data-{TTE}"
feature_set = dbutils.jobs.taskValues.get(ptask, "feature_set",
                                       default="cdh_reference_data.ml_kickstarter_success",
                                       debugValue="cdh_reference_data.ml_kickstarter_success")

COVCOLS=dbutils.widgets.get("COVCOLS")
SPLIT_RULES=dbutils.widgets.get("SPLIT_RULES")
event=dbutils.widgets.get("event")


# COMMAND ----------

lungDefaultSplit=f"onehot,continuous,onehot"
lungDefaultCovcols="is_female,karno,age"
ptask=f"cdh-ml-bart-data-{TTE}"

# COMMAND ----------
%md
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

matrix_run_id=dbutils.jobs.taskValues.get(ptask, 'matrix_run_id', 
                                      default='9bdbcd51c1ce44dba60cdf509df27de6',
                                      debugValue='9bdbcd51c1ce44dba60cdf509df27de6')

print(matrix_run_id)
#event_data_txt=mlflow.artifacts.load_text(matrix_uri)

mfname=dbutils.jobs.taskValues.get(ptask, 'matrix_name', 
                                      default='73e324badc784c4bbeee971b7561cdf9',
                                      debugValue='73e324badc784c4bbeee971b7561cdf9')
print(mfname)


# COMMAND ----------

# M = 200 # number of trees
# DRAWS = 200
# TUNE = 1000
# CORES = 4

ptask="cdh-ml-bart-init"
M = dbutils.jobs.taskValues.get(ptask, "M",
                                    default=200,
                                    debugValue=200)
DRAWS = dbutils.jobs.taskValues.get(ptask, "DRAWS", 
                                    default=200,
                                    debugValue=200)
TUNE = dbutils.jobs.taskValues.get(ptask, "TUNE", 
                                    default=1000,
                                   debugValue=1000)
RUN_NAME = dbutils.jobs.taskValues.get(ptask, "RUN_NAME", 
                                    default="lung_cancer",
                                   debugValue="lung_cancer")

# COMMAND ----------

#CORES = dbutils.jobs.taskValues.get(ptask, "CORES", debugValue=4)
dbutils.widgets.text("SEED",defaultValue="2")
dbutils.widgets.text("ALPHA",defaultValue="0.95")
# what is BETA dbutils.widgets.text("BETA",defaultValue="2")

# COMMAND ----------

# get from task name at some point
SEED=eval(dbutils.widgets.get("SEED"))
ALPHA=eval(dbutils.widgets.get("ALPHA"))
# what is BETA BETA=eval(dbutils.widgets.get("BETA"))

# COMMAND ----------
def GetDistCPUCount():
    nWorkers = int(spark.sparkContext.getConf().get('spark.databricks.clusterUsageTags.clusterTargetWorkers'))
    GetType = spark.sparkContext.getConf().get('spark.databricks.clusterUsageTags.clusterNodeType')
    GetSubString = pd.Series(GetType).str.split(pat = '_', expand = True)
    GetNumber = GetSubString[1].str.extract('(\d+)')
    ParseOutString = GetNumber.iloc[0,0]
    WorkerCPUs = int(ParseOutString)
    nCPUs = nWorkers * WorkerCPUs
    return nCPUs

#TODO set TaskValues here
CORES=5 #int(spark.sparkContext.getConf().get('spark.databricks.clusterUsageTags.clusterTargetWorkers'))

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

####import scipy.stats as sp
#TODO Log this in data? or in post-data/pre-model deteministic analytics
####tevent=training.select(event).collect()
####offset = sp.norm.ppf(np.mean(tevent)) #expected value of 'event' to z
#offset=training[event].summary("mean")
#TODO log to mlflow
####print(np.mean(tevent))
####print(offset)


# COMMAND ----------

####event_data=expanded[event].to_numpy()
#TODO log to mlflow
import numpy as np
from mlflow import MlflowClient
mlClient=MlflowClient()
npfile=mlClient.download_artifacts(matrix_run_id,mfname)
matrixrun = mlflow.get_run(run_id=matrix_run_id)
metrics = matrixrun.data.metrics
params = matrixrun.data.params
offset=metrics["offset norm.ppf(mean)"]
npmean=metrics["mean(event)"]
print(offset)
print(npmean)
# COMMAND ----------
import networkx as nx
import matplotlib.pyplot as plt
import cloudpickle as pickle
expanded=np.load(npfile,allow_pickle=True)
covmatrix=expanded[:,1:-1]
print(covmatrix)
tte_data=expanded[:,-1]
print(tte_data)
#TODO environment variable
TMPPATH='/dbfs/ml/tmp/'
# COMMAND ----------

# BART
# M = 200 # number of trees
# DRAWS = 2000
# TUNE = 1000
# CORES = 4
# SPLIT_RULES = "[pmb.ContinuousSplitRule(), pmb.ContinuousSplitRule(), pmb.OneHotSplitRule(), pmb.OneHotSplitRule()]
core_run_name=f"{RUN_NAME}_{TTE}"
#resume run 
with mlflow.start_run(run_id=matrix_run_id) as mlflow_run:

    # run pymc
#TODO log to mlflow
    #mlflow.log_numpy("bartarray",expanded[bart_covariates].to_pandas().to_numpy())
    with pm.Model() as bart:
        x_data = pm.Data("x_data",covmatrix).astype('int32')
#        event_data=pm.Deterministic(event,expanded[event])
        x_shape=x_data.shape[0]
        f = pmb.BART("f", X=x_data, Y=tte_data, m=M, alpha = ALPHA , split_rules=splits)
        z = pm.Deterministic("z", f + offset)
        mu = pm.Deterministic("mu", pm.math.invprobit(z))
        y_pred = pm.Bernoulli("y_pred", p=mu, observed=tte_data, shape=x_shape)
        # nuts_sampler=[“pymc”, “nutpie”, “blackjax”, “numpyro”].
        bdata = pm.sample(nuts_sampler="pymc",random_seed=SEED, draws=DRAWS, tune = TUNE, cores=CORES, return_inferencedata=True)
#TODO Log model 
    filename = f'{TMPPATH}model.pkl'
    with open(filename, 'wb') as f:
        pickle.dump({'model': bart, 'trace': bdata}, f)
    mlflow.log_artifact(filename)  
    with bart:
    # pm.set_data({"x":pd.DataFrame(test_x), "off":off_test})
        pm.set_data({"x_data":pd.DataFrame(covmatrix)})
        pp = pm.sample_posterior_predictive(bdata, var_names = ["y_pred", "f", "z", "mu"])
   
#model_to_graphviz returns a graphviz.dot.Digraph so you can save that via 
# the formats supported 57.
# So for example, if you save your graph into a variable g, 
# g = pm.model_to_graphviz(model) you can save those via 
# g.render("graphname", format="png"). As for saving tables, 
# something like this could work 43, but you will probably 
# have to fix the rendering a bit. You can alternatively also 
# run to_latex(), to_markdown() or to_html() on your summary table to save it.
   
# model graph

#    mg = pm.model_to_graphviz(bart).render(mpath) #,'plain_with_params')
    #mpath=f"{TMPPATH}modelviz_{TTE}.png"
if (False):
    mG = pm.model_to_networkx(bdata) #,'plain_with_params')
    #mg.f
    #print(type(mg))
    ###mg.render(mpath,format='png', view=False)
    fig = plt.figure(figsize=(12,12))
#    ax = plt.subplot(111)
#    ax.set_title('Graph - Shapes', fontsize=10)
    nx.draw_networkx(mG)
    ax = plt.gca()
    ax.margins(0.08)
    plt.axis("off")
    plt.tight_layout()
    plt.show()
    plt.savefig(mpath, format="PNG")    
###    mlflow.log_figure(mg, mpath)
    mlflow.log_artifact(mpath)
    #artifact_uri = mlflow_run.info.artifact_uri
    #mvimage=modelviz.render(format="png")
    #print(type(mvimage))
    ##print(mvimage)
    #mlflow.log_image(mvimage,mfname)
#    mvimage = mlflow.artifacts.load_image(artifact_uri + '/' + mvfn)
#    print(mvimage)
#    pm.model_to_graphviz(bart)
    #akb add
    #TODO log to mlflow
print(pp)
print(bart)
print(bdata)
# COMMAND ----------

spark.catalog.dropTempView("expanded")
