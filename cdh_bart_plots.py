# Databricks notebook source
#%pip install fastprogress

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id",  dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-client-id"))
spark.conf.set("fs.azure.account.oauth2.client.secret", dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-client-secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint", dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-tenant-id-endpoint"))

PYTENSOR_FLAGS='allow_gc=False,scan__allow_gc=False,int_division=floatX,base_compiledir=/dbfs/mnt/ml/pytensor/'

# COMMAND ----------

import mlflow

from fastprogress import fastprogress
fastprogress.printing = lambda: True
import os
os.environ["CUDA_ROOT"]='/usr/local/cuda'
#os.environ["PYTENSOR_FLAGS"]='allow_gc=True' #,floatX=float64'
#os.environ["PYTENSORS_FLAGS"]=PYTENSOR_FLAGS
import pytensor as pt
import pymc as pm
import pymc_bart as pmb
import pyspark.pandas as ps
#import matplotlib.pyplot as plt
####import pytensor #.configdefaults import config
####print(pytensor.config)
#print(pymc.__version__)
#print(pymc_bart.__version__)

#import sklearn as skl
#import scipy.stats as sp
#import shared.simsurv_func as ssf



# COMMAND ----------

#%run ./pymc_bart_functions 
#import json
#context_str = dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()
#context = json.loads(context_str)
#print(context)

# COMMAND ----------

#from pymc/pymc_bart_functions import pymc_bart_censored
#import importlib 
# String should match the same format you would normally use to import
#pbf = importlib.import_module(thisdir)
#import shared
from cdh_bart_functions import *
# Then you can use it as if you did `import my_package.my_module`
#my_module.my_function()
#import ./pymc_bart_functions as pbf
#%run  ../pymc/pymc_bart_functions.py
#import pymc_bart_censored

# COMMAND ----------


import pyspark.sql.functions as F
# import pyspark
# M = 200 # number of trees
# DRAWS = 200
# TUNE = 100
# CORES = 4
# SPLIT_RULES = "[pmb.ContinuousSplitRule(), pmb.ContinuousSplitRule(), pmb.OneHotSplitRule(), pmb.OneHotSplitRule()]"

experiment_id=dbutils.jobs.taskValues.get("cdh-ml-model",
                                          "experiment_id",
                                          debugValue=2221480985583347)
experiment_id=2221480985583347

# COMMAND ----------

#run_name = dbutils.jobs.taskValues.get("cdh-ml-init", 
#                                         "run_name", 
#                                         debugValue="test2")

#run_id = dbutils.jobs.taskValues.get("cdh-ml-run",
#                                    "run_id_main",
#                                    debugValue = "5c4b0bab2668466ea9ac022e482adc35")
continuous="pmb.ContinuousSplitRule()"
onehot="pmb.OneHotSplitRule()"

# COMMAND ----------

lungDefaultSplit=f"onehot,continuous,onehot"
lungDefaultCovcols="is_female,karno,age"
ptask="cdh-ml-bart-data"
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
time = dbutils.jobs.taskValues.get(ptask, "time", 
                                      default='days',
                                      debugValue='days')

# COMMAND ----------

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

# COMMAND ----------

#CORES = dbutils.jobs.taskValues.get(ptask, "CORES", debugValue=4)
dbutils.widgets.text("SEED",defaultValue="2")
dbutils.widgets.text("ALPHA",defaultValue="0.95")
dbutils.widgets.text("BETA",defaultValue="2")
# get from task name at some point
dbutils.widgets.text("RUN_NAME",defaultValue="lung_model")
SEED=eval(dbutils.widgets.get("SEED"))
ALPHA=eval(dbutils.widgets.get("ALPHA"))
BETA=eval(dbutils.widgets.get("BETA"))
RUN_NAME=dbutils.widgets.get("RUN_NAME")

# COMMAND ----------

#TODO set TaskValues here
CORES=8
#mlflow.set_experiment(experiment_id=experiment_id)

# COMMAND ----------

#TODO MLFLow Log
print(feature_set) #.keys())
bart_covariates=[time]+COVCOLS.split(',')
print(bart_covariates) 
bart_split_rules=SPLIT_RULES.split(',')

# COMMAND ----------

training=spark.table(feature_set)
print(training.count())
print(training)
print(training.printSchema())
print(time)
print(event)
###print(scols)

# COMMAND ----------

import scipy.stats as sp
import numpy as np
tevent=training.select(event).collect()
print(np.mean(tevent))
offset = sp.norm.ppf(np.mean(tevent)) #expected value of 'event' to z

#offset=training[event].summary("mean")
print(offset)

#print(offset_np)
#training.show()

# COMMAND ----------

lxref=bart_censored(time,training) #f"{db}.{tbl}")
xref=spark.createDataFrame(lxref,[time,'n'+time])
print(xref)
print(xref.printSchema())
###xref.show() should be a log
# merge drop time, rename ntime to time, sent event to False, innter is default
censored=xref.join(training,time).drop(*(time,event)).withColumnRenamed('n'+time,time).withColumn(event,F.lit(0)) #.cast('boolean'))
x_data_full= censored[bart_covariates]
x_data_full.write.mode("overwrite").option("overwriteSchema", "true").format("parquet").saveAsTable("ml_bart_x_data")
censored.createOrReplaceTempView ("censored")
censored=ps.read_table("censored")

censored_shape=censored.count()
#df2 = spark.sql("select * from people")
#sorted(df.collect()) == sorted(df2.collect())

# COMMAND ----------

##x_data_full= censored[bart_covariates]

##x_data=np.array(x_data_full.to_numpy()).astype(int)

# COMMAND ----------

# BART
# M = 200 # number of trees
# DRAWS = 2000
# TUNE = 1000
# CORES = 4
# SPLIT_RULES = "[pmb.ContinuousSplitRule(), pmb.ContinuousSplitRule(), pmb.OneHotSplitRule(), pmb.OneHotSplitRule()]

with mlflow.start_run(experiment_id=experiment_id, run_name=RUN_NAME) as mlflow_run:

# COMMAND ----------

    with bart:
    # pm.set_data({"x":pd.DataFrame(test_x), "off":off_test})
        bdata = pm.sample(random_seed=SEED, draws=DRAWS, tune = TUNE, cores=CORES)
        pm.set_data({"x":counterfactual})
        pp = pm.sample_posterior_predictive(bdata, var_names = ["y_pred", "f", "z", "mu"])


        # get survival
        x_out = np.concatenate([x_sk.to_numpy(), x_sk.to_numpy()], axis=0)
        bart_sv_fx = ssf.get_sv_fx(pp, x_out)

        # get the original and counterfactual
        og_shp, _ = x_sk.shape
        or_bart_sv_fx = bart_sv_fx[0:og_shp,:]
        cf_bart_sv_fx = bart_sv_fx[og_shp:, :]

        # get mean and quantile
        or1 = or_bart_sv_fx.mean(axis=0)
        orp = np.quantile(or_bart_sv_fx, q=[0.05,0.95], axis=0)
        cf1 = cf_bart_sv_fx.mean(axis=0)
        cfp = np.quantile(cf_bart_sv_fx, q=[0.05,0.95], axis=0)


        plt_time = np.unique(b_tr_t)

        # plot
        fig = plt.figure()
        plt.step(plt_time, or1, label = "male", color="darkblue")
        plt.step(plt_time, orp[0], color="darkblue", alpha=.4)
        plt.step(plt_time, orp[1], color="darkblue", alpha=.4)
        plt.step(plt_time, cf1, label = "female", color="darkorange")
        plt.step(plt_time, cfp[0], color="darkorange", alpha=.4)
        plt.step(plt_time, cfp[1], color="darkorange", alpha=.4)
        plt.legend()
        mlflow.log_figure(fig, "male_female.png")

# COMMAND ----------

spark.catalog.dropTempView("censored")
