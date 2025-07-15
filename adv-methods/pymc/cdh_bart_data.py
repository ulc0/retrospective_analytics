# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Grab data sets from BART
# MAGIC
# MAGIC library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
# MAGIC #sparkR.session()
# MAGIC sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "2g"))
# MAGIC if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
# MAGIC   Sys.setenv(SPARK_HOME = "/home/spark")
# MAGIC }
# MAGIC install.packages('BART')
# MAGIC library(BART)
# MAGIC data(lung)
# MAGIC data(transplant)
# MAGIC data(bladder)
# MAGIC #data(phoneme)
# MAGIC data(ACTG175)
# MAGIC data(alligator)
# MAGIC data(xdm20.train)
# MAGIC data(xdm20.test)
# MAGIC data(ydm20.train)
# MAGIC data(ydm20.test)
# MAGIC #lung = pd.read_csv("lung.csv")
# MAGIC write.csv(lung,"lung.csv")
# MAGIC write.csv(transplant,"transplant.csv")
# MAGIC write.csv(bladder,"bladder.csv")
# MAGIC write.csv(ACTG175,"actg175.csv")
# MAGIC
# MAGIC experiment_id=experiment_id=dbutils.widgets.get('experiment_id')
# COMMAND ----------

import mlflow
import mlflow.spark
mlflow.spark.autolog()
dbutils.widgets.text('task_key','cdh-ml-bart-days')
RUN_NAME=dbutils.widgets.get("task_key")   ##{{task_key}}
print(RUN_NAME)
TTE=RUN_NAME.split('-')[-1]
#TODO log 
print(TTE)

lungDefaultSplit=f"onehot,continuous,onehot"
lungDefaultCovcols="is_female,karno,age"
lungDefaultEvent="event"
ksDefaultSplit=f"onehot,continuous,onehot"
ksDefaultCovcols="cat2,cat5,cat10"
ksDefaultEvent="event"
ksDefaultTTE="days"


# COMMAND ----------

# SPLIT_RULES = "[pmb.ContinuousSplitRule(), pmb.ContinuousSplitRule(), pmb.OneHotSplitRule(), pmb.OneHotSplitRule()]"


#TODO reorder columns so TTE, event are first
dbutils.widgets.text("event",defaultValue=ksDefaultEvent)
# this is where we put code to use other TTE periods
dbutils.widgets.text("covcols",
                     defaultValue=ksDefaultCovcols)
#SPLIT_RULES = TPARMS["SPLIT_RULES"]
# source of feature store, gold table location, tbl is input tbl name
dbutils.widgets.text("db",defaultValue="cdh_reference_data")
dbutils.widgets.text("tbl",defaultValue="ml_lung_cancer")

#where to save modelling set, may deprecate
dbutils.widgets.text("fs",defaultValue="cdh_premier_exploratory")
#split rules, shou*ld be based on dtype
#dbutils.widgets.text("split_rules",defaultValue='cont,cont,onehot')
#SPLIT_RULES=[pmb.ContinuousSplitRule(), pmb.ContinuousSplitRule(), pmb.OneHotSplitRule(), pmb.OneHotSplitRule()]",
dbutils.widgets.text("experiment_id","2220358835149509")
# COMMAND ----------

EVENT=dbutils.widgets.get("event")
# this is where we put code to use other TTE periods
COVCOLS=dbutils.widgets.get("covcols")
db=dbutils.widgets.get("db")
fs=dbutils.widgets.get("fs")
tbl=dbutils.widgets.get("tbl")
#split_rules=dbutils.widgets.get("split_rules")

# COMMAND ----------

experiment_id=dbutils.widgets.get('experiment_id')

mlflow.set_experiment(experiment_id=experiment_id)


# COMMAND ----------

import os
import pyspark
import pyspark.sql.functions as F


# COMMAND ----------
training=spark.table(f"{db}.{tbl}")

# COMMAND ----------
#TODO Schema
training.printSchema()

# COMMAND ----------
import scipy.stats as sp
import numpy as np
import pyspark.pandas as ps
from shared.cdh_pymc_bart_functions import bart_expanded
from shared.numpy_func import numpy_to_dict
# COMMAND ----------

with mlflow.start_run(run_name=RUN_NAME) as run:  
    run_id = run.info.run_id
    artifact_uri = run.info.artifact_uri
    #TODO write featureset in data, MLFLow Log
    mlflow.log_param('training_ds',training_ds) #.keys())
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
# OLD CODE    training=spark.table(feature_set)
    #TODO log to mlflow
    mlflow.log_metric('record count',training.count())
    mlflow.log_param('schema',training.schema) #.printSchema())
    #print(training)
 
#    print(TTE)
#    print(event)

    #TODO Log this in data? or in post-data/pre-model deteministic analytics
    tevent=training.select(EVENT).collect()
    mean_event=np.mean(tevent)
    offset = sp.norm.ppf(mean_event) #expected value of 'event' to z
    mlflow.log_metric('mean(event)',mean_event)
    mlflow.log_metric('offset norm.ppf(mean)',offset)

    time_arr = training.select(F.collect_set(TTE)).first()[0]
    time_arr.sort()
    mlflow.log_param('time_arr',time_arr)
    xref=spark.createDataFrame(bart_expanded(time_arr=time_arr),[TTE,'n'+TTE])
    # merge drop TTE, rename nTTE to TTE, sent event to False, innter is default
    prebart=xref.join(training,TTE).drop(*(TTE,EVENT)).withColumnRenamed('n'+TTE,TTE).withColumn(EVENT,F.lit(0)).union(training).to_pandas() #.cast('boolean'))
        # no schema for pyspark pandas mlflow.log_param('schema',expanded.schema) #.printSchema())

    mlflow.log_param("expanded rows",prebart.count())

    #TODO consider going directly to *pytensor*
    #TODO consider different write
###############    prebart.createOrReplaceTempView("prebart")
#############    prebart=ps.read_table("prebart")

    event_data=prebart.to_numpy().astype('int32')
    #print(isinstance(event_data, np.ndarray))
#    #TODO log to mlflow
#    mlflow.log_dict(numpy_to_dict(event_data),'matrix.json')
    ###expanded_matrix: NumpyDataset = mlflow.data.from_numpy(event_data)
    ###mlflow.log_input(expanded_matrix, context="expanded_matrix")

# temp directory, should use tempfile
    mfname="{tbl}_{TTE}_matrix.npy"
    mpath=f"/dbfs/ml/{mfname}"
    #with open(mfname,'wb') as mfile:      
    #    np.save(mfile, event_data,allow_pickle=True,fix_imports=False)
    #np.savetxt(mfname,event_data,header='Expanded Numpy Matrix')
    event_data.dump(mpath)
    mlflow.log_artifact(mpath)
    os.remove(mpath)
### one way    mlflow.log_text(np.array2string(event_data, formatter={'float_kind':lambda x: "%.2f" % x}),mfname)
#mlflow.log_text(ndarray.to)

    dbutils.jobs.taskValues.set('matrix_run_id',run_id) #artifact_uri+mfname)
    dbutils.jobs.taskValues.set('matrix_name',mfname) #artifact_uri+mfname)

#    event_data_txt = np.loadtxt(mfname, dtype=int)
# FUTURE    expanded_matrix: NumpyDataset = mlflow.data.from_numpy(event_data)
#    mlflow.log_input(expanded_matrix, context="expanded_matrix")
############# No this crashes    mlflow.log_text(event_data,mfname) 

    #TODO include trace? or other metrics
    #print(event_data)

    # run pymc
#TODO log to mlflow
    #mlflow.log_numpy("bartarray",expanded[bart_covariates].to_pandas().to_numpy())

# COMMAND ----------

#spark.catalog.dropTempView("prebart")


