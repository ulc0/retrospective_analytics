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


lungDefaultSplit=f"onehot,continuous,onehot"
lungDefaultCovcols="is_female,karno,age"
lungDefaultEvent="event"
ksDefaultSplit=f"onehot,continuous,onehot"
ksDefaultCovcols="cat2,cat5,cat10"
ksDefaultEvent="event"
ksDefaultTTE="days"

# COMMAND ----------

# SPLIT_RULES = "[pmb.ContinuousSplitRule(), pmb.ContinuousSplitRule(), pmb.OneHotSplitRule(), pmb.OneHotSplitRule()]"
dbutils.widgets.text('task_key','cdh-ml-bart-init')
RUN_NAME=dbutils.widgets.get("task_key")   ##{{task_key}}
print(RUN_NAME)
TTE=RUN_NAME.split('-')[-1]
#TODO log 
print(TTE)


#TODO reorder columns so TTE, event are first
dbutils.widgets.text("event",defaultValue="event")
# this is where we put code to use other TTE periods
dbutils.widgets.text("covcols",
                     defaultValue="age,karno,is_female")
#SPLIT_RULES = TPARMS["SPLIT_RULES"]
# source of feature store, gold table location, tbl is input tbl name
dbutils.widgets.text("db",defaultValue="cdh_reference_data")
dbutils.widgets.text("tbl",defaultValue="ml_lung_cancer")

#where to save modelling set, may deprecate
dbutils.widgets.text("fs",defaultValue="cdh_premier_exploratory")
#split rules, should be based on dtype
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

EVENT=dbutils.widgets.get("event")
# this is where we put code to use other TTE periods
experiment_id=dbutils.widgets.get('experiment_id')

# COMMAND ----------

import os
import pyspark
import pyspark.sql.functions as F


# COMMAND ----------

training_ds=f"{db}.{tbl}"
sqlstring=f"select {TTE},{COVCOLS},{EVENT} from {training_ds}"
print(sqlstring)
training=spark.sql(sqlstring)

# COMMAND ----------

#TODO Schema
training.printSchema()
# MOVE TO DATA?
#training_data: SparkDataset = mlflow.data.load_delta(feature_set)
#TODO why doesn't this write?
#training.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(feature_set)
#dbutils.jobs.taskValues.set("feature_set",feature_set)
# COMMAND ----------

# prediction set
#sqlstring=f"select {EVENT},{TTE},{COVCOLS} from {feature_set}" # group by {TTE},{COVCOLS}"
#print(sqlstring)
#prediction=spark.sql(sqlstring)
#prediction.printSchema()
## GO DIRECTLY TO MATRIX
#prediction_set=feature_set+"_prediction"
#prediction.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(prediction_set)

# COMMAND ----------
import scipy.stats as sp
import numpy as np
import pyspark.pandas as ps
from shared.cdh_pymc_bart_functions import bart_expanded
# obs from shared.numpy_func import numpy_to_dict
# COMMAND ----------

experiment_id=dbutils.widgets.get('experiment_id')

mlflow.set_experiment(experiment_id=experiment_id)

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
    mlflow.log_param('time_arr',time_arr)
    xref=spark.createDataFrame(bart_expanded(time_arr=time_arr),[TTE,'n'+TTE])
    # merge drop TTE, rename nTTE to TTE, sent event to False, innter is default
    prebart=xref.join(training,TTE).drop(*(TTE,EVENT)).withColumnRenamed('n'+TTE,TTE).withColumn(EVENT,F.lit(0)).union(training) #.cast('boolean'))
        # no schema for pyspark pandas mlflow.log_param('schema',expanded.schema) #.printSchema())

    prebart.createOrReplaceTempView("prebart")
    prebart=ps.read_table("prebart")
    #mlflow.log_param('record count',expanded.count())

    mlflow.log_param("expanded rows",prebart.count())
    #TODO consider going directly to *pytensor*
    #TODO consider different write
    event_data=prebart.to_numpy().astype('int32')
    #print(isinstance(event_data, np.ndarray))
    mfname=f"np/{tbl}_{TTE}_matrix.npy"
    mpath=f"{mfname}"
    with open(mfname,'wb') as mfile:      
        np.save(mfile, event_data,allow_pickle=True,fix_imports=False)
    np.savetxt(mfname,event_data,header='Expanded Numpy Matrix')
    event_data.dump(mpath)
    mlflow.log_artifact(mpath)
    os.remove(mpath)
### one way    mlflow.log_text(np.array2string(event_data, formatter={'float_kind':lambda x: "%.2f" % x}),mfname)
    #mlflow.log_text(ndarray.to)
    dbutils.jobs.taskValues.set('matrix_run_id',run_id) #artifact_uri+mfname)
    dbutils.jobs.taskValues.set('matrix_name',mfname) #artifact_uri+mfname)
#OBS log to mlflow use numpy data dump
    #mlflow.log_numpy("bartarray",expanded[bart_covariates].to_pandas().to_numpy())

# COMMAND ----------

spark.catalog.dropTempView("prebart")


#dbutils.jobs.taskValues.set("split_rules",split_rules)
#dbutils.jobs.taskValues.set("COVCOLS",COVCOLS)
#dbutils.jobs.taskValues.set("EVENT",EVENT)
#dbutils.jobs.taskValues.set("TTE",TTE)