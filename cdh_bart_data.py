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
# MAGIC experiment_id=dbutils.jobs.taskValues.get('cdh-ml-model','experiment_id')

# COMMAND ----------

#TODO ML Specific ETL cluster with mlflow
if (False):
    import mlflow
    import mlflow.spark
    mlflow.spark.autolog()
    mlflow.set_experiment(experiment_id=experiment_id)

# COMMAND ----------

dbutils.widgets.text('task_key','cdh-ml-bart-init')
task_name=dbutils.widgets.get("task_key")   ##{{task_key}}
TTE=task_name.split('-')[-1]
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
dbutils.widgets.text("split_rules",defaultValue='cont,cont,onehot')
#SPLIT_RULES=[pmb.ContinuousSplitRule(), pmb.ContinuousSplitRule(), pmb.OneHotSplitRule(), pmb.OneHotSplitRule()]",

# COMMAND ----------

event=dbutils.widgets.get("event")
# this is where we put code to use other TTE periods
covcols=dbutils.widgets.get("covcols")
db=dbutils.widgets.get("db")
fs=dbutils.widgets.get("fs")
tbl=dbutils.widgets.get("tbl")
split_rules=dbutils.widgets.get("split_rules")

# COMMAND ----------

import os
import pyspark
import pyspark.sql.functions as F


# COMMAND ----------

training_ds=f"{db}.{tbl}"
sqlstring=f"select {TTE},{covcols},{event} from {training_ds}"
print(sqlstring)
training=spark.sql(sqlstring)

# COMMAND ----------

#TODO Schema
training.printSchema()
feature_set=f"{fs}.{tbl}_{TTE}"
training.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(feature_set)

# COMMAND ----------

# prediction set
sqlstring=f"select count(*) as observations,{TTE},{covcols} from {feature_set} group by {TTE},{covcols}"
print(sqlstring)
prediction=spark.sql(sqlstring)
prediction.printSchema()
prediction_set=f"{fs}.{tbl}_{TTE}_prediction"
prediction.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(prediction_set)

# COMMAND ----------

dbutils.jobs.taskValues.set("prediction_set",prediction_set)
dbutils.jobs.taskValues.set("feature_set",feature_set)
dbutils.jobs.taskValues.set("split_rules",split_rules)
dbutils.jobs.taskValues.set("covcols",covcols)
dbutils.jobs.taskValues.set("event",event)
