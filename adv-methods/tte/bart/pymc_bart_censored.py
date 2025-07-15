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

# COMMAND ----------

#TODO reorder columns so time, event are first
dbutils.widgets.text("event",defaultValue="event")
dbutils.widgets.text("time",defaultValue="time")
# this is where we put code to use other time periods
dbutils.widgets.text("covcols",
                     defaultValue="[ 'meal_calories', 'ecog', 'age', 'karno_patient', 'is_female', 'karno_physician', 'weight_loss']")
dbutils.widgets.text("db",defaultValue="cdh_reference_data")
dbutils.widgets.text("tbl",defaultValue="ml_lung_cancer")
levent=dbutils.widgets.get("event")
ltime=dbutils.widgets.get("time")
# this is where we put code to use other time periods
lcovcols=dbutils.widgets.get("covcols")
db=dbutils.widgets.get("db")
tbl=dbutils.widgets.get("tbl")

# COMMAND ----------

import os
thisdir=(os.getcwd())+'.pymc_bart_functions'
print(thisdir)
print(os.listdir())
#import sys
#print("\n".join(sys.path))

# COMMAND ----------

#from pymc/pymc_bart_functions import pymc_bart_censored
import importlib 
# String should match the same format you would normally use to import
#pbf = importlib.import_module(thisdir)
import shared.pymc_bart_functions as pbf
# Then you can use it as if you did `import my_package.my_module`
#my_module.my_function()
#import ./pymc_bart_functions as pbf
#%run  ../pymc/pymc_bart_functions.py
#import pymc_bart_censored

# COMMAND ----------

# so union doesn't like the column ordering
censored=pbf.pymc_bart_censored("event","days",lcovcols,schema,tbl)
training=spark.sql(f"select {ltime},{','.join(lcovcols)},{levent} from {schema}.{tbl}")
###print(scols)


db="cdh_reference_data"
tbl="ml_bart_lung_cancer"
training_ds=f"{db}.{tbl}"
training.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(training_ds)

dbutils.jobs.taskValues.set("")
