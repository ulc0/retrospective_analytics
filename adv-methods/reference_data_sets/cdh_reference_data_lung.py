# Databricks notebook source
import numpy as np
import pandas as pd

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.functions as W
import pyspark.sql.types as T
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import Bucketizer
import pyspark

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC dbutils.widgets.text('experiment_id','1348027641275128')
# MAGIC EXPERIMENT_ID=dbutils.widgets.get('experiment_id')
# MAGIC # DEV EXPERIMENT_ID=2256023545557195
# MAGIC RUN_NAME="Lung Cancer Data"
# MAGIC RUN_ID='00ef4757db584e17be2d71559fd91def'

# COMMAND ----------

# MAGIC %md
# MAGIC ####NCCTG Lung Cancer Data
# MAGIC Survival in patients with advanced lung cancer from the North Central Cancer Treatment Group.
# MAGIC Performance scores rate how well the patient can perform usual aily activities  
# MAGIC
# MAGIC **Source:** Loprinzi CL. Laurie JA. Wieand HS. Krook JE. Novotny PJ. Kugler JW. Bartel J. Law M. Bateman
# MAGIC M. Klatt NE. et al. Prospective evaluation of prognostic variables from patient-completed questionnaires. North Central Cancer Treatment Group. Journal of Clinical Oncology. 12(3):601-7,
# MAGIC 1994

# COMMAND ----------

# MAGIC %md
# MAGIC **inst**: Institution code  
# MAGIC **time**: Survival time in days  
# MAGIC **status**: censoring status 1=censored, 2=dead  
# MAGIC **age**: Age in years  
# MAGIC **sex**: Male=1 Female=2  
# MAGIC **ph.ecog**: ECOG performance score (0=good 5=dead)  
# MAGIC **ph.karno**: Karnofsky performance score (bad=0-good=100) rated by physician  
# MAGIC **pat.karno**: Karnofsky performance score as rated by patient  
# MAGIC **meal.cal**: Calories consumed at meals  
# MAGIC **wt.loss**: Weight loss in last six months  

# COMMAND ----------

# MAGIC %r
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
# MAGIC install.packages('survival')
# MAGIC library(survival)
# MAGIC data(colon)
# MAGIC library(BART)
# MAGIC data(lung)
# MAGIC data(transplant)
# MAGIC data(bladder)
# MAGIC #data(phoneme)
# MAGIC data(ACTG175)
# MAGIC #data(alligator)
# MAGIC #data(xdm20.train)
# MAGIC #data(xdm20.test)
# MAGIC #data(ydm20.train)
# MAGIC #data(ydm20.test)
# MAGIC #lung = pd.read_csv("lung.csv")
# MAGIC write.csv(lung,"test-data/lung.csv")
# MAGIC write.csv(transplant,"test-data/transplant.csv")
# MAGIC write.csv(bladder,"test-data/bladder.csv")
# MAGIC write.csv(ACTG175,"test-data/actg175.csv")
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #library(nycflights13)
# MAGIC flights_tbl <- copy_to(sc, nycflights13::flights, "flights")
# MAGIC airlines_tbl <- copy_to(sc, nycflights13::airlines, "airlines")

# COMMAND ----------

# MAGIC %md
# MAGIC library(SparkR)
# MAGIC library(BART)
# MAGIC library(sparklyr)
# MAGIC library(dplyr)
# MAGIC library(ggplot2)
# MAGIC
# MAGIC sc <- spark_connect(method = "databricks")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC times <- c(1.5, 2.5, 3.0)
# MAGIC delta <- c(1, 1, 0)
# MAGIC surv.pre.bart(times = times, delta = delta)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC data(lung)
# MAGIC lungs<-as.DataFrame(lung)

# COMMAND ----------

import pandas as pd
import pyspark.pandas as ps
#import numpy as np
lungc_dtypes={'inst':str, 'time':int, 'ecog':int, 'status':int, 'age':int, 'sex':int, 'ph.ecog':float,
       'ph.karno':float, 'pat.karno':float, 'meal.cal':float, 'wt.loss':float}
lungc_rename={"inst": "person_id", "time": "days","ph.ecog":"ecog","ph.karno":"karno_physician","pat.karno":"karno_patient","meal.cal":"meal_calories","wt.loss":"weight_loss"}
#lungc_name={"person_id",  "days","ecog","karno_physician","karno_patient","meal_calories","weight_loss"}

#plung=lungs.toDF(*lungc_name)
plung=pd.read_csv("test-data/lung.csv",dtype=lungc_dtypes).rename(columns=lungc_rename)
plung.drop('Unnamed: 0', axis=1, inplace=True)
print(plung)
#boolean_cols=[]
#plung[column_names] = plung[column_names].astype(bool)

# COMMAND ----------


# configure analytic dataset
# adjust time to months, weeks
#plung["days"]=plung["time"]
plung["months"] = (plung["days"]//30)+1
plung["weeks"] = (plung["days"]//7)+1
#plung["years"] = (plung["days"]//365)+1
# delta ==> True=Death, False=Censored
plung["is_female"] = (plung.sex-1).astype(int) # Not Boolean
plung["event"] = (plung.status - 1 ).astype(int) # NOt Boolean
plung.drop(['status','sex'],axis=1,inplace=True)

plung.to_csv("test-data/lung_enhanced.tsv")


# COMMAND ----------

from pyspark.sql.functions import coalesce
plungs=spark.createDataFrame(plung)

  #plung["karno"] = plung["ph.karno"].fillna(plung["pat.karno"])
plungs=plungs.withColumn("karno",coalesce(plungs.karno_physician,plungs.karno_patient).cast("Integer"))  
#lungs.withColumn("karno",coalesce(df.karno_physician,df.karno_patient)) 

# COMMAND ----------

## use
## from pyspark.sql.functions import coalesce
##df.withColumn("karno",coalesce(df.B,df.A))
# print(plung.columns)
###print(plung)
dcols=list(plungs.columns)
print(dcols)
#get rid of dot


# COMMAND ----------

# MAGIC %md
# MAGIC dcols=[d.replace(".","_") for d in dcols]
# MAGIC #dcols=list(plung.columns)
# MAGIC print(dcols)
# MAGIC plung.columns=dcols
# MAGIC #get rid of dot

# COMMAND ----------

time="days"
event="event"
covcols=list(set(dcols)-set([time]+[event]))
print(covcols)
scols=list([event]+[time]+covcols)        
print(scols)
lungs=(plungs[scols])

# COMMAND ----------


db="cdh_reference_data"
tbl="ml_lung_cancer"
lungs.write.mode("overwrite").option("overwriteSchema", "true").format("parquet").saveAsTable(f"{db}.{tbl}")

# COMMAND ----------


