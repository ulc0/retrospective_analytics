# Databricks notebook source

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

import pandas as pd
import pyspark.pandas as ps
import numpy as np

#kickstart=lungs.toDF(*lungc_name)
kickstart=pd.read_csv("test-data/kickstarter.csv")
d = dict.fromkeys(kickstart.select_dtypes(np.int64).columns, np.int32)
kickstart = kickstart.astype(d)
#kickstart.drop('Unnamed: 0', axis=1, inplace=True)
print(kickstart)
#boolean_cols=[]
#kickstart[column_names] = kickstart[column_names].astype(bool)

# COMMAND ----------


# configure analytic dataset
# adjust time to months, weeks
#kickstart["days"]=kickstart["time"]
kickstart["days"] = (kickstart["tte"])
kickstart["months"] = (kickstart["tte"]//30)+1
kickstart["weeks"] = (kickstart["tte"]//7)+1
#kickstart["years"] = (kickstart["days"]//365)+1
# delta ==> True=Death, False=Censored
#kickstart["is_female"] = (kickstart.sex-1).astype(int) # Not Boolean
#kickstart["event"] = (kickstart.status - 1 ).astype(int) # NOt Boolean
#kickstart.drop(['status','sex'],axis=1,inplace=True)




# COMMAND ----------

from pyspark.sql.functions import coalesce
data=spark.createDataFrame(kickstart)

  #kickstart["karno"] = kickstart["ph.karno"].fillna(kickstart["pat.karno"])
#data=data.withColumn("karno",coalesce(data.karno_physician,data.karno_patient).cast("Integer"))  
#lungs.withColumn("karno",coalesce(df.karno_physician,df.karno_patient)) 

# COMMAND ----------

## use
## from pyspark.sql.functions import coalesce
##df.withColumn("karno",coalesce(df.B,df.A))
# print(kickstart.columns)
###print(kickstart)
dcols=list(data.columns)
print(dcols)
#get rid of dot


# COMMAND ----------

# MAGIC %md
# MAGIC dcols=[d.replace(".","_") for d in dcols]
# MAGIC #dcols=list(kickstart.columns)
# MAGIC print(dcols)
# MAGIC kickstart.columns=dcols
# MAGIC #get rid of dot

# COMMAND ----------

#time="days"
#event="event"
#covcols=list(set(dcols)-set([time]+[event]))
#print(covcols)
#scols=list([event]+[time]+covcols)        
#print(scols)
#lungs=(data[scols])

# COMMAND ----------


db="cdh_reference_data"
tbl="ml_kickstarter_success"
data.write.mode("overwrite").option("overwriteSchema", "true").format("parquet").saveAsTable(f"{db}.{tbl}")
