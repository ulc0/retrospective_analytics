# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC https://github.com/ulc0/woodwork
# MAGIC
# MAGIC Evaluate `woodwork` 

# COMMAND ----------

#%pip install scikit-learn==0.22.0 # for featuretools
#NO NO NO PERMISSIONS %pip install git+https://github.com/ulc0/woodwork
#moved to cluster
%pip install "woodwork[spark,updater]"
%pip install "featuretools[spark,sql,tsfresh,updater]"
#display(dbutils.fs.ls('/databricks-datasets'))


# COMMAND ----------

# MAGIC %md
# MAGIC import logging
# MAGIC logger = logging.getLogger('py4j')
# MAGIC logger.info("My test info statement")
# MAGIC import sys # Put at top if not already there
# MAGIC sh = logging.StreamHandler(sys.stdout)
# MAGIC sh.setLevel(logging.DEBUG)
# MAGIC logger.addHandler(sh

# COMMAND ----------

# MAGIC %md
# MAGIC ### evaluate original premier file
# MAGIC dir = "/home/tnk6/premier/"
# MAGIC
# MAGIC # Read in the visit tables
# MAGIC # p_id =  spark.read.parquet(dir + 'vw_covid_id/')
# MAGIC pat_all = spark.read.parquet(dir + "vw_covid_pat_all/")
# MAGIC pat_all.show()

# COMMAND ----------

import pyspark.pandas as ps
import pandas as pd
from pyspark.ml.stat import Summarizer
import woodwork as ww 
import featuretools as ft
import statsmodels
#import mlflow #_skinny as mlflow
from mlflow_extend import mlflow


# COMMAND ----------

mlflow_id=mlflow.set_experiment("/Users/ulc0@cdc.gov/PremierPATDEMOLogical")
#mlflow.autolog()
mlflow.end_run()
mlflow.start_run()


# COMMAND ----------

# set options for featuretools use
ps.set_option("compute.default_index_type", "distributed")


# COMMAND ----------


ptables=spark.sql("SHOW TABLES from cdh_premier")

tableNames=([data[0] for data in ptables.select('tableName').collect()])
tableNames=tableNames[:-1]
print(tableNames)




# COMMAND ----------

tableNames=['patdemo','patbbill','patcpt','paticd_proc','lab_res','gen_lab','vitals']
pdb=pd.DataFrame(columns=['table','col_name','data_type','comment'])
for table in tableNames:
    desc=spark.sql("describe cdh_premier.{}".format(table)).toPandas()
    desc['table']=table
    desc['col_name']=desc['col_name'].str.lower()
    pdb=pd.concat([pdb,desc])
    
psb=ps.from_pandas(pdb.groupby(['col_name','data_type'])['table'].count().sort_values(ascending=False) )
print(psb)
