# Databricks notebook source
import  pyspark.sql.functions as F 
tbl_name="edav_prd_cdh.cdh_abfm_phi_exploratory.mpox_xgboost_data"
df=spark.table(tbl_name)
allcols=df.schema.names
print(allcols[:6])
#define columns to sum
cols_to_sum = allcols[2:]
#print(cols_to_sum)


# COMMAND ----------

from functools import reduce
from operator import add
from pyspark.sql.functions import col

sums=df.withColumn('sum', F.expr('+'.join(cols_to_sum)))
display(sums)

# COMMAND ----------

select_str='sum('+'), sum('.join(cols_to_sum)+')'
#print(select_str)

# COMMAND ----------




#create new DataFrame that contains sum of specific columns
df_new =spark.sql("select "+select_str+f" from {tbl_name}") 
#df.select(F.sum('abnormalities_of_the_oral_mucosa_salivary_glands')) 
display(df_new)
