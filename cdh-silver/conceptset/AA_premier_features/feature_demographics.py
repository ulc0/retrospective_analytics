# Databricks notebook source
# MAGIC %md 
# MAGIC # Demographics Notebook for the premier dataset link
# MAGIC
# MAGIC Demographics from the target encounter are collected, coded and placed in a array variable
# MAGIC
# MAGIC dem list = [gender, hispanic_in, age, race]

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Requirements
# MAGIC - Demographics given for the target encounter
# MAGIC     

# COMMAND ----------

# MAGIC %md
# MAGIC ## Set-up

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.davsynapseanalyticsdev.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.davsynapseanalyticsdev.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-client-id"))
spark.conf.set("fs.azure.account.oauth2.client.secret.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-client-secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-tenant-id-endpoint"))

# COMMAND ----------

demo = spark.table("cdh_premier.patdemo")
#target_enc = spark.table("poc_premier.scope")

# COMMAND ----------

# Libs
#import os
#import numpy as np
#import pandas as pd

import pyspark.sql.functions as F
#from pyspark.sql.window import Window as W
#import pyspark.sql.types as T
#from pyspark.sql.functions import PandasUDFType

# COMMAND ----------

# MAGIC %md
# MAGIC demo_target = (target_enc  
# MAGIC                .select("pat_key")  
# MAGIC                .join(demo.select("pat_key","age","race","gender","hispanic_ind"), on = "pat_key", how="left")  
# MAGIC               )  

# COMMAND ----------

demo_target = demo.select("pat_key","medrec_key","age","race","gender","hispanic_ind", "medrec_key", "disc_mon", "disc_mon_seq", "i_o_ind", "days_from_index", "disc_status")
   

# COMMAND ----------

# bin age
# - bins age by 10 years
def get_age_bin(sdf):
    age_buc = (F.when((F.col("age") >=0) & (F.col("age") < 10), "0-9")
               .when((F.col("age") >=10) & (F.col("age") < 20), "10-19")
               .when((F.col("age") >= 20) & (F.col("age") < 30), "20-29")
               .when((F.col("age") >= 30) & (F.col("age") < 40), "30-39")
               .when((F.col("age") >= 40) & (F.col("age") < 50), "40-49")
               .when((F.col("age") >= 50) & (F.col("age") < 60), "50-59")
               .when((F.col("age") >= 60) & (F.col("age") < 70), "60-69")
               .when((F.col("age") >= 70) & (F.col("age") < 80), "70-79")
               .when((F.col("age") >= 80) & (F.col("age") < 90), "80-89")
               .when((F.col("age") >= 90) & (F.col("age") < 100), "90-99")
               .otherwise(">=100")
              )
    out = (sdf
           .withColumn("age_bin", age_buc)
           .drop("age")
          )
    return(out)

# COMMAND ----------

demo2 = get_age_bin(demo_target)

# COMMAND ----------

demo2.display()

# COMMAND ----------

demo2.write.mode("overwrite").format("delta").saveAsTable("poc_premier.demographics")
