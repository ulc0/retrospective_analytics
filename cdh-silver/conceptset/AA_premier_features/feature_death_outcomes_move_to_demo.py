# Databricks notebook source
# MAGIC %md 
# MAGIC # Death Outcome Notebook for the premier dataset link

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Requirements
# MAGIC - disc_status_desc == Expired
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

disstat = spark.table("cdh_premier.disstat")
#target_enc = spark.table("poc_premier.scope")

# COMMAND ----------

# Libs
import os
import numpy as np
#import pandas as pd

import pyspark.sql.functions as F
from pyspark.sql.window import Window as W
import pyspark.sql.types as T
from pyspark.sql.functions import PandasUDFType

# COMMAND ----------

# MAGIC %md 
# MAGIC # Prefilter on target encounters

# COMMAND ----------

# MAGIC %md
# MAGIC # Join the disstat
# MAGIC target_enc = target_enc.join(disstat, on = "disc_status", how = "left")

# COMMAND ----------

def get_death(sdf):
    out = (sdf
           .filter(F.lower(F.col("disc_status_desc")).contains("expired"))
           .select( "pat_key")
           .withColumn("death_ind", F.lit(1))
          )
    return out

# COMMAND ----------

#death = get_death(target_enc)
death=get_death(disstat)

# COMMAND ----------

# death.count()
# death.display()

# COMMAND ----------

death.write.mode("overwrite").format("delta").saveAsTable("poc_premier.outcomes_death")
