# Databricks notebook source
# MAGIC %md 
# MAGIC # ICU Outcome Notebook for the premier dataset link

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Requirements
# MAGIC - "r&b icu" or "r&b step down"
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

patbill = spark.table("cdh_premier.patbill")
chgmstr = spark.table("cdh_premier.chgmstr")

# Use the target encounters to prefilter
#target_enc = spark.table("poc_premier.scope")

# COMMAND ----------

# Libs
import os
import numpy as np
import pandas as pd

import pyspark.sql.functions as F
from pyspark.sql.window import Window as W
import pyspark.sql.types as T
from pyspark.sql.functions import PandasUDFType

# COMMAND ----------

# MAGIC %md
# MAGIC Dynamic definition of icu and step down based on text analysis of vendor provided lookup table

# COMMAND ----------

# get chg codes
icu_code = list(chgmstr.filter(F.lower(F.col("std_chg_desc")).contains("r&b icu")).select("std_chg_code").toPandas()["std_chg_code"])
step_code = list(chgmstr.filter(F.lower(F.col("std_chg_desc")).contains("r&b step down")).select("std_chg_code").toPandas()["std_chg_code"])

codes = icu_code + step_code
print(codes)
# print(icu_code)
# print(step_code)

# COMMAND ----------

# MAGIC %md
# MAGIC # get the icu and step down encounters billing
# MAGIC patbill_filt = (patbill
# MAGIC                 .filter(F.col("std_chg_code").isin(codes))
# MAGIC                )

# COMMAND ----------

# get the icu and step down encounters billing
icu_enc = (patbill
    .filter(F.col("std_chg_code").isin(codes))
   )

# COMMAND ----------

# MAGIC %md
# MAGIC def get_icu_enc(target, dtst):
# MAGIC     out = (target
# MAGIC            .join(dtst, on = "pat_key", how = "inner")
# MAGIC            .distinct()
# MAGIC            .withColumn("icu_enc", F.lit(1))
# MAGIC           )
# MAGIC     return out

# COMMAND ----------

# MAGIC %md
# MAGIC # get the icu encounters
# MAGIC icu_enc = get_icu_enc(target_enc.select("pat_key"), patbill_filt.select("pat_key"))

# COMMAND ----------

icu_enc.count()

# COMMAND ----------

icu_enc.count()

# COMMAND ----------

icu_enc.write.mode("overwrite").format("delta").saveAsTable("poc_premier.outcomes_icu")
