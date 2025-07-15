# Databricks notebook source
# MAGIC %md
# MAGIC ## Set-up

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.davsynapseanalyticsdev.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.davsynapseanalyticsdev.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-client-id"))
spark.conf.set("fs.azure.account.oauth2.client.secret.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-client-secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-tenant-id-endpoint"))

# COMMAND ----------

icd_lookup = spark.table("cdh_premier.icdcode")
icd_enc = spark.table("cdh_premier.paticd_diag")
proc_enc = spark.table("cdh_premier.paticd_proc")
hosp_chg = spark.table("cdh_premier.hospchg") 
patbill = spark.table("cdh_premier.patbill")
vitals = spark.table("cdh_premier.vitals")
genlab = spark.table("cdh_premier.genlab")
labres = spark.table("cdh_premier.lab_res")
hospchg = spark.table("cdh_premier.hospchg")
chgmstr = spark.table("cdh_premier.chgmstr")

# Use pat_demo and readmit
demo = spark.table("cdh_premier.patdemo")
readmit = spark.table("cdh_premier.readmit")

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
# MAGIC # Target Encounters

# COMMAND ----------

# MAGIC %md
# MAGIC ## Identify Target Encounters

# COMMAND ----------

# MAGIC %md
# MAGIC Primary or secondary covid-19 ICD-10 code
# MAGIC   - Feb 1, 2020 - April 30, 2021
# MAGIC First covid 19 inpatient visit = target
# MAGIC   - Admission
# MAGIC       - start: Feb 1, 2020 - April 1, 2020
# MAGIC       - end: March 1, 2020 - April 1, 2020
# MAGIC           - icd-10 B97.29 (maybe just B97.2) "Coronavirus as the cause of diseases classified to other chapters"
# MAGIC       - start: April 1, 2020
# MAGIC       - end: April 30, 2021
# MAGIC           - U07.1 "COVID-19, virus identified"

# COMMAND ----------

def get_covid_icd(icd):
    """identify encounters with primary/secondary covid icd codes"""
    covid = (icd
             .filter(
                 (F.col("icd_code").contains("B97.2") | F.col("icd_code").contains("U07.1")) &
                 (F.col("icd_pri_sec").isin(["P", "S"])))
             .select("pat_key","icd_code")
    )
    return covid

def get_incl_cov_enc(readmit, dem, cov_icd):
    """identify enc in the inclusion periods"""
    out = (
        readmit
        .select("pat_key", "medrec_key", "disc_mon", "disc_mon_seq", "i_o_ind", "days_from_index", "disc_status")
        .join(
            dem.select("pat_key", "adm_mon", "los"), 
            on = "pat_key", how="left"
        )
        .withColumn("adm_mon", F.col("adm_mon").cast("integer"))
        .withColumn("disc_mon", F.col("disc_mon").cast("integer"))
        .withColumn("disc_mon_seq", F.col("disc_mon_seq").cast("integer"))
        .join(cov_icd, on="pat_key", how="left")
        .select(
            "*",
            # Period 1 covid
            F.when(
                (
                    (F.col("adm_mon") >= 2020102)
                    & (F.col("adm_mon") < 2020204)
                    & (F.col("disc_mon") >= 2020103)
                    & (F.col("disc_mon") < 2020204)
                    & (F.col("icd_code").contains( "B97.2")
                    & (F.col("i_o_ind") == "I")
                ),
                1,
            )
            .otherwise(0)
            .alias("cov_p1"),
            # Period 2 covid
            F.when(
                (
                    (F.col("adm_mon") >= 2020204)
                    & (F.col("adm_mon") <= 2021204)
                    & (F.col("icd_code") == "U07.1")
                    & (F.col("i_o_ind") == "I")
                ),
                1,
            )
            .otherwise(0)
            .alias("cov_p2")
        )
        .withColumn("cov_ind", F.when((F.col("cov_p1") == 1) | (F.col("cov_p2") == 1), 1).otherwise(0))
        .drop("cov_p1", "cov_p2", "icd_code")
    )
    return out

def filter_cov_ind(sdf):
    """keep only the inpatient encounters with covid occuring in the given time frame"""
    out = (sdf
           .filter(F.col("cov_ind") == 1)
          )
    return out

def get_target_visit(sdf):
    """order inclusion encounters and then keep the first row (sets the target visit as the first covid visit in the given range)"""
    out = (sdf                                                                                     
           .withColumn("row",
                       F.row_number()
                       .over(W.partitionBy(F.col("medrec_key"))
                             .orderBy(F.col("days_from_index")))
                  )
           .withColumn("target",
                       F.when((F.col("row") == 1), 1)
                       .otherwise(0)
                      )
           .filter(F.col("target") == 1)
           .drop("row")
          )
    return out

# COMMAND ----------

# apply transformations
# identify covid encounters
cov_icd_outcomes = get_covid_icd(icd_enc)
# identify inclusion encounters
#cov_inc_icd = get_incl_cov_enc(readmit, demo, cov_icd)
# remove non inclusion encounters
#pat_cov_ind = filter_cov_ind(cov_inc_icd)
# identify target encounters
#target_enc = get_target_visit(pat_cov_ind)

# COMMAND ----------


print(cov_icd_outcomes.count())
print(cov_icd_outcomes.select("medrec_key").distinct().count())

# There should be the same number of encounters as patients (one target encounter per patient)

# COMMAND ----------

cov_icd_outcomes.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Target Encounters

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS poc_premier")
spark.sql("DROP table if exists poc_premier.cov_icd_outcomes")

# COMMAND ----------

#TODO remove   "disc_mon", "disc_mon_seq", "i_o_ind", "days_from_index", "disc_status"
target_enc.write.mode("overwrite").format("delta").option("mergeSchema", "true").saveAsTable("poc_premier.cov_icd_outcomes")

# COMMAND ----------

tg = spark.table("poc_premier.cov_icd_outcomes")

# COMMAND ----------

tg.count()
