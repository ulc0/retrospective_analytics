# Databricks notebook source
# MAGIC %md
# MAGIC       spark.conf.set("fs.azure.account.auth.type", "OAuth")
# MAGIC       spark.conf.set("fs.azure.account.oauth.provider.type",
# MAGIC                     "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# MAGIC       spark.conf.set("fs.azure.account.oauth2.client.id",  dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-client-id"))
# MAGIC       spark.conf.set("fs.azure.account.oauth2.client.secret", dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-client-secret"))
# MAGIC       spark.conf.set("fs.azure.account.oauth2.client.endpoint", dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-tenant-id-endpoint"))

# COMMAND ----------

#spark.sql("SHOW DATABASES").show(truncate=False)
import pandas as pd
from datetime import date
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import isnan, when, count, col, desc, asc, row_number, concat_ws, coalesce,to_timestamp,regexp_replace, to_date, date_format, concat
from pyspark.sql.functions import pandas_udf
import pyspark.sql.functions as F
from pyspark.sql.window import Window

from functools import reduce



spark.conf.set('spark.sql.shuffle.partitions',7200*4)

# COMMAND ----------

# MAGIC %md # Testing pretrained models 
# MAGIC #texttbl=f"{schema}.ml_fs_abfm_note_textdoc"
# MAGIC texttbl=dbutils.jobs.taskValues.get("fs_abfm_notes_textdoc","texttable")
# MAGIC dbschema=texttbl.split(".")[0]

# COMMAND ----------

def deleteMarkup(colNote):        
     rft = F.regexp_replace(colNote, r"\\[a-z]+(-?\d+)?[ ]?|\\'([a-fA-F0-9]{2})|[{}]|[\n\r]|\r\n?","") 
     #xlm = regexp_replace(rft, r"<[^>]*>|&[^;]+;", "")
     font_header1 = F.regexp_replace(rft, "Tahoma;;", "")
     font_header2 = F.regexp_replace(font_header1, "Arial;Symbol;| Normal;heading 1;", "")
     font_header3 = F.regexp_replace(font_header2, "Segoe UI;;", "")
     font_header4 = F.regexp_replace(font_header3, "MS Sans Serif;;", "")
     return font_header4  

# COMMAND ----------

# MAGIC %md Doing RTF cleaning and SPARK NLP pipeline in here since I believe it runs faster for the data compute

# COMMAND ----------


#dbschema=f"edav_prd_cdh.cdh_abfm_phi_exploratory"
case_control_cohort = (
    spark.table(texttbl).select("*",(deleteMarkup("notes")).alias("note_clean_misc"))
    
)



# COMMAND ----------

# MAGIC %md Then we save the clean data here, and go to the GPU for using the transformers models or any other model powered by GPU compute

# COMMAND ----------

case_control_cohort.write.mode("overwrite").format("delta").saveAsTable(f"{dbschema}.ml_nlp_mpox_notes")


# COMMAND ----------

print(f"{dbschema}.ml_nlp_mpox_notes")

# COMMAND ----------


