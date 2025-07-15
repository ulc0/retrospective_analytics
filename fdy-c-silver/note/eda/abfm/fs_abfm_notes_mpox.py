# Databricks notebook source
#spark.sql("SHOW DATABASES").show(truncate=False)
#import pandas as pd
from pyspark.sql.window import Window
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import isnan, when, count, col, desc, asc, row_number, concat_ws, coalesce,to_timestamp,regexp_replace, to_date, date_format, concat, lower, udf
import pyspark.sql.functions as F
from functools import reduce
from pyspark.sql import DataFrame
#   RTF from striprtf.striprtf import rtf_to_text

spark.conf.set('spark.sql.shuffle.partitions',7200*4)
spark.conf.set("spark.databricks.io.cache.enabled", True)

# COMMAND ----------

# Converting function to UDF 
### RTF convertRTF = udf(lambda rtf: rtf_to_text(rtf,errors="ignore"),StringType())

# COMMAND ----------

#mandatory parameters and names. The orchestrator will always pass these

headnode="fs_abfm_notes_partition"
stbl=dbutils.jobs.taskValues.get(taskKey=headnode,
                                 key="rawtable",
                                 default="edav_prd_cdh.cdh_abfm_phi_exploratory.ml_fs_abfm_notes",
                                 debug="edav_prd_cdh.cdh_abfm_phi_exploratory.ml_fs_abfm_notes",
                                 )


# COMMAND ----------

# MAGIC %md ## Cleaning functions

# COMMAND ----------

print(stbl)
txt="merged"

# COMMAND ----------

words_to_match_mpox = [
    'monkeypox', 'monkypox', 'monkey pox', 
    'monkepox pox', 'monky pox', 'monkey-pox', 
    'monke-pox', 'monky-pox', 'orthopox',
    'orthopoxvirus', 'parapoxvirus',
    'ortho pox', 'poxvi', 'monkeypox', 
    'mpox', 'm-pox', 'mpx'
    ]
regex_pattern = "|".join(words_to_match_mpox) # F.col("notes_text").rlike(regex_pattern)

# COMMAND ----------

# MAGIC %md
# MAGIC import striprtf
# MAGIC rtf_id="6F61C832-1290-486D-B63E-BD0BE0A01A3E"
# MAGIC xml_id="3213f7ee-1a12-4214-ab9c-c84a1c69c514"
# MAGIC hex_id="5C250B00-0DA9-4054-B528-1D0886600C9E"
# MAGIC zzz_id="E91FEC3A-6F02-4FD2-AFAF-F305E6E4A594"
# MAGIC pipe_id="CCB68260-540C-4CC9-84D4-2E1DA3C2CD70"
# MAGIC html_id="f2368a09-41e1-4b0e-94e4-a6812b0bffb8"
# MAGIC patrec=spark.sql(f"select * from {stbl} where patient_id='{rtf_id}'").limit(1)
# MAGIC patrec.show()

# COMMAND ----------

from shared.configs import set_secret_scope


set_secret_scope()

# COMMAND ----------

notes=spark.table(stbl+"_"+txt)
#rtf_notes=notes.withColumn("note_text",convertRTF(col("note_text")))
mpox_notes=notes.withColumn("mpox_notes",F.when(F.col('note_text').rlike(regex_pattern),True).otherwise(False))
mpox_notes.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(stbl+f"_mpox")
# add diagnosis here
