# Databricks notebook source
# MAGIC %md 
# MAGIC # Objective
# MAGIC
# MAGIC - This notebook is to scan the whole clean file. Once some lingering pattertns are found we would count the number of occurrences and determine how clean a data set is
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, rand, concat_ws
import mlflow

from delta.tables import *


# COMMAND ----------

dbutils.widgets.text("EXPERIMENT_ID", defaultValue="3389456715448166")
dbutils.widgets.text("SENTENCE_TABLE", defaultValue = "edav_prd_cdh.cdh_abfm_phi_exploratory.ml_mpox_cohort_notes_sentences_run9")


# COMMAND ----------

EXPERIMENT_ID=dbutils.widgets.get("EXPERIMENT_ID")
sentencetable = dbutils.widgets.get("SENTENCE_TABLE")

print(sentencetable)


# COMMAND ----------


display(spark.table(sentencetable))

# COMMAND ----------

df = (
  spark.table(sentencetable)  
  .withColumn("key_id", concat_ws("-", F.col("note_id"), F.col("note_sent")))
)

# COMMAND ----------

# MAGIC %md ### Checking utf and markup patterns

# COMMAND ----------

# MAGIC %md 
# MAGIC In this section we revised some markup patterns that are still lingering and that can flag possible additional cleaning cases.These patterns correspond to those found in ABFM data set, an EHR oriented data set avaiable in CDC DataHub. 

# COMMAND ----------

# MAGIC %md Now, with the identified patten cases we looked at the full data set to identify what are the patterns found and determine how clean a data set is

# COMMAND ----------


## Using some previously identified patterns from markup cleaning, we try to flag some cases that may need some cleaning.
numerator_df = (
  df  
  .where("""
         note_text like '%b>' 
         or note_text like '%u>' 
         or note_text like '%div>' 
         or note_text like '%xlm' 
         or note_text like '%>'
         or note_text like '%tr>'
         or note_text like '%<b'
         or note_text like '%%'         
         """)
)

display(numerator_df)

numerator = numerator_df.select("key_id").distinct().count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Summary 

# COMMAND ----------


# the denominator is the number of sentences in the data set
denominator = (
    df
    .select("key_id")
    .distinct()
    .count()
)

summary = ( 
           round((1 - (numerator/denominator)),5)*100.0           
)


print("number of total sentences with some sort of markup to clean", numerator)
print("number of total sentences",denominator)
print("percentage of total sentences cleaned", summary)

# COMMAND ----------

with mlflow.start_run(experiment_id=EXPERIMENT_ID):
    mlflow.log_metric("number of total sentences with some sort of markup to clean", numerator)
    mlflow.log_metric("number of total sentences", denominator)
    mlflow.log_metric("percentage of total sentences cleaned", summary)
