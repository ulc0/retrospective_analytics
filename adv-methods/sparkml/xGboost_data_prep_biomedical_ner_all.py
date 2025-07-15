# Databricks notebook source
import mlflow
from pyspark.sql.functions import struct, col
mlflow.__version__

#

# COMMAND ----------

dbutils.widgets.text("gboostTable","edav_prd_cdh.cdh_abfm_phi_exploratory.note_silver_biomedical_ner_all")
gboostTable=dbutils.widgets.get("gboostTable")

# COMMAND ----------

gboostTable

# COMMAND ----------

dbutils.widgets.text('input_table' , defaultValue= "edav_prd_cdh.cdh_abfm_phi_exploratory.note_silver_biomedical_ner_all",label="Huggingface Hub Name")
input_table = dbutils.widgets.get("input_table")
print(input_table)

dbutils.widgets.text('model_suffix' , defaultValue= "biomedical_ner_all",label="Huggingface Hub Name")
model_suffix = dbutils.widgets.get("model_suffix")
print(model_suffix)

# Maximize Pandas output text width.

#import pandas as pd
from datetime import date
from itertools import chain
import math
# pd.set_option('display.max_colwidth', 200)


#import specific functions from pyspark sql types
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType, DoubleType

from pyspark.ml.feature import VectorSlicer
from pyspark.ml.linalg import Vectors
from pyspark.sql.types import Row
import sparkml

# spark configuration
spark.conf.set('spark.sql.shuffle.partitions', 1600)
spark.conf.set('spark.databricks.adaptive.autoOptimizeShuffle.enabled', True)
spark.conf.set('spark.databricks.queryWatchdog.enabled', True)

#import pyspark sql functions with alias
from pyspark.sql.functions import isnan, when, count, col, desc, asc, row_number, concat_ws, coalesce,to_timestamp, greatest, least, ceil, expr
import pyspark.sql.functions as F
from pyspark.sql.window import Window 
from pyspark.sql import SparkSession, DataFrame

from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, trim, length, col

#spark.sql("SHOW DATABASES").show(truncate=False)
spark.conf.set('spark.sql.shuffle.partitions',7200*4)


# COMMAND ----------

entities_to_select = spark.table(f"{input_table}")
#"edav_prd_cdh.cdh_abfm_phi_exploratory.ft_abfm_notes_hf"+f"_{model_suffix}")

# COMMAND ----------

notes_stats = (
    entities_to_select
    .select("person_id","mpox_visit","note_text",) 
    .dropDuplicates() 
    .withColumn("note_lenght",
                F.length(F.col("note_text"))
                )      
)
"""
display(
    notes_stats
    .summary("count", "25%", "50%", "75%", "mean", "stddev")
#    .groupBy("mpox_exposed")
)

display(
    notes_stats
    .groupBy("note_lenght")
    .count()
    .sort("count", ascending = False)
    )

notes_stats.count()
"""

# COMMAND ----------


spark.conf.set("spark.sql.pivotMaxValues", 100000)
entities_to_select_filtered = (
    entities_to_select
    .filter(length(f"word_{model_suffix}") > 2)
    .withColumn("cleaned_text", regexp_replace(f"word_{model_suffix}", r"([^\s\w]|_)+", ""))
    .withColumn("cleaned_text", trim(regexp_replace(col("cleaned_text"), r"\s+", " ")))
    .drop(f"word_{model_suffix}")
    .withColumnRenamed("cleaned_text",f"word_{model_suffix}")
)
"""
display(
    entities_to_select_filtered
    .select(f"word_{model_suffix}","mpox_visit")
    .groupBy(f"word_{model_suffix}","mpox_visit")    
    .count()
    .sort("mpox_visit","count", ascending = False)
)
"""
data_set = (
    entities_to_select_filtered
    .select("person_id","mpox_visit",f"word_{model_suffix}")
    .groupBy("person_id","mpox_visit")
    .pivot(f"word_{model_suffix}")
    .count()
    .fillna(0)
    .withColumnRenamed("mpox_visit", "label")
    
)
"""
display(data_set)
print(entities_to_select_filtered.select(f"word_{model_suffix}").dropDuplicates().count())
print(data_set.select("person_id").dropDuplicates().count())
#display(entities_to_select_filtered)
"""

#display(data_set)

# COMMAND ----------

#cat_variable = ["statecode", "age_group", "gender"]
cat_variable = ["age_group", "gender"]

#dependent_variables = (
#    dataframe.select(*cat_variable)              
#)

indep_variable = ["label"]

db = data_set.drop("patientuid")
display(db)

# COMMAND ----------

#display(dependent_variables)
#print((dependent_variables.count(), len(dependent_variables.columns)))

#display(
#    dataframe
#    .select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in dataframe.columns])
#)

# COMMAND ----------

len(db.columns) 

# COMMAND ----------

print("total number of patients that had a disease/ symthom identified with the ner", data_set.where("label =1").select("person_id").dropDuplicates().count())

# COMMAND ----------

db.where("label =1").count()

# COMMAND ----------

db.columns

# COMMAND ----------

db.select([F.col(x).alias(x.replace(' ', '_')) for x in db.columns]).write.mode("overwrite").format("delta").saveAsTable(f"{gboostTable}")
