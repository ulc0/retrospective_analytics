# Databricks notebook source
import mlflow
from pyspark.sql.functions import struct, col
mlflow.__version__

#

# COMMAND ----------

dbutils.widgets.text('model_name' , defaultValue= "alvaroalon2/biobert_diseases_ner",label="Huggingface Hub Name")
architecture = dbutils.widgets.get("model_name")
print(architecture)

# COMMAND ----------


suffix=architecture.split('/')[1].replace('-',"_")
print(suffix)

dbutils.widgets.text('input_table' , defaultValue= f"edav_prd_cdh.cdh_abfm_phi_exploratory.aix_feedback_note_bronze_mpox",label="Huggingface Hub Name")
input_table = dbutils.widgets.get("input_table")
print(input_table)

input_table=f"{input_table}_{suffix}"

print(input_table)

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
from pyspark.sql.functions import regexp_replace, trim, length, col, lower

#spark.sql("SHOW DATABASES").show(truncate=False)
spark.conf.set('spark.sql.shuffle.partitions',7200*4)


# COMMAND ----------

dbutils.widgets.text("xgboostTable","edav_prd_cdh.cdh_abfm_phi_exploratory.aix_feedback_note_bronze_mpox")
xgboostTable=dbutils.widgets.get("xgboostTable")

xgboostTable=f"{xgboostTable}_{suffix}_xgboost"

print(xgboostTable)

# COMMAND ----------

# putting this here to stop momentarely the workflow and continue to the next step, will delete on a large run. I had already the tables precomputed
#dbutils.notebook.exit("whatever reason to make it stop")

# COMMAND ----------

#edav_prd_cdh.cdh_abfm_phi_exploratory.ml_nlp_mpox_notes
mpox_cohort = (
    spark.table("edav_prd_cdh.cdh_abfm_phi_exploratory.ft_abfm_notes_hf_biobert_diseases_ner") # cohort of interest, includes labels
    .withColumnRenamed("patientuid", "person_id")
    .withColumnRenamed("mpox_visit","label")
    .select("person_id",'label')
    .dropDuplicates()
    .join(        
        spark.table("edav_prd_cdh.cdh_abfm_phi_ra.note_bronze")
        .select(
            'person_id',
            "note_id",      
            )
        ,['person_id']
        , 'left'
    )    
)

#display(mpox_cohort)


# COMMAND ----------

entities_to_select = (
    mpox_cohort
    .join(spark.table(f"{input_table}"), ['note_id'], 'left')
    .fillna(0)
    .withColumn("snippet",
        F.when(F.col("snippet").isNull(), "no_sign_symtom_identified")
        .otherwise(F.col("snippet"))
    )
)

#display(entities_to_select)

# COMMAND ----------

notes_stats = (
    entities_to_select
    .select("person_id","label","snippet") 
    .dropDuplicates() 
    .withColumn("note_lenght",
                F.length(F.col("snippet"))
                )      
)


# COMMAND ----------


spark.conf.set("spark.sql.pivotMaxValues", 100000)
entities_to_select_filtered = (
    entities_to_select    
    .withColumn("snippet", regexp_replace("snippet", r"([^\s\w]|_)+", ""))
    .withColumn("snippet", trim(regexp_replace(col("snippet"), r"\s+", " ")))    
    .withColumn("snippet", lower(col("snippet")))
    .withColumnRenamed("cleaned_text","snippet")
)

data_set = (
    entities_to_select_filtered
    .select("person_id","label","snippet")
    .where(length("snippet") > 2)
    .groupBy("person_id","label")
    .pivot("snippet")
    .count()
    .fillna(0)    
)

display(data_set)

# COMMAND ----------

cat_variable = ["age_group", "gender"]
indep_variable = ["label"]
db = data_set.drop("patientuid")
display(db)
print(len(db.columns))

# COMMAND ----------

print("total number of patients that had a disease/ symthom identified with the ner", data_set.where("label =1").select("person_id").dropDuplicates().count())

# COMMAND ----------

db.where("label =1").count()

# COMMAND ----------

db.columns

# COMMAND ----------

xgboostTable

# COMMAND ----------

spark.sql("DROP TABLE IF EXISTS xgboostTable".format(xgboostTable))

# COMMAND ----------



# COMMAND ----------

db.select([F.col(x).alias(x.replace(' ', '_')) for x in db.columns]).write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable(f"{xgboostTable}")

# COMMAND ----------

xgboostTable

# COMMAND ----------


print(xgboostTable)
display(spark.table(xgboostTable))

print(len(spark.table(xgboostTable).columns))

# COMMAND ----------


