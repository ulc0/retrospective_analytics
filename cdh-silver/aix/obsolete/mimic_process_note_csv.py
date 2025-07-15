# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

dbutils.widgets.text("INPUT_DIR",'/Volumes/edav_dev_cdh/cdh_ml/metadata_data/mimic/note')
dbutils.widgets.text("CATALOG",'edav_dev_cdh')
dbutils.widgets.text("SCHEMA",'cdh_mimic')
dbutils.widgets.text("INPUT_CSVS",'radiology,discharge')

# COMMAND ----------

INPUT_DIR=dbutils.widgets.get("INPUT_DIR")
CATALOG=dbutils.widgets.get("CATALOG")
SCHEMA=dbutils.widgets.get("SCHEMA")
INPUT_CSVS=dbutils.widgets.get("INPUT_CSVS")
INPUT_CSV_LIST=INPUT_CSVS.split(',')		#['radiology','discharge']
print(INPUT_CSV_LIST)


# COMMAND ----------

from pyspark.sql.types import *
import json

# COMMAND ----------

mimic_detail_schema = StructType([ 
	StructField('note_id', 
				StringType(), False), 
	StructField('person_id', 
				StringType(), False), 
	StructField('field_name', 
				StringType(), True), 
	StructField('field_value', 
				StringType(), True), 
	StructField('field_ordinal', 
				IntegerType(), True), 
]) 

# COMMAND ----------

for INPUT_CSV in INPUT_CSV_LIST:
	print(INPUT_CSV)
	mimic_detail=spark.read.options(header=True,multiLine=True,schema=mimic_detail_schema).csv(f"{INPUT_DIR}/{INPUT_CSV}.csv.gz") #.filter(mimic_detail.field_value != "___")
	display(mimic_detail)
	mimic_detail.write.mode("overwrite").format("delta").saveAsTable(f"{CATALOG}.{SCHEMA}.{INPUT_CSV}")
