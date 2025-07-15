# Databricks notebook source
dbutils.widgets.text("INPUT_DIR",'/Volumes/edav_dev_cdh/cdh_ml/cdh_ml_vol/data/mimic/note')
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

# MAGIC %md  
# MAGIC
# MAGIC https://aws.amazon.com/blogs/big-data/perform-biomedical-informatics-without-a-database-using-mimic-iii-data-and-amazon-athena/  
# MAGIC
# MAGIC
# MAGIC
# MAGIC ```python
# MAGIC df = spark.read.csv('s3://'+mimiccsvinputbucket+'/NOTEEVENTS.csv.gz',\
# MAGIC 	header=True,\
# MAGIC 	schema=schema,\
# MAGIC 	multiLine=True,\
# MAGIC 	quote='"',\
# MAGIC 	escape='"')
# MAGIC   ```

# COMMAND ----------

# MAGIC %md
# MAGIC |-- note_id: string (nullable = true)  
# MAGIC  |-- subject_id: string (nullable = true)  
# MAGIC  |-- hadm_id: string (nullable = true)  
# MAGIC  |-- note_type: string (nullable = true)  
# MAGIC  |-- note_seq: string (nullable = true)  
# MAGIC  |-- charttime: string (nullable = true)  
# MAGIC  |-- storetime: string (nullable = true)  
# MAGIC  |-- text: string (nullable = true)  

# COMMAND ----------

mimic_note_schema = StructType([ 
	StructField('note_id', 
				StringType(), False), 
	StructField('subject_id', 
				StringType(), False), 
	StructField('hadm_id', 
				StringType(), True), 
	StructField('note_type', 
				StringType(), True), 
	StructField('note_seq', 
				StringType(), True), 
	StructField('charttime', 
				StringType(), False), 
	StructField('storetime', 
				StringType(), False), 
	StructField('text', 
				StringType(), False), 
	StructField('note_source', 
				StringType(), False), 
]) 
import pyspark.sql.functions as F
#Create empty DataFrame directly.
#mimic_note_df = spark.createDataFrame([], mimic_note_schema)
#mimic_note_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### initial review
# MAGIC #### radiology detail drop field_name, use field_value as HCPCS concept code 
# MAGIC #### discharge detail 
# MAGIC csv_fn=['radiology','discharge']
# MAGIC for csv in csv_fn:
# MAGIC     for suffix in ['','_detail']:
# MAGIC         raw_notes=spark.read.options(header=True,multiLine=True,schema=mimic_schema,).csv(f"{INPUT_DIR}{csv}{suffix}.csv")
# MAGIC         display(raw_notes)

# COMMAND ----------

for INPUT_CSV in INPUT_CSV_LIST:
	print(INPUT_CSV)
	mimic_unstructured=spark.read.options(header=True,multiLine=True,quote='"',escape='"',schema=mimic_note_schema).csv(f"{INPUT_DIR}/{INPUT_CSV}.csv.gz").withColumn("note_source",F.lit(f"{INPUT_CSV}")) #.drop(drop_cols) #.withColumnsRenamed(col_map)
	mimic_unstructured.printSchema()
#	mimic_note_df=mimic_note_df.union(mimic_unstructured) #.orderBy("subject_id","note_seq","note_type",) #TODO add monotonically increasing here ...
#	display(mimic_note_df)
	mimic_unstructured.write.mode("overwrite").format("delta").saveAsTable(f"{CATALOG}.{SCHEMA}.{INPUT_CSV}")

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
	mimic_detail=spark.read.options(header=True,multiLine=True,schema=mimic_detail_schema).csv(f"{INPUT_DIR}/{INPUT_CSV}_detail.csv.gz") #.filter(mimic_detail.field_value != "___")
	display(mimic_detail)
	mimic_detail.write.mode("overwrite").format("delta").saveAsTable(f"{CATALOG}.{SCHEMA}.{INPUT_CSV}_detail")
