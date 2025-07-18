# Databricks notebook source
SOURCE_DIR='/Volumes/edav_dev_cdh/cdh_ml/cdh_ml_vol/data/mimic/note/'
CATALOG='edav_dev_cdh'
SCHEMA='cdh_ml'


# COMMAND ----------

# MAGIC %sh
# MAGIC cd /Volumes/edav_dev_cdh/cdh_ml/cdh_ml_vol/data/mimic/note/
# MAGIC head radiology.csv

# COMMAND ----------

from pyspark.sql.types import *
notes_schema = StructType([ 
	StructField('person_id', 
				StringType(), False), 
	StructField('sectionname', 
				StringType(), True), 
	StructField('observation_datetime', 
				StringType(), False), 
	StructField('note', 
				StringType(), False), 
	StructField('note_type', 
				StringType(), True), 
	StructField('note_group1', 
				StringType(), True), 
	StructField('note_group2', 
				StringType(), True), 
	StructField('note_group3', 
				StringType(), True), 
	StructField('note_group4', 
				StringType(), True), 
	StructField('serviceprovidernpi', 
				StringType(), True), 
	StructField('provider_id', 
				StringType(), True), 
]) 

mimic_schema = StructType([ 
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
	StructField('observation_datetime', 
				StringType(), False), 
	StructField('store_datetime', 
				StringType(), False), 
	StructField('text', 
				StringType(), False), 
]) 
col_map={
"subject_id":"person_id",
"charttime":"observation_datetime",
"text":"note_text",
}

# COMMAND ----------

# MAGIC %md
# MAGIC # initial review
# MAGIC # radiology detail drop field_name, use field_value as HCPCS concept code 
# MAGIC # discharge detail 
# MAGIC csv_fn=['radiology','discharge']
# MAGIC for csv in csv_fn:
# MAGIC     for suffix in ['','_detail']:
# MAGIC         raw_notes=spark.read.options(header=True,multiLine=True,schema=mimic_schema,).csv(f"{SOURCE_DIR}{csv}{suffix}.csv")
# MAGIC         display(raw_notes)

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

import pyspark.sql.functions as F

# COMMAND ----------

rad_unstructured=spark.read.options(header=True,multiLine=True,schema=mimic_schema,quote='"',
escape='"').csv(f"{SOURCE_DIR}radiology.csv.gz").withColumnsRenamed(col_map).drop("storetime") #.drop(F.col("note_id"),)
display(rad_unstructured)
#disc_notes=spark.read.options(header=True,multiLine=True,schema=mimic_schema,).csv(f"{SOURCE_DIR}discharge.csv")
#display(disc_notes)

# COMMAND ----------


rad_detail=spark.read.options(header=True,multiLine=True,).csv(f"{SOURCE_DIR}radiology_detail.csv.gz").withColumnsRenamed(col_map).drop('field_ordinal')
display(rad_detail)

# COMMAND ----------


rad_index=rad_unstructured.select(['note_id','person_id','observation_datetime'])
display(rad_index)

# COMMAND ----------

rad_ts=rad_detail.where(rad_detail.field_name.isin('exam_code')).withColumnRenamed('field_value','concept_code').drop('field_name').withColumn('vocabulary_id',F.lit('HCPCS')).join(rad_index,['note_id','person_id']).drop('note_id').dropDuplicates()
display(rad_ts)

# COMMAND ----------

rad_unstructured.drop("note_id").dropDuplicates().write.mode("overwrite").format("delta").saveAsTable(f"{CATALOG}.{SCHEMA}.radiology_unstructured")
rad_ts.write.mode("overwrite").format("delta").saveAsTable(f"{CATALOG}.{SCHEMA}.radiology_timeseries")
