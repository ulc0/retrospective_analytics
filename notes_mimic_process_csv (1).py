# Databricks notebook source
SOURCE_DIR='/Volumes/edav_dev_cdh/cdh_ml/cdh_ml_vol/data/mimic/note/'
CATALOG='edav_dev_cdh'
SCHEMA='cdh_ml'
INPUT_CSV_LIST=['radiology','discharge']

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /Volumes/edav_dev_cdh/cdh_ml/cdh_ml_vol/data/mimic/note/
# MAGIC head radiology.csv

# COMMAND ----------

from pyspark.sql.types import *
import json


with open('../../note_schema.json','r') as j:
	notes_json=json.load(j)
	print(notes_json)
	notes_schema=StructType.fromJson(notes_json)


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
	StructField('observation_datetime', 
				StringType(), False), 
	StructField('store_datetime', 
				StringType(), False), 
	StructField('text', 
				StringType(), False), 
	StructField('note_source', 
				StringType(), False), 
]) 
import pyspark.sql.functions as F
#Create empty DataFrame directly.
mimic_note_df = spark.createDataFrame([], mimic_note_schema)
mimic_note_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### initial review
# MAGIC #### radiology detail drop field_name, use field_value as HCPCS concept code 
# MAGIC #### discharge detail 
# MAGIC csv_fn=['radiology','discharge']
# MAGIC for csv in csv_fn:
# MAGIC     for suffix in ['','_detail']:
# MAGIC         raw_notes=spark.read.options(header=True,multiLine=True,schema=mimic_schema,).csv(f"{SOURCE_DIR}{csv}{suffix}.csv")
# MAGIC         display(raw_notes)

# COMMAND ----------



col_map={
"subject_id":"person_id",
"charttime":"observation_datetime",
"text":"note_text",
}
drop_cols=["storetime"]

# COMMAND ----------

for INPUT_CSV in INPUT_CSV_LIST:
	print(INPUT_CSV)
	mimic_unstructured=spark.read.options(header=True,multiLine=True,quote='"',escape='"').csv(f"{SOURCE_DIR}{INPUT_CSV}.csv.gz").withColumn("note_source",F.lit(f"{INPUT_CSV}"))
	#display(mimic_unstructured)
	mimic_note_df=mimic_note_df.union(mimic_unstructured).orderBy("subject_id","note_seq","note_type",) #TODO add monotonically increasing here ...
display(mimic_note_df)
#.withColumnsRenamed(col_map).drop("storetime") #

# COMMAND ----------

dict_rename={"note_id":"note_id",  
            "subject_id":"person_id",
}
mimic_index=mimic_note_df.select(['note_id','subject_id','observation_datetime'])
display(mimic_index)

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
import pyspark.sql.functions as F
#Create empty DataFrame directly.
mimic_detail_df = spark.createDataFrame([], mimic_detail_schema)
mimic_detail_df.printSchema()

# COMMAND ----------


for INPUT_CSV in INPUT_CSV_LIST:
    print(INPUT_CSV)
    mimic_detail=spark.read.options(header=True,multiLine=True,schema=mimic_detail_schema).csv(f"{SOURCE_DIR}{INPUT_CSV}_detail.csv.gz") #.filter(mimic_detail.field_value != "___")
    #.withColumnsRenamed(col_map).drop('field_ordinal')
    #display(mimic_detail)
    mimic_detail_df=mimic_detail.union(mimic_detail_df)
display(mimic_detail_df)

# COMMAND ----------

display(mimic_detail_df.select('field_name').distinct())


# COMMAND ----------

mimic_ts=mimic_detail_df.join(mimic_index,['note_id','subject_id']).dropDuplicates()
display(mimic_ts)

# COMMAND ----------

mimic_unstructured.drop("note_id").dropDuplicates().write.mode("overwrite").format("delta").saveAsTable(f"{CATALOG}.{SCHEMA}.unstructured")
mimic_ts.write.mode("overwrite").format("delta").saveAsTable(f"{CATALOG}.{SCHEMA}.timeseries")
