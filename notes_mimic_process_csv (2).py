# Databricks notebook source
INPUT_DIR='/Volumes/edav_dev_cdh/cdh_ml/cdh_ml_vol/data/mimic/note/'
CATALOG='edav_dev_cdh'
SCHEMA='cdh_ml'
INPUT_CSV_LIST=['radiology','discharge']

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /Volumes/edav_dev_cdh/cdh_ml/cdh_ml_vol/data/mimic/note/
# MAGIC head radiology.csv

# COMMAND ----------

from pyspark.sql.types import *
import pyspark.sql.functions as F
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

NDIR=[f"{INPUT_DIR}/{INPUT_CSV}.csv.gz" for INPUT_CSV in INPUT_CSV_LIST]
print(NDIR)
mimic_note_df=spark.read.options(header=True,multiLine=True,quote='"',escape='"',).csv(NDIR).withColumnsRenamed(colsRenamed).withColumn('provider_id',F.lit(1)).withColumn("note_id", F.monotonically_increasing_id())
display(mimic_note_df)
#.withColumnsRenamed(col_map).drop("storetime") #

# COMMAND ----------

mimic_index=mimic_note_df.select(['mimic_note_id','note_id','person_id','observation_datetime'])
display(mimic_index)

# COMMAND ----------

#import pyspark.sql.functions as F
#Create empty DataFrame directly.

# COMMAND ----------

colsRenamed={
"subject_id":"person_id",
"charttime":"observation_datetime",
"text":"note_text",
"note_id":"mimic_note_id",
}


NDIR=[f"{INPUT_DIR}/{INPUT_CSV}_detail.csv.gz" for INPUT_CSV in INPUT_CSV_LIST]
print(NDIR)
mimic_ts=spark.read.options(header=True,multiLine=True,quote='"',escape='"',).csv(NDIR).withColumnsRenamed(colsRenamed).withColumn('provider_id',F.lit(1))
display(mimic_ts)

# COMMAND ----------

display(mimic_ts.select('field_name').distinct())


# COMMAND ----------

bronze_ts=mimic_ts.join(mimic_index,['mimic_note_id','subject_id']).dropDuplicates()
display(bronze_ts)

# COMMAND ----------

bronze_notes.drop("note_id").dropDuplicates().write.mode("overwrite").format("delta").saveAsTable(f"{CATALOG}.{SCHEMA}.note_bronze_mimic")
bronze_ts.write.mode("overwrite").format("delta").saveAsTable(f"{CATALOG}.{SCHEMA}.timeseries_mimic")
