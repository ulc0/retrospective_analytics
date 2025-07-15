# Databricks notebook source
dbutils.widgets.text("INPUT_DIR",'/Volumes/edav_dev_cdh/cdh_ml/metadata_data/mimic/')
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
import pyspark.sql.functions as F
import json

mimic_index = spark.table(f"{CATALOG}.{SCHEMA}_ra.note_fact_xref")
mimic_index.printSchema()
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
    mimic_detail=spark.table(f"{CATALOG}.{SCHEMA}.{INPUT_CSV}_detail")
    display(mimic_detail.select("field_name").distinct())
    #.withColumnsRenamed(col_map).drop('field_ordinal')
    #display(mimic_detail)
    mimic_detail_df=mimic_detail.union(mimic_detail_df)
display(mimic_detail_df)

# COMMAND ----------

mimic_detail_df.select("field_name").distinct().collect()

# COMMAND ----------


mimic_detail_df=mimic_detail_df.join(mimic_index,'note_id','outer')
display(mimic_detail_df)

# COMMAND ----------

mimic_detail_full=mimic_detail_df.withColumn('visit_type',F.split(mimic_detail_df['note_id'], '-').getItem(1)).withColumn('visit_sequence',F.split(mimic_detail_df['note_id'], '-').getItem(2))
display(mimic_detail_full)

# COMMAND ----------

display(mimic_detail_full.select('field_name').distinct())
display(mimic_detail_full.select('field_ordinal').distinct())

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

mimic_facts=mimic_detail_full.filter(mimic_detail_full.field_name.isin(['exam_code','cpt_code',]))
display(mimic_facts)

# COMMAND ----------


# TODO, break out feature table from field_name, field_value
mimic_ts=mimic_detail_df.dropDuplicates().withColumnsRenamed(col_map).drop(*drop_cols).filter(~F.col("field_name").isin(["exam_name","author"]))
display(mimic_ts)
