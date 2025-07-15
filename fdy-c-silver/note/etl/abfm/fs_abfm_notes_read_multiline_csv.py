# Databricks notebook source
#spark.sql("SHOW DATABASES").show(truncate=False)
#import mlflow.spark
#mlflow.spark.autolog()
spark.conf.set('spark.sql.shuffle.partitions',7200*4)
spark.conf.set("spark.databricks.io.cache.enabled", True)


# COMMAND ----------


dbutils.widgets.text("INPUT_DIR",'abfss://cdh@edavcdhproddlmprd.dfs.core.windows.net/raw/abfm_phi/20230922')
dbutils.widgets.text("INPUT_CSV","PatientNote")
#dbutils.widgets.text("OUTPUT_TABLE",f"{lower(INPUT_CSV)}_ml",)
dbutils.widgets.text("CATALOG",defaultValue="edav_prd_cdh",)
dbutils.widgets.text("SCHEMA",defaultValue="cdh_abfm_phi_exploratory",)

# COMMAND ----------



INPUT_DIR=dbutils.widgets.get("INPUT_DIR")
INPUT_CSV=dbutils.widgets.get("INPUT_CSV")
OUTPUT_TABLE=f"{INPUT_CSV}_ml"
OUTPUT_TABLE=OUTPUT_TABLE.lower()
CATALOG=dbutils.widgets.get("CATALOG")
SCHEMA=dbutils.widgets.get("SCHEMA")


ndir=f"{INPUT_DIR}/{INPUT_CSV}/"
print(ndir)

# COMMAND ----------

# MAGIC %md
# MAGIC raw/abfm_phi/20230922/PatientAdvanceDirective/
# MAGIC https://edavcdhproddlmprd.dfs.core.windows.net/cdh/raw/abfm_phi/20230922/PatientAdvanceDirective/
# MAGIC
# MAGIC
# MAGIC raw/abfm_lds/20240731/PatientNote

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DateType,TimestampType

#/Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data/PatientNote000000000005.csv
# Define the structure for the data frame 
#patientuid,sectionname,encounterdate,note,type,group1,group2,group3,group4,serviceprovidernpi,practiceid

notes_schema = StructType([ 
	StructField('person_id', 
				StringType(), False), 
	StructField('sectionname', 
				StringType(), True), 
	StructField('observation_datetime', 
				StringType(), False), 
	StructField('note_raq', 
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
from pyspark.sql.functions import monotonically_increasing_id


#sort_order=['note_id','practiceid','patientuid','encounterdate','group1']
# Applying custom schema to data frame 

#df = spark.read.option("delimiter",'\t').csv(oname,) 
#df = spark.read.schema(schema).option( "header", True).option("multiline",True).option("lineSep","\n").csv(fname) 

# Display the updated schema 
#df.printSchema() 

#display(df)

#emp_RDD = spark.emptyRDD()
#notes_df = spark.createDataFrame(data=emp_RDD,                                        schema=notes_schema)
#notes_df.show()

# COMMAND ----------

notes=spark.read.options(header=True,multiLine=True,quote='"',escape='"',schema=notes_schema,).csv(ndir).select(*).withColumn("note_id", monotonically_increasing_id())
#	.orderBy(*sort_order)
display(notes)

# COMMAND ----------

notes.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f"{CATALOG}.{SCHEMA}.{OUTPUT_TABLE}")

# COMMAND ----------

print(notes.count())

# COMMAND ----------

# MAGIC %md
# MAGIC single=notes.where('patientuid=="4a37a29d-b16a-4e7c-9a64-391d23c7e564"')

# COMMAND ----------

# MAGIC %md
# MAGIC CATALOG='edav_prd_cdh'
# MAGIC SCHEMA='cdh_sandbox'
# MAGIC OUTPUT_TABLE='abfm_lds_patientnote'
# MAGIC single.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f"{CATALOG}.{SCHEMA}.{OUTPUT_TABLE}")
