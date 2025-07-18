# Databricks notebook source
# MAGIC %md
# MAGIC Spec:   
# MAGIC Accept standard UC parameters  
# MAGIC **LIMIT** parameter for testing, in lieu of cohort for this   
# MAGIC **INPUT_DIR** location of csv files (default it to read all in directory CDH Engineering Style)  
# MAGIC **INPUT_CSVS** one or more subdirectories of the INPUT directory that have the raw csv files, multi separated by comma  

# COMMAND ----------

dbutils.widgets.text("INPUT_DIR",'abfss://cdh@edavcdhproddlmprd.dfs.core.windows.net/raw/abfm_phi/20230922')
dbutils.widgets.text("INPUT_CSVS","PatientNote,PatientNoteProblem,PatientNoteProcedure,PatientNoteResultObservation")
dbutils.widgets.text("OUTPUT_TABLE",'note_bronze')
dbutils.widgets.text("CATALOG",defaultValue="edav_prd_cdh",)
dbutils.widgets.text("SCHEMA",defaultValue="cdh_abfm_phi_exploratory",)
dbutils.widgets.text("LIMIT",defaultValue="0",)

# COMMAND ----------

#spark.sql("SHOW DATABASES").show(truncate=False)
#import mlflow.spark
#mlflow.spark.autolog()
spark.conf.set('spark.sql.shuffle.partitions',7200*4)
spark.conf.set("spark.databricks.io.cache.enabled", True)


# COMMAND ----------



INPUT_DIR=dbutils.widgets.get("INPUT_DIR")
INPUT_CSVS=dbutils.widgets.get("INPUT_CSVS")
INPUT_CSV_LIST=INPUT_CSVS.split(',')
OUTPUT_TABLE=f"note_bronze"
OUTPUT_TABLE=OUTPUT_TABLE.lower()
CATALOG=dbutils.widgets.get("CATALOG")
SCHEMA=dbutils.widgets.get("SCHEMA")
RECLIMIT=int(dbutils.widgets.get("LIMIT"))


# COMMAND ----------

# MAGIC %md
# MAGIC raw/abfm_phi/20230922/PatientAdvanceDirective/
# MAGIC https://edavcdhproddlmprd.dfs.core.windows.net/cdh/raw/abfm_phi/20230922/PatientAdvanceDirective/
# MAGIC
# MAGIC
# MAGIC raw/abfm_lds/20240731/PatientNote

# COMMAND ----------

from pyspark.sql.types import *
import pyspark.sql.functions as F
import json

#TODO a library of JSON Schemas
with open('../../note_schema.json','r') as j:
	notes_json=json.load(j)
note_schema=StructType.fromJson(notes_json)
note_names=note_schema.names
print(note_names)


# COMMAND ----------

# https://kb.databricks.com/sql/gen-unique-increasing-values?from_search=155818339
#from pyspark.sql.window import *
#window = Window.partitionBy("practiceid").orderBy(F.col('xnote_id'))
#notes_df = notes_df.withColumn('note_id', F.row_number().over(window)).drop("tbl_no","xnote_id").withColumnsRenamed(colsRenamed)
#notes_df.show()

# COMMAND ----------

colsRenamed={
	"patientuid":"person_id",
	"sectionname":"note_title",
	"encounterdate":"note_datetime",
	"note":"note_text",
	"group":"note_type",
	"practiceid":"provider_id",
#	"group":"note_title",
	}
NDIR=[f"{INPUT_DIR}/{INPUT_CSV}/" for INPUT_CSV in INPUT_CSV_LIST]
print(NDIR)
print(RECLIMIT)
#TODO Put Schema check here
bronze_notes=spark.read.options(header=True,multiLine=True,quote='"',escape='"',).csv(NDIR).withColumn("group",F.concat_ws('|',F.trim(F.col("group1")),F.trim(F.col("group2")),F.trim(F.col("group3")),F.trim(F.col("group4")),)).withColumn("note_id", F.monotonically_increasing_id()).withColumnsRenamed(colsRenamed)
bronze_names=bronze_notes.schema.names
print(bronze_names)	
# orfdered intersection
bronze_names_sorted=[col for col in note_names if col in bronze_names]
print(bronze_names_sorted)



# COMMAND ----------

#TODO put markup removal here? or is that a separate process to a new table with two columns?

# COMMAND ----------


display(bronze_notes)

# COMMAND ----------

bronze_notes.select(*bronze_names_sorted).orderBy("provider_id","person_id","note_datetime").write.mode("overwrite").option("overwriteSchema", "true").partitionBy("provider_id").format("delta").saveAsTable(f"{CATALOG}.{SCHEMA}.{OUTPUT_TABLE}")

# COMMAND ----------

bronze_notes.count()

# COMMAND ----------

# MAGIC %md
# MAGIC single=notes.where('patientuid=="4a37a29d-b16a-4e7c-9a64-391d23c7e564"')

# COMMAND ----------

# MAGIC %md
# MAGIC CATALOG='edav_prd_cdh'
# MAGIC SCHEMA='cdh_sandbox'
# MAGIC OUTPUT_TABLE='abfm_lds_patientnote'
# MAGIC single.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f"{CATALOG}.{SCHEMA}.{OUTPUT_TABLE}")
