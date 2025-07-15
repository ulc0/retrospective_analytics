# Databricks notebook source
# MAGIC %md ## Tables to explore in ABFM
# MAGIC
# MAGIC 1. visitdiagnosis
# MAGIC 2. patientsocialhistoryobservation
# MAGIC 3. patientnoteresultobservation
# MAGIC 4. patientnoteprocedure
# MAGIC 5. **patientnoteproblem**
# MAGIC 6. **patientnote**
# MAGIC 7. patientproblem

# COMMAND ----------

# mandatory parameters and names. The orchestrator will always pass these

dbutils.widgets.text("notetable", defaultValue="ft_abfm_notes_text") #,patientnoteproblem,patientnoteresultobservation") #"patientnoteproblem,patientnoteresultobservation")
dbutils.widgets.text("src_catalog", defaultValue="edav_prd_cdh")
dbutils.widgets.text("src_schema", defaultValue="cdh_abfm_phi")
dbutils.widgets.text("experiment_id", defaultValue="3389456715448166")
#dbutils.widgets.text("schema", defaultValue="edav_prd_cdh.cdh_abfm_phi")
dbutils.widgets.text("catalog",defaultValue="edav_prd_cdh",)
dbutils.widgets.text("schema",defaultValue="cdh_abfm_phi_exploratory",)


# COMMAND ----------

#cohortTbl = dbutils.widgets.get("cohorttbl")
#LIMIT = dbutils.widgets.get("LIMIT")
EXPERIMENT_ID=dbutils.widgets.get("experiment_id")
src_schema = dbutils.widgets.get("src_schema")
schema = dbutils.widgets.get("schema")
src_catalog = dbutils.widgets.get("src_catalog")
catalog = dbutils.widgets.get("catalog")
print(catalog)
notetable = dbutils.widgets.get("notetable")
### remove hardcoding notetable="patientnote,patientnoteproblem,patientnoteresultobservation"
print(notetable)
#notetable = notetable.split(",")  # allow multiple tables

dbutils.widgets.text("output_table", defaultValue="")
output_table=dbutils.widgets.get("output_table")
print(output_table)

# COMMAND ----------

import pyspark.sql.functions as F
import os
os.environ["PYSPARK_PIN_THREAD"]="False"
#import mlflow
#import mlflow.spark

#mlflow.spark.autolog()
#mlflow.set_experiment(experiment_id=EXPERIMENT_ID)
#mlflow.end_run()
#run=mlflow.start_run(experiment_id=EXPERIMENT_ID,run_name=run_name)
#run_id=run.info.run_id
#dbutils.jobs.taskValues.set("run_id",run_id)
spark.conf.set("spark.sql.shuffle.partitions", 7200 * 4)
spark.conf.set("spark.databricks.io.cache.enabled", True)

# COMMAND ----------

# MAGIC %md ## Tables to explore in ABFM
# MAGIC
# MAGIC 1. visitdiagnosis
# MAGIC 2. patientsocialhistoryobservation
# MAGIC 3. patientnoteresultobservation
# MAGIC 4. patientnoteprocedure
# MAGIC 5. **patientnoteproblem**
# MAGIC 6. **patientnote**
# MAGIC 7. patientproblem
# MAGIC
# MAGIC visitdiagnosis.problemcomment
# MAGIC #Include all diagnoses recorded during the encounter.	 51,458,151 rows 	51458151
# MAGIC patientsocialhistoryobservation	This section contains data defining the patient‚Äôs occupational, personal (e.g., tobacco, alcohol), social, and environmental history and health risk factors, as well as administrative data such as marital status, race, ethnicity and religious affiliation. Social history can have significant influence on a patient‚Äôs physical, psychological and emotional health and wellbeing so should be considered in the development of a should be considered in the development of a complete record.	153,338,255 rows 	153338255
# MAGIC
# MAGIC patientnoteresultobservation	This section describes any data about patient in Result Observation Notes format or XML format with 'Section Name' for which data is documented.	114,520,405 rows 	114520405
# MAGIC
# MAGIC patientnoteprocedure	This section describes any data about patient in Procedure Notes format or XML format with 'Section Name' for which data is documented.	685,087 rows 	685087
# MAGIC
# MAGIC patientnoteproblem	This section describes any data about patient in Problem Notes format or XML format with 'Section Name' for which data is documented.	1,955,650 rows 	1955650
# MAGIC
# MAGIC patientnote	This section describes any data about patient in Notes format or XML format with 'Section Name' for which data is documented.	 557,464,443 rows 	557464443
# MAGIC <table> must be enclosed in <html></html>
# MAGIC
# MAGIC patientproblem	This section lists and describes all relevant clinical problems at the time the document is generated. At a minimum, all pertinent current and historical problems should be listed. This section should include the problem list.	162,736,059 rows 	162736059
# MAGIC
# MAGIC patientnotemedication	This section describes any data about patient in Medication Notes format or XML format with 'Section Name' for which data is documented.	2,975,127 rows 	2975127
# MAGIC patientnotevitalobservation	This section describes any data about patient in Vital Signs Notes format or XML format with 'Section Name' for which data is documented.	3,412,119 rows 	3412119
# MAGIC
# MAGIC
# MAGIC visitdiagnosis,patientsocialhistoryobservation,patientnoteresultobservation,patientnoteprocedure,patientnoteproblem
# MAGIC ,patientnote,patientnotemedication,patientnotevitalobservation

# COMMAND ----------


"""
htmlstring = '<?xml version="1.0"?><html>'
#htmlstring = '<html>
notes = notes.withColumn("encoding",
    F.when(F.col("note_text").contains("rtf1"), "rtf") 
    .when(F.col("note_text").like("<?xm%"), "xml") 
    .when(F.col("note_text").like("<htm%"), "htm") 
    .when(F.col("note_text").contains(htmlstring), "htm") 
    .when(F.col("note_text").like("<SOA%"), "sop") 
    .when(F.col("note_text").like("<CHRT%"), "xml")
    .when(F.col("note_text").like("<Care%"), "xml")
    .otherwise("utf"))

sqlstring1b=",len(trim(note_text)) as note_len, CASE WHEN contains(note_text,'rtf1') THEN 'rtf' " + \
"WHEN starts_with(note_text,'<?xm') THEN 'xml' " + \
"WHEN starts_with(note_text,'<htm') THEN 'htm' " + \
f"WHEN  contains(note_text,'{htmlstring}')  THEN 'htm' " + \
"WHEN starts_with(note_text,'<CHRT') THEN 'xml' " + \
"WHEN starts_with(note_text,'<Care%') THEN 'xml' " + \
"ELSE 'utf' END as encoding"
"""

# COMMAND ----------

"""
# no need to order the cohort
#cohort = f"select distinct person_id from {cohortTbl}  order by person_id {LIMIT}"
# cohort ="edav_prd_cdh.cdh_abfm_phi_exploratory.run9_STI_cohort "
def notes_query(nschema, ntable):  # ,nbegin,nend):
    sqlstring1 = f" (select person_id as person_id,'{ntable}' as note_type,practiceid as provider_id, visit_start_date as note_datetime"
    sqlstring1 = f" (select person_id,provider_id, visit_start_date as note_datetime"
    #    sqlstring1a="concat_ws('|',sectionname,note,type,note_group1,note_group2,note_group3) as note_line,1 as stop_id "
    sqlstring1a = f",sectionname as note_section,note as note_text,type,note_group1,note_group2,note_group3,note_group4"
    sqlstring1a = f",note as note_text,note_type,concat_ws(' ',trim(note_group1),trim(note_group2),trim(note_group3),trim(note_group4)) as note_group"
    #sqlstring2a = ",substring(note_text,1,4) as note_stub"
    sqlstring2 = f" from {nschema}.{ntable} "
    ##sqlstring_predicate=f" where person_id in (select distinct person_id from {cohort}) UNION"
    ##sqlstring_predicate=f"where (visit_start_date)<={sbegin} and year(visit_start_date)<={send} UNION"
#no longer used
    sqlstring_predicate = (
        f" where person_id in ({cohort}) order by person_id, note_datetime) UNION"
    )
    #    sqlstring_predicate=f"(select distinct person_id from {nschema}.{ntable} where year(visit_start_date)<={nbegin} and year(visit_start_date)<={nend} ) order by person_id, visit_start_date UNION"
    #    sqlstring=sqlstring1+sqlstring1a+substring2a+sqlstring1b+sqlstring2+sqlstring_predicate
    sqlstring = (
        sqlstring1
        + sqlstring1a
    #    + sqlstring2a
        #+ sqlstring1b
        + sqlstring2
    #    + sqlstring_predicate
    )
    return sqlstring
    """

# COMMAND ----------

"""
#from shared.configs import set_secret_scope

#set_secret_scope()
ss = ""
sqlstring1b="" # was encoding assignement
for ntbl in notetable:
    print(ntbl)
    ss = ss + notes_query(f"{src_catalog}.{src_schema}", ntbl)  # ,sbegin,send)
sqlstring = f"select * {sqlstring1b} from ("+ss[:-5]+")"  # remove last union
print(sqlstring)

# sqlstring=sqlstring+f" order by person_id,note_datetime" #,note_type,stop_id "
sqlstring=f"create or replace table {output_table} as "+sqlstring
print(sqlstring)
"""

# COMMAND ----------

# MAGIC %md
# MAGIC [note](https://ohdsi.github.io/CommonDataModel/cdm54.html#note) Table Description  
# MAGIC
# MAGIC The NOTE table captures unstructured information that was recorded by a provider about a patient in free text (in ASCII, or preferably in UTF8 format) notes on a given date. The type of note_text is CLOB or varchar(MAX) depending on RDBMS.
# MAGIC
# MAGIC User Guide  
# MAGIC
# MAGIC NA  
# MAGIC
# MAGIC ETL Conventions  
# MAGIC
# MAGIC HL7/LOINC CDO is a standard for consistent naming of documents to support a range of use cases: retrieval, organization, display, and exchange. It guides the creation of LOINC codes for clinical notes. CDO annotates each document with 5 dimensions:  
# MAGIC
# MAGIC * **Kind of Document**: Characterizes the general structure of the document at a macro level (e.g. Anesthesia Consent)  
# MAGIC * **Type of Service**: Characterizes the kind of service or activity (e.g. evaluations, consultations, and summaries). The notion of time sequence, e.g., at the beginning (admission) at the end (discharge) is subsumed in this axis. Example: Discharge Teaching.  
# MAGIC * **Setting**: Setting is an extension of CMS’s definitions (e.g. Inpatient, Outpatient)  
# MAGIC * **Subject Matter Domain (SMD)**: Characterizes the subject matter domain of a note (e.g. Anesthesiology)  
# MAGIC * **Role**: Characterizes the training or professional level of the author of the document, but does not break down to specialty or subspecialty (e.g. Physician) Each combination of these 5 dimensions rolls up to a unique LOINC code.  
# MAGIC According to CDO requirements, only 2 of the 5 dimensions are required to properly annotate a document; Kind of Document and any one of the other 4 dimensions. However, not all the permutations of the CDO dimensions will necessarily yield an existing LOINC code. Each of these dimensions are contained in the OMOP Vocabulary under the domain of ‘Meas Value’ with each dimension represented as a Concept Class.

# COMMAND ----------

# MAGIC %md
# MAGIC person_id string  
# MAGIC provider_id int  
# MAGIC note_datetime timestamp  
# MAGIC type string  
# MAGIC group string  
# MAGIC note_len int  
# MAGIC encoding string  
# MAGIC note_text string  

# COMMAND ----------

import pyspark.sql.functions as F
dropCols=["note_group1","note_group2","note_group3","note_group4",]

# COMMAND ----------

from pyspark.sql.functions import monotonically_increasing_id

df=spark.table(f"{src_catalog}.{src_schema}.{notetable}")
silver_df = df.distinct().select("*").withColumn("note_id", monotonically_increasing_id()).withColumnRenamed("type","note_type")
display(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC create or replace table IDENTIFIER(:catalog || '.' || :schema || '.' || :output_table) as 
# MAGIC select distinct person_id,provider_id, visit_start_date as note_datetime,
# MAGIC concat_ws(' ',trim(note_group1),trim(note_group2),trim(note_group3),trim(note_group4)) as note_group,
# MAGIC sectionname as note_section,note as note_text,type as note_type
# MAGIC from IDENTIFIER( :src_catalog || '.' || :src_schema: || '.' || :notetable )
# MAGIC

# COMMAND ----------

otbl=f"{catalog}.{schema}.{output_table}"
print(otbl)
silver_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(otbl)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC htmlstring = '<?xml version="1.0"?><html>'
# MAGIC # for additional primary cleanse/ta
# MAGIC                                  
# MAGIC notes = notes.select(
# MAGIC     "*",
# MAGIC     F.when(F.col("note_stub").contains("rtf1}"), "rtf") 
# MAGIC     .when(F.col("note_stub").like("<?xm%"), "xml") 
# MAGIC     .when(F.col("note_stub").like("<htm%"), "htm") 
# MAGIC     .when(F.col("note_text").contains(htmlstring), "htm") 
# MAGIC     .when(F.col("note_stub").like("<SOA%"), "sop") 
# MAGIC     .when(F.col("note_stub").like("<CHRT%"), "xml")
# MAGIC     .when(F.col("note_stub").like("<Care%"), "xml")
# MAGIC     #                   .when(notes.note_stub=='<Care','xml')
# MAGIC     #                   .when(notes.note_stub=='u><','htm')
# MAGIC     .otherwise("utf").alias("encoding"),
# MAGIC ).drop("note_stub")
# MAGIC
# MAGIC notes.display()

# COMMAND ----------

"""
htmlstring = '<?xml version="1.0"?><html>'

notes = notes.withColumn("encoding",
    F.when(F.col("note_text").contains("rtf1"), "rtf") 
    .when(F.col("note_text").like("<?xm%"), "xml") 
    .when(F.col("note_text").like("<htm%"), "htm") 
    .when(F.col("note_text").contains(htmlstring), "htm") 
    .when(F.col("note_text").like("<SOA%"), "sop") 
    .when(F.col("note_text").like("<CHRT%"), "xml")
    .when(F.col("note_text").like("<Care%"), "xml")
    .otherwise("utf"))
"""


#notes.display()

# COMMAND ----------

# moved to next pipeline
#encodings=notes.select("encoding").distinct().collect()
#print(encodings)

# COMMAND ----------

## already written notes.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(output_table)
#dbutils.jobs.taskValues.set("basetable", output_table)
##dbutils.jobs.taskValues.set("run_id",run_id)

