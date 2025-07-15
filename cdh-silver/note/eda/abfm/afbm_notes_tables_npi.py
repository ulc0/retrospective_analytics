# Databricks notebook source
#spark.sql("SHOW DATABASES").show(truncate=False)
#import pandas as pd
from pyspark.sql.window import Window
from datetime import date
from pyspark.sql import SparkSession, DataFrame
#from pyspark.sql.functions import isnan, when, count, col, desc, asc, row_number, concat_ws, coalesce,to_timestamp,regexp_replace, to_date, date_format, concat, lower, substring
import pyspark.sql.functions as F
from functools import reduce
import json
import mlflow.spark
#mlflow.spark.autolog()
spark.conf.set('spark.sql.shuffle.partitions',7200*4)
spark.conf.set("spark.databricks.io.cache.enabled", True)

# COMMAND ----------





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

# COMMAND ----------

#mandatory parameters and names. The orchestrator will always pass these
dbutils.widgets.text('notetable',defaultValue="patientnote")
# defaultValue = "visitdiagnosis,patientsocialhistoryobservation,patientnoteresultobservation,patientnoteprocedure,patientnoteproblem,patientnote,patientnotemedication,patientnotevitalobservation")
dbutils.widgets.text('schema',defaultValue="edav_prd_cdh.cdh_abfm_phi")

#schema='edav_prd_cdh.cdh_abfm_phi' #dbutils,widgets.get('ml-head-node','schema')
schema=dbutils.widgets.get('schema')
ntable=dbutils.widgets.get('notetable')
print(ntable)
notetable=ntable.split(",") #allow multiple tables
#notetable=json.loads(ntable)

#stbl=f"{schema}.{notetable}"
#print(stbl)

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


cohort="'498efd8b-9571-4d68-bf29-9b49951c53b2'"

def notes_query(nschema,ntable): # ,nbegin,nend):
    sqlstring1=f" select patientuid as person_id,'{ntable}' as note_type, encounterdate as note_datetime"
#    sqlstring1a="concat_ws('|',sectionname,note,type,group1,group2,group3) as note_line,1 as stop_id "
    sqlstring1a=",sectionname as note_section,note as note_text,type,group1,group2,group3,group4"
    sqlstring2a=",substring(note_text,1,4) as note_stub"
    sqlstring1b=", regexp_count(trim(note), ' ') as nspaces, length(trim(note)) as tlen "
    sqlstring2=f" from {nschema}.{ntable} "
    ##sqlstring_predicate=f" where patientuid in (select distinct patientuid from {cohort}) UNION"
    ##sqlstring_predicate=f"where (encounterdate)<={sbegin} and year(encounterdate)<={send} UNION"
    sqlstring_predicate=f" where patientuid in ({cohort}) UNION"
#    sqlstring_predicate=f"(select distinct patientuid from {nschema}.{ntable} where year(encounterdate)<={nbegin} and year(encounterdate)<={nend} ) order by patientuid, encounterdate UNION"
#    sqlstring=sqlstring1+sqlstring1a+substring2a+sqlstring1b+sqlstring2+sqlstring_predicate    
    sqlstring=sqlstring1+sqlstring1a+sqlstring2a+sqlstring1b+sqlstring2+sqlstring_predicate    
    return sqlstring




# COMMAND ----------

from shared.configs import set_secret_scope
set_secret_scope()
ss=""
for n in notetable:
  print(n)
  ss=ss+notes_query(schema,n)#,sbegin,send)
sqlstring=ss[:-5] # remove last union
print(sqlstring)
#sqlstring=sqlstring+f" order by person_id,note_datetime" #,note_type,stop_id "

# COMMAND ----------

#person_id
#note_type
#note_datetime
#sectionname
#note_text
#type
#group1
#group2
#group3
#note_stub
#nspaces
#tlen
#encoding
pivot_ids=["person_id","note_datetime","note_type","note_section","note_stub","tlen","nspaces"]
pivot_vals=["group1","group2","group3","group4"]
#type(sqlstring)
##limit=" limit 5000"
## use cohort limit=" limit 20000"
limit= ""
notes=spark.sql(sqlstring+limit).unpivot(pivot_ids,pivot_vals,"note_group","note_text").filter("note_text is not Null").withColumn("note_stub",F.substring(F.col("note_text"),1,4))


# COMMAND ----------

#notes=notes.withColumn("note_document",substring('text',1,3))
#display(notes)

htmlstring='<?xml version="1.0" ?><html>'

# COMMAND ----------

# for additional primary cleanse/tag
notes=notes.select("*",
                    F.when(notes.note_stub == '{\\rt', 'rtf')
                   .when(notes.note_stub =='<?xm' , 'xml')
                   .when(notes.note_stub=='<tab','htm')
                   .when(notes.note_stub=='<htm','htm')
                   .when(notes.note_stub=='<htm','htm')
                   .when(F.substring(F.col("note_text"),1,24)==htmlstring,'htm')
                   .when(notes.note_stub=='<SOA','sop')
                   .when(notes.note_stub=='<CHRT','xml')
                   .when(notes.note_stub=='<Care','xml')
                   .when(notes.note_stub=='<Care','xml')
                    .when(notes.note_stub=='u><','htm')
                   .otherwise('utf').alias('encoding'))

#notes=notes.withColumn('encoding',F.when(notes.note_stub == '{\\rtf', 'rtf')
#                   .when(notes.note_text==htmlstring,'utf')
#                   .when(notes.note_stub =='<?xml' , 'xml')
#                   .when(notes.note_stub=='<html','utf')
#                   .when(notes.note_text=='u><','utf')
#                   .otherwise('utf'))
notes.display()


# COMMAND ----------

#notes.groupby("provider_id","encoding").count().show(truncate=False)
#provider_xref=spark.sql("select count(*) as notes group by provider_id,encoding from partition_notes")

# COMMAND ----------

otbl=f"{schema}_exploratory.ml_fs_abfm_notes_eda"
print(otbl)
dbutils.jobs.taskValues.set(key="basetable",value=otbl)
notes.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(otbl)


#mpox_notes.write.mode("overwrite").format("delta").saveAsTable(stbl)

