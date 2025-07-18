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
#import mlflow.spark
#mlflow.spark.autolog()
spark.conf.set('spark.sql.shuffle.partitions',7200*4)
spark.conf.set("spark.databricks.io.cache.enabled", True)

# COMMAND ----------

import os

# COMMAND ----------

#https://edavsynapsedatalake.dfs.core.windows.net/cdh/raw/abfm_phi/20230922/PatientNote/PatientNote000000004999.csv
#https://edavsynapsedatalake.blob.core.windows.net/cdh/raw/abfm_phi/20230922/PatientNote/PatientNote000000004999.csv
#raw/abfm_phi/20230922/PatientNote/PatientNote000000004999.csv
fpath=""
adlsContainerName 	= "cdh"
adlsFolderName 		= "raw"
storage_account_name="edavsynapsedatalake"
baseLocation =  "abfss://" + 'cdh' + "@" + storage_account_name + ".dfs.core.windows.net/"
input_dir = os.path.join(baseLocation, f"raw/abfm_phi/20230922/")
xname="/Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data/"
table="PatientNote"
fname=f"{input_dir}{table}/{table}000000000005.csv"
oname="/Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data/fixed_PatientNote000000000005.csv"
fname="/Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data/PatientNote000000000005.csv"
content = []
with open(fname, 'r') as file_in:
    content = file_in.readlines()
print(content[1])



# COMMAND ----------

import re
uuid_regex="^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
pattern = re.compile(uuid_regex, re.IGNORECASE)

def is_patientuid(input_text):
    return pattern.match(input_text)

r=re.search(uuid_regex,content[1][:36])
#m=re.match(uuid_regex,content[1])
print(content[1])
print(r[0])
#print(m)


##prog = re.compile(pattern)
##result = prog.match(string)


# COMMAND ----------

###NO NO NO the patientuid is not a vaid UID
from uuid import UUID

def is_valid_uuid(uuid_to_test, version=4):
    """
    Check if uuid_to_test is a valid UUID.
    
     Parameters
    ----------
    uuid_to_test : str
    version : {1, 2, 3, 4}
    
     Returns
    -------
    `True` if uuid_to_test is a valid UUID, otherwise `False`.
    
     Examples
    --------
    >>> is_valid_uuid('c9bf9e57-1685-4c89-bafb-ff5af830be8a')
    True
    >>> is_valid_uuid('c9bf9e58')
    False
    """
    
    try:
        #uuid_obj = UUID(uuid_to_test, version=version)
        r=re.search(uuid_regex,uuid_to_test)
        if r is not None:
            uuid_obj=r[0]
        else:
            uuid_obj=""
    except ValueError:
        return False
    return str(uuid_obj) == uuid_to_test


# COMMAND ----------

#is_valid_uuid(content[1][:36])
is_valid_uuid("4C5BDD37-3BC8-4C1B-B85B-CA078BF18844",4)
is_valid_uuid("F4253D4C-541D-4C8C-8E49-790D3E72B40C")
#is_valid_uuid('fakljdf%%')

# COMMAND ----------

import string
kstring=(string.punctuation+' '+string.ascii_letters+string.digits)
print(kstring)
print('k' in kstring)

# COMMAND ----------


new_content = [] 
lineCount=0
clean_line=""
for line in content:
    #print(line)
    if(len(line)>37): # not first line and must be a patientuid line
        test_uid=line[:36]
        if is_valid_uuid(test_uid):
                print(f"saving...{test_uid}")
#               clean_line=clean_line.replace('\00', '').replace('\\n','').replace('\\r','')+'\\n'
#               clean_line = clean_line+line.replace('\00', '').replace('\n','').replace('\13','')
#               cline=[l in line if l in string.printable ]
                cline=''.join(i for i in clean_line if (i in kstring))
                #print(cline)
                new_content.append(cline+'\\n')                
                clean_line=""
                print(lineCount)
        else:
            print(test_uid)
        clean_line = clean_line+line    
    lineCount=lineCount+1


with open(oname, 'w') as file_out:
    file_out.writelines(new_content)



# COMMAND ----------

import csv

records = []
reader = csv.reader(open(oname), quotechar='"')

for record in reader:
    print (record[0])
    records.append(record)

# COMMAND ----------

rec=[2,13,14,15,16,19,20,21]
for r in rec:
    print(new_content[r])

# COMMAND ----------

#oname="/Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data/fixed_PatientNote000000000005.csv"


# PySpark - Apply custom schema to a DataFrame by changing names 

# Import the libraries SparkSession, StructType, 
# StructField, StringType, IntegerType 
from pyspark.sql import SparkSession 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,DateType,TimestampType


#/Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data/PatientNote000000000005.csv
# Define the structure for the data frame 
#patientuid,sectionname,encounterdate,note,type,group1,group2,group3,group4,serviceprovidernpi,practiceid

schema = StructType([ 
	StructField('patientuid', 
				StringType(), False), 
	StructField('sectionname', 
				StringType(), False), 
	StructField('encounterdate', 
				TimestampType(), False), 
	StructField('note', 
				StringType(), False), 
	StructField('type', 
				StringType(), False), 
	StructField('group1', 
				StringType(), False), 
	StructField('group2', 
				StringType(), False), 
	StructField('group3', 
				StringType(), False), 
	StructField('group4', 
				StringType(), False), 
	StructField('serviceprovidernpi', 
				IntegerType(), False), 
	StructField('practiceid', 
				IntegerType(), False), 
]) 

# Applying custom schema to data frame 

df = spark.read.schema(schema).option( "header", True).option("multiline",True).option("lineSep","\n").csv(fname) 

# Display the updated schema 
df.printSchema() 

#display(df)



# COMMAND ----------

display(df)

# COMMAND ----------

# no need to order the cohort
cohort = f"select distinct patientuid from {cohortTbl}  order by patientuid {LIMIT}"
#cohort ="cdh_abfm_phi_exploratory.run9_STI_cohort "
def notes_query(nschema,ntable): # ,nbegin,nend):
    sqlstring1=f" select patientuid as person_id,'{ntable}' as note_type, encounterdate as note_datetime"
#    sqlstring1a="concat_ws('|',sectionname,note,type,group1,group2,group3) as note_line,1 as stop_id "
    sqlstring1a=",sectionname as note_section,note as note_text,type,group1,group2,group3,group4"
    sqlstring2a=",substring(note_text,1,4) as note_stub"
    sqlstring1b=", regexp_count(trim(note), ' ') as nspaces, length(trim(note)) as tlen "
    sqlstring2=f" from {nschema}.{ntable}"
    ##sqlstring_predicate=f" where patientuid in (select distinct patientuid from {cohort}) UNION"
    ##sqlstring_predicate=f"where (encounterdate)<={sbegin} and year(encounterdate)<={send} UNION"
    sqlstring_predicate=f" where patientuid in ({cohort}) order by person_id, note_datetime UNION"
#    sqlstring_predicate=f"(select distinct patientuid from {nschema}.{ntable} where year(encounterdate)<={nbegin} and year(encounterdate)<={nend} ) order by patientuid, encounterdate UNION"
#    sqlstring=sqlstring1+sqlstring1a+substring2a+sqlstring1b+sqlstring2+sqlstring_predicate    
    sqlstring=sqlstring1+sqlstring1a+sqlstring2a+sqlstring1b+sqlstring2+sqlstring_predicate    
    return sqlstring


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
#pivot_ids=["person_id","note_datetime","note_type","note_section","note_stub","tlen","nspaces"]
#pivot_vals=["group1","group2","group3","group4"]
#type(sqlstring)
##limit=" limit 5000"
## use cohort limit=" limit 20000"
#limit= ""
notes=spark.sql(sqlstring)
#.unpivot(pivot_ids,pivot_vals,"note_group","note_text").filter("note_text is not Null").withColumn("note_stub",F.substring(F.col("note_text"),1,4))


# COMMAND ----------

#notes=notes.withColumn("note_document",substring('text',1,3))
display(notes)

htmlstring='<?xml version="1.0" ?><html>'

# COMMAND ----------

# for additional primary cleanse/tag
notes=notes.select("*",
                    F.when(notes.note_stub == '{\\rt', 'rtf')
                   .when(notes.note_stub =='<?xm' , 'xml')
                   .when(notes.note_stub=='<htm','htm')
                   .when(F.substring(F.col("note_text"),1,24)=='<?xml version="1.0" ?><html>','htm')
                   .when(notes.note_stub=='<SOA','sop')
                   .when(notes.note_stub=='<CHRT','xml')
                   .when(notes.note_stub=='<Care','xml')
#                   .when(notes.note_stub=='<Care','xml')
                  #                   .when(notes.note_stub=='u><','htm')
                   .otherwise('utf').alias('encoding'))

#notes=notes.withColumn('encoding',F.when(notes.note_stub == '{\\rtf', 'rtf')
#                   .when(notes.note_text==htmlstring,'utf')
#                   .when(notes.note_stub =='<?xml' , 'xml')
#                   .when(notes.note_stub=='<html','utf')
#                   .when(notes.note_text=='u><','utf')
#                   .otherwise('utf'))
notes.display()


# COMMAND ----------

notes.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(otbl)

# COMMAND ----------

#notes.groupby("provider_id","encoding").count().show(truncate=False)
#provider_xref=spark.sql("select count(*) as notes group by provider_id,encoding from partition_notes")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC dbutils.jobs.taskValues.set(key="basetable",value=otbl)
# MAGIC for t in ['rtf','utf','xml','sop','htm']:  #'xml','htm',]:
# MAGIC   filtered_sdf=notes.filter(F.col("encoding")==t).write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(otbl+"_"+t)
# MAGIC
# MAGIC
# MAGIC #mpox_notes.write.mode("overwrite").format("delta").saveAsTable(stbl)
# MAGIC
