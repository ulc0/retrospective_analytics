# Databricks notebook source
# MAGIC %md
# MAGIC UMLS TERMINOLOGY API
# MAGIC
# MAGIC https://documentation.uts.nlm.nih.gov/automating-downloads.html
# MAGIC

# COMMAND ----------

import os, requests, json
dbutils.widgets.text('CATALOG','edav_dev_cdh')
dbutils.widgets.text('SCHEMA','cdh_ml')
dbutils.widgets.text('VOLUME','metadata_data')
dbutils.widgets.text('ATHENA_URI','https://athena.ohdsi.org/api/v1/vocabularies/zip/f6076ed6-ce8a-4970-8dbe-2487f75110bc')
dbutils.widgets.text("API_KEY","API_KEY")



# COMMAND ----------

CATALOG=dbutils.widgets.get('CATALOG')
SCHEMA=dbutils.widgets.get('SCHEMA')
VOLUME=dbutils.widgets.get('VOLUME')
ATHENA_URI=dbutils.widgets.get('ATHENA_URI')
API_KEY=dbutils.widgets.get('API_KEY')
FILENAME='athena_download_v5.zip'

VOLDIR=f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}/[FILENAME]"
print(VOLDIR)
#https://github.com/OHDSI/CommonDataModel/tree/v5.3.1
CPT_CMD=f"java -Dumls-apikey={API_KEY} -jar cpt4.jar 5 5"
#edav_dev_cdh.cdh_ml.metadata_data

# COMMAND ----------

import os, tempfile

temp_dir = tempfile.mkdtemp() 
print(temp_dir)
os.environ["TEMP_DIR"]=temp_dir

# get release dictionary, this is an open endpoint that lists the current data products with a list of dictionaries. In each dictionary the 'endpoint' key is the URI to the list of releases for that data product

r = requests.get(ATHENA_URI)
# check status code for response received
# success code - 200
#print(l)


# COMMAND ----------

# print content of request
open(VOLDIR , 'wb').write(r.content)
print(VOLDIR)
#dbutils.fs.cp(temp_dir+FILENAME,f"{VOLDIR}")


# COMMAND ----------


