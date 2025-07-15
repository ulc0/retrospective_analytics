# Databricks notebook source
import os, requests, json
dbutils.widgets.text('CATALOG','edav_dev_cdh')
dbutils.widgets.text('SCHEMA','cdh_ml')
dbutils.widgets.text('VOLUME','metadata_data/athena/20250220/')
dbutils.widgets.text('ATHENA_FILE','/Volumes/edav_dev_cdh/cdh_ml/metadata_data/vocabulary_download_v5_20250220.zip')
dbutils.widgets.text("API_KEY","789be32d-9376-4611-a0d5-6e5ff6f7955c")



# COMMAND ----------

#CATALOG=dbutils.widgets.get('CATALOG')
#SCHEMA=dbutils.widgets.get('SCHEMA')
#VOLUME=dbutils.widgets.get('VOLUME')
#ATHENA_FILE=dbutils.widgets.get('ATHENA_FILE')
API_KEY=dbutils.widgets.get('API_KEY')
os.environ['API_KEY']=API_KEY
#https://github.com/OHDSI/CommonDataModel/tree/v5.3.1

#edav_dev_cdh.cdh_ml.metadata_data

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /Volumes/edav_prd_cdh/cdh_ml/metadata_data/athena/20250220/
# MAGIC #mkdir logs
# MAGIC unzip -o ../vocabulary_download_v5_250220.zip
# MAGIC java -Dumls-apikey=$API_KEY -jar cpt4.jar 5 6
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC cd /Volumes/edav_dev_cdh/cdh_ml/metadata_data/athena/20250220
# MAGIC ls -ll
# MAGIC java -Dumls-apikey=$API_KEY -jar cpt4.jar 5 5
