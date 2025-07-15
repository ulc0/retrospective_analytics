# Databricks notebook source
import os, requests, json
dbutils.widgets.text('CATALOG','edav_dev_cdh')
dbutils.widgets.text('SCHEMA','cdh_ml')
dbutils.widgets.text('VOLUME','metadata_data')
dbutils.widgets.text('ATHENA_FILE','/Volumes/edav_dev_cdh/cdh_ml/metadata_data/vocabulary_download_v5_{f6076ed6-ce8a-4970-8dbe-2487f75110bc}_1733837516799.zip')
dbutils.widgets.text("API_KEY","insert-API_KEY")



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
# MAGIC cd /Volumes/edav_dev_cdh/cdh_ml/metadata_data
# MAGIC unzip vocabulary_download_v5_{f6076ed6-ce8a-4970-8dbe-2487f75110bc}_1733837516799.zip

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /Volumes/edav_dev_cdh/cdh_ml/metadata_data
# MAGIC java -Dumls-apikey=$API_KEY -jar cpt4.jar 5 5
