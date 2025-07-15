# Databricks notebook source
# MAGIC %md
# MAGIC UMLS TERMINOLOGY API
# MAGIC
# MAGIC https://documentation.uts.nlm.nih.gov/automating-downloads.html
# MAGIC

# COMMAND ----------

import os, requests, json
dbutils.widgets.text('CATALOG','edav_prd_cdh')
dbutils.widgets.text('SCHEMA','cdh_ml')
CATALOG=dbutils.widgets.get('CATALOG')
SCHEMA=dbutils.widgets.get('SCHEMA')

DOWNLOAD_URI=f"https://uts-ws.nlm.nih.gov/download?url="
RELEASE_URI =f"https://uts-ws.nlm.nih.gov/releases"

# COMMAND ----------

# get release dictionary, this is an open endpoint that lists the current data products with a list of dictionaries. In each dictionary the 'endpoint' key is the URI to the list of releases for that data product

l = requests.get(RELEASE_URI)
# check status code for response received
# success code - 200
#print(l)
# print content of request
d=l.json()
data_list=d["releaseTypes"]
print(data_list)



# COMMAND ----------

all_data=[]
for data in data_list:
    l = requests.get(data['endpoint'])
    # check status code for response received
    # success code - 200
    #print(l)
    # print content of request
    d=l.json()
    #data_list=d["releaseTypes"]
    #print(data_list)
    print(d)
    all_data=all_data+d

# COMMAND ----------

import pandas as pd
releases=spark.createDataFrame(pd.DataFrame.from_records(all_data)) #.drop(columns=['product']))
display(releases)


# COMMAND ----------

releases.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f"{CATALOG}.{SCHEMA}.umls_releases")

