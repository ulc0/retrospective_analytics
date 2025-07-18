# Databricks notebook source
# MAGIC %md
# MAGIC UMLS TERMINOLOGY API
# MAGIC
# MAGIC https://documentation.uts.nlm.nih.gov/automating-downloads.html
# MAGIC

# COMMAND ----------

dbutils.widgets.text('DIRECTORY','/Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data/')
DIRECTORY=dbutils.widgets.get('DIRECTORY')


# COMMAND ----------

import os, requests


# COMMAND ----------

import json

def write_json(fn,dict):
    with open(fn, "w") as fp:
        json.dump(dict, fp)
    return

# COMMAND ----------

# get release dictionary, this is an open endpoint that lists the current data products with a list of dictionaries. In each dictionary the 'endpoint' key is the URI to the list of releases for that data product
RELEASE_URI =f"https://uts-ws.nlm.nih.gov/releases"
l = requests.get(RELEASE_URI)
# check status code for response received
# success code - 200
#print(l)
# print content of request
d=l.json()
data_list=d["releaseTypes"]
print(data_list)



# COMMAND ----------

write_json(DIRECTORY+'releases.json',data_list)

# COMMAND ----------

ddict={l["product"]:{} for l in data_list }
print(ddict)


# COMMAND ----------

for p in ddict:
    for r in data_list:
        if r['product']==p:
            ddict[p][r['releaseType']]=r['endpoint']
    
print(ddict)

# COMMAND ----------

write_json(DIRECTORY+'products.json',ddict)
