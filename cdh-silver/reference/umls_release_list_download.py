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

# MAGIC %md
# MAGIC # get release dictionary, this is an open endpoint that lists the current data products with a list of dictionaries. In each dictionary the 'endpoint' key is the URI to the list of releases for that data product
# MAGIC print(RELEASE_URI)
# MAGIC l = requests.get(RELEASE_URI)
# MAGIC # check status code for response received
# MAGIC # success code - 200
# MAGIC print(l)
# MAGIC # print content of request
# MAGIC d=l.json()
# MAGIC data_list=d["releaseTypes"]
# MAGIC print(data_list)
# MAGIC
# MAGIC

# COMMAND ----------

#from RELEASE-URL
all_data = [
    {
        "product": "RxNorm",
        "releaseType": "RxNav-in-a-Box",
        "endpoint": "https://uts-ws.nlm.nih.gov/releases?releaseType=rxnav-in-a-box",
    },
    {
        "product": "RxNorm",
        "releaseType": "RxNorm Full Monthly Release",
        "endpoint": "https://uts-ws.nlm.nih.gov/releases?releaseType=rxnorm-full-monthly-release",
    },
    {
        "product": "RxNorm",
        "releaseType": "RxNorm Prescribable Content Monthly Release",
        "endpoint": "https://uts-ws.nlm.nih.gov/releases?releaseType=rxnorm-prescribable-content-monthly-release",
    },
    {
        "product": "RxNorm",
        "releaseType": "RxNorm Prescribable Content Weekly Updates",
        "endpoint": "https://uts-ws.nlm.nih.gov/releases?releaseType=rxnorm-prescribable-content-weekly-updates",
    },
    {
        "product": "RxNorm",
        "releaseType": "RxNorm Weekly Updates",
        "endpoint": "https://uts-ws.nlm.nih.gov/releases?releaseType=rxnorm-weekly-updates",
    },
    {
        "product": "SNOMED CT",
        "releaseType": "SNOMED CT CORE Problem List Subset",
        "endpoint": "https://uts-ws.nlm.nih.gov/releases?releaseType=snomed-ct-core-problem-list-subset",
    },
    {
        "product": "SNOMED CT",
        "releaseType": "SNOMED CT International Edition",
        "endpoint": "https://uts-ws.nlm.nih.gov/releases?releaseType=snomed-ct-international-edition",
    },
    {
        "product": "SNOMED CT",
        "releaseType": "SNOMED CT Spanish Edition",
        "endpoint": "https://uts-ws.nlm.nih.gov/releases?releaseType=snomed-ct-spanish-edition",
    },
    {
        "product": "SNOMED CT",
        "releaseType": "SNOMED CT to ICD-10-CM Mapping Resources",
        "endpoint": "https://uts-ws.nlm.nih.gov/releases?releaseType=snomed-ct-to-icd-10-cm-mapping-resources",
    },
    {
        "product": "SNOMED CT",
        "releaseType": "SNOMED CT US Edition",
        "endpoint": "https://uts-ws.nlm.nih.gov/releases?releaseType=snomed-ct-us-edition",
    },
    {
        "product": "SNOMED CT",
        "releaseType": "SNOMED CT US Edition Transitive Closure Resources",
        "endpoint": "https://uts-ws.nlm.nih.gov/releases?releaseType=snomed-ct-us-edition-transitive-closure-resources",
    },
    {
        "product": "UMLS",
        "releaseType": "UMLS Full Release",
        "endpoint": "https://uts-ws.nlm.nih.gov/releases?releaseType=umls-full-release",
    },
    {
        "product": "UMLS",
        "releaseType": "UMLS Metathesaurus Full Subset",
        "endpoint": "https://uts-ws.nlm.nih.gov/releases?releaseType=umls-metathesaurus-full-subset",
    },
    {
        "product": "UMLS",
        "releaseType": "UMLS Metathesaurus MRCONSO File",
        "endpoint": "https://uts-ws.nlm.nih.gov/releases?releaseType=umls-metathesaurus-mrconso-file",
    },
]

# COMMAND ----------

# MAGIC %md
# MAGIC all_data=[]
# MAGIC for data in data_list:
# MAGIC     d=data['endpoint']
# MAGIC     l = requests.get(d)
# MAGIC     # check status code for response received
# MAGIC     # success code - 200
# MAGIC     print(l)
# MAGIC     # print content of request
# MAGIC     #d=l.json()
# MAGIC     #data_list=d["releaseTypes"]
# MAGIC     #print(data_list)
# MAGIC     print(d)
# MAGIC     all_data=all_data+[d]

# COMMAND ----------

import pandas as pd
releases=spark.createDataFrame(pd.DataFrame.from_records(all_data)) #.drop(columns=['product']))
display(releases)


# COMMAND ----------

releases.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f"{CATALOG}.{SCHEMA}.umls_releases")

