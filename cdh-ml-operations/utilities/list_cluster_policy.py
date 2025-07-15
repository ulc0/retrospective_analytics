# Databricks notebook source
import requests
import base64
import os

token = dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-adf-access-token-v1")
#base_url_21 = f"https://adb-8219004871211837.17.azuredatabricks.net/api/2.1/"
base_url_21 = f"https://adb-5189663741904016.16.azuredatabricks.net/api/2.1/"
#base_url_20 = f"https://adb-8219004871211837.17.azuredatabricks.net/api/2.0/"
base_url_20 = f"https://adb-5189663741904016.16.azuredatabricks.net/api/2.0/"
headers={"Authorization": f"Bearer {token}"}
os.environ["DATABRICKS_TOKEN"]=token
os.environ["DATABRICKS_HOST"]=base_url_20

# COMMAND ----------

# MAGIC %md
# MAGIC [Cluster Policies Reference](https://learn.microsoft.com/en-us/azure/databricks/administration-guide/clusters/policy-definition)

# COMMAND ----------

response = requests.get(f"{base_url_20}policies/clusters/list", headers=headers)
if response.ok:
    print(response.json())
    job_json=response.json()







# COMMAND ----------

plist={}
for p in job_json['policies']:
    if 'ML' in p["name"] and 'CDH' in p["name"]:
        print(p["name"])    
        plist[p["policy_id"]]=p["name"]
print(plist)

# COMMAND ----------

import json

with open("../compute/prd/cluster_policy_list.json", 'w', encoding='utf-8') as f:
    json.dump(job_json,f, ensure_ascii=False, indent=4)
with open("../compute/prd/ml_job_compute.json", 'w', encoding='utf-8') as f:
    json.dump(plist,f, ensure_ascii=False, indent=4)    
