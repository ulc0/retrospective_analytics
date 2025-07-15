# Databricks notebook source
token ="dapi13bfcf3655bac3ba43df591cff836ac5-3" #dbutils.secrets.get(scope="dbs-scope-dev-kv-CDH", key="cdh-adb-adf-access-token-v1")

# COMMAND ----------

import requests
import base64
import os


base_url_21 = f"https://adb-1881246389460182.2.azuredatabricks.net/api/2.1/"
#base_url_21 = f"https://adb-5189663741904016.16.azuredatabricks.net/api/2.1/"
base_url_20 = f"https://adb-1881246389460182.2.azuredatabricks.net/api/2.0/"
#base_url_20 = f"https://adb-5189663741904016.16.azuredatabricks.net/api/2.0/"
headers={"Authorization": f"Bearer {token}"}
os.environ["DATABRICKS_TOKEN"]=token
os.environ["DATABRICKS_HOST"]=base_url_20

# COMMAND ----------

# MAGIC %md
# MAGIC [Cluster Policies Reference](https://learn.microsoft.com/en-us/azure/databricks/administration-guide/clusters/policy-definition)
# MAGIC [Cluster Policies API](https://docs.databricks.com/api/azure/workspace/clusterpolicies/list)

# COMMAND ----------

response = requests.get(f"{base_url_20}policies/clusters/list", headers=headers)
print(response.json())
if response.ok:
    print(response.json())
    job_json=response.json()







# COMMAND ----------

plist={}
for p in job_json['policies']:
    if 'ML' in p["name"] and 'CDH' in p["name"]:
        print(p["name"])    
        plist[p["policy_id"]]=p["name"]
        print(p)
print(plist)

# COMMAND ----------

import json

with open("../compute/dev/cluster_policy_list.json", 'w', encoding='utf-8') as f:
    json.dump(job_json,f, ensure_ascii=False, indent=4)
with open("../compute/dev/ml_job_compute.json", 'w', encoding='utf-8') as f:
    json.dump(plist,f, ensure_ascii=False, indent=4)    
