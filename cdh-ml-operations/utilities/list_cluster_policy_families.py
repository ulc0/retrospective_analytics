# Databricks notebook source
import requests
import base64
import os
import pandas as pd
import json

token =dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-adf-access-token-v1")
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

#['policy_id', 'name', 'definition', 'policy_family_id', 'policy_family_version', 'policy_family_definition_overrides', 'created_at_timestamp', 'is_default', 'description', 'libraries']
response = requests.get(f"{base_url_20}policies/clusters/list", headers=headers)
if response.ok:
    cluster_policies_json=response.json()
    cp=pd.DataFrame(cluster_policies_json["policies"])  
    print(cp[['policy_id', 'name']])

# COMMAND ----------

"""
3   001ABF32847D185C      CDH_Job_Only_Cluster - Engineering Legacy ACL
4   0004E4A64B47C0B4              CDH_Job_Only_Cluster - Engineering UC
5   00176EE1B8F5BA62                      CDH_Job_Only_Cluster - ML CPU
6   0011495B5DDB6085                      CDH_Job_Only_Cluster - ML GPU
"""
cdh_policy_ids=["001ABF32847D185C","0004E4A64B47C0B4","00176EE1B8F5BA62","0011495B5DDB6085",]

# COMMAND ----------

#policy_id="0011495B5DDB6085"
for policy_id in cdh_policy_ids:
    response = requests.get(f"{base_url_20}policies/clusters/get?policy_id={policy_id}", headers=headers)
    print(response.json())
    if response.ok:
        job_json=response.json()
        cp=json.dumps(job_json, indent=4)
        print(cp)






# COMMAND ----------

plist=job_json["job-cluster"]
print(plist)

# COMMAND ----------

import json

with open("../compute/prd/cluster_policy_list.json", 'w', encoding='utf-8') as f:
    json.dump(job_json,f, ensure_ascii=False, indent=4)
with open("../compute/prd/ml_job_compute.json", 'w', encoding='utf-8') as f:
    json.dump(plist,f, ensure_ascii=False, indent=4)    
