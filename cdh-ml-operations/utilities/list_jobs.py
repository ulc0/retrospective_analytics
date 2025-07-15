# Databricks notebook source
import requests
import base64
import os
import json

token = dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-adf-access-token-v1")
#base_url_21 = f"https://adb-8219004871211837.17.azuredatabricks.net/api/2.1/"
base_url_21 = f"https://adb-5189663741904016.16.azuredatabricks.net/api/2.1/"
#base_url_20 = f"https://adb-8219004871211837.17.azuredatabricks.net/api/2.0/"
base_url_20 = f"https://adb-5189663741904016.16.azuredatabricks.net/api/2.0/"
headers={"Authorization": f"Bearer {token}"}
os.environ["DATABRICKS_TOKEN"]=token
os.environ["DATABRICKS_HOST"]=base_url_20

# COMMAND ----------

# MAGIC %sh
# MAGIC #0011495B5DDB6085
# MAGIC databricks jobs configure --version=2.1
# MAGIC databricks cluster-policies  list > cluster_policies.json #--help

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks jobs configure --version=2.1
# MAGIC databricks jobs list --version 2.1 --output JSON --expand-tasks --all > alljobs.json #--help

# COMMAND ----------

# MAGIC %md
# MAGIC response = requests.get(f"{base_url_21}jobs/list", headers=headers)
# MAGIC if response.ok:
# MAGIC     #print(response.text)
# MAGIC     job_json=response.json()
# MAGIC     print(job_json)
# MAGIC     with open("./alljobs.json", 'w', encoding='utf-8') as f:
# MAGIC         json.dump(job_json,f, ensure_ascii=False, indent=4)
# MAGIC
# MAGIC

# COMMAND ----------

with open('alljobs.json', 'r') as f:
    job_json=json.load(f)["jobs"]
print(job_json)

# COMMAND ----------


for j in job_json:
    #jsname=j["creator_user_name"]#['settings']['name']
    #print(jsname)
    if 'cdh' in j["settings"]["name"]:  #['s']=="ulc0@cdc.gov":
        jj=j["settings"]["name"]
       # print(j)
        jname=f"../workflows/{jj}.json"
        xjname=jname.replace(' ','_').replace('-','_')
        print(xjname)
        with open(xjname, 'w', encoding='utf-8') as f:
            json.dump(j,f, ensure_ascii=False, indent=4)

# COMMAND ----------

# MAGIC %sh
# MAGIC pwd
# MAGIC ls ../workflows/
