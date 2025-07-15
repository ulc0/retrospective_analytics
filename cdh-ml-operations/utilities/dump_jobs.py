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
# MAGIC curl --netrc --request GET \  
# MAGIC   https://adb-1234567890123456.7.azuredatabricks.net/api/2.0/workspace/export \  
# MAGIC   --header 'Accept: application/json' \  
# MAGIC   --data '{ "path": "/Repos/me@example.com/MyFolder/MyNotebook", "format": "SOURCE", "direct_download": true }'

# COMMAND ----------

job_name="cdh_ml_etl_premier_exploratory"
#in future pull this from api by name
job_id=104267624722607 #""
#job_id=19525331748519
job_id=104267624722607



# COMMAND ----------

response = requests.get(f"{base_url_21}jobs/list", headers=headers)
if response.ok:
    #print(response.text)
    job_json=response.json()






# COMMAND ----------

# MAGIC %sh
# MAGIC pwd
# MAGIC ls ../workflows/

# COMMAND ----------

import json
jobs=job_json["jobs"]
for j in jobs:
    #jsname=j["creator_user_name"]#['settings']['name']
    #print(jsname)
    #if 'ulc' in jsname:  #['s']=="ulc0@cdc.gov":
        jj=j["settings"]["name"]
        jname=f"../workflows/{jj}.json"
        print(jname)
        with open(jname, 'w', encoding='utf-8') as f:
            json.dump(j,f, ensure_ascii=False, indent=4)
