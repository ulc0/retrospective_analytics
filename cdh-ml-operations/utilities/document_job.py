# Databricks notebook source
import requests
import base64
import os

token = dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-adf-access-token-v1")
base_url_21 = f"https://adb-8219004871211837.17.azuredatabricks.net/api/2.1/"
base_url_21 = f"https://adb-5189663741904016.16.azuredatabricks.net/api/2.1/"
base_url_20 = f"https://adb-8219004871211837.17.azuredatabricks.net/api/2.0/"
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

response = requests.get(f"{base_url_21}jobs/get", params={"job_id":job_id}, headers=headers)
if response.ok:
    #print(response.text)
    job_json=response.json()
    print(job_json)




# COMMAND ----------

import json

settings=job_json["settings"]
setting_keys=list(settings.keys())
print(setting_keys)


# COMMAND ----------

git_source=settings["git_source"]
giturl=f"{git_source['git_url']}/tree/{git_source['git_branch']}/"
print(giturl)
#https://github.com/cdcent/cdh-ml-featurestore/tree/exploratory
tasklist=[]
for task in settings["tasks"]:
  task_key=task["task_key"]
  task_path=task["notebook_task"]["notebook_path"]
  depends=task["depends_on"]
  print(depends)
  for tk in depends:
      print(tk["task_key"])

