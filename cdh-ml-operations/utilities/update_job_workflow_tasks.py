# Databricks notebook source
import requests
import base64
import os
dbutils.widgets.text("job_id","723783140786053")#,"1108635922512600")
token = 'dapi55b41bb30e7ab39c31ba2b7bee533ee0-3' #dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-adf-access-token-v1")
base_url_21_dev = f"https://adb-8219004871211837.17.azuredatabricks.net/api/2.1/"
base_url_21 = f"https://adb-5189663741904016.16.azuredatabricks.net/api/2.1/"
base_url_20_dev = f"https://adb-8219004871211837.17.azuredatabricks.net/api/2.0/"
base_url_20 = f"https://adb-5189663741904016.16.azuredatabricks.net/api/2.0/"
headers={"Authorization": f"Bearer {token}"}
os.environ["DATABRICKS_TOKEN"]=token
os.environ["DATABRICKS_HOST"]=base_url_20_dev

# COMMAND ----------

# MAGIC %md
# MAGIC curl --netrc --request GET \  
# MAGIC   https://adb-1234567890123456.7.azuredatabricks.net/api/2.0/workspace/export \  
# MAGIC   --header 'Accept: application/json' \  
# MAGIC   --data '{ "path": "/Repos/me@example.com/MyFolder/MyNotebook", "format": "SOURCE", "direct_download": true }'

# COMMAND ----------

job_id=dbutils.widgets.get("job_id")
#in future pull this from api by name
#job_id=104267624722607 #""
#job_id=19525331748519
## cluster id is hardcoded to PRODUCTION right now, and is for key "settings"/create or "new_settings"/reset
existing_cluster_id="1109-202015-25ul7dyg"
#existing_cluster_id="1026-182657-15shaat1"
existing_cluster_id="0616-211319-eyelok6a"
existing_cluster_id="1109-202017-ti5qsauv" #ETL3 for now
existing_cluster_id="1010-185232-guk05xqt" #ML Cluster
ML_CLUSTER="0427-171205-6qa2hhxn"
existing_cluster_id=ML_CLUSTER

# COMMAND ----------

response = requests.get(f"{base_url_21_dev}jobs/get", params={"job_id":job_id}, headers=headers)
if response.ok:
    orig_job_params=response.json()
    #print(response.text)
    print(orig_job_params)
else:
    print(response.text)




# COMMAND ----------

#os.getcwd()
orig_settings=orig_job_params["settings"]
orig_setting_keys=list(orig_settings.keys())
job_name=orig_settings["name"]
print(job_name)

# COMMAND ----------

import json

json_file=f"../jobs/{job_name}.json"

with open(json_file, "rb") as f:
    new_job_params = json.load(f)
print(new_job_params)

# COMMAND ----------

new_settings=new_job_params #["settings"]
new_setting_keys=list(new_settings.keys())
print(new_setting_keys)


# COMMAND ----------

skey="new_settings"
job_payload={}
job_payload["job_id"]=job_id
job_payload[skey]={}

# COMMAND ----------

print(job_payload)
for k in orig_setting_keys:
  job_payload[skey][k]=orig_settings[k]

# COMMAND ----------

tasklist=[]
# not even sure we need a loop here
for task in new_settings["tasks"]:
  task["existing_cluster_id"]=existing_cluster_id
  print(task)
  tasklist=tasklist+[task]
#print(tasklist)
#print("Original: ")
#print(settings["tasks"])
job_payload[skey]["tasks"]=tasklist

# COMMAND ----------

# MAGIC %md
# MAGIC tasklist=[]
# MAGIC #### NO LOOP NEEDED FOR EXISTING COMPUTE not even sure we need a loop here
# MAGIC for task in new_settings["tasks"]:
# MAGIC   tasklist=tasklist+[task]
# MAGIC
# MAGIC job_payload[skey]["tasks"]=tasklist

# COMMAND ----------

print(json.dumps(job_payload))
#print(job_payload[skey].keys())
json_file=f"../jobs/{job_name}_prd.json"

with open(json_file, "w") as f:
    json.dump(job_payload,f)

# COMMAND ----------

response = requests.post(f"{base_url_21_dev}jobs/reset", json=job_payload, headers=headers)
if not response.ok:
    print(response.text)



    
    


# COMMAND ----------

# MAGIC %md
# MAGIC curl --request GET "https://adb-5189663741904016.16.azuredatabricks.net/api/2.1/jobs/get" \
# MAGIC      --header "Authorization: Bearer ${DATABRICKS_TOKEN}" \
# MAGIC      --data '{ "job_id":104267624722607 }'
