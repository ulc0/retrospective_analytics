# Databricks notebook source
import requests
import base64
import os
import json

token = 'dapi1cb78398751ca4117da978b1414f6196-3'

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
# MAGIC #curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
# MAGIC databricks -v

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks jobs configure --version=2.1
# MAGIC databricks jobs list --version=2.1 --output JSON --expand-tasks --all  > ../workflows/alljobs.json #--help
# MAGIC #databricks jobs get --job-id 588145818710408          > ../workflows/hamlet_text_removal.json

# COMMAND ----------

# MAGIC %sh
# MAGIC #0011495B5DDB6085
# MAGIC databricks jobs configure --version=2.1
# MAGIC databricks cluster-policies  list > ../workflows/cluster_policies.json #--help

# COMMAND ----------

xjname='../workflows/alljobs.json'

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC response = requests.get(f"{base_url_21}jobs/list", headers=headers)
# MAGIC if response.ok:
# MAGIC     #print(response.text)
# MAGIC     jobs=response.json()["jobs"]
# MAGIC     print(jobs)
# MAGIC #    with open(xjname, 'w', encoding='utf-8') as f:
# MAGIC #        json.dump(job_json,f, ensure_ascii=False, indent=4)
# MAGIC

# COMMAND ----------

with open(xjname, 'r') as f:
    jobs=json.load(f)["jobs"]
print(jobs)

# COMMAND ----------

"""
5   00176EE1B8F5BA62                      CDH_Job_Only_Cluster - ML CPU
6   0011495B5DDB6085                      CDH_Job_Only_Cluster - ML GPU
"""
ml_policies=["0011495B5DDB6085","6063781CC900251C",]
ml_policies=["00176EE1B8F5BA62","0011495B5DDB6085",]
run_as_user_names=['run9@cdc.gov','ulc0@cdc.gov','tqi6@cdc.gov',]

# COMMAND ----------

# MAGIC %md
# MAGIC "job_clusters": [
# MAGIC             {
# MAGIC                 "job_cluster_key": "CDH_ML_GPU_15_2",
# MAGIC                 "new_cluster": {
# MAGIC                     "cluster_name": "",
# MAGIC                     "spark_version": "15.2.x-gpu-ml-scala2.12",
# MAGIC                     "azure_attributes": {
# MAGIC                         "first_on_demand": 1,
# MAGIC                         "availability": "ON_DEMAND_AZURE",
# MAGIC                         "spot_bid_max_price": -1.0
# MAGIC                     },
# MAGIC                     "node_type_id": "Standard_NC12s_v3",
# MAGIC                     "custom_tags": {
# MAGIC                         "cluster_type": "center"
# MAGIC                     },
# MAGIC                     "enable_elastic_disk": true,
# MAGIC                     "policy_id": "0011495B5DDB6085",
# MAGIC                     "data_security_mode": "SINGLE_USER",
# MAGIC                     "runtime_engine": "STANDARD",
# MAGIC                     "autoscale": {
# MAGIC                         "min_workers": 2,
# MAGIC                         "max_workers": 8
# MAGIC                     }
# MAGIC                 }
# MAGIC             }
# MAGIC         ],

# COMMAND ----------

for j in jobs:
    #jsname=j["creator_user_name"]#['settings']['name']
    #print(jsname)
    jsettings=j["settings"]
#sloppy but it doesn't matter if we write out twice
    if 'job_clusters' in jsettings.keys():  #['s']=="ulc0@cdc.gov":
        jc=jsettings['job_clusters'][0]["new_cluster"]
        #print(jc)
        #print(type(jc))
        #print(jc.keys())
        if "policy_id" in jc.keys():
            if jc["policy_id"] in ml_policies:
                jj=jsettings["name"]
                print(j["run_as_user_name"])
                print(jj)
                jname=f"../workflows/{jj}.json"
                xjname=jname.replace(' ','_').replace('-','_')
                #print(xjname)
                with open(xjname, 'w', encoding='utf-8') as f:
                    json.dump(j,f, ensure_ascii=False, indent=4)

# COMMAND ----------

for j in jobs:
    settings= j["settings"]
    if 'run_as_user_name' in j.keys() and j['run_as_user_name'] in run_as_user_names:
        print(settings["name"])
        #print(settings.keys())
    if 'job_clusters' in settings.keys():
        for cluster in settings["job_clusters"]:
            if "new_cluster" in cluster.keys():
                jc=cluster["new_cluster"] 
                if "policy_id" in jc.keys() and jc["policy_id"] in ml_policies:
                    print(f"Policy: {jc['policy_id']} Job ID: {j['job_id']} Run As {j['run_as_user_name']}")

