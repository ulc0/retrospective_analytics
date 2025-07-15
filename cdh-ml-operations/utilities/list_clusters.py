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

response = requests.get(f"{base_url_20}clusters/list", headers=headers)
if response.ok:
    #print(response.text)
    job_json=response.json()







# COMMAND ----------

import json
clusters=job_json["clusters"]
for j in clusters:
    cname=j['cluster_name']
    print(cname)
    if "CDH" in cname:
        jname=f"../compute/prd/{cname}.json"
        print(jname)
        with open(jname, 'w', encoding='utf-8') as f:
            json.dump(j,f, ensure_ascii=False, indent=4)
    if "job" in cname:
        jname=f"../compute/jobs/{cname}.json"
        print(jname)
        with open(jname, 'w', encoding='utf-8') as f:
            json.dump(j,f, ensure_ascii=False, indent=4)            
#with open("../compute/prd/clusters_list.json", 'w', encoding='utf-8') as f:
#    json.dump(job_json,f, ensure_ascii=False, indent=4)

# COMMAND ----------

#/api/2.0/libraries/all-cluster-statuses
response = requests.get(f"{base_url_20}libraries/all-cluster-statuses", headers=headers)
if response.ok:
    #print(response.text)
    job_json=response.json()

print(job_json)




# COMMAND ----------

ls="library_statuses"
lib_list=[]
s=job_json["statuses"]
for c in s:
    if ls in c.keys():    
        l=c["library_statuses"][0]
#        for lk in l:
#            print(lk)
        lib_list=lib_list+[l["library"]]
#    for ld in c["library_statuses"]:
#        print(ld)    


# COMMAND ----------

ll={}
for l in lib_list:
     for lk in l.keys():
        print(lk)
        print(l[lk])         

# COMMAND ----------

print(lib_list)
jname=f"../compute/package_list.json"
with open(jname, 'w', encoding='utf-8') as f:
    json.dump(lib_list,f, ensure_ascii=False, indent=4)     
