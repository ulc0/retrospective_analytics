# Databricks notebook source
import requests
import base64
import os

###token = dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-adf-access-token-v1")
token="dapi8f3a8e88111d529e920174ac9a8e5017-3"
#base_url_21 = f"https://adb-8219004871211837.17.azuredatabricks.net/api/2.1/"
base_url_21 = f"https://adb-5189663741904016.16.azuredatabricks.net/api/2.1/"
base_url_20 = f"https://adb-8219004871211837.17.azuredatabricks.net/api/2.0/"
#base_url_20 = f"https://adb-5189663741904016.16.azuredatabricks.net/api/2.0/"
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

#### https://docs.databricks.com/api/azure/workspace/queries/get
response = requests.get(f"{base_url_20}preview/sql/queries", headers=headers)
if response.ok:
  #  print(response.text)
    job_json=response.json()

else:
    print(response.status_code)






# COMMAND ----------


#### 
import json
queries=job_json["results"]
for j in queries:
    cname=j['name']
    print(cname)
    if "CDH" in cname:
        print(cname)
        jname="".join(i for i in cname if i not in "\/:*?<>|").replace(" ","_")
        s=f"../queries/{jname}.json"
        print(jname)
        print(j["query"])
        with open(s, 'w', encoding='utf-8') as f:
            json.dump(j["query"],f, ensure_ascii=False, indent=4)
#    if "job" in cname:
#        jname=f"../compute/jobs/{cname}.json"
#        print(jname)
#        with open(jname, 'w', encoding='utf-8') as f:
#            json.dump(j,f, ensure_ascii=False, indent=4)            
#with open("../compute/prd/query_list.json", 'w', encoding='utf-8') as f:
#    json.dump(job_json,f, ensure_ascii=False, indent=4)

# COMMAND ----------

query_id="56f659c4-8e0c-422f-ad6b-771e2ee387a4"
#### https://docs.databricks.com/api/azure/workspace/queries/get
response = requests.get(f"{base_url_20}preview/sql/queries/{query_id}", headers=headers)
if response.ok:
  #  print(response.text)
    job_json=response.json()

else:
    print(response.status_code)


