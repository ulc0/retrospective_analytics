# Databricks notebook source
import requests
import base64
import os
import json

token = dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-adf-access-token-v1")
base_url = f"https://adb-8219004871211837.17.azuredatabricks.net/api/2.0/"
base_url = f"https://adb-5189663741904016.16.azuredatabricks.net/api/2.0/"

headers={"Authorization": f"Bearer {token}"}
os.environ["DATABRICKS_TOKEN"]=token
os.environ["DATABRICKS_HOST"]=base_url

# COMMAND ----------

response = requests.get(f"{base_url}clusters/list", headers=headers)
cluster_list=response.json()


# COMMAND ----------

#print(json.dumps(cluster_list, indent=4, sort_keys=False))
with open('../compute/cluster_list.json', 'w') as fp:
    json.dump(cluster_list, fp, indent=4, sort_keys=False)

# COMMAND ----------

dbutils.widgets.text("cluster_id","0630-181430-yfox4zns")
cluster_id=dbutils.widgets.get("cluster_id")

# COMMAND ----------

cluster_params={"cluster_id":f"{cluster_id}"}
response = requests.get(f"{base_url}clusters/get",params=cluster_params, headers=headers)
cluster_info=response.json()
print(response.text)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ```    "spark_conf": {
# MAGIC         "fs.azure.account.oauth2.client.secret": "{{secrets/dbs-scope-CDH/cdh-adb-client-secret}}",
# MAGIC         "spark.databricks.delta.preview.enabled": "true",
# MAGIC         "fs.azure.account.oauth2.client.endpoint": "{{secrets/dbs-scope-CDH/cdh-adb-tenant-id-endpoint}}",
# MAGIC         "spark.databricks.queryWatchdog.enabled": "true",
# MAGIC         "spark.databricks.adaptive.autoOptimizeShuffle.enabled": "true",
# MAGIC         "fs.azure.account.oauth2.client.id": "{{secrets/dbs-scope-CDH/cdh-adb-client-id}}",
# MAGIC         "spark.driver.maxResultSize": "16g",
# MAGIC         "spark.databricks.queryWatchdog.outputRatioThreshold": "1000",
# MAGIC         "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
# MAGIC         "fs.azure.account.auth.type": "OAuth"
# MAGIC     },
# MAGIC

# COMMAND ----------

env_vars_hf={"spark_env_vars": {
        "HF_HUB_DISABLE_PROGRESS_BARS": "1",
        "TRANSFORMERS_CACHE": "/dbfs/mnt/ml/transformers/cache/",
        "MLFLOW_TRACKING_URI": "databricks",
        "HUGGINGFACE_HUB_CACHE": "/dbfs/mnt/ml/huggingface/cache/"}}
env_vars_scispacy={"spark_env_vars": {
        "SCISPACY_CACHE": "/dbfs/mnt/ml/scispacy_cache/",
        "MLFLOW_TRACKING_URI": "databricks",
}}
env_vars_pt={"spark_env_vars": {
        "PYTENSOR_FLAGS": "'allow_gc=False,floatX=float64'",
        "MLFLOW_TRACKING_URI": "databricks",
}}
             

# COMMAND ----------

# MAGIC %sh
# MAGIC pwd
# MAGIC ls ../compute

# COMMAND ----------

cluster_name=cluster_info["cluster_name"]
print(cluster_name)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC cluster_id=cluster_params["cluster_id"]
# MAGIC #cluster_id="0616-211319-eyelok6a"
# MAGIC cluster_name="CDH_Cluster_Python_SQL_AD_ML_GPU"
# MAGIC response = requests.get(f"{base_url}clusters/get", params=cluster_params, headers=headers)
# MAGIC cluster_info=response.json()
# MAGIC print(response.text)

# COMMAND ----------

# MAGIC %md
# MAGIC response = requests.post(f"{base_url}clusters/delete", json=cluster_params, headers=headers)
# MAGIC cluster_info=response.json()
# MAGIC print(response.text)

# COMMAND ----------

spark_conf= { 
    "fs.azure.account.oauth2.client.secret": "{{secrets/dbs-scope-CDH/cdh-adb-client-secret}}",
    "spark.databricks.adaptive.autoOptimizeShuffle.enabled": "true",
    "spark.databricks.queryWatchdog.enabled": "true",
    "fs.azure.account.auth.type": "OAuth",
    "spark.driver.maxResultSize": "16g",
    "spark.databricks.delta.preview.enabled": "true",
    "fs.azure.account.oauth2.client.endpoint": "{{secrets/dbs-scope-CDH/cdh-adb-tenant-id-endpoint}}",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "spark.databricks.queryWatchdog.outputRatioThreshold": "1000",
    "fs.azure.account.oauth2.client.id": "{{secrets/dbs-scope-CDH/cdh-adb-client-id}}"
}
#print(spark_conf)
json.dumps(spark_conf)

# COMMAND ----------

o=cluster_info
### Put new values here
o["spark_conf"]=spark_conf
print(o)
response = requests.post(f"{base_url}clusters/edit", json=o, headers=headers)
#cluster_info=response.json()
print(response.text)

# COMMAND ----------

# MAGIC %md
# MAGIC response = requests.post(f"{base_url}clusters/start", json=cluster_params, headers=headers)
# MAGIC cluster_info=response.json()
# MAGIC ##response = requests.get(f"{base_url}permissions/cluster/{new_cluster}", json=new_cluster, headers=headers)
# MAGIC ##new_cluster=response.json()
# MAGIC print(response.text)

# COMMAND ----------

# MAGIC %md
# MAGIC response = requests.get(f"{base_url}permissions/clusters/{cluster_id}", headers=headers)
# MAGIC #cl=json.loads(response.text)
# MAGIC permissions=response.json()
# MAGIC print(permissions)
# MAGIC #print(cl)
# MAGIC #for c in cl:
# MAGIC #  print(c) #["cluster_id"]+" "+c["cluster_name"])
# MAGIC
# MAGIC
# MAGIC
# MAGIC     
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC u=   {
# MAGIC       "user_name": "ulc0@cdc.gov",
# MAGIC      # "group_name": "string",
# MAGIC      # "service_principal_name": "string",
# MAGIC       "permission_level": "IS_OWNER"
# MAGIC     }
# MAGIC
# MAGIC
# MAGIC #print(u)
# MAGIC #u[0]["user_name"]="ulc0@cdc.gov"
# MAGIC #u["display_name"]='Belisle, Kate (CDC/DDPHSS/CSELS/DHIS (CTR))'
# MAGIC #u[0]["permission_level"]="CAN_MANAGE"
# MAGIC
# MAGIC #print(u)
# MAGIC
# MAGIC acl=permissions["access_control_list"]+[u]
# MAGIC print(acl)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC acltext="access_control_list"
# MAGIC response = requests.put(f"{base_url}permissions/clusters/{cluster_id}", params={acltext:u}, headers=headers)
# MAGIC #cl=json.loads(response.text)
# MAGIC npers=response.json()
# MAGIC print(npers)
# MAGIC #print(cl)
# MAGIC #for c in cl:
# MAGIC #  print(c) #["cluster_id"]+" "+c["cluster_name"])

# COMMAND ----------

# MAGIC %md
# MAGIC curl --request GET "https://adb-5189663741904016.16.azuredatabricks.net/api/2.0/clusters/get" \
# MAGIC      --header "Authorization: Bearer ${DATABRICKS_TOKEN}" \
# MAGIC      --data '{ "cluster_id":"0616-211319-eyelok6a" }'
