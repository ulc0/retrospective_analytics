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

endpoint="libraries/cluster-status"
endpoint="libraries/all-cluster-statuses"


# COMMAND ----------

cluster_id="1205-155834-nbaeyfbx"

# COMMAND ----------

response = requests.get(f"{base_url}{endpoint}", headers=headers)
cluster_list=response.json()
print(cluster_list)

# COMMAND ----------

dbutils.widgets.text("cluster_id", "0630-181430-yfox4zns")
cluster_id=dbutils.widgets.get("cluster_id")

# COMMAND ----------

cluster_params={"cluster_id":f"{cluster_id}"}
response = requests.get(f"{base_url}clusters/get",params=cluster_params, headers=headers)
cluster_info=response.json()
print(response.text)

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

is_pymc= [
 "/CDH/init_scripts/init_mlflow.sh",
 "/CDH/init_scripts/init_pymc_bart.sh",
]
is_spacy= [
 "/CDH/init_scripts/init_mlflow.sh",
 "/CDH/init_scripts/init_scispacy.sh",
 "/CDH/init_scripts/init_sparknlp.sh",
]
is_other_nlp= [
 "/CDH/init_scripts/init_mlflow.sh",
 "/CDH/init_scripts/init_other_nlp.sh",
]
is_survival=[
     "/CDH/init_scripts/init_lifelines.sh",
      "/CDH/init_scripts/init_mlflow.sh",
]
is_nlp=list(set(is_spacy+is_other_nlp))
is_bart=list(set(is_pymc+is_survival))

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

l_init_scripts=list(set(is_bart+is_nlp))
print(l_init_scripts)
#  o={ "workspace" : { "destination" : "/Users/user1@databricks.com/my-init.sh" } }
lis=[]
d={"destination":"blank_init"}
for l in l_init_scripts:
    d["destination"]=l
    d["Workspace"]=d
    lis=lis+[d]
print(lis)
#o["cluster_id"]=cluster_id
#response = requests.post(f"{base_url}clusters/edit", json=o, headers=headers)
#cluster_info=response.json()
#print(response.text)

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
