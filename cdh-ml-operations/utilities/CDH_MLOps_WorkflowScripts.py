# Databricks notebook source
# MAGIC %pip install databricks-api

# COMMAND ----------

import os

# COMMAND ----------

token = dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-adf-access-token-v1")
os.environ["DATABRICKS_AAD_TOKEN"]=token

# COMMAND ----------

# MAGIC %sh
# MAGIC databricks workspace -h

# COMMAND ----------

# MAGIC %md
# MAGIC dbutils.widgets.text('data_source', '')
# MAGIC dbutils.widgets.text('project', '')
# MAGIC dbutils.widgets.text('database', '')
# MAGIC #dbutils.widgets.text('target_database', '')
# MAGIC dbutils.widgets.text('sqls_to_migrate', '')
# MAGIC dbutils.widgets.text('notebook_to_migrate', '')
# MAGIC dbutils.widgets.text('tables_to_migrate', '')
# MAGIC dbutils.widgets.dropdown("auto_refresh_data", "True", ["True", "False"])
# MAGIC dbutils.widgets.text("notify_on_auto_refresh", "")

# COMMAND ----------

# MAGIC %md
# MAGIC data_source = dbutils.widgets.get('data_source')
# MAGIC project = dbutils.widgets.get('project')
# MAGIC database = dbutils.widgets.get('database')
# MAGIC tables_to_migrate = dbutils.widgets.get('tables_to_migrate')
# MAGIC sqls_to_migrate = dbutils.widgets.get('sqls_to_migrate')
# MAGIC notebook_to_migrate = dbutils.widgets.get('notebook_to_migrate')
# MAGIC auto_refresh_data = dbutils.widgets.get('auto_refresh_data')
# MAGIC notify_on_auto_refresh = dbutils.widgets.get('notify_on_auto_refresh')
# MAGIC
# MAGIC source_database = database + "_exploratory"
# MAGIC target_database = database + "_vis"
# MAGIC
# MAGIC
# MAGIC sqls_to_migrate_array = [] if sqls_to_migrate is None or len(sqls_to_migrate) == 0 else sqls_to_migrate.split(',')
# MAGIC tables_to_migrate_array = [] if tables_to_migrate is None or len(tables_to_migrate) == 0 else tables_to_migrate.split(',')

# COMMAND ----------

# MAGIC %md ### Copy Init Scripts

# COMMAND ----------

data_source='ulc0@cdc.gov/cdh-ml-operations/utilities/copy_init_scripts.sh'



# COMMAND ----------

#TODO - validation
# notebook should not be a path - i.e /CDH/Visualizations/PointClickCare/Projects/vis-poc/dashboard-aggr-1 is not allowed
# notebook name should match

# COMMAND ----------

# MAGIC %md
# MAGIC curl --netrc --request GET \
# MAGIC   https://adb-1234567890123456.7.azuredatabricks.net/api/2.0/workspace/export \
# MAGIC   --header 'Accept: application/json' \
# MAGIC   --data '{ "path": f"{datasource}/init_alteryx.sh", "format": "AUTO", “direct_download”: true }'

# COMMAND ----------

job_payload = { "path": data_source, "format": "JUPYTER", "direct_download": "true" }
response = requests.get(f"{base_url}workspace/export", params=job_payload, headers=headers)
if not response.ok:
    print(response.text)

xform_response = response.text  
xform_response = xform_response.casefold().replace(source_database.casefold(),target_database.casefold())
#print(xform_response)

# job_payload = { "path": directory }
# response = requests.post(f"{base_url}/workspace/mkdirs", json =job_payload, headers=headers)
# if not response.ok:
 print(response.text)

message_bytes = xform_response.encode('utf-8')
base64_bytes = base64.encodebytes(message_bytes)
base64_message = base64_bytes.decode('utf-8')



# COMMAND ----------

notebook_path = f"/CDH/Visualizations/{data_source}/Projects/{project}/{notebook_to_migrate}"
curated_import_path = f"/Repos/{data_source}/{notebook_to_migrate}"
job_payload = { "path": notebook_path, "format": "JUPYTER", "direct_download": "true" }
response = requests.get(f"{base_url}workspace/export", params=job_payload, headers=headers)
if not response.ok:
    print(response.text)

xform_response = response.text  
xform_response = xform_response.casefold().replace(source_database.casefold(),target_database.casefold())
#print(xform_response)

# job_payload = { "path": directory }
# response = requests.post(f"{base_url}/workspace/mkdirs", json =job_payload, headers=headers)
# if not response.ok:
#     print(response.text)

message_bytes = xform_response.encode('utf-8')
base64_bytes = base64.encodebytes(message_bytes)
base64_message = base64_bytes.decode('utf-8')

job_payload = { "path": curated_import_path, "content": base64_message, "language": "python", "overwrite": "true", "format": "JUPYTER" }
response = requests.post(f"{base_url}workspace/import", json =job_payload, headers=headers)

if response.ok:
    dbutils.notebook.run(
            curated_import_path, 36000
        )
else:
    print(response.text)
    
    
    
    #/CDH/Visualizations/PointClickCare/Projects/vis-poc/dashboard-aggr-1

# COMMAND ----------

# MAGIC %md ### TODO - Depending on the auto_sync_on_source_refresh parameter, we add a flag for CDH 2.0 ETL to run this notebook when the primary datasource refreshes

# COMMAND ----------

auto_refresh = bool(auto_refresh_data)
if auto_refresh:
    print('auto refresh')
