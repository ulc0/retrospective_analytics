# Databricks notebook source
# MAGIC %md
# MAGIC # Set up Azure storage connection
# MAGIC spark.conf.set("fs.azure.account.auth.type.davsynapseanalyticsdev.dfs.core.windows.net", "OAuth")
# MAGIC spark.conf.set("fs.azure.account.oauth.provider.type.davsynapseanalyticsdev.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.id.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-client-id"))
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.secret.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-client-secret"))
# MAGIC spark.conf.set("fs.azure.account.oauth2.client.endpoint.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-tenant-id-endpoint"))

# COMMAND ----------

# MAGIC %md
# MAGIC configs = {"fs.azure.account.auth.type": "OAuth",
# MAGIC            "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
# MAGIC            "fs.azure.account.oauth2.client.id": dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-client-id"),
# MAGIC            "fs.azure.account.oauth2.client.secret": dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-client-secret"),
# MAGIC            "fs.azure.account.oauth2.client.endpoint":dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-tenant-id-endpoint") }

# COMMAND ----------

# DBTITLE 1,Mount Data Delivery
# MAGIC %md
# MAGIC cdh_mnt= '/mnt/cdh'
# MAGIC abfss_uri="abfss://cdh@davsynapseanalyticsdev.dfs.core.windows.net"
# MAGIC #if not any(mount.mountPoint == cdh_mnt for mount in dbutils.fs.mounts()):
# MAGIC #  try:
# MAGIC dbutils.fs.updateMount(
# MAGIC     source = abfss_uri,
# MAGIC     mount_point = cdh_mnt,
# MAGIC     extra_configs = configs )
# MAGIC #  except Exception as e:
# MAGIC #    print("already mounted. Try to unmount first")

# COMMAND ----------

# MAGIC %md
# MAGIC pwd
# MAGIC date -d @1681149639000
# MAGIC date -d @1655929734000

# COMMAND ----------

# MAGIC %sh
# MAGIC #initscripts="/Workspace/CDH/init_scripts/"
# MAGIC ls /
# MAGIC # abfss doesn't like shell commands, and it not recommended by databricks. Current best practice is use /Workspace
# MAGIC #cdhls=dbutils.fs.ls(cdh_mnt)
# MAGIC #ml=cdh_mnt+'/machinelearning'
# MAGIC #initscripts='/scripts'
# MAGIC #mlscripts=ml+initscripts
# MAGIC #dbutils.fs.mkdirs(mlscripts)
# MAGIC #cdhmlls=dbutils.fs.ls(mlscripts)
# MAGIC #print(cdhmlls)
# MAGIC
# MAGIC # Out[15]: True

# COMMAND ----------

# MAGIC %sh
# MAGIC cd ../compute/init_scripts
# MAGIC #cd /Workspace
# MAGIC ls -ll
# MAGIC cp  *.sh /Workspace/CDH/Analytics/init_scripts/
# MAGIC ls -ll /Workspace/CDH/Analytics/init_scripts/

# COMMAND ----------

# MAGIC %sh
# MAGIC cat /Workspace/CDH/Analytics/init_scripts/*.sh

# COMMAND ----------

# MAGIC %sh 
# MAGIC ls -ll /databricks/python3/bin
# MAGIC cat /databricks/python3/pyvenv.cfg
# MAGIC #rm /databricks/conda/LICENSE

# COMMAND ----------

# MAGIC %sh
# MAGIC #cd /Workspace/CDH/Analytics/init_scripts
# MAGIC #pip install ecos
# MAGIC #sh init_scikit_survival.sh
# MAGIC
