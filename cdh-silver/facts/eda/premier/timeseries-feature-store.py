# Databricks notebook source
orgId=spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")
print(orgId)
if orgId!='5189663741904016':
  spark.conf.set("fs.azure.account.auth.type.davsynapseanalyticsdev.dfs.core.windows.net", "OAuth")
  spark.conf.set("fs.azure.account.oauth.provider.type.davsynapseanalyticsdev.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
  spark.conf.set("fs.azure.account.oauth2.client.id.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-client-id"))
  spark.conf.set("fs.azure.account.oauth2.client.secret.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-client-secret"))
  spark.conf.set("fs.azure.account.oauth2.client.endpoint.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-tenant-id-endpoint"))


# COMMAND ----------

from databricks import feature_store
#from databricks.feature_store import feature_table, FeatureLookup
#from importlib.metadata import version 
#version('feature_store')
#feature_store.__version__


# COMMAND ----------

fs = feature_store.FeatureStoreClient()

# COMMAND ----------

silver_schema="edav_prd_cdh.cdh_premier_exploratory"
gold_schema="edav_prd_cdh.cdh_premier_gold"
pkeys=['person_id','vocabulary_id','episode_number']
tskeys=['episode_datetime','episode_number']

# COMMAND ----------

spark.sql(f"show tables from {silver_schema}")


# COMMAND ----------

tbl="fs_icd_encounters"
fsname=f"{gold_schema}.{tbl}"
tsdf=spark.sql(f"select * from {silver_schema}.{tbl}")



# COMMAND ----------

from pyspark.sql.functions import collect_list, size
dups=(tsdf
   .groupBy(tsdf.columns[1:])
   .agg(collect_list("person_id").alias("ids"))
   .where(size("ids") > 1))
print(dups)

# COMMAND ----------

ts=tsdf.schema
print(ts)

# COMMAND ----------

fs.create_table(name=fsname,primary_keys=pkeys,timestamp_keys=tskeys,schema=ts,
    description=f"premier_{tbl}")

# COMMAND ----------

fs.write_table(
    name=fsname,
 #   primary_keys=pkeys,
 #   timestamp_keys=tskeys,
    df=tsdf,
    mode="overwrite",
 #   schema=features_df.schema
)
