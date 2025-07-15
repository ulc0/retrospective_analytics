from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark)

def set_secret_scope():
  orgId=spark.conf.get("spark.databricks.clusterUsageTags.clusterOwnerOrgId")
  print(orgId)
  if orgId!='5189663741904016':
    spark.conf.set("fs.azure.account.auth.type.davsynapseanalyticsdev.dfs.core.windows.net", "OAuth")
    spark.conf.set("fs.azure.account.oauth.provider.type.davsynapseanalyticsdev.dfs.core.windows.net", 
                   "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
    spark.conf.set("fs.azure.account.oauth2.client.id.davsynapseanalyticsdev.dfs.core.windows.net",
                   dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-client-id"))
    spark.conf.set("fs.azure.account.oauth2.client.secret.davsynapseanalyticsdev.dfs.core.windows.net", )
    spark.conf.set("fs.azure.account.oauth2.client.endpoint.davsynapseanalyticsdev.dfs.core.windows.net",
                   dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-tenant-id-endpoint"))
  else:
      spark.conf.set("fs.azure.account.auth.type", "OAuth")
      spark.conf.set("fs.azure.account.oauth.provider.type",
                    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
      spark.conf.set("fs.azure.account.oauth2.client.id",  dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-client-id"))
      spark.conf.set("fs.azure.account.oauth2.client.secret", dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-client-secret"))
      spark.conf.set("fs.azure.account.oauth2.client.endpoint", dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-tenant-id-endpoint"))

   