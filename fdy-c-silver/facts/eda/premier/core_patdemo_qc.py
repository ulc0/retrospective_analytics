# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.davsynapseanalyticsdev.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.davsynapseanalyticsdev.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-client-id"))
spark.conf.set("fs.azure.account.oauth2.client.secret.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-client-secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-tenant-id-endpoint"))

# COMMAND ----------

# DBTITLE 1,l
# MAGIC %sql
# MAGIC use edav_prd_cdh.cdh_premier;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT i_o_ind,count(pat_key) as num_encounters,sum(cast((adm_mon<>disc_mon) as INTEGER)) as cross_month_encounters
# MAGIC from patdemo
# MAGIC group by i_o_ind
# MAGIC --where adm_mon<>disc_mon

# COMMAND ----------

# MAGIC %sql
# MAGIC select 

# COMMAND ----------

# MAGIC %sql
# MAGIC select i_o_ind,sequence,count(sequence) as visits,min(los), max(los), avg(los) from
# MAGIC (Select (cast(disc_mon_seq as INTEGER)) as sequence, los,i_o_ind
# MAGIC from patdemo)
# MAGIC where sequence>1
# MAGIC group by i_o_ind,sequence
# MAGIC order by i_o_ind,sequence

# COMMAND ----------

# MAGIC %sql
# MAGIC select p.icd_code,count(p.PAT_KEY) as num_codes
# MAGIC from edav_prd_cdh.cdh__prdedav_prd_cdh.cdh_.cdh_premier_v2.paticd_diag p join edav_prd_cdh.cdh_premier_v2.icdcode
# MAGIC group by p.ICD_CODE,ICD_DESC
# MAGIC order by num_codes desc
# MAGIC limit 1000
