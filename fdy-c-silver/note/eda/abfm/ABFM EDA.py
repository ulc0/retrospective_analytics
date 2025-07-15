# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type.davsynapseanalyticsdev.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.davsynapseanalyticsdev.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-client-id"))
spark.conf.set("fs.azure.account.oauth2.client.secret.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-client-secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint.davsynapseanalyticsdev.dfs.core.windows.net", dbutils.secrets.get(scope="dbs-scope-CDH", key="apps-tenant-id-endpoint"))


# COMMAND ----------

schm="edav_prd_cdh.cdh_abfm_phi"
tblList=["patientnote",
#"patientnotemedication",
"patientnoteproblem",
#"patientnoteprocedure",
"patientnoteresultobservation",
"patientresultobservation",
#"patientnotevitalobservation",

]
fldList=[              
"patientuid",              
"sectionname",             
"encounterdate",         
"note",                   
"type",
"group1",
"group2",
"group3",
"group4",

]

# COMMAND ----------

# MAGIC %md
# MAGIC hftoken="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"

# COMMAND ----------

# MAGIC %md
# MAGIC from transformers import pipeline,AutoTokenizer, AutoModelForTokenClassification
# MAGIC
# MAGIC import os
# MAGIC import torch
# MAGIC print(torch.cuda.is_available())
# MAGIC device = 0 if torch.cuda.is_available() else -1
# MAGIC os.environ['TRANSFORMERS_CACHE'] = '/dbfs/hugging_face_transformers_cache/'
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC piitokenizer = AutoTokenizer.from_pretrained("bigcode/starpii", device=device,use_auth_token=hftoken)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC piimodel = AutoModelForTokenClassification.from_pretrained("bigcode/starpii", use_auth_token=hftoken)

# COMMAND ----------

# MAGIC %md
# MAGIC classifier = pipeline("token-classification",
# MAGIC                       model=piimodel, #"bigcode/starpii",
# MAGIC                       tokenizer=piitokenizer, 
# MAGIC                       device=device)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

for tbl in tblList:
    print(tbl)
    df=spark.table(f"{schm}.{tbl}").select(*fldList)
   # df.printSchema()
   # df.show(truncate=False, vertical=True)

# COMMAND ----------

df.sort(col("type").desc()).write.options(header='True',delimiter="\t").csv("./abfmnotes.txt") #show(vertical=True, truncate=False)
