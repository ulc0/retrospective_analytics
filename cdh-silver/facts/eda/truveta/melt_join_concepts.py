# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC https://github.com/synthetichealth/synthea/wiki/CSV-File-Data-Dictionary

# COMMAND ----------

dpath="/Volumes/edav_prd_dh/cdh_ml/data"
schema='cdh_ml'
catalog='edav_dev_cdh'

tlist=spark.sql(f"show tables {catalog}.{schema}")
# COMMAND ----------

import json
sc={}
print(flist)
for fn in flist:
    tname=fn.split('.')[0]
    fname=dpath+fn
    print(fname)
    table=f"{catalog}.{schema}.{prefix}_{tname}"
    print(table)
    sdf=spark.read.option("header", True).csv(fname)
    sj=json.loads(sdf.schema.json())
    for meta in sj["fields"]:
        sc[meta["name"]]=meta["type"]
    sdf.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(table)
#print(sc)
with open('synthea_dict.json','w') as d:
    json.dump(sc,d)
