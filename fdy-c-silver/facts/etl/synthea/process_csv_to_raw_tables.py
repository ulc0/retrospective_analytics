# Databricks notebook source


# COMMAND ----------

# MAGIC %md
# MAGIC https://github.com/synthetichealth/synthea/wiki/CSV-File-Data-Dictionary

# COMMAND ----------

dpath="/Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data/mitre_sample/"
schema='dev_cdh_ml_test'
catalog='edav_dev_cdh_test'
prefix='raw_synthea'
flist=['allergies.csv', 
'careplans.csv',
'claims.csv',
'claims_transactions.csv',
'conditions.csv',
'devices.csv',
'encounters.csv',
'imaging_studies.csv',
'immunizations.csv',
'medications.csv',
'observations.csv',
'organizations.csv',
'patients.csv','payer_transitions.csv','payers.csv','procedures.csv','providers.csv','supplies.csv',]

# COMMAND ----------

# MAGIC %md
# MAGIC ```
# MAGIC import json
# MAGIC sc={}
# MAGIC for tname in tlist:
# MAGIC     table=f"{catalog}.{schema}.{prefix}_{tname}"
# MAGIC     print(table)
# MAGIC     sdf=spark.table(table)
# MAGIC     #sdf.printSchema()
# MAGIC     sj=json.loads(sdf.schema.json())
# MAGIC     for meta in sj["fields"]:
# MAGIC         sc[meta["name"]]=meta["type"]
# MAGIC print(sc)
# MAGIC with open('synthea_dict.json','w') as d:
# MAGIC     json.dump(sc,d)
# MAGIC ```    

# COMMAND ----------

# MAGIC %md
# MAGIC /Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data/mitre_sample/claims.csv

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
