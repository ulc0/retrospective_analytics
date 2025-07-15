# Databricks notebook source
# dictionary to control hcup, currently maintained in code, should be user maintained
# base_url=https://hcup-us.ahrq.gov/toolssoftware/ccsr/DXCCSR_v2023-1.zip
#C:\Users\ulc0\Downloads\DXCCSR_v2022-1.zip
# key is both address and .csv file name
base_url="https://hcup-us.ahrq.gov/toolssoftware/ccsr/"

# COMMAND ----------


dbutils.widgets.text("FEATURE","ccsr")
dbutils.widgets.text("VERSION","2023_1")
FEATURE=dbutils.widgets.get("FEATURE")
VERSION=dbutils.widgets.get("VERSION")



# COMMAND ----------

#releases=hcup[FEATURE]
#release=releases[VERSION]
cpath=f"/Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data/hcup/{FEATURE}/{VERSION}/"
print(cpath)

# COMMAND ----------

flist=[f.path for f in dbutils.fs.ls(cpath) if f.isFile() ]
print(flist)
csv=flist[-1]
print(csv)

# COMMAND ----------

hcup=spark.read.options(header=True, quote="'").csv(csv)
display(hcup)
