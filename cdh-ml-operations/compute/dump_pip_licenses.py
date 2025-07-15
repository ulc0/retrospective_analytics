# Databricks notebook source
# MAGIC %sh
# MAGIC pip-licenses --with-system --format=json --output-file=/Volumes/edav_prd_cdh/cdh_ml/metadata_compute/pip_license.json
# MAGIC pip-licenses --with-system --format=json-license-finder --output-file=/Volumes/edav_prd_cdh/cdh_ml/metadata_compute/pip_license_finder.json
# MAGIC

# COMMAND ----------

import json
import pandas as pd
import os

# COMMAND ----------

#TODO parameterize with cluster configurations
vol=f"/Volumes/edav_prd_cdh/cdh_ml/metadata_compute"
cluster="_std"

# COMMAND ----------


os.environ["VOL"]=vol
os.environ["CLUSTER"]=cluster

# COMMAND ----------

fns=[f"{vol}/pip_license.json",] #f"{vol}/pip_license_finder.json",]
pkgs=[]
#for fn in fns:
with open(fns[0],'r') as js:
    d=json.load(js)
#print(d)
#    pkgs.append(d)
#print(pkgs)

# COMMAND ----------

with open(f"licenses{cluster}.json","w") as fp:
    json.dump(d,fp)

# COMMAND ----------

print(d)

# COMMAND ----------


licenses=pd.DataFrame(d)
display(licenses)
licenses.to_excel(f"licenses{cluster}.xlsx")

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -ll license*
# MAGIC cp licenses$CLUSTER.xlsx $VOL
# MAGIC cp licenses$CLUSTER.json $VOL
