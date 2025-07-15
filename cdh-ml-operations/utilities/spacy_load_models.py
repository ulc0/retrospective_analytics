# Databricks notebook source
# MAGIC %sh
# MAGIC #mkdir /dbfs/packages/sdist
# MAGIC #mkdir /dbfs/mnt/ml
# MAGIC #mkdir /dbfs/mnt/ml/scispacy_cache

# COMMAND ----------
# MAGIC %pip freeze
# COMMAND ----------
dbutils.widgets.text("PATH",defaultValue="/Volumes/edav_dev_cdh/cdh_ml/metadata_compute/packages/wheels/")
PATH=dbutils.widgets.get("PATH")
print(PATH)
dbutils.widgets.text("VERSION",defaultValue="3.8.0")
VERSION=dbutils.widgets.get("VERSION")
print(VERSION)
#https://github.com/explosion/spacy-models/releases/download/en_core_web_sm-3.8.0/en_core_web_sm-3.8.0.tar.gz
url="https://github.com/explosion/spacy-models/releases/download/"

# COMMAND ----------

models=[ 
    f"en_core_web_sm",
    f"en_core_web_md",
    f"en_core_web_lg",
    f"en_core_web_trf",
]
# https://github.com/explosion/spacy-models/releases/download/en_core_web_sm-3.8.0/en_core_web_sm-3.8.0.tar.gz
"""
fpath='/packages/downloads/'
dpath=f"dbfs:{fpath}"
cpath=f"/dbfs{fpath}"
dbutils.fs.mkdirs(dpath)
"""
# COMMAND ----------


import requests, os
for model in models:
    fname=f"{model}-{VERSION}.tar.gz"
    fpath=f"{model}-{VERSION}/"
#    if not (os.path.isfile(path+fname)):
    print(fname+" downloading...")
    rurl = url + fpath+fname
    print(rurl)
    r = requests.get(rurl)
    open(PATH+fname , 'wb').write(r.content)
#    dbutils.fs.cp(dpath+fname,path+fname)
#    dbutils.fs.rm(dpath+fname)
#dbutils.fs.ls(dpath)
dbutils.fs.ls(PATH)




# COMMAND ----------

# MAGIC %md
# MAGIC ls /dbfs/packages/downloads

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC cp /dbfs/packages/downloads/en_core_web_lg-3.6.0-py3-none-any.whl /Volumes/edav_dev_cdh_test/dev_cdh_ml_test/compute/packages/wheels/en_core_web_lg-3.6.0-py3-none-any.whl

# COMMAND ----------

# MAGIC %md
# MAGIC cd /dbfs/packages/sdist
# MAGIC ls -llRh
# MAGIC #pip install en_core_sci_lg-0.5.1.tar.gz
# MAGIC cat scispacy_models.sh
# MAGIC
# MAGIC
