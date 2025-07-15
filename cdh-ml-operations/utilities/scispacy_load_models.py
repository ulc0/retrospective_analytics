# Databricks notebook source
# MAGIC %sh
# MAGIC ls -ll /Volumes/edav_prd_cdh/cdh_ml/metadata_compute/packages/wheels
# MAGIC #pip install wget
# MAGIC #mkdir /dbfs/packages/sdist
# MAGIC #mkdir /dbfs/mnt/ml
# MAGIC #mkdir /dbfs/mnt/ml/scispacy_cache

# COMMAND ----------

dbutils.widgets.text("PATH",defaultValue="/Volumes/edav_prd_cdh/cdh_ml/metadata_compute/packages/wheels")
PATH=dbutils.widgets.get("PATH")
print(PATH)
dbutils.widgets.text("EXT",defaultValue="tar.gz")
EXT=dbutils.widgets.get("EXT")
print(EXT)
dbutils.widgets.text("VERSION",defaultValue="5.4")
VERSION=dbutils.widgets.get("VERSION")
print(VERSION)

##if (ext=="tar.gz"):
#    outdir="tgz"
#elif (ext=="whl"):
#    outdir="wheels"
#else: 
#    outdir=ext
outdir="wheels"
# COMMAND ----------


ext="tar.gz"
url=f"https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.{VERSION}/"
spacyUrl="https://github.com/explosion/spacy-models/releases/download/"

models=[ f"en_core_sci_scibert-0.{VERSION}.{ext}",
 f"en_core_sci_sm-0.{VERSION}.{ext}",
 f"en_core_sci_md-0.{VERSION}.{ext}",
 f"en_core_sci_lg-0.{VERSION}.{ext}",
 f"en_ner_bc5cdr_md-0.{VERSION}.{ext}",
 f"en_ner_craft_md-0.{VERSION}.{ext}",
 f"en_ner_bionlp13cg_md-0.{VERSION}.{ext}",
 f"en_ner_jnlpba_md-0.{VERSION}.{ext}",
]


spacyModels=[ 
    f"en_core_web_sm",
    f"en_core_web_md",
    f"en_core_web_lg",
    f"en_core_web_trf",
]
path=f"{PATH}/{outdir}/"
#fpath='/packages/downloads/'
#dpath=f"dbfs:{fpath}"
#cpath=f"/dbfs{fpath}"

print(models)

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /Volumes/edav_prd_cdh/cdh_ml/metadata_compute/packages/wheels
# MAGIC wget https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.4/en_core_sci_scibert-0.5.4.tar.gz

# COMMAND ----------

# MAGIC %md
# MAGIC import wget
# MAGIC
# MAGIC for fname in models:
# MAGIC         #print(fname+" already downloadsed")
# MAGIC         #print(fname+" downloading...")
# MAGIC         rurl = url + fname
# MAGIC         print(rurl)
# MAGIC         filename=wget.download(rurl)
# MAGIC         print(filename)
# MAGIC #        open(cpath+fname , 'wb').write(r.content)
# MAGIC #        dbutils.fs.cp(dpath+fname,path+fname)
# MAGIC #        dbutils.fs.rm(dpath+fname)
# MAGIC #        dbutils.fs.ls(dpath)

# COMMAND ----------

import os
os.environ["XDG_CACHE_HOME"]=f"{PATH}"   #"/Volumes/edav_prd_cdh/cdh_ml/metadata_compute/packages/wheels/"

# COMMAND ----------

import requests, os
for fname in models:
    #if(dbutils.fs.ls(path+fname)==path+fname):
        #print(fname+" already downloadsed")
    #else:
        print(fname+" downloading...")
        rurl = url + fname
        print(rurl)
        print(cpath)
        r = requests.get(rurl)
        open(cpath+fname , 'wb').write(r.content)
        dbutils.fs.cp(dpath+fname,path+fname)
        dbutils.fs.rm(dpath+fname)
        dbutils.fs.ls(dpath)


# COMMAND ----------

# MAGIC %md
# MAGIC mstring=["pip install "+m for m in models]
# MAGIC pipcmd="\n".join(mstring)
# MAGIC print(pipcmd)
# MAGIC open(path+"scispacy_models.sh",'w').write(pipcmd)
# MAGIC
# COMMAND ----------
# MAGIC %pip freeze


# COMMAND ----------

# MAGIC %md
# MAGIC cd /dbfs/packages/sdist
# MAGIC ls -llRh
# MAGIC #pip install en_core_sci_lg-0.5.1.{ext}
# MAGIC cat scispacy_models.sh
# MAGIC
# MAGIC
