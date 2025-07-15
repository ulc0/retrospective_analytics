# Databricks notebook source
# MAGIC %sh
# MAGIC #mkdir /dbfs/packages/sdist
# MAGIC #mkdir /dbfs/mnt/ml
# MAGIC #mkdir /dbfs/mnt/ml/scispacy_cache

# COMMAND ----------

dbutils.widgets.text("DEST",defaultValue="/Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data/spatial/")
dest=dbutils.widgets.get("DEST")
print(dest)
ddest=f"dbfs:/"
#ddest="/dbfs/tmp"
cdest=f"dbfs:/{dest}"


# COMMAND ----------

from shared.stateabbr import us_state_to_abbrev,abbrev_to_us_state 

# COMMAND ----------

#https://github.com/OpenDataDE/State-zip-code-GeoJSON
#https://github.com/OpenDataDE/State-zip-code-GeoJSON/raw/master/ak_alaska_zip_codes_geo.min.json
#geojson_url=f"https://github.com/OpenDataDE/State-zip-code-GeoJSON/raw/master/{state_abbr}_alaska_zip_codes_geo.min.json"
print(abbrev_to_us_state)

# COMMAND ----------

ver="5.3"
state_abbr=""
state_name=""
url=f"https://github.com/OpenDataDE/State-zip-code-GeoJSON/raw/master/"


# COMMAND ----------

import requests, os
for state in abbrev_to_us_state.keys():
    state_abbr=state.lower()
    state_name=abbrev_to_us_state[state].lower()
    fname=f"{state_abbr}_{state_name}_zip_codes_geo.min.json"
    print(fname+" downloading...")
    rurl = url + fname
    print(rurl)
    r = requests.get(rurl)
    geojson=r.json
    print(geojson)
    #open(ddest+fname , 'wb').write(r.content)
    #dbutils.fs.cp(ddest+fname,dest+fname)
    #dbutils.fs.rm(ddest+fname)
#dbutils.fs.ls(ddest)

