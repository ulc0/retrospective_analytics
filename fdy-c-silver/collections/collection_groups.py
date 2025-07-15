# Databricks notebook source
fg_name="edav_prd_cdh.cdh_abfm_phi_exploratory.ml_feature_groups"
fgDict={"MPOX":
["INF006",
"INF007",
"INF010",
"FAC019",
"HIVEXPOSURE",
"STDEXPOSURE",
"STDSCREENING",
"HIVSCREENING",
"OTHERVIRUSSCREENING",]}

# COMMAND ----------

import pandas as pd
key="MPOX"
fg=pd.DataFrame(fgDict[key],columns=["feature_name"])
fg["feature_group"]=key
fgdf=spark.createDataFrame(fg)
display(fgdf)

# COMMAND ----------

fgdf.write.mode("overwrite").format("delta").saveAsTable(fg_name)

