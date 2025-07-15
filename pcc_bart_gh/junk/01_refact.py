# Databricks notebook source
import pyspark.sql.functions as F
import pyspark.sql.window as W
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import scipy.stats as ana

from pyspark.sql.functions import col, concat, expr, lit

# COMMAND ----------

import util.utilities as u


# COMMAND ----------

import mlflow

# COMMAND ----------



# COMMAND ----------

# icd_desc=spark.read.table("cdh_premier.icdcode")
icd_code=spark.read.table("cdh_premier.paticd_diag")
patcpt=spark.read.table("cdh_premier.patcpt")
patlabres=spark.read.table("cdh_premier.lab_res")
genlab=spark.read.table("cdh_premier.genlab")
# disstat=spark.read.table("cdh_premier.disstat")
# pattype=spark.read.table("cdh_premier.pattype")
patbill=spark.read.table("cdh_premier.patbill")
# providers=spark.read.table("cdh_premier.providers")
# ccsr_lookup = spark.table("cdh_reference_data.ccsr_codelist")

# COMMAND ----------

patdemo=spark.table("cdh_premier.patdemo")
readmit=spark.table("cdh_premier.readmit")

# COMMAND ----------

# genlab.display()

# COMMAND ----------

# patdemo.filter(F.col("pat_key")=="357354613").display()

# COMMAND ----------

# patdemo.filter(F.col("medrec_key") == "452574972").display()

# COMMAND ----------

KEY = "452574972"

# COMMAND ----------

readmit_s = (readmit
    .filter(F.col("medrec_key") == KEY)
    )

# readmit_s.display()

# COMMAND ----------

ENC = [row[0] for row in readmit_s.select("pat_key").collect()]

# COMMAND ----------

ENC

# COMMAND ----------

patdemo_s = (patdemo
    .filter(F.col("medrec_key") == KEY)
    ).cache()

icd_code=spark.read.table("cdh_premier.paticd_diag").filter(F.col("pat_key").isin(ENC)).cache()

patcpt=spark.read.table("cdh_premier.patcpt").filter(F.col("pat_key").isin(ENC)).cache()

patlabres=spark.read.table("cdh_premier.lab_res").filter(F.col("pat_key").isin(ENC)).cache()

genlab=spark.read.table("cdh_premier.genlab").filter(F.col("pat_key").isin(ENC)).cache()

patbill=spark.read.table("cdh_premier.patbill").filter(F.col("pat_key").isin(ENC)).cache()

readmit_s.cache() 

readmit_s.count()
patdemo_s.count()
icd_code.count()
patcpt.count()
patlabres.count()
genlab.count()
patbill.count()
    

# COMMAND ----------

print(readmit_s.count())
print(patdemo_s.count())
print(icd_code.count())
print(patcpt.count())
print(patlabres.count())
print(genlab.count())
print(patbill.count())

# COMMAND ----------

# check if readmit and patdemo fields are the same
# r_col = readmit_s.columns
# p_col = patdemo_s.columns
# s_c = [r for r in r_col if r in p_col]


# (readmit_s.select(*s_c)
# .join(patdemo_s.select(*s_c), on = "pat_key", how="left")).display()

# look correct to me

# COMMAND ----------

readmit = readmit_s.toPandas()
patdemo = patdemo_s.toPandas()
paticd = icd_code.toPandas()
patcpt = patcpt.toPandas()
patlabres = patlabres.toPandas()
patgenlab = genlab.toPandas()
patbill = patbill.toPandas()

# COMMAND ----------

import importlib
import datetime
import numpy as np

# COMMAND ----------

importlib.reload(u)
enc_data = u.premier_time_converter(patdemo, readmit)

# COMMAND ----------

# enc_data.get_enc_data()
# enc_data.index_row
# enc_data.set_adm_dt()
# np.array(enc_data.enc_data["dfi"], dtype="timedelta64[D]") + np.datetime64("2009-01-01")

print(enc_data.adm_dt)
print(enc_data.calc_los)
print(enc_data.disc_dt)

# COMMAND ----------

def get_adm_dt(ind_disc, dfi):
    adm = ind_disc + datetime.timedelta(dfi)
    return adm

pr["adm_dt"] = pr.apply(lambda x: get_adm_dt(x["ind_disc"], x["dfi"]), axis=1)

def get_disc_dt(adm_dt, los):
    disc = adm_dt + datetime.timedelta(los)
    return disc

pr["disc_dt"] = pr.apply(lambda x: get_disc_dt(x["adm_dt"], x["los"]), axis=1)


# COMMAND ----------

patgenlab[patgenlab["RESULT_DAY_NUMBER"].astype("int") > 0]
# patgenlab

# COMMAND ----------

pr["cal_dfp"] = pr["adm_dt"] - pr["disc_dt"].shift(1)

# COMMAND ----------

pr

# COMMAND ----------

# todo
# - add in the time placement for the other events
# - put this all in a class for processing

# COMMAND ----------

# get date
# r1["disc_mon"].str[0:4]
# pr_ind["disc_mon"].str[-2:].iat[0]
int("1")
