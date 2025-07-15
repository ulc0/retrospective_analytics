# Databricks notebook source
# MAGIC %md
# MAGIC Objective is to prep time series tables
# MAGIC - time for encounter
# MAGIC - time for events
# MAGIC - save as tables

# COMMAND ----------

# MAGIC %md
# MAGIC # Libraries

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.window as W
import numpy as np
import pandas as pd
# import matplotlib.pyplot as plt
# import scipy.stats as ana


# COMMAND ----------

# MAGIC %md
# MAGIC # Read Data

# COMMAND ----------

# icd_desc=spark.read.table("cdh_premier.icdcode")
icd_code=spark.table("cdh_premier.paticd_diag")
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

# MAGIC %md
# MAGIC ## create a subset

# COMMAND ----------

def get_sub(readmit):
    out = (readmit
        .select("medrec_key")
        .distinct()
        .sample(fraction = 0.000001)
    )
    return out

def get_sub_dtst(sub, readmit, patdemo):
    ids = [i[0] for i in sub.collect()]
    r_out = (readmit
            .filter(F.col("medrec_key").isin(*ids))
            )
    p_out = (patdemo
            .filter(F.col("medrec_key").isin(*ids))
            )
    # r_out = (sub
    #         .join(readmit, on="medrec_key", how="left")
    #         )
    # p_out = (sub
    #         .join(patdemo, on="medrec_key", how="left")
    #         )

    return r_out, p_out

# COMMAND ----------

sub = get_sub(readmit)
sub.cache()
print(sub.count())

r_sub, p_sub = get_sub_dtst(sub, readmit, patdemo)
print(r_sub.cache().count())
print(p_sub.cache().count())

# COMMAND ----------

# join pat encs
rvar = ["pat_key", "medrec_key", "disc_mon", "disc_mon_seq", "days_from_prior", "i_o_ind", "days_from_index", "calc_los"]
pvar = ["pat_key", "medrec_key", "disc_mon", "disc_mon_seq", "adm_mon", "i_o_ind", "los"]



def join_pat_encs(r_sub, p_sub, rvar, pvar):
    out = (r_sub
            .select(*rvar)
            .join(p_sub
                    # .select(*pvar)
                    .select("pat_key", "medrec_key", F.col("disc_mon").alias("disc_monp"),
                                # F.col("disc_mon_seq").alias("disc_mon_seqp"), 
                                F.col("adm_mon"), 
                                # F.col("i_o_ind").alias("i_o_indp"),
                                F.col("los")
                                ),
                    on = ["pat_key","medrec_key"], how = "left")
            .select("pat_key", "medrec_key", "disc_mon", "adm_mon", 
            # "disc_monp", 
            "disc_mon_seq", 
            # "disc_mon_seqp", 
            "days_from_prior", "days_from_index", "los", "calc_los")
            )
    
    return out  


# COMMAND ----------

jpr = join_pat_encs(r_sub, p_sub, rvar, pvar)

jpr.display()
# jpr.filter(F.col("i_o_ind") != F.col("i_o_indp")).display()


# COMMAND ----------

# convert dates
def to_numeric(dtst, toint, keep):
    out = (dtst
            .select(
                *keep,
                *[F.col(v).cast("int") for v in toint])
            )
    return out

def get_dates(dtst):
    idx = (dtst
            .filter((F.col("days_from_index") == 0))
            # this will take the first enc if multiple with dfi==0
            .withColumn("min_disc_mon_seq", F.min(F.col("disc_mon_seq")).over(W.Window.partitionBy(F.col("medrec_key"))))
            .filter(F.col("disc_mon_seq") == F.col("min_disc_mon_seq"))
            # get the date
            .withColumn("idx_adm_dt", 
                    F.concat(F.col("adm_mon").substr(1,4), 
                        F.lit("-"),
                        F.col("adm_mon").substr(6,7),
                        F.lit("-"),
                        F.lit("01")
                    )
                )
            .withColumn("idx_disc_dt", 
                    F.concat(F.col("disc_mon").substr(1,4), 
                        F.lit("-"),
                        F.col("disc_mon").substr(6,7),
                        F.lit("-"),
                        F.lit("01")
                    )
                )
            # set as date types
            .withColumn("idx_adm_dt", F.to_date(F.col("idx_adm_dt"), "yyyy-MM-dd"))
            .withColumn("idx_disc_dt", F.to_date(F.col("idx_disc_dt"), "yyyy-MM-dd"))
            # get month split
            .withColumn("idx_adm_mon", F.month(F.col("idx_adm_dt")))
            .withColumn("idx_disc_mon", F.month(F.col("idx_disc_dt")))
            # if months are same the set idx_disc_date to idx_disc_dt + calc_los and idx_adm_dt to idx_disc_dt
            # otherwise set idx_disc_date to idx_disc_dt and idx_adm_dt to idx_disc_dt - calc_los
            .withColumn("idx_set_type", F.when(
                F.col("idx_adm_mon") == F.col("idx_disc_mon"), 
            ))

            # adjust idx date by the calc los
            .withColumn("idx_disc_date", F.date_add(F.col("idx_adm_date"), F.col("calc_los")))
            .select("medrec_key", "idx_disc_date")
            # .withColumn("disc_dt", F.date_add(F.col("dt"), F.col("days_from_index")))
    )

    # out = (
    #     dtst
    #     .join(idx, on="medrec_key", how="left")
    #     .withColumn("adm_date", F.date_add(F.col("idx_date"), F.col("days_from_index")))
    # )
    return idx
    # return out

    

# COMMAND ----------

int_l = ["disc_mon_seq", "days_from_prior", "days_from_index", "los", "calc_los"]
keep = ["pat_key", "medrec_key", "disc_mon", "adm_mon"]
n_jrp = to_numeric(jpr, int_l, keep)
dts = get_dates(n_jrp)
display(dts)


# COMMAND ----------

[i[0] for i in sub.collect()]

# COMMAND ----------

r_sub.display()
