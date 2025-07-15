# Databricks notebook source
# MAGIC %pip install scikit-survival

# COMMAND ----------

import pyspark.sql.functions as F
import pyspark.sql.functions as W
from pyspark.ml.feature import StringIndexer
from pyspark.ml.feature import Bucketizer
import pandas as pd
import numpy as np
import sksurv as sks

# COMMAND ----------

# MAGIC %r
# MAGIC library(BART)
# MAGIC library(SparkR)
# MAGIC library(tidyverse)

# COMMAND ----------

# MAGIC %md
# MAGIC # KM curv on full dataset

# COMMAND ----------

full = spark.read.table("bart_pcc.end005_full")

# COMMAND ----------

# drop patients with prior
full2 = (
    full
    .filter(
        (F.col("p1_ccsr")==0 )&
        (F.col("p2_ccsr")==0)
               )
)
full2.display()

# COMMAND ----------

full2.count()

# COMMAND ----------


# Clean the dataset and adjust/coarsen time
def get_dtst_cln(dtst, xvar, coarse):
    out = (
        dtst
        .select("medrec_key", "tte_ccsr", "p3_ccsr_first", *xvar)
        .fillna(180, ["tte_ccsr"])
        .withColumn("tte_ccsr", F.col("tte_ccsr")-30)
        .withColumn("tte_ccsr", F.ceil(F.col("tte_ccsr")/coarse))
    )

    # age
    bucketizer = Bucketizer(splits=[ 17, 30, 40, 50, 66],inputCol="age", outputCol="age_bin")
    out = bucketizer.setHandleInvalid("keep").transform(out)
    age_dict = pd.DataFrame({0:"18-29", 1:"30-39", 2:"40-49", 3:"50-65"}, columns = ["id", "label"])
    out = (out
           .drop("age")
           .withColumn("age_bin", F.col("age_bin").cast("short"))
        )  
    
    # race
    model = StringIndexer(inputCol="raceeth", outputCol="race").fit(out)
    out = model.transform(out)
    out = out.withColumn("race", F.col("race").cast("short"))
    race_dict = out.select("raceeth","race").distinct().toPandas()
    out = out.drop("raceeth")

    # gender
    model = StringIndexer(inputCol="gender", outputCol="gendr").fit(out)
    out = model.transform(out)
    out = out.withColumn("gendr", F.col("gendr").cast("short"))
    gender_dict = out.select("gender","gendr").distinct().toPandas()
    out = out.drop("gender")

    return out, age_dict, race_dict, gender_dict

# COMMAND ----------

xvars =  "covid_patient", "age", "gender", "io_flag", "raceeth"
coarsen = 30 #coarsen to months

# Apply cleaning Train
train2, age_dict, race_dict, gender_dict = get_dtst_cln(full2, xvars, coarsen)

train2 = (train2
    .withColumn("tte_ccsr", F.col("tte_ccsr").cast("short"))
    .withColumn("p3_ccsr_first", F.col("p3_ccsr_first").cast("short"))
    .withColumn("covid_patient", F.col("covid_patient").cast("short"))
    .withColumn("io_flag", F.col("io_flag").cast("short"))
)
train2.display()

# COMMAND ----------

# get sk_y
# tt = np.array(train2.select("tte_ccsr").collect())
# delta = np.array(train2.select("p3_ccsr_first").collect())
# cov_pat = np.array(train2.select("covid_patient").collect())

# COMMAND ----------

# get sk_x
ALL_VARS2 = [
    "tte_ccsr",
    "p3_ccsr_first",
    "covid_patient", 
    "io_flag",
    "age_bin",
    "race",
    "gendr"]
sv_all = np.array(train2.select(*ALL_VARS2).collect())

# COMMAND ----------

uniq1, uniq_cnt = np.unique(sv_all, axis=0,return_counts=True)
print(uniq1.shape)
print(uniq_cnt)

# COMMAND ----------

# MAGIC %md
# MAGIC Only 1081 unique combinations of the ~3,000,000 full cohort

# COMMAND ----------

def get_y_sklearn(status, t_event):
    y = np.array(list(zip(np.array(status, dtype="bool"), t_event)), dtype=[("Status","?"),("Survival_in_days", "<f8")])
    return y

# COMMAND ----------

# get the sklearn y type
sv_y = get_y_sklearn(sv_all[:,1], sv_all[:,0])
sv_y2 = sv_y.reshape(sv_y.shape[0],1)

# COMMAND ----------

# sv_y = get_y_sklearn(delta, tt)
# sv_y2 = sv_y.reshape(sv_y.shape[0],1)

# COMMAND ----------

from sksurv.nonparametric import kaplan_meier_estimator
import matplotlib.pyplot as plt

# COMMAND ----------

# seperate the covid from non covid
y_ncov = sv_y2[sv_all[:,2]==0]
y_ncov=y_ncov.reshape(y_ncov.shape[0],)
# .reshape((sv_all[:,2]==0).shape[0],)
y_cov = sv_y2[sv_all[:,2]==1]
y_cov = y_cov.reshape(y_cov.shape[0],)

# .reshape((sv_all[:,2]==1).shape[0],)

# COMMAND ----------

# MAGIC %md
# MAGIC # KPM

# COMMAND ----------

t_f, sv_f, ci_f = kaplan_meier_estimator(
    sv_y["Status"], 
    sv_y["Survival_in_days"], 
    conf_type="log-log"
)

# COMMAND ----------

plt.step(t_f, sv_f, where="post")
plt.fill_between(t_f, ci_f[0], ci_f[1], alpha=0.25, step="post")
plt.ylim(.95, 1)
# plt.xlim(.5,6)
plt.ylabel("est. probability of survival $\hat{S}(t)$")
plt.xlabel("time $t$")

# COMMAND ----------

t_cv, sv_cv, ci_cv = kaplan_meier_estimator(
    y_cov["Status"], 
    y_cov["Survival_in_days"], 
    conf_type="log-log"
)

t_nc, sv_nc, ci_nc = kaplan_meier_estimator(
    y_ncov["Status"], 
    y_ncov["Survival_in_days"], 
    conf_type="log-log"
)

# COMMAND ----------

plt.step(t_cv, sv_cv, where="post", color="red", label="covid")
plt.step(t_nc, sv_nc, where="post", color="blue", label="noncovid")
plt.fill_between(t_cv, ci_cv[0], ci_cv[1], alpha=0.25, step="post", color="red")
plt.fill_between(t_nc, ci_nc[0], ci_nc[1], alpha=0.25, step="post", color="blue")
plt.ylim(.95, 1)
plt.legend()
plt.ylabel("est. probability of survival $\hat{S}(t)$")
plt.xlabel("time $t$")

# COMMAND ----------

# MAGIC %md
# MAGIC The KPM plots indicate that the covid patients have a slightly decreased of survival probability over the 5 month time series.

# COMMAND ----------

# MAGIC %md
# MAGIC # TRAIN TEST Split

# COMMAND ----------

def train_test_cov_ncov(dtst, cov_col=2, prop=None, dnsmp_ncov=None, seed = 99):
    np.random.seed(seed)

    #cv train test
    cv_n = int(dtst[dtst[:,cov_col]==1,:].shape[0]*prop)
    cv_msk = np.random.choice(dtst[dtst[:,cov_col]==1,:].shape[0], cv_n, replace=False)
    cv_test = dtst[dtst[:,cov_col]==1,:][cv_msk,:]
    cv_train = np.delete(dtst[dtst[:, cov_col]==1,:], cv_msk, axis=0)

    #ncvcv test
    # downsample
    if dnsmp_ncov is not None:
        dsmp_n = int(dtst[dtst[:,cov_col]==0,:].shape[0]*dnsmp_ncov)
        dsmp_msk = np.random.choice(dtst[dtst[:,cov_col]==0,:].shape[0], dsmp_n, replace=False)
        dtst = dtst[dtst[:,cov_col]==0,:][dsmp_msk,:]
    # split
    ncv_n = int(dtst[dtst[:,cov_col]==0,:].shape[0]*prop)
    ncv_msk = np.random.choice(dtst[dtst[:,cov_col]==0,:].shape[0], ncv_n, replace=False)
    ncv_test = dtst[dtst[:,cov_col]==0,:][ncv_msk,:]
    ncv_train = np.delete(dtst[dtst[:, cov_col]==0,:], ncv_msk, axis=0)

    train = np.vstack([cv_train, ncv_train])
    test = np.vstack([cv_test, ncv_test])

    return train, test

# apply
train, test = train_test_cov_ncov(sv_all, 2, prop=0.01, dnsmp_ncov=None)

print(train.shape)
print(test.shape)


# COMMAND ----------

print(f"train cov: {train[train[:,2]==1].shape[0]}")
print(f"train ncov: {train[train[:,2]==0].shape[0]}")
print(f"test cov: {test[test[:,2]==1].shape[0]}")
print(f"test ncov: {test[test[:,2]==0].shape[0]}")

# COMMAND ----------

ALL_VARS2
# 'tte_ccsr','p3_ccsr_first','covid_patient','io_flag','age_bin','race', 'gendr'

uni_test, uni_test_cnt = np.unique(test, axis=0, return_counts=True)
uni_train, uni_train_cnt = np.unique(train, axis=0, return_counts=True)


# COMMAND ----------

print(uni_test.shape)
print(uni_train.shape)
print(uni_test[uni_test[:,0]<5].shape)
print(uni_train[uni_train[:,0]<5].shape)

# COMMAND ----------

# MAGIC %md
# MAGIC # Cox analyses

# COMMAND ----------

train_y = get_y_sklearn(train[:,1], train[:,0])
test_y = get_y_sklearn(test[:,1], test[:,0])
# sv_y2 = train_y.reshape(train_y.shape[0],1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## SKSURV

# COMMAND ----------

import sksurv.linear_model
from sksurv.metrics import (
    concordance_index_censored,
    concordance_index_ipcw,
    cumulative_dynamic_auc,
    integrated_brier_score,
)

# COMMAND ----------

# cph = sksurv.linear_model.CoxPHSurvivalAnalysis().fit(sv_all[:,2:], sv_y)
train_cph = sksurv.linear_model.CoxPHSurvivalAnalysis().fit(train[:,2:], train_y)


# COMMAND ----------

np.exp(train_cph.coef_)

# COMMAND ----------

test_sv1 = train_cph.predict_survival_function(test[:,2:])
test_hz1 = train_cph.predict_cumulative_hazard_function(test[:,2:])
test_t = test_hz1[0].x
tshape = test_hz1[0].x.shape[0]
test_hz1 = np.array([fn(fn.x) for fn in test_hz1])

# COMMAND ----------

# MAGIC %md
# MAGIC ## C-index

# COMMAND ----------


idx_quant = [np.array(np.round(x),dtype="int") for x in (tshape*.25, tshape*.5, tshape*.75)]

# # empy array
test_cindx = np.zeros(shape=(len(idx_quant)))
# pb_cindx = np.zeros(shape=(len(idx_quant)))

# get c-index
for idx, i in enumerate(idx_quant):
    test_cindx[idx] = concordance_index_censored(
                event_indicator=test_y["Status"],
                event_time=test_y["Survival_in_days"],
                estimate=test_hz1[:,i]
            )[0]
    
test_cindx

# COMMAND ----------

# MAGIC %md
# MAGIC ## tt-AUC

# COMMAND ----------

test_rsk = train_cph.predict(test[:,2:])

# COMMAND ----------

print(test_rsk[msk_tst].shape)
test_y[msk_tst].shape

# COMMAND ----------

msk_trn = train_y["Survival_in_days"] < 5
msk_tst = test_y["Survival_in_days"] < 5
auc, mean_auc = cumulative_dynamic_auc(train_y[msk_trn], test_y[msk_tst], test_hz1[msk_tst,:3], test_t[:3])

# COMMAND ----------

mean_auc
auc

# COMMAND ----------

msk_trn = train_y["Survival_in_days"] < 5
msk_tst = test_y["Survival_in_days"] < 5
train_rsk = train_cph.predict(train[:,2:])
auc, mean_auc = cumulative_dynamic_auc(train_y[msk_trn], train_y[msk_trn], train_rsk[msk_trn], test_t[:3])

# COMMAND ----------

auc

# COMMAND ----------


concordance_index_censored(test_y["Status"], test_y["Survival_in_days"], estimate=test_hz1)

# COMMAND ----------

# MAGIC %md
# MAGIC ## lifelines

# COMMAND ----------

import lifelines
import pandas as pd

# COMMAND ----------

cphl = lifelines.CoxPHFitter().fit(pd.DataFrame(train, columns=ALL_VARS2), "tte_ccsr", "p3_ccsr_first")

# COMMAND ----------

cphl.print_summary()

# COMMAND ----------

cphl.plot_partial_effects_on_outcome("covid_patient", values=[0,1])

# COMMAND ----------

# MAGIC %md
# MAGIC # Sample dataset

# COMMAND ----------

# sample %
smp_n = int(sv_all.shape[0]*0.1)

# sample 1
sv_smp1 = sv_all[np.random.choice(sv_all.shape[0], smp_n, replace=False), :]

print(sv_smp1.shape)
uniq2, uniq_cnt2 = np.unique(sv_smp1, axis=0, return_counts=True)
print(uniq2.shape)
print(uniq1.shape)

# COMMAND ----------

cphl2 = lifelines.CoxPHFitter().fit(pd.DataFrame(sv_smp1, columns=ALL_VARS2), "tte_ccsr", "p3_ccsr_first")
cphl2.print_summary()

# COMMAND ----------

cphl2.plot_partial_effects_on_outcome("covid_patient", values=[0,1])

# COMMAND ----------

# sample %
smp_n = int(sv_all.shape[0]*0.05)

# sample 1
sv_smp1 = sv_all[np.random.choice(sv_all.shape[0], smp_n, replace=False), :]


print(sv_smp1.shape)
uniq2, uniq_cnt2 = np.unique(sv_smp1, axis=0, return_counts=True)
print(uniq2.shape)
print(uniq1.shape)

# COMMAND ----------

print(sv_smp1[sv_smp1[:,2]==0].shape[0])

# COMMAND ----------

cphl2 = lifelines.CoxPHFitter().fit(pd.DataFrame(sv_smp1, columns=ALL_VARS2), "tte_ccsr", "p3_ccsr_first")
cphl2.print_summary()

cphl2.plot_partial_effects_on_outcome("covid_patient", values=[0,1])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Downsample covid
# MAGIC works fine

# COMMAND ----------

# sample %
cv_n = int(sv_all[sv_all[:,2]==1,:].shape[0]*0.5)
cv_smp = sv_all[sv_all[:,2]==1,:][np.random.choice(sv_all[sv_all[:,2]==1,:].shape[0], cv_n, replace=False)]

ncv_n = int(sv_all[sv_all[:,2]==0,:].shape[0]*0.05)
ncv_smp = sv_all[sv_all[:,2]==0,:][np.random.choice(sv_all[sv_all[:,2]==0,:].shape[0], ncv_n, replace=False)]

print(cv_smp.shape[0], ncv_smp.shape[0])

smp3 = np.vstack([cv_smp, ncv_smp])
smp3.shape


# print(sv_smp1.shape)
# uniq2, uniq_cnt2 = np.unique(sv_smp1, axis=0, return_counts=True)
# print(uniq2.shape)
# print(uniq1.shape)

# COMMAND ----------

cphl3 = lifelines.CoxPHFitter().fit(pd.DataFrame(smp3, columns=ALL_VARS2), "tte_ccsr", "p3_ccsr_first")
cphl3.print_summary()

cphl3.plot_partial_effects_on_outcome("covid_patient", values=[0,1])

# COMMAND ----------

# MAGIC %md
# MAGIC ## downsample outcome
# MAGIC - does not work

# COMMAND ----------

# sample %
i_n = int(sv_all[sv_all[:,1]==1,:].shape[0]*0.8)
i_smp = sv_all[sv_all[:,1]==1,:][np.random.choice(sv_all[sv_all[:,1]==1,:].shape[0], i_n, replace=False)]

o_n = int(sv_all[sv_all[:,1]==0,:].shape[0]*0.01)
o_smp = sv_all[sv_all[:,1]==0,:][np.random.choice(sv_all[sv_all[:,1]==0,:].shape[0], o_n, replace=False)]

print(i_smp.shape[0], o_smp.shape[0])

smp4 = np.vstack([i_smp, o_smp])
smp4.shape


# COMMAND ----------

cphl4 = lifelines.CoxPHFitter().fit(pd.DataFrame(smp4, columns=ALL_VARS2), "tte_ccsr", "p3_ccsr_first")
cphl4.print_summary()

cphl4.plot_partial_effects_on_outcome("covid_patient", values=[0,1])
