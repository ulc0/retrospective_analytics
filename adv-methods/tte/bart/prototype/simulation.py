# Databricks notebook source
# MAGIC %md
# MAGIC import mlflow as ml
# MAGIC ml.end_run()

# COMMAND ----------

#import sksurv as sks
#import sksurv.preprocessing
####import sksurv.metrics
#import sksurv.datasets
###import sksurv.linear_model
###import sksurv.ensemble

from pathlib import Path
#import arviz as az
#import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
#import sklearn as skl
#import scipy.stats as sp

#import pymc as pm
#import pymc_bart as pmb
import pandas as pd

import importlib
#import mlflow as ml
import shared.simsurv_func as ssf
#import subprocess
OUTPUTS = "outputs"
# LAMBDA = "np.exp(2 + 0.4*(x_mat[:,0] + x_mat[:,1]))"
TRAIN_CSV = "outputs/train.csv"
RBART_CSV = "outputs/rbart_surv.csv"

# T = 30
#run= ml.start_run(run_name="test6") 
#run_info = ml.active_run()

np.random.seed(99)
###########################################################################
# Simulate data





# COMMAND ----------

# MAGIC %md
# MAGIC """
# MAGIC sv_mat, x_mat, lmbda, a, tlat, cens, t_event, status, T = ssf.sim_surv(
# MAGIC                 N=N, 
# MAGIC                 # T=T,
# MAGIC                 x_vars=X_VARS,
# MAGIC                 a = ALPHA,
# MAGIC                 alpha_f = ALPHA_F,
# MAGIC                 lambda_f = LAMBDA,
# MAGIC                 cens_scale=CENS_SCALE,
# MAGIC                 cens_ind = CENS_IND,
# MAGIC                 err_ind = False)
# MAGIC ###
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC """
# MAGIC def sim_surv(N=100, 
# MAGIC             x_vars = 1, 
# MAGIC             lambda_f=None, 
# MAGIC             a=2, 
# MAGIC             alpha_f = None, 
# MAGIC             seed=999, 
# MAGIC             cens_ind = True,
# MAGIC             cens_scale = 20,
# MAGIC             err_ind = False):
# MAGIC     # np.random.seed(seed)
# MAGIC """

# COMMAND ----------

pX_VARS = 5
pALPHA = 3
pALPHA_F = "1 + (1.5 * x_mat[:,0]) + x_mat[:,1]"
pLAMBDA = "np.exp(1 + .2*x_mat[:,0] + .3*x_mat[:,1] + 0.8*np.sin(x_mat[:,0] * x_mat[:,1]) + np.power((x_mat[:,2] - 0.5),2))"
pCENS_SCALE = 60
pCENS_IND = False


dbutils.widgets.text("x_vars", "5")
#x_vars=dbutils.job.taskValues.get("X_VARS",X_VARS),
dbutils.widgets.text("x_vars","3")
dbutils.widgets.text("alpha_f", "1 + (1.5 * x_mat[:,0]) + x_mat[:,1]")
dbutils.widgets.text("lambda_f","np.exp(1 + .2*x_mat[:,0] + .3*x_mat[:,1] + 0.8*np.sin(x_mat[:,0] * x_mat[:,1]) + np.power((x_mat[:,2] - 0.5),2))")
dbutils.widgets.text("cens_scale","60")
dbutils.widgets.text("cens_ind","False")
dbutils.widgets.text("err_ind","False")


# COMMAND ----------

x_vars=eval(dbutils.widgets.get("x_vars"))
#x_vars=dbutils.job.taskValues.get("X_VARS",X_VARS),
a = eval(dbutils.widgets.get("x_vars"))
alpha_f = (dbutils.widgets.get("alpha_f"))
lambda_f = dbutils.widgets.get("lambda_f")
cens_scale=eval(dbutils.widgets.get("cens_scale"))
cens_ind =eval( dbutils.widgets.get("cens_ind"))
err_ind = eval(dbutils.widgets.get("err_ind"))


# COMMAND ----------

import scipy.stats as sp
N=500

x_mat = np.zeros((N, x_vars))
for x in np.arange(x_vars):
    x1 = sp.bernoulli.rvs(.5, size = N)
    x_mat[:,x] = x1
# calculate lambda
print(x_mat)
# set lambda
if lambda_f is None:
    lmbda = np.exp(2 + 0.3*(x_mat[:,0] + x_mat[:,1]) + x_mat[:,2])
else:
    lmbda = eval(lambda_f)

# set alpha if specified
if alpha_f is None:
    a = np.repeat(a, N)
else:
    a = eval(alpha_f)

# add error
if err_ind:
    error = sp.norm.rvs(0, .5, size = N)
    lmbda=lmbda + error


# get the event times
tlat = np.zeros(N)
for idx, l in enumerate(lmbda):
    # generate event times 
    unif = np.random.uniform(size=1)
    ev = lmbda[idx] * np.power((-1 * np.log(unif)), 1/a[idx])
    tlat[idx] = ev

if cens_ind:
    # censor
    cens = np.ceil(np.random.exponential(size = N, scale = cens_scale))

    # min cen and surv event
    t_event  = np.minimum(cens, np.ceil(tlat))
    status = (tlat <= cens) * 1
else:
    cens=np.zeros(N)
    t_event = np.ceil(tlat)
    status = np.ones(N)

# get max event time
#T = int(t_event.max())
# get time series
#t = np.linspace(0,T, T)
t = np.linspace(0,int(t_event.max()), int(t_event.max()))



# COMMAND ----------

# get surv curve true
sv_mat = np.zeros((N, t.shape[0]))
for idx, l in enumerate(lmbda):
    sv = np.exp(-1 * np.power((t/l), a[idx]))
    sv_mat[idx,:] = sv
    
print(sv_mat)   


# COMMAND ----------

#return sv_mat, x_mat, lmbda, a, tlat, cens, t_event, status, T
import mlflow as ml


# log param alpha
ml.log_param("alpha", alpha)
# log param labmda
ml.log_param("lambda", lambda_f)
# log param N
ml.log_param("N", n)
# log param T (# timepoint probabilites generated)
ml.log_param("T", t)
# log param X_VARS
ml.log_param("X_VARS", x_vars)
# log parm CENS_SCALE
ml.log_param("CENS_SCALE", cens_scale)
# log parm CENS_IND
ml.log_param("CENS_IND", cens_ind)
#####

def get_x_info(x_mat):
    x = np.unique(x_mat, axis=0, return_index=True, return_counts=True)
    x_out, x_idx, x_cnt = x[0], x[1], x[2]
    return x_out, x_idx, x_cnt

# log param x_info
x_out, x_idx, x_cnt = ssf.get_x_info(x_mat)
try:
    ml.log_param("X_INFO", str(list(zip(x_out, x_cnt))))
except:
    print("error")


# log metric cen percent calculated
# log metric status event calculated
def get_status_perc(status):
    out = status.sum()/status.shape[0]
    cens = 1-out
    return out, cens
event_calc, cens_calc = ssf.get_status_perc(status)
ml.log_metric("EVENT_PERC", event_calc)
ml.log_metric("CENS_PERC", cens_calc)

def get_event_time_metric(t_event):
    t_mean = t_event.mean()
    t_max = t_event.max()
    return t_mean, t_max
# log metric t_event mean
# log metric t_event max
t_mean, t_max = ssf.get_event_time_metric(t_event)
ml.log_metric("T_EVENT_MEAN", t_mean)
ml.log_metric("T_EVENT_MAX", t_max)

def get_train_matrix(x_mat, t_event, status):
    et = pd.DataFrame({"status": status, "time":t_event})
    train = pd.concat([et, pd.DataFrame(x_mat)],axis=1)
    return train
# log artif train dataset
train = ssf.get_train_matrix(x_mat, t_event, status)
# train.to_csv("outputs/train.csv")
train.to_csv(TRAIN_CSV)
ml.log_artifact("outputs/train.csv")


# log artif plot curves
title = "actual_survival"
ssf.plot_sv(x_mat, sv_mat, T, title=title, save = True, dir=OUTPUTS)
ml.log_artifact(f"{OUTPUTS}/{title}.png")


def get_y_sklearn(status, t_event):
    y = np.array(list(zip(np.array(status, dtype="bool"), t_event)), dtype=[("Status","?"),("Survival_in_days", "<f8")])
    return y

# get sklearn components
y_sk = ssf.get_y_sklearn(status, t_event)
x_sk = train.iloc[:,2:]
