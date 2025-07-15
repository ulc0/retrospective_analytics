# Databricks notebook source
# MAGIC %md
# MAGIC Identify 2 proprotional, 2 non proportional and 2 complex models to test

# COMMAND ----------

# MAGIC %pip install scikit-survival

# COMMAND ----------

#%pip install arviz

# COMMAND ----------

#%pip install pymc

# COMMAND ----------

#%pip install pymc-bart

# COMMAND ----------

#%pip install matplotlib

# COMMAND ----------

# import sksurv as sks
# import sksurv.preprocessing
# import sksurv.metrics
# import sksurv.datasets
# import sksurv.linear_model
# import sksurv.ensemble

# from pathlib import Path
# import matplotlib
# # import arviz as az
# import matplotlib.pyplot as plt
# import numpy as np
# import pandas as pd
# import numpy as np
# import sklearn as skl
# import scipy.stats as sp

# import pymc as pm
# import pymc_bart as pmb
# import pandas as pd

# import importlib
# import mlflow as ml
# import simsurv_func as ssf
# import subprocess

# COMMAND ----------

# minimal packages for simulation
import simsurv_func as ssf
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

import sksurv as sks
import sksurv.preprocessing
import sksurv.metrics
import sksurv.datasets
import sksurv.linear_model
import sksurv.ensemble

# COMMAND ----------

plt.ioff()
np.random.seed(99)

# COMMAND ----------

# MAGIC %md
# MAGIC # Test Run For Kate

# COMMAND ----------

OUTPUTS = "outputs"
ALPHA = 2
LAMBDA = "np.exp(2 + 0.4*(x_mat[:,0] + x_mat[:,1]))"
TRAIN_CSV = "outputs/train.csv"
RBART_CSV = "outputs/rbart_surv.csv"
N = 100
# T = 30
X_VARS = 2
CENS_SCALE = 5 # 40
CENS_IND = False

sv_mat, x_mat, lmbda, a, tlat, cens, t_event, status, T = ssf.sim_surv(
                N=N, 
                # T=T,
                x_vars=X_VARS,
                a = ALPHA,
                lambda_f = LAMBDA,
                cens_scale=CENS_SCALE,
                cens_ind = CENS_IND,
                err_ind = False)


x_out, x_idx, x_cnt = ssf.get_x_info(x_mat)
event_calc, cens_calc = ssf.get_status_perc(status)
t_mean, t_max = ssf.get_event_time_metric(t_event)
train = ssf.get_train_matrix(x_mat, t_event, status)
title = "actual_survival"
ssf.plot_sv(x_mat, sv_mat, T, title=title, save = False, show=True, dir=OUTPUTS)

print(f"cens_calc: {cens_calc} \
    \nt_mean: {t_mean} \
    \nt_max: {t_max}")


# COMMAND ----------

    y_sk = ssf.get_y_sklearn(status, t_event)
    x_sk = train.iloc[:,2:]

# COMMAND ----------

cph = sksurv.linear_model.CoxPHSurvivalAnalysis()
cph.fit(x_sk, y_sk)

cph_surv = cph.predict_survival_function(pd.DataFrame(x_out))

cph_sv_t = cph_surv[0].x
cph_sv_val = [sf(cph_sv_t) for sf in cph_surv]
cph_sv_t = np.concatenate([np.array([0]), cph_sv_t])
cph_sv_val = [np.concatenate([np.array([1]), sv]) for sv in cph_sv_val]

# log artif plot curves
title = "cph_surv_pred"
ssf.plot_sv(x_mat, cph_sv_val, t=cph_sv_t, title = title, save=False, show=True, dir="outputs")

# COMMAND ----------

    ###################################################################################
    #  model rsf
    rsf = sksurv.ensemble.RandomSurvivalForest(
        n_estimators=1000, min_samples_split=100, min_samples_leaf=15, n_jobs=-1, random_state=20
    )
    rsf.fit(x_sk, y_sk)
    # predict rsf
    rsf_surv = rsf.predict_survival_function(pd.DataFrame(x_out))
    # get plotable predictions
    # rsf_sv_val = [sf(np.arange(T)) for sf in rsf_surv]
    rsf_sv_t = rsf_surv[0].x
    rsf_sv_val = [sf(rsf_sv_t) for sf in rsf_surv]
    rsf_sv_t = np.concatenate([np.array([0]), rsf_sv_t])
    rsf_sv_val = [np.concatenate([np.array([1]), sv]) for sv in rsf_sv_val]
    # log artif plot curves
    title = "rsf_surv_pred"
    ssf.plot_sv(x_mat, rsf_sv_val, t=rsf_sv_t, title=title, save=False, show=True, dir="outputs")

# COMMAND ----------

# MAGIC %md
# MAGIC # Actual Trials

# COMMAND ----------

# Proportional
OUTPUTS = "outputs"
ALPHA = 2
LAMBDA = "np.exp(2 + 0.4*(x_mat[:,0] + x_mat[:,1]))"
TRAIN_CSV = "outputs/train.csv"
RBART_CSV = "outputs/rbart_surv.csv"
N = 100
# T = 30
X_VARS = 2
CENS_SCALE = 5 # 40
CENS_IND = False

sv_mat, x_mat, lmbda, a, tlat, cens, t_event, status, T = ssf.sim_surv(
                N=N, 
                # T=T,
                x_vars=X_VARS,
                a = ALPHA,
                lambda_f = LAMBDA,
                cens_scale=CENS_SCALE,
                cens_ind = CENS_IND,
                err_ind = False)


x_out, x_idx, x_cnt = ssf.get_x_info(x_mat)
event_calc, cens_calc = ssf.get_status_perc(status)
t_mean, t_max = ssf.get_event_time_metric(t_event)
train = ssf.get_train_matrix(x_mat, t_event, status)
title = "actual_survival"
ssf.plot_sv(x_mat, sv_mat, T, title=title, save = False, show=True, dir=OUTPUTS)

print(f"cens_calc: {cens_calc} \
    \nt_mean: {t_mean} \
    \nt_max: {t_max}")


# COMMAND ----------

OUTPUTS = "outputs"
ALPHA = 2
LAMBDA = "np.exp(2 + .3*(x_mat[:,0] + x_mat[:,1] + x_mat[:,2] + 2* x_mat[:,3]))"
TRAIN_CSV = "outputs/train.csv"
RBART_CSV = "outputs/rbart_surv.csv"
N = 100
# T = 30
X_VARS = 4
CENS_SCALE = 7
CENS_IND = True

sv_mat, x_mat, lmbda, a, tlat, cens, t_event, status, T = ssf.sim_surv(
                N=N, 
                # T=T,
                x_vars=X_VARS,
                a = ALPHA,
                lambda_f = LAMBDA,
                cens_scale=CENS_SCALE,
                cens_ind = CENS_IND,
                err_ind = False)


x_out, x_idx, x_cnt = ssf.get_x_info(x_mat)
event_calc, cens_calc = ssf.get_status_perc(status)
t_mean, t_max = ssf.get_event_time_metric(t_event)
train = ssf.get_train_matrix(x_mat, t_event, status)
title = "actual_survival"
ssf.plot_sv(x_mat, sv_mat, T, title=title, save = False, show=True, dir=OUTPUTS)

print(f"cens_calc: {cens_calc} \
    \nt_mean: {t_mean} \
    \nt_max: {t_max}")

# COMMAND ----------

# MAGIC %md
# MAGIC # notes on alpha and lambda metrics
# MAGIC ## Alpha
# MAGIC - alpha increase pulls the middle of each curve up and to the right.
# MAGIC - low alpha means quick drop
# MAGIC - high alpha means long wait for drop
# MAGIC
# MAGIC ## Lambda
# MAGIC - controls the rate of the curve
# MAGIC - high alpha is a long c curv down
# MAGIC - 3 components exp({base} + {cov_base_mult} ({coeff}*{cov})
# MAGIC - cov_vase_mult     controls generally the covariate effect [0,1]
# MAGIC - base              controls time out (bigger = longer)
# MAGIC - coef              controls specific covariate effects
# MAGIC - bigger lambda raises the curve

# COMMAND ----------

# MAGIC %md
# MAGIC # Non-proprtional

# COMMAND ----------

OUTPUTS = "outputs"
ALPHA = 2
ALPHA_F = "1 + 2 * x_mat[:,0]"
LAMBDA = "np.exp(2 + .4*(3 * x_mat[:,0] +  x_mat[:,1]))"
TRAIN_CSV = "outputs/train.csv"
RBART_CSV = "outputs/rbart_surv.csv"
N = 100
X_VARS = 2
CENS_SCALE = 5
CENS_IND = True

sv_mat, x_mat, lmbda, a, tlat, cens, t_event, status, T = ssf.sim_surv(
                N=N, 
                # T=T,
                x_vars=X_VARS,
                a = ALPHA,
                alpha_f = ALPHA_F,
                lambda_f = LAMBDA,
                cens_scale=CENS_SCALE,
                cens_ind = CENS_IND,
                err_ind = False)


x_out, x_idx, x_cnt = ssf.get_x_info(x_mat)
event_calc, cens_calc = ssf.get_status_perc(status)
t_mean, t_max = ssf.get_event_time_metric(t_event)
train = ssf.get_train_matrix(x_mat, t_event, status)
title = "actual_survival"
ssf.plot_sv(x_mat, sv_mat, T, title=title, save = False, show=True, dir=OUTPUTS)

print(f"cens_calc: {cens_calc} \
    \nt_mean: {t_mean} \
    \nt_max: {t_max}")

# COMMAND ----------

OUTPUTS = "outputs"
ALPHA = 2
ALPHA_F = "1 + (1.5 * x_mat[:,0]) + x_mat[:,1]"
# LAMBDA = "np.exp(3 + 0.4*(x_mat[:,0] + 0.5*x_mat[:,1] + 1.2*x_mat[:,2] + 0.8*x_mat[:,3]))"
LAMBDA = "np.exp(2 + .2*(3 * x_mat[:,0] - 1.2*x_mat[:,1] + 2*x_mat[:,2]))"
# LAMBDA = "np.exp(2 + .4*(3 * x_mat[:,0] +  x_mat[:,1]))"
TRAIN_CSV = "outputs/train.csv"
RBART_CSV = "outputs/rbart_surv.csv"
N = 100
# T = 30
X_VARS = 3
CENS_SCALE = 4
CENS_IND = True

sv_mat, x_mat, lmbda, a, tlat, cens, t_event, status, T = ssf.sim_surv(
                N=N, 
                # T=T,
                x_vars=X_VARS,
                a = ALPHA,
                alpha_f = ALPHA_F,
                lambda_f = LAMBDA,
                cens_scale=CENS_SCALE,
                cens_ind = CENS_IND,
                err_ind = False)


x_out, x_idx, x_cnt = ssf.get_x_info(x_mat)
event_calc, cens_calc = ssf.get_status_perc(status)
t_mean, t_max = ssf.get_event_time_metric(t_event)
train = ssf.get_train_matrix(x_mat, t_event, status)
title = "actual_survival"
ssf.plot_sv(x_mat, sv_mat, T, title=title, save = False, show=True, dir=OUTPUTS)

print(f"cens_calc: {cens_calc} \
    \nt_mean: {t_mean} \
    \nt_max: {t_max}")

# COMMAND ----------

# Non Linear
OUTPUTS = "outputs"
ALPHA = 2
ALPHA_F = "1 + (1.5 * x_mat[:,0]) + x_mat[:,1]"
LAMBDA = "np.exp(1 + .2*x_mat[:,0] + .3*x_mat[:,1] + 0.8*np.sin(x_mat[:,0] * x_mat[:,1]) + np.power((x_mat[:,2] - 0.5),2))"
TRAIN_CSV = "outputs/train.csv"
RBART_CSV = "outputs/rbart_surv.csv"
N = 100
# T = 30
X_VARS = 5
CENS_SCALE = 5
CENS_IND = False

sv_mat, x_mat, lmbda, a, tlat, cens, t_event, status, T = ssf.sim_surv(
                N=N, 
                # T=T,
                x_vars=X_VARS,
                a = ALPHA,
                alpha_f = ALPHA_F,
                lambda_f = LAMBDA,
                cens_scale=CENS_SCALE,
                cens_ind = CENS_IND,
                err_ind = False)


x_out, x_idx, x_cnt = ssf.get_x_info(x_mat)
event_calc, cens_calc = ssf.get_status_perc(status)
t_mean, t_max = ssf.get_event_time_metric(t_event)
train = ssf.get_train_matrix(x_mat, t_event, status)
title = "actual_survival"
ssf.plot_sv(x_mat, sv_mat, T, title=title, save = False, show=True, dir=OUTPUTS)

print(f"cens_calc: {cens_calc} \
    \nt_mean: {t_mean} \
    \nt_max: {t_max}")
