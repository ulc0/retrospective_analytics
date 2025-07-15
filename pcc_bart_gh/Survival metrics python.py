# Databricks notebook source
# MAGIC %pip install scikit-survival

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import numpy as np
import matplotlib.pyplot as plt

%matplotlib inline
import pandas as pd

from sklearn import set_config
from sklearn.impute import SimpleImputer
from sklearn.model_selection import train_test_split
from sklearn.pipeline import make_pipeline

from sksurv.datasets import load_flchain, load_gbsg2
from sksurv.functions import StepFunction
from sksurv.linear_model import CoxPHSurvivalAnalysis, CoxnetSurvivalAnalysis
from sksurv.metrics import (
    concordance_index_censored,
    concordance_index_ipcw,
    cumulative_dynamic_auc,
    integrated_brier_score,
)
from sksurv.nonparametric import kaplan_meier_estimator
from sksurv.preprocessing import OneHotEncoder, encode_categorical
from sksurv.util import Surv

set_config(display="text")  # displays text representation of estimators
plt.rcParams["figure.figsize"] = [7.2, 4.8]

# COMMAND ----------

import scipy.optimize as opt


def generate_marker(n_samples, hazard_ratio, baseline_hazard, rnd):
    # create synthetic risk score
    X = rnd.randn(n_samples, 1)

    # create linear model
    hazard_ratio = np.array([hazard_ratio])
    logits = np.dot(X, np.log(hazard_ratio))

    # draw actual survival times from exponential distribution,
    # refer to Bender et al. (2005), https://doi.org/10.1002/sim.2059
    u = rnd.uniform(size=n_samples)
    time_event = -np.log(u) / (baseline_hazard * np.exp(logits))

    # compute the actual concordance in the absence of censoring
    X = np.squeeze(X)
    actual = concordance_index_censored(np.ones(n_samples, dtype=bool), time_event, X)
    return X, time_event, actual[0]


def generate_survival_data(n_samples, hazard_ratio, baseline_hazard, percentage_cens, rnd):
    X, time_event, actual_c = generate_marker(n_samples, hazard_ratio, baseline_hazard, rnd)

    def get_observed_time(x):
        rnd_cens = np.random.RandomState(0)
        # draw censoring times
        time_censor = rnd_cens.uniform(high=x, size=n_samples)
        event = time_event < time_censor
        time = np.where(event, time_event, time_censor)
        return event, time

    def censoring_amount(x):
        event, _ = get_observed_time(x)
        cens = 1.0 - event.sum() / event.shape[0]
        return (cens - percentage_cens) ** 2

    # search for upper limit to obtain the desired censoring amount
    res = opt.minimize_scalar(censoring_amount, method="bounded", bounds=(0, time_event.max()))

    # compute observed time
    event, time = get_observed_time(res.x)

    # upper time limit such that the probability
    # of being censored is non-zero for `t > tau`
    tau = time[event].max()
    y = Surv.from_arrays(event=event, time=time)
    mask = time < tau
    X_test = X[mask]
    y_test = y[mask]

    return X_test, y_test, y, actual_c


def simulation(n_samples, hazard_ratio, n_repeats=100):
    measures = (
        "censoring",
        "Harrel's C",
        "Uno's C",
    )
    data_mean = {}
    data_std = {}
    for measure in measures:
        data_mean[measure] = []
        data_std[measure] = []

    rnd = np.random.RandomState(seed=987)
    # iterate over different amount of censoring
    for cens in (0.1, 0.25, 0.4, 0.5, 0.6, 0.7):
        data = {
            "censoring": [],
            "Harrel's C": [],
            "Uno's C": [],
        }

        # repeaditly perform simulation
        for _ in range(n_repeats):
            # generate data
            X_test, y_test, y_train, actual_c = generate_survival_data(
                n_samples, hazard_ratio, baseline_hazard=0.1, percentage_cens=cens, rnd=rnd
            )

            # estimate c-index
            c_harrell = concordance_index_censored(y_test["event"], y_test["time"], X_test)
            c_uno = concordance_index_ipcw(y_train, y_test, X_test)

            # save results
            data["censoring"].append(100.0 - y_test["event"].sum() * 100.0 / y_test.shape[0])
            data["Harrel's C"].append(actual_c - c_harrell[0])
            data["Uno's C"].append(actual_c - c_uno[0])

        # aggregate results
        for key, values in data.items():
            data_mean[key].append(np.mean(data[key]))
            data_std[key].append(np.std(data[key], ddof=1))

    data_mean = pd.DataFrame.from_dict(data_mean)
    data_std = pd.DataFrame.from_dict(data_std)
    return data_mean, data_std


def plot_results(data_mean, data_std, **kwargs):
    index = pd.Index(data_mean["censoring"].round(3), name="mean percentage censoring")
    for df in (data_mean, data_std):
        df.drop("censoring", axis=1, inplace=True)
        df.index = index

    ax = data_mean.plot.bar(yerr=data_std, **kwargs)
    ax.set_ylabel("Actual C - Estimated C")
    ax.yaxis.grid(True)
    ax.axhline(0.0, color="gray")

# COMMAND ----------

hazard_ratio = 2.0
ylim = [-0.035, 0.035]
mean_1, std_1 = simulation(100, hazard_ratio)
plot_results(mean_1, std_1, ylim=ylim)

# COMMAND ----------

# MAGIC %md
# MAGIC # Time-dependent AUC

# COMMAND ----------

x, y = load_flchain()

x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=0.2, random_state=0)

# COMMAND ----------

num_columns = ["age", "creatinine", "kappa", "lambda"]

imputer = SimpleImputer().fit(x_train.loc[:, num_columns])
x_test_imputed = imputer.transform(x_test.loc[:, num_columns])

# COMMAND ----------

y_events = y_train[y_train["death"]]
train_min, train_max = y_events["futime"].min(), y_events["futime"].max()

y_events = y_test[y_test["death"]]
test_min, test_max = y_events["futime"].min(), y_events["futime"].max()

assert (
    train_min <= test_min < test_max < train_max
), "time range or test data is not within time range of training data."

# COMMAND ----------

times = np.percentile(y["futime"], np.linspace(5, 81, 15))
print(times)

# COMMAND ----------

def plot_cumulative_dynamic_auc(risk_score, label, color=None):
    auc, mean_auc = cumulative_dynamic_auc(y_train, y_test, risk_score, times)

    plt.plot(times, auc, marker="o", color=color, label=label)
    plt.xlabel("days from enrollment")
    plt.ylabel("time-dependent AUC")
    plt.axhline(mean_auc, color=color, linestyle="--")
    plt.legend()


for i, col in enumerate(num_columns):
    plot_cumulative_dynamic_auc(x_test_imputed[:, i], col, color=f"C{i}")
    ret = concordance_index_ipcw(y_train, y_test, x_test_imputed[:, i], tau=times[-1])

# COMMAND ----------

from sksurv.datasets import load_veterans_lung_cancer

va_x, va_y = load_veterans_lung_cancer()

va_x_train, va_x_test, va_y_train, va_y_test = train_test_split(
    va_x, va_y, test_size=0.2, stratify=va_y["Status"], random_state=0
)

# COMMAND ----------

cph = make_pipeline(OneHotEncoder(), CoxPHSurvivalAnalysis())
cph.fit(va_x_train, va_y_train)

# COMMAND ----------

va_times = np.arange(8, 184, 7)
cph_risk_scores = cph.predict(va_x_test)
cph_auc, cph_mean_auc = cumulative_dynamic_auc(va_y_train, va_y_test, cph_risk_scores, va_times)

plt.plot(va_times, cph_auc, marker="o")
plt.axhline(cph_mean_auc, linestyle="--")
plt.xlabel("days from enrollment")
plt.ylabel("time-dependent AUC")
plt.grid(True)

# COMMAND ----------

va_times

# COMMAND ----------

from sksurv.ensemble import RandomSurvivalForest

rsf = make_pipeline(OneHotEncoder(), RandomSurvivalForest(n_estimators=100, min_samples_leaf=7, random_state=0))
rsf.fit(va_x_train, va_y_train)

# COMMAND ----------

rsf_chf_funcs = rsf.predict_cumulative_hazard_function(va_x_test, return_array=False)
rsf_risk_scores = np.row_stack([chf(va_times) for chf in rsf_chf_funcs])

rsf_auc, rsf_mean_auc = cumulative_dynamic_auc(va_y_train, va_y_test, rsf_risk_scores, va_times)

# COMMAND ----------

plt.plot(va_times, cph_auc, "o-", label=f"CoxPH (mean AUC = {cph_mean_auc:.3f})")
plt.plot(va_times, rsf_auc, "o-", label=f"RSF (mean AUC = {rsf_mean_auc:.3f})")
plt.xlabel("days from enrollment")
plt.ylabel("time-dependent AUC")
plt.legend(loc="lower center")
plt.grid(True)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Brier Score

# COMMAND ----------

from sksurv.ensemble import RandomSurvivalForest


# COMMAND ----------

gbsg_X, gbsg_y = load_gbsg2()
gbsg_X = encode_categorical(gbsg_X)

gbsg_X_train, gbsg_X_test, gbsg_y_train, gbsg_y_test = train_test_split(
    gbsg_X, gbsg_y, stratify=gbsg_y["cens"], random_state=1
)

# COMMAND ----------

rsf_gbsg = RandomSurvivalForest(max_depth=2, random_state=1)
rsf_gbsg.fit(gbsg_X_train, gbsg_y_train)

cph_gbsg = CoxnetSurvivalAnalysis(l1_ratio=0.99, fit_baseline_model=True)
cph_gbsg.fit(gbsg_X_train, gbsg_y_train)

# COMMAND ----------

score_cindex = pd.Series(
    [
        rsf_gbsg.score(gbsg_X_test, gbsg_y_test),
        cph_gbsg.score(gbsg_X_test, gbsg_y_test),
        0.5,
    ],
    index=["RSF", "CPH", "Random"],
    name="c-index",
)

score_cindex.round(3)

# COMMAND ----------

rsf_gbsg.score(gbsg_X_test, gbsg_y_test)

# COMMAND ----------

lower, upper = np.percentile(gbsg_y["time"], [10, 90])
gbsg_times = np.arange(lower, upper + 1)

# COMMAND ----------

# np.percentile(gbsg_y["time"], [10,90])
np.arange(lower, upper + 1)

# COMMAND ----------

rsf_surv_prob = np.row_stack([fn(gbsg_times) for fn in rsf_gbsg.predict_survival_function(gbsg_X_test)])
cph_surv_prob = np.row_stack([fn(gbsg_times) for fn in cph_gbsg.predict_survival_function(gbsg_X_test)])


# COMMAND ----------

rsf_gbsg2 = rsf_gbsg.predict_survival_function(gbsg_X_test)

# COMMAND ----------

np.percentile(rsf_gbsg2[0].x, np.linspace(10,80, 2*len(rsf_gbsg2[0].x)))
np.percentile()
# np.linspace(10,80, 2*len(rsf_gbsg2[0].x))
# len(rsf_gbsg2[1].x)
# rsf_gbsg2[1].x

# COMMAND ----------

random_surv_prob = 0.5 * np.ones((gbsg_y_test.shape[0], gbsg_times.shape[0]))


# COMMAND ----------

km_func = StepFunction(*kaplan_meier_estimator(gbsg_y_test["cens"], gbsg_y_test["time"]))
km_surv_prob = np.tile(km_func(gbsg_times), (gbsg_y_test.shape[0], 1))

# COMMAND ----------

rsf_surv_prob

# COMMAND ----------

print(rsf_surv_prob.shape)
print(gbsg_times.shape)
print(gbsg_y_test.shape)
print(gbsg_y_test)
print(gbsg_times)

# COMMAND ----------

rsf_surv_prob.x

# COMMAND ----------

score_brier = pd.Series(
    [
        integrated_brier_score(gbsg_y, gbsg_y_test, prob, gbsg_times)
        for prob in (rsf_surv_prob, cph_surv_prob, random_surv_prob, km_surv_prob)
    ],
    index=["RSF", "CPH", "Random", "Kaplan-Meier"],
    name="IBS",
)

pd.concat((score_cindex, score_brier), axis=1).round(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Brier 2

# COMMAND ----------

import numpy as np
from sksurv.datasets import load_gbsg2
from sksurv.linear_model import CoxPHSurvivalAnalysis
from sksurv.metrics import integrated_brier_score
from sksurv.preprocessing import OneHotEncoder

# COMMAND ----------

X, y = load_gbsg2()
X.loc[:, "tgrade"] = X.loc[:, "tgrade"].map(len).astype(int)
Xt = OneHotEncoder().fit_transform(X)

# COMMAND ----------

est = CoxPHSurvivalAnalysis(ties="efron").fit(Xt, y)

# COMMAND ----------

survs = est.predict_survival_function(Xt)
times = np.arange(365, 1826)
preds = np.asarray([[fn(t) for t in times] for fn in survs])

# COMMAND ----------

preds.shape

# COMMAND ----------

preds

# COMMAND ----------

times

# COMMAND ----------

y

# COMMAND ----------

score = integrated_brier_score(y, y, preds, times)
print(score)
