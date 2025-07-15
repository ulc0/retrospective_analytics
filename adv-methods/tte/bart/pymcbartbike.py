# Databricks notebook source
from pathlib import Path

import arviz as az
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pymc as pm
import pymc_bart as pmb
# COMMAND ----------

from sklearn.model_selection import train_test_split

# COMMAND ----------

# MAGIC %md 
# MAGIC [Out of Sample Predictions, survival](https://www.pymc.io/projects/bart/en/latest/examples/BART_introduction.html#out-of-sample-predictions)

# COMMAND ----------

print(f"Running on PyMC v{pm.__version__}")
RANDOM_SEED = 5781
np.random.seed(RANDOM_SEED)
az.style.use("arviz-darkgrid")

# COMMAND ----------

bikes = pd.read_csv(pm.get_data("bikes.csv"))

# COMMAND ----------

features = ["hour", "temperature", "humidity", "workingday"]

X = bikes[features]
Y = bikes["count"]

# COMMAND ----------

train_test_hour_split = 19

train_bikes = bikes.query("hour <= @train_test_hour_split")
test_bikes = bikes.query("hour > @train_test_hour_split")

X_train = train_bikes[features]
Y_train = train_bikes["count"]

X_test = test_bikes[features]
Y_test = test_bikes["count"]

# COMMAND ----------

with pm.Model() as model_oos_ts:
    X = pm.MutableData("X", X_train)
    Y = Y_train
    α = pm.Exponential("α", 1 / 10)
    μ = pmb.BART("μ", X, np.log(Y))
    y = pm.NegativeBinomial("y", mu=pm.math.exp(μ), alpha=α, observed=Y, shape=μ.shape)
    idata_oos_ts = pm.sample(random_seed=RANDOM_SEED)
    posterior_predictive_oos_ts_train = pm.sample_posterior_predictive(
        trace=idata_oos_ts, random_seed=RANDOM_SEED
    )

# COMMAND ----------

with model_oos_ts:
    X.set_value(X_test)
    posterior_predictive_oos_ts_test = pm.sample_posterior_predictive(
        trace=idata_oos_ts, random_seed=RANDOM_SEED
    )

# COMMAND ----------

fig, ax = plt.subplots(
    nrows=2, ncols=1, figsize=(8, 7), sharex=True, sharey=True, layout="constrained"
)

az.plot_ppc(data=posterior_predictive_oos_ts_train, kind="cumulative", observed_rug=True, ax=ax[0])
ax[0].set(title="Posterior Predictive Check (train)", xlim=(0, 1_000))

az.plot_ppc(data=posterior_predictive_oos_ts_test, kind="cumulative", observed_rug=True, ax=ax[1])
ax[1].set(title="Posterior Predictive Check (test)", xlim=(0, 1_000));

# COMMAND ----------

fig, ax = plt.subplots(figsize=(12, 6))
az.plot_hdi(
    x=X_train.index,
    y=posterior_predictive_oos_ts_train.posterior_predictive["y"],
    hdi_prob=0.94,
    color="C0",
    fill_kwargs={"alpha": 0.2, "label": r"94$\%$ HDI (train)"},
    smooth=False,
    ax=ax,
)
az.plot_hdi(
    x=X_train.index,
    y=posterior_predictive_oos_ts_train.posterior_predictive["y"],
    hdi_prob=0.5,
    color="C0",
    fill_kwargs={"alpha": 0.4, "label": r"50$\%$ HDI (train)"},
    smooth=False,
    ax=ax,
)
ax.plot(X_train.index, Y_train, label="train (observed)")
az.plot_hdi(
    x=X_test.index,
    y=posterior_predictive_oos_ts_test.posterior_predictive["y"],
    hdi_prob=0.94,
    color="C1",
    fill_kwargs={"alpha": 0.2, "label": r"94$\%$ HDI (test)"},
    smooth=False,
    ax=ax,
)
az.plot_hdi(
    x=X_test.index,
    y=posterior_predictive_oos_ts_test.posterior_predictive["y"],
    hdi_prob=0.5,
    color="C1",
    fill_kwargs={"alpha": 0.4, "label": r"50$\%$ HDI (test)"},
    smooth=False,
    ax=ax,
)
ax.plot(X_test.index, Y_test, label="test (observed)")
ax.axvline(X_train.shape[0], color="k", linestyle="--", label="train/test split")
ax.legend(loc="center left", bbox_to_anchor=(1, 0.5))
ax.set(
    title="BART model predictions for bike rentals",
    xlabel="observation index",
    ylabel="number of rentals",
);
