# Databricks notebook source
from pathlib import Path

import arviz as az
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pymc as pm
import pymc_bart as pmb
import mlflow
import time

from sklearn.model_selection import train_test_split

%config InlineBackend.figure_format = "retina"
print(f"Running on PyMC v{pm.__version__}")

# COMMAND ----------

# get the coal dataset from pm
coal = np.loadtxt(pm.get_data("coal.csv"))

# discretize data
years = int(coal.max() - coal.min())
bins = years // 4
hist, x_edges = np.histogram(coal, bins=bins)

# compute the location of the centers of the discretized data
x_centers = x_edges[:-1] + (x_edges[1] - x_edges[0]) / 2

# xdata needs to be 2D for BART
x_data = x_centers[:, None]

# express data as the rate number of disaster per year
y_data = hist

# COMMAND ----------

# create mlflow experiment if not created
# experiment_id = mlflow.create_experiment("/Users/twj8@cdc.gov/pymc_test/pymc_bart_00")
# experiment = mlflow.get_experiment(experiment_id)

# otherwise use experiment ID
experiment_id = 1805967508847905

# COMMAND ----------

# start the mlflow run
mlflow.start_run(experiment_id = experiment_id)
mlflow.set_tags({"Version notes": "Coal Miner inital run", 
                 "mlflow.runName":time.strftime("%D-%H:%M:%S",time.localtime())})

# COMMAND ----------

# Run PYMC w/ BART
with pm.Model() as model_coal:
    #specs
    CHAINS = 4
    SAMPLES = 1000
    TUNE = 1000
    RANDOM_SEED = 99
    # priors
    priors = {"M":20}

    # Model
    μ_ = pmb.BART("μ_", X=x_data, Y=np.log(y_data), m=priors["M"])
    μ = pm.Deterministic("μ", pm.math.exp(μ_))
    y_pred = pm.Poisson("y_pred", mu=μ, observed=y_data)

    # Mlflow log
    mlflow.log_param("Prior", priors)
    mlflow.log_param("Chains", CHAINS)
    mlflow.log_param("Samples", SAMPLES)
    mlflow.log_param("Tuning samples", TUNE)
    mlflow.log_param("Random seed", RANDOM_SEED)

    # Sample
    idata_coal = pm.sample(SAMPLES, chains=CHAINS, tune=TUNE,  random_seed=RANDOM_SEED)



# COMMAND ----------

az.summary(idata_coal).to_csv("trace_summary.csv")
mlflow.log_artifact("trace_summary.csv")

# COMMAND ----------

fig, ax = plt.subplots(figsize=(10, 6))

rates = idata_coal.posterior["μ"] / 4
rate_mean = rates.mean(dim=["draw", "chain"])
ax.plot(x_centers, rate_mean, "w", lw=3)
ax.plot(x_centers, y_data / 4, "k.")
az.plot_hdi(x_centers, rates, smooth=False)
az.plot_hdi(x_centers, rates, hdi_prob=0.5, smooth=False, plot_kwargs={"alpha": 0})
ax.plot(coal, np.zeros_like(coal) - 0.5, "k|")
ax.set_xlabel("years")
ax.set_ylabel("rate")

mlflow.log_figure(fig, "coal_plot.png")

# COMMAND ----------

mlflow.end_run()
