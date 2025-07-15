# %% [markdown]
# (BART_introduction)=
# # Bayesian Additive Regression Trees: Introduction
# :::{post} Dec 21, 2021
# :tags: BART, non-parametric, regression 
# :category: intermediate, explanation
# :author: Osvaldo Martin
# :::

# %%
from pathlib import Path

import arviz as az
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pymc as pm
import pymc_bart as pmb
import os

# NO SKLEARN IS NO BUENO from sklearn.model_selection import train_test_split


print(f"Running on PyMC v{pm.__version__}")

# %%

from itertools import chain

def _indexing(x, indices):
    """
    :param x: array from which indices has to be fetched
    :param indices: indices to be fetched
    :return: sub-array from given array and indices
    """
    # np array indexing
    if hasattr(x, 'shape'):
        return x[indices]

    # list indexing
    return [x[idx] for idx in indices]

def train_test_split(*arrays, test_size=0.25, shufffle=True, random_seed=1):
    """
    splits array into train and test data.
    :param arrays: arrays to split in train and test
    :param test_size: size of test set in range (0,1)
    :param shufffle: whether to shuffle arrays or not
    :param random_seed: random seed value
    :return: return 2*len(arrays) divided into train ans test
    """
    # checks
    assert 0 < test_size < 1
    assert len(arrays) > 0
    length = len(arrays[0])
    for i in arrays:
        assert len(i) == length

    n_test = int(np.ceil(length*test_size))
    n_train = length - n_test

    if shufffle:
        perm = np.random.RandomState(random_seed).permutation(length)
        test_indices = perm[:n_test]
        train_indices = perm[n_test:]
    else:
        train_indices = np.arange(n_train)
        test_indices = np.arange(n_train, length)

    return list(chain.from_iterable((_indexing(x, train_indices), _indexing(x, test_indices)) for x in arrays))

# %%
RANDOM_SEED = 5781
np.random.seed(RANDOM_SEED)
az.style.use("arviz-darkgrid")
plt.rcParams["figure.figsize"] = [10, 6]
rng = np.random.default_rng(42)

# %% [markdown]
# ## BART overview

# %% [markdown]
# Bayesian additive regression trees (BART) is a non-parametric regression approach. If we have some covariates $X$ and we want to use them to model $Y$, a BART model (omitting the priors) can be represented as:
# 
# $$Y = f(X) + \epsilon$$
# 
# where we use a sum of $m$ [regression trees](https://en.wikipedia.org/wiki/Decision_tree_learning) to model $f$, and $\epsilon$ is some noise. In the most typical examples $\epsilon$ is normally distributed, $\mathcal{N}(0, \sigma)$. So we can also write:
# 
# $$Y \sim \mathcal{N}(\mu=BART(X), \sigma)$$
# 
# In principle nothing restricts us to use a sum of trees to model other relationship. For example we may have:
# 
# $$Y \sim \text{Poisson}(\mu=BART(X))$$
# 
# One of the reason BART is Bayesian is the use of priors over the regression trees. The priors are defined in such a way that they favor shallow trees with leaf values close to zero. A key idea is that a single BART-tree is not very good at fitting the data but when we sum many of these trees we get a good and flexible approximation.

# %%
try:
    df = pd.read_csv(os.path.join("..", "data", "marketing.csv"), sep=";", decimal=",")
except FileNotFoundError:
    df = pd.read_csv(pm.get_data("marketing.csv"), sep=";", decimal=",")

n_obs = df.shape[0]

df.head()

# %%
fig, ax = plt.subplots()
ax.plot(df["youtube"], df["sales"], "o", c="C0")
ax.set(title="Sales as a function of Youtube budget", xlabel="budget", ylabel="sales");

# %%
X = df["youtube"].to_numpy().reshape(-1, 1)
Y = df["sales"].to_numpy()

# %%
with pm.Model() as model_marketing_full:
    w = pmb.BART("w", X=X, Y=np.log(Y), m=200, shape=(2, n_obs))
    y = pm.Gamma("y", mu=pm.math.exp(w[0]), sigma=pm.math.exp(w[1]), observed=Y)

pm.model_to_graphviz(model=model_marketing_full)

# %%
with model_marketing_full:
    idata_marketing_full = pm.sample(random_seed=rng)
    posterior_predictive_marketing_full = pm.sample_posterior_predictive(
        trace=idata_marketing_full, random_seed=rng
    )

# %%
posterior_mean = idata_marketing_full.posterior["w"].mean(dim=("chain", "draw"))[0]

w_hdi = az.hdi(ary=idata_marketing_full, group="posterior", var_names=["w"])

pps = az.extract(
    posterior_predictive_marketing_full, group="posterior_predictive", var_names=["y"]
).T

# %%
idx = np.argsort(X[:, 0])


fig, ax = plt.subplots()
az.plot_hdi(x=X[:, 0], y=pps, ax=ax, fill_kwargs={"alpha": 0.3, "label": r"Likelihood $94\%$ HDI"})
az.plot_hdi(
    x=X[:, 0],
    hdi_data=np.exp(w_hdi["w"].sel(w_dim_0=0)),
    ax=ax,
    fill_kwargs={"alpha": 0.6, "label": r"Mean $94\%$ HDI"},
)
ax.plot(X[:, 0][idx], np.exp(posterior_mean[idx]), c="black", lw=3, label="Posterior Mean")
ax.plot(df["youtube"], df["sales"], "o", c="C0", label="Raw Data")
ax.legend(loc="upper left")
ax.set(
    title="Sales as a function of Youtube budget - Posterior Predictive",
    xlabel="budget",
    ylabel="sales",
);


