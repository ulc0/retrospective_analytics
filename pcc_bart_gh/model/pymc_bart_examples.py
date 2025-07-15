# Databricks notebook source
# MAGIC %pip install pymc-bart

# COMMAND ----------

import arviz as az
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pymc as pm
import pymc_bart as pmb

# COMMAND ----------

RANDOM_SEED=5781
np.random.seed(RANDOM_SEED)
az.style.use("arviz-darkgrid")

# COMMAND ----------

coal = np.loadtxt(pm.get_data("coal.csv"))
print(coal)

# COMMAND ----------

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

y_data

# COMMAND ----------

with pm.Model() as model_coal:
    mu_ = pmb.BART("mu_", X=x_data, Y=np.log(y_data), m=20)
    mu = pm.Deterministic("mu", pm.math.exp(mu_))
    y_pred = pm.Poisson("y_pred", mu=mu, observed=y_data)
    idata_coal = pm.sample(random_seed=RANDOM_SEED)

# COMMAND ----------

idata_coal.posterior["mu"][3]

# COMMAND ----------

x_centers

# COMMAND ----------

rates = idata_coal.posterior["mu"]/4
rates_mean = rates.mean(dim=["draw", "chain"])
rates_mean

# COMMAND ----------

_, ax = plt.subplots(figsize=(10, 6))

rates = idata_coal.posterior["mu"] / 4
rate_mean = rates.mean(dim=["draw", "chain"])
ax.plot(x_centers, rate_mean, "w", lw=3)
ax.plot(x_centers, y_data / 4, "k.")
az.plot_hdi(x_centers, rates, smooth=False)
az.plot_hdi(x_centers, rates, hdi_prob=0.5, smooth=False, plot_kwargs={"alpha": 0})
ax.plot(coal, np.zeros_like(coal) - 0.5, "k|")
ax.set_xlabel("years")
ax.set_ylabel("rate");

# COMMAND ----------

plt.step(x_data, rates.sel(chain=1, draw=[3,10,11,12]).T)
plt.step(x_data, rates.sel(chain=0, draw=[3,10,11,12]).T)

# COMMAND ----------

# MAGIC %md
# MAGIC Biking Example

# COMMAND ----------

bikes = pd.read_csv(pm.get_data("bikes.csv"))
print(bikes)

features = ["hour", "temperature", "humidity", "workingday"]

X = bikes[features]
Y = bikes["count"]

# COMMAND ----------

with pm.Model() as model_bikes:
    alpha = pm.Exponential("alpha", 1)
    mu = pmb.BART("mu", X, np.log(Y), m=50)
    y = pm.NegativeBinomial("y", mu=pm.math.exp(mu), alpha=alpha, observed=Y)
    idata_bikes = pm.sample(compute_convergence_checks=False, random_seed=RANDOM_SEED)

# COMMAND ----------

pmb.plot_dependence(mu, X=X, Y=Y, grid=(2, 2), func=np.exp);

# COMMAND ----------

# MAGIC %md
# MAGIC Survival attempt

# COMMAND ----------

s = np.random.rand(80)

# COMMAND ----------

t = np.tile([1,2,3,4], 20)
n = np.repeat([1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20],4)


# s = np.ones(80)
s = np.random.rand(80)

c = 0
c2 = 0
for i in s:
    # print("c: ", c)
    # print("c2: ", c2)
    # print("s[i]: ", i)
    if c2 == 1:
        s[c] = 0
        # print("c2 = 1 so set s = 0")
        if c % 4 == 0:
            # print("4th set c2 0")
            c2 = 0
        # print("s[i] post: ", s[c])
        c += 1
        continue
    if i > 0.8:
        # print("ge .8")
        s[c] = 1
        c2 = 1
    else:
        # print("not ge .8")
        s[c] = 0
    if c % 4 == 0:
        # print("4th set c2 0")
        if c != 0:
            # print("here2")
            c2 = 0
    # print("s[i] post: ", s[c])
    c += 1

# s
p = pd.DataFrame({"n":n, "t":t, "s":s})


# COMMAND ----------

s

# COMMAND ----------

p.groupby(n).sum()

# p.loc[67, s] = 0
# p[p.n == 17]

# COMMAND ----------

# p.loc[72:76]
p.loc[73, "s"] = 0 


# COMMAND ----------

# p.groupby(n).sum()

# COMMAND ----------

sv = p.s
tv = p.t

# COMMAND ----------

plt.hist(p[p.s == 1].t)


# COMMAND ----------

# MAGIC %pip install scikit-survival

# COMMAND ----------

dbutils.library.restartPython() 

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
%matplotlib inline

from sklearn import set_config
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OrdinalEncoder

from sksurv.datasets import load_gbsg2
from sksurv.preprocessing import OneHotEncoder
from sksurv.ensemble import RandomSurvivalForest

# COMMAND ----------

X, y = load_gbsg2()

grade_str = X.loc[:, "tgrade"].astype(object).values[:, np.newaxis]
grade_num = OrdinalEncoder(categories=[["I", "II", "III"]]).fit_transform(grade_str)

X_no_grade = X.drop("tgrade", axis=1)
Xt = OneHotEncoder().fit_transform(X_no_grade)
Xt.loc[:, "tgrade"] = grade_num

# COMMAND ----------

random_state = 20

X_train, X_test, y_train, y_test = train_test_split(
    Xt, y, test_size=0.25, random_state=random_state)

# COMMAND ----------

y_train
