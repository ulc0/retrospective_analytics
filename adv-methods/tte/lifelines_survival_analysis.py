# Databricks notebook source
# MAGIC %md
# MAGIC ### Load libraries

# COMMAND ----------

# MAGIC %pip install lifelines

# COMMAND ----------

import numpy as np
import pyspark as ps
import pandas as pd
from lifelines import KaplanMeierFitter
from lifelines import CoxPHFitter
import matplotlib.pyplot as plt

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Source
# MAGIC
# MAGIC Loprinzi CL. Laurie JA. Wieand HS. Krook JE. Novotny PJ. Kugler JW. Bartel J. Law M. Bateman M. Klatt NE. et al. Prospective evaluation of prognostic variables from patient-completed questionnaires. North Central Cancer Treatment Group. Journal of Clinical Oncology. 12(3):601-7, 1994.

# COMMAND ----------

data = spark.table("cdh_reference_data.ml_lung_cancer").toPandas()
data.head()
TIME="days"
EVENT="event"
COV=['age', 'is_female', 'ecog', 'karno_physician','karno', 'meal_calories', 'weight_loss']

# COMMAND ----------

# MAGIC %md
# MAGIC ### Variable description
# MAGIC * inst: Institution code
# MAGIC * time: Survival time in days
# MAGIC * status: censoring status 0 = censored, 1 = dead
# MAGIC * age: Age in years
# MAGIC * is_female
# MAGIC * ecog: ECOG performance score as rated by the physician. 0 = asymptomatic, 1 = symptomatic but completely ambulatory, 2 = in bed <50% of the day, 3 = in bed > 50% of the day but not bedbound, 4 = bedbound
# MAGIC * karno_physician: Karnofsky performance score (bad=0-good=100) rated by physician
# MAGIC * karno: Karnofsky performance score as rated by patient
# MAGIC * meal_calories = Calories consumed at meals
# MAGIC * weight_loss = Weight loss in last six months <---

# COMMAND ----------

data = data[[TIME, EVENT]+COV]
#data["status"] = data["status"] - 1
#data["sex"] = data["sex"] - 1
data.head()

# COMMAND ----------

data.dtypes

# COMMAND ----------

data.isnull().sum()

# COMMAND ----------

data.columns

# COMMAND ----------

#data["ph.karno"].fillna(data["ph.karno"].mean(), inplace = True)
#data["pat.karno"].fillna(data["pat.karno"].mean(), inplace = True)
#data["meal.cal"].fillna(data["meal.cal"].mean(), inplace = True)
#data["wt.loss"].fillna(data["wt.loss"].mean(), inplace = True)
data.dropna(inplace=True)
data["ecog"] = data["ecog"].astype("int64")

# COMMAND ----------

data.isnull().sum()

# COMMAND ----------

data.shape

# COMMAND ----------

T = data[TIME]
E = data[EVENT]
plt.hist(T, bins = 50)
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fitting a non-parametric model [Kaplan Meier Curve]

# COMMAND ----------

kmf = KaplanMeierFitter()
kmf.fit(durations = T, event_observed = E)
kmf.plot_survival_function()

# COMMAND ----------

kmf.survival_function_.plot()
plt.title('Survival function')

# COMMAND ----------

kmf.plot_cumulative_density()

# COMMAND ----------

kmf.median_survival_time_

# COMMAND ----------

from lifelines.utils import median_survival_times

median_ = kmf.median_survival_time_
median_confidence_interval_ = median_survival_times(kmf.confidence_interval_)
print(median_)
print(median_confidence_interval_)

# COMMAND ----------

ax = plt.subplot(111)

m = (data["is_female"] == 0)

kmf.fit(durations = T[m], event_observed = E[m], label = "Male")
kmf.plot_survival_function(ax = ax)

kmf.fit(T[~m], event_observed = E[~m], label = "Female")
kmf.plot_survival_function(ax = ax, at_risk_counts = True)

plt.title("Survival of different gender group")

# COMMAND ----------

ecog_types = data.sort_values(by = ['ecog'])["ecog"].unique()[:4]
print(ecog_types)
for i, ecog_types in enumerate(ecog_types):
    ax = plt.subplot(2, 2, i + 1)
    ix = data['ecog'] == ecog_types
    kmf.fit(T[ix], E[ix], label = ecog_types)
    kmf.plot_survival_function(ax = ax, legend = False)
    plt.title(ecog_types)
    plt.xlim(0, 1200)

plt.tight_layout()

# COMMAND ----------

data['ecog'].value_counts()

# COMMAND ----------

data = data[data["ecog"] != 3]
data.shape

# COMMAND ----------

data['ecog'].value_counts()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fitting Cox Proportional Hazard Model

# COMMAND ----------

# MAGIC %md
# MAGIC The Cox proportional hazards model, by contrast, is not a fully parametric model. Rather it is a semi-parametric model because even if the regression parameters (the betas) are known, the distribution of the outcome remains unknown.

# COMMAND ----------

# MAGIC %md
# MAGIC [Cox Proportional Hazard Model (lifelines webpage)](https://lifelines.readthedocs.io/en/latest/Survival%20Regression.html)

# COMMAND ----------

# MAGIC %md
# MAGIC Cox proportional hazards regression model assumptions includes:
# MAGIC
# MAGIC * Independence of survival times between distinct individuals in the sample,
# MAGIC * A multiplicative relationship between the predictors and the hazard, and
# MAGIC * A constant hazard ratio over time. This assumption implies that, the hazard curves for the groups should be proportional and cannot cross.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Hazard and Hazard Ratio

# COMMAND ----------

# MAGIC %md
# MAGIC * Hazard is defined as the slope of the survival curve — a measure of how rapidly subjects are dying.
# MAGIC * The hazard ratio compares two treatments. If the hazard ratio is 2.0, then the rate of deaths in one treatment group is twice the rate in the other group.

# COMMAND ----------

data.head()

# COMMAND ----------

dummies_ecog = pd.get_dummies(data["ecog"],
                         prefix = 'ecog')
dummies_ecog.head(4)

# COMMAND ----------

dummies_ecog = dummies_ecog[["ecog_1", "ecog_2"]]
data = pd.concat([data, dummies_ecog], 
                  axis = 1)
data.head()

# COMMAND ----------

data = data.drop("ecog", axis = 1)
data.head()

# COMMAND ----------

cph = CoxPHFitter()
cph.fit(data, duration_col = TIME, event_col = EVENT)

cph.print_summary() 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Interpretation

# COMMAND ----------

# MAGIC %md
# MAGIC * Wt.loss has a coefficient of about -0.01.
# MAGIC
# MAGIC * We can recall that in the Cox proportional hazard model, a higher hazard means more at risk of the event occurring.
# MAGIC The value $exp(-0.01)$ is called the hazard ratio.
# MAGIC
# MAGIC * Here, a one unit increase in wt loss means the baseline hazard will increase by a factor 
# MAGIC of $exp(-0.01)$ = 0.99 -> about a 1% decrease. 
# MAGIC
# MAGIC * Similarly, the values in the ecog column are: \[0 = asymptomatic, 1 = symptomatic but completely ambulatory, 2 = in bed $<$50\% of the day\]. The value of the coefficient associated with ecog2, $exp(1.20)$, is the value of ratio of hazards associated with being "in bed $<$50% of the day (coded as 2)" compared to asymptomatic (coded as 0, base category).

# COMMAND ----------

plt.subplots(figsize=(10, 6))
cph.plot()

# COMMAND ----------

cph.plot_partial_effects_on_outcome(covariates = 'age',
                                    values = [50, 60, 70, 80],
                                    cmap = 'coolwarm')

# COMMAND ----------

cph.check_assumptions(data, p_value_threshold = 0.05)

# COMMAND ----------

from lifelines.statistics import proportional_hazard_test

results = proportional_hazard_test(cph, data, time_transform='rank')
results.print_summary(decimals=3, model="untransformed variables")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Parametric [Accelerated Failure Time Model (AFT)]

# COMMAND ----------

# MAGIC %md
# MAGIC [AFT Lifelines package webpage](https://lifelines.readthedocs.io/en/latest/Survival%20Regression.html#accelerated-failure-time-models)

# COMMAND ----------

from lifelines import WeibullFitter,\
                      ExponentialFitter,\
                      LogNormalFitter,\
                      LogLogisticFitter


# Instantiate each fitter
wb = WeibullFitter()
ex = ExponentialFitter()
log = LogNormalFitter()
loglogis = LogLogisticFitter()

# Fit to data
for model in [wb, ex, log, loglogis]:
  model.fit(durations = data[TIME],
            event_observed = data[EVENT])
  # Print AIC
  print("The AIC value for", model.__class__.__name__, "is",  model.AIC_)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Fit the weibull fitter and print summary

# COMMAND ----------

from lifelines import WeibullAFTFitter
weibull_aft = WeibullAFTFitter()
weibull_aft.fit(data, duration_col=TIME, event_col=EVENT)

weibull_aft.print_summary(3)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Interpretation of the coefficients

# COMMAND ----------

# MAGIC %md
# MAGIC * A unit increase in $x_i$ means the average/median survival time changes by a factor of $exp(b_i)$.
# MAGIC * Suppose $b_i$ was positive, then the factor $exp(b_i)$ is greater than 1, which will decelerate the event time since we divide time by the factor ⇿ increase mean/median survival. Hence, it will be a protective effect.
# MAGIC * Likewise, a negative $b_i$ will hasten the event time ⇿ reduce the mean/median survival time.
# MAGIC * This interpretation is opposite of how the sign influences event times in the Cox model!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 
# MAGIC
# MAGIC * Sex, which contains [0: Male and 1: Female], has a positive coefficient. 
# MAGIC * This means being a female subject compared to male changes mean/median survival time by exp(0.416) = 1.516,  approximately a 52% increase in mean/median survival time.

# COMMAND ----------

print(weibull_aft.median_survival_time_)
print(weibull_aft.mean_survival_time_)

# COMMAND ----------

plt.subplots(figsize=(10, 6))
weibull_aft.plot()

# COMMAND ----------

plt.subplots(figsize=(10, 6))
weibull_aft.plot_partial_effects_on_outcome('age', range(50, 80, 10), cmap='coolwarm')

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


