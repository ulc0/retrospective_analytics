# %% [markdown]
# ### Load libraries

# %%
import numpy as np
import pandas as pd
from lifelines import KaplanMeierFitter
from lifelines import CoxPHFitter
import matplotlib.pyplot as plt

# %% [markdown]
# ### Data Source
# 
# Loprinzi CL. Laurie JA. Wieand HS. Krook JE. Novotny PJ. Kugler JW. Bartel J. Law M. Bateman M. Klatt NE. et al. Prospective evaluation of prognostic variables from patient-completed questionnaires. North Central Cancer Treatment Group. Journal of Clinical Oncology. 12(3):601-7, 1994.

# %%
data = pd.read_csv("lung.csv", index_col = 0)
data.head()

# %%
data.shape

# %% [markdown]
# ### Variable description
# * inst: Institution code
# * time: Survival time in days
# * status: censoring status 0 = censored, 1 = dead
# * age: Age in years
# * sex: Male = 0 Female = 1
# * ph.ecog: ECOG performance score as rated by the physician. 0 = asymptomatic, 1 = symptomatic but completely ambulatory, 2 = in bed <50% of the day, 3 = in bed > 50% of the day but not bedbound, 4 = bedbound
# * ph.karno: Karnofsky performance score (bad=0-good=100) rated by physician
# * pat.karno: Karnofsky performance score as rated by patient
# * meal.cal = Calories consumed at meals
# * wt.loss = Weight loss in last six months
# 

# %%
data = data[['time', 'status', 'age', 'sex', 'ph.ecog', 'ph.karno','pat.karno', 'meal.cal', 'wt.loss']]
data["status"] = data["status"] - 1
data["sex"] = data["sex"] - 1
data.head()

# %%
data.dtypes

# %%
data.isnull().sum()

# %%
data.columns

# %%
data["ph.karno"].fillna(data["ph.karno"].mean(), inplace = True)
data["pat.karno"].fillna(data["pat.karno"].mean(), inplace = True)
data["meal.cal"].fillna(data["meal.cal"].mean(), inplace = True)
data["wt.loss"].fillna(data["wt.loss"].mean(), inplace = True)
data.dropna(inplace=True)
data["ph.ecog"] = data["ph.ecog"].astype("int64")

# %%
data.isnull().sum()

# %%
data.shape

# %%
T = data["time"]
E = data["status"]
plt.hist(T, bins = 50)
plt.show()

# %% [markdown]
# ## Fitting a non-parametric model [Kaplan Meier Curve]

# %%
kmf = KaplanMeierFitter()
kmf.fit(durations = T, event_observed = E)
kmf.plot_survival_function()

# %%
kmf.survival_function_.plot()
plt.title('Survival function')

# %%
kmf.plot_cumulative_density()

# %%
kmf.median_survival_time_

# %%
from lifelines.utils import median_survival_times

median_ = kmf.median_survival_time_
median_confidence_interval_ = median_survival_times(kmf.confidence_interval_)
print(median_)
print(median_confidence_interval_)

# %%
ax = plt.subplot(111)

m = (data["sex"] == 0)

kmf.fit(durations = T[m], event_observed = E[m], label = "Male")
kmf.plot_survival_function(ax = ax)

kmf.fit(T[~m], event_observed = E[~m], label = "Female")
kmf.plot_survival_function(ax = ax, at_risk_counts = True)

plt.title("Survival of different gender group")

# %%
ecog_types = data.sort_values(by = ['ph.ecog'])["ph.ecog"].unique()

for i, ecog_types in enumerate(ecog_types):
    ax = plt.subplot(2, 2, i + 1)
    ix = data['ph.ecog'] == ecog_types
    kmf.fit(T[ix], E[ix], label = ecog_types)
    kmf.plot_survival_function(ax = ax, legend = False)
    plt.title(ecog_types)
    plt.xlim(0, 1200)

plt.tight_layout()

# %%
data['ph.ecog'].value_counts()

# %%
data = data[data["ph.ecog"] != 3]
data.shape

# %%
data['ph.ecog'].value_counts()

# %% [markdown]
# ## Fitting Cox Proportional Hazard Model

# %% [markdown]
# The Cox proportional hazards model, by contrast, is not a fully parametric model. Rather it is a semi-parametric model because even if the regression parameters (the betas) are known, the distribution of the outcome remains unknown.

# %% [markdown]
# [Cox Proportional Hazard Model (lifelines webpage)](https://lifelines.readthedocs.io/en/latest/Survival%20Regression.html)

# %% [markdown]
# Cox proportional hazards regression model assumptions includes:
# 
# * Independence of survival times between distinct individuals in the sample,
# * A multiplicative relationship between the predictors and the hazard, and
# * A constant hazard ratio over time. This assumption implies that, the hazard curves for the groups should be proportional and cannot cross.

# %% [markdown]
# ### Hazard and Hazard Ratio

# %% [markdown]
# * Hazard is defined as the slope of the survival curve — a measure of how rapidly subjects are dying.
# * The hazard ratio compares two treatments. If the hazard ratio is 2.0, then the rate of deaths in one treatment group is twice the rate in the other group.

# %%
data.head()

# %%
dummies_ecog = pd.get_dummies(data["ph.ecog"],
                         prefix = 'ecog')
dummies_ecog.head(4)

# %%
dummies_ecog = dummies_ecog[["ecog_1", "ecog_2"]]
data = pd.concat([data, dummies_ecog], 
                  axis = 1)
data.head()

# %%
data = data.drop("ph.ecog", axis = 1)
data.head()

# %%
cph = CoxPHFitter()
cph.fit(data, duration_col = 'time', event_col = 'status')

cph.print_summary() 

# %% [markdown]
# ### Interpretation

# %% [markdown]
# * Wt.loss has a coefficient of about -0.01.
# 
# * We can recall that in the Cox proportional hazard model, a higher hazard means more at risk of the event occurring.
# The value $exp(-0.01)$ is called the hazard ratio.
# 
# * Here, a one unit increase in wt loss means the baseline hazard will increase by a factor 
# of $exp(-0.01)$ = 0.99 -> about a 1% decrease. 
# 
# * Similarly, the values in the ecog column are: \[0 = asymptomatic, 1 = symptomatic but completely ambulatory, 2 = in bed $<$50\% of the day\]. The value of the coefficient associated with ecog2, $exp(1.20)$, is the value of ratio of hazards associated with being "in bed $<$50% of the day (coded as 2)" compared to asymptomatic (coded as 0, base category).

# %%
plt.subplots(figsize=(10, 6))
cph.plot()

# %%
cph.plot_partial_effects_on_outcome(covariates = 'age',
                                    values = [50, 60, 70, 80],
                                    cmap = 'coolwarm')

# %%
cph.check_assumptions(data, p_value_threshold = 0.05)

# %%
from lifelines.statistics import proportional_hazard_test

results = proportional_hazard_test(cph, data, time_transform='rank')
results.print_summary(decimals=3, model="untransformed variables")

# %% [markdown]
# ## Parametric [Accelerated Failure Time Model (AFT)]

# %% [markdown]
# [AFT Lifelines package webpage](https://lifelines.readthedocs.io/en/latest/Survival%20Regression.html#accelerated-failure-time-models)

# %%
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
  model.fit(durations = data["time"],
            event_observed = data["status"])
  # Print AIC
  print("The AIC value for", model.__class__.__name__, "is",  model.AIC_)

# %% [markdown]
# ### Fit the weibull fitter and print summary

# %%
from lifelines import WeibullAFTFitter
weibull_aft = WeibullAFTFitter()
weibull_aft.fit(data, duration_col='time', event_col='status')

weibull_aft.print_summary(3)

# %% [markdown]
# ## Interpretation of the coefficients

# %% [markdown]
# * A unit increase in $x_i$ means the average/median survival time changes by a factor of $exp(b_i)$.
# * Suppose $b_i$ was positive, then the factor $exp(b_i)$ is greater than 1, which will decelerate the event time since we divide time by the factor ⇿ increase mean/median survival. Hence, it will be a protective effect.
# * Likewise, a negative $b_i$ will hasten the event time ⇿ reduce the mean/median survival time.
# * This interpretation is opposite of how the sign influences event times in the Cox model!

# %% [markdown]
# ## Example 
# 
# * Sex, which contains [0: Male and 1: Female], has a positive coefficient. 
# * This means being a female subject compared to male changes mean/median survival time by exp(0.416) = 1.516,  approximately a 52% increase in mean/median survival time.

# %%
print(weibull_aft.median_survival_time_)
print(weibull_aft.mean_survival_time_)

# %%
plt.subplots(figsize=(10, 6))
weibull_aft.plot()

# %%
plt.subplots(figsize=(10, 6))
weibull_aft.plot_partial_effects_on_outcome('age', range(50, 80, 10), cmap='coolwarm')

# %%


# %%


# %%



