# Databricks notebook source
import lifelines as ll

# COMMAND ----------

coh = spark.table("bart_pcc.end005_full")
smp1 = spark.table("bart_pcc.end005_ccsr_smp1")
smp2 = spark.table("bart_pcc.end005_ccsr_smp2")


# COMMAND ----------

# MAGIC %md
# MAGIC This notebook contains some exploratory samples of the Kaplan Meier plots on the full and subsets of data. 
# MAGIC
# MAGIC NOTE: The timeline is not currently adjusted for the 31 day incubation period, so all survival curves have a latent start at 31 days. This will be adjusted with the featurization pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC # KPM Full

# COMMAND ----------

def data_prep(coh):
    coh_1 = (
    coh.select(["tte_ccsr","p3_ccsr_first", "covid_patient"])
    .na.fill({"tte_ccsr":180})
    ).toPandas()

    coh_t = coh_1["tte_ccsr"]
    coh_e = coh_1["p3_ccsr_first"]
    coh_g = coh_1["covid_patient"]
    ix = (coh_g == 1)

    return coh_t, coh_e, coh_g, ix

# COMMAND ----------

coh_t, coh_e, coh_g, ix = data_prep(coh)

# COMMAND ----------

coh_1 = (
    coh.select(["tte_ccsr","p3_ccsr_first", "covid_patient"])
    .na.fill({"tte_ccsr":180})
).toPandas()

coh_t = coh_1["tte_ccsr"]
coh_e = coh_1["p3_ccsr_first"]
coh_g = coh_1["covid_patient"]
ix = (coh_g == 1)



# COMMAND ----------

# create KMF
kmf = ll.KaplanMeierFitter()

# COMMAND ----------

# Fit and plot the full dataset
kmf.fit(coh_t, event_observed = coh_e)

# COMMAND ----------

kmf.plot_survival_function()

# COMMAND ----------


kmf.fit(coh_t[~ix], event_observed = coh_e[~ix])
ax = kmf.plot_survival_function(label="noncov")
kmf.fit(coh_t[ix], event_observed = coh_e[ix])
kmf.plot_survival_function(ax=ax, label="cov")


# COMMAND ----------

# MAGIC %md
# MAGIC # KPM Subset1

# COMMAND ----------

coh_t, coh_e, coh_g, ix = data_prep(smp1)

# COMMAND ----------

kmf.fit(coh_t, event_observed = coh_e)
kmf.plot_survival_function()

# COMMAND ----------

kmf.fit(coh_t[~ix], event_observed = coh_e[~ix])
ax = kmf.plot_survival_function(label="noncov")
kmf.fit(coh_t[ix], event_observed = coh_e[ix])
kmf.plot_survival_function(ax = ax, label="cov")

# COMMAND ----------

# MAGIC %md
# MAGIC # KPM Subset 2

# COMMAND ----------

coh_t, coh_e, coh_g, ix = data_prep(smp2)

# COMMAND ----------

kmf.fit(coh_t, event_observed = coh_e)
kmf.plot_survival_function()

# COMMAND ----------

kmf.fit(coh_t[~ix], event_observed = coh_e[~ix])
ax = kmf.plot_survival_function(label="noncov")
kmf.fit(coh_t[ix], event_observed = coh_e[ix])
kmf.plot_survival_function(ax = ax, label="cov")

# COMMAND ----------

# MAGIC %md
# MAGIC Overall the subsamples have agreeable survival curves to the full sample.
