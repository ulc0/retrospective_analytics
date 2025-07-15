# Databricks notebook source
# MAGIC %pip install h2o_pysparkling_3.1

# COMMAND ----------

from pysparkling import *
hc = H2OContext.getOrCreate()

# COMMAND ----------

import h2o
from h2o.estimators.coxph import H2OCoxProportionalHazardsEstimator
h2o.init()

# Import the heart dataset into H2O:
heart = h2o.import_file("http://s3.amazonaws.com/h2o-public-test-data/smalldata/coxph_test/heart.csv")

# Split the dataset into a train and test set:
train, test = heart.split_frame(ratios = [.8], seed = 1234)

# Build and train the model:
heart_coxph = H2OCoxProportionalHazardsEstimator(start_column="start",
                                                 stop_column="stop",
ties="breslow")

heart_coxph.train(x="age",
            y="event",
            training_frame=train)

# Generate predictions on a test set (if necessary):
pred = heart_coxph.predict(test)

# Get baseline hazard:
hazard = heart_coxph.baseline_hazard_frame

# Get baseline survival:
survival = heart_coxph.baseline_survival_frame

# Get model concordance:
heart_coxph.model_performance().concordance()
