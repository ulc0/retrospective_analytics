# Databricks notebook source
import h2o
from h2o.estimators.coxph import H2OCoxProportionalHazardsEstimator
h2o.init()

# import the heart dataset
heart = h2o.import_file("http://s3.amazonaws.com/h2o-public-test-data/smalldata/coxph_test/heart.csv")

# split the dataset into train and test datasets
train, test = heart.split_frame(ratios = [.8], seed=1234)

#specify the init parameter's value
init = 3

# initialize an train a CoxPH model
coxph = H2OCoxProportionalHazardsEstimator(start_column="start",
                                           stop_column="stop",
                                           ties="breslow",
                                           init=init)
coxph.train(x="age", y="event", training_frame=heart)


# COMMAND ----------

from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import  *
temp_path=".."

df = spark.createDataFrame([(0.0,), (1.0,), (2.0,)], ["input"])
ohe = OneHotEncoder()
ohe.setInputCols(["input"])

ohe.setOutputCols(["output"])

model = ohe.fit(df)
model.setOutputCols(["output"])

model.getHandleInvalid()

model.transform(df).head().output

single_col_ohe = OneHotEncoder(inputCol="input", outputCol="output")
single_col_model = single_col_ohe.fit(df)
single_col_model.transform(df).head().output

ohePath = temp_path + "/ohe"
ohe.save(ohePath)
loadedOHE = OneHotEncoder.load(ohePath)
loadedOHE.getInputCols() == ohe.getInputCols()

modelPath = temp_path + "/ohe-model"
model.save(modelPath)
loadedModel = OneHotEncoderModel.load(modelPath)
loadedModel.categorySizes == model.categorySizes

loadedModel.transform(df).take(1) == model.transform(df).take(1)
