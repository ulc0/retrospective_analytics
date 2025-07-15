# Databricks notebook source
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
import pandas as pd #for url

# COMMAND ----------

training_data=spark.createDataFrame(pd.read_csv("/Workspace/Repos/ulc0@cdc.gov/cdh-adv-methods/examples/foundry/titanic_train.csv"))
test_data=spark.createDataFrame(pd.read_csv("/Workspace/Repos/ulc0@cdc.gov/cdh-adv-methods/examples/foundry/titanic_test.csv"))
#training_data=spark.read.csv("/Workspace/Repos/ulc0@cdc.gov/cdh-adv-methods/examples/foundry/housing_features_and_labels.csv")
display(training_data)



# COMMAND ----------

# we can pandas dummy these too
# Converting the Sex Column 
sexIdx = StringIndexer(inputCol='Sex', outputCol='SexIndex') 
sexEncode = OneHotEncoder(inputCol='SexIndex', outputCol='SexVec')   
# Converting the Embarked Column 
embarkIdx = StringIndexer(inputCol='Embarked',outputCol='EmbarkIndex') 
embarkEncode = OneHotEncoder(inputCol='EmbarkIndex'outputCol='EmbarkVec') 

# COMMAND ----------

feature_names=['PassengerId', 'Survived', 'Pclass', 'SexVec', 'Age', 'SibSp', 'Parch', 'Ticket', 'Fare', 'Cabin',  'EmbarkedVec',]
numeric_cols=['PassengerId', 'Survived', 'Pclass',  'Age', 'SibSp', 'Parch', 'Ticket', 'Fare', 'Cabin', ]
dropCols=['Name',]
#feature_names=training_data.drop(*dropCols).columns
#print(feature_names)
assembler = VectorAssembler(inputCols=['Pclass', 
                                       'SexVec','Age', 
                                       'SibSp','Parch', 
                                       'Fare','EmbarkVec'], outputCol="features")
features_df = assembler.transform(training_data)
display(features_df)

# COMMAND ----------

# MAGIC %md
# MAGIC indexer = StringIndexer(inputCol="category", outputCol="categoryIndex")
# MAGIC encoder = OneHotEncoder(inputCol="categoryIndex", outputCol="categoryVec")
# MAGIC assembler = VectorAssembler(inputCols=["feature1", "feature2", "categoryVec"], outputCol="features")

# COMMAND ----------

lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

# COMMAND ----------



# Fit the model
lrModel = lr.fit(features_df)
trainingSummary = lrModel.summary
#    df_custom = ctx.spark_session.createDataFrame([["Hello"], ["World"]], schema=["phrase"])
#    df_input = housing_inference_data.dataframe().limit(10)
# Remove comment to write contents of df_custom to housing_data on build
# housing_data.write_dataframe(df_custom)
# Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
trainingSummary.roc.show()
print("areaUnderROC: " + str(trainingSummary.areaUnderROC))

# COMMAND ----------


# Obtain the objective per iteration
objectiveHistory = trainingSummary.objectiveHistory
print("objectiveHistory:")
for objective in objectiveHistory:
    print(objective)





# COMMAND ----------

# Set the model threshold to maximize F-Measure
fMeasure = trainingSummary.fMeasureByThreshold
maxFMeasure = fMeasure.groupBy().max('F-Measure').select('max(F-Measure)').head()
bestThreshold = fMeasure.where(fMeasure['F-Measure'] == maxFMeasure['max(F-Measure)']) \
    .select('threshold').head()['threshold']
lr.setThreshold(bestThreshold)
