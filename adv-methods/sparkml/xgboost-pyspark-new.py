# Databricks notebook source
# MAGIC %md # Regression with XGBoost and MLlib pipelines
# MAGIC
# MAGIC This notebook uses a bike-sharing dataset to illustrate MLlib pipelines and the XGBoost machine learning algorithm. The challenge is to predict the number of bicycle rentals per hour based on the features available in the dataset such as day of the week, weather, season, and so on. Demand prediction is a common problem across businesses; good predictions allow a business or service to optimize inventory and to match supply and demand to make customers happy and maximize profitability. 
# MAGIC
# MAGIC For more information about the PySpark ML SparkXGBRegressor estimator used in this notebook, see [Xgboost SparkXGBRegressor API](https://xgboost.readthedocs.io/en/stable/python/python_api.html#xgboost.spark.SparkXGBRegressor).
# MAGIC
# MAGIC ## Requirements
# MAGIC Databricks Runtime for Machine Learning 12.2 LTS ML or above.
# MAGIC
# MAGIC https://archive.ics.uci.edu/dataset/275/bike+sharing+dataset

# COMMAND ----------

pip install ucimlrepo

# COMMAND ----------

from xgboost.spark import SparkXGBRegressor

# COMMAND ----------

from ucimlrepo import fetch_ucirepo 
  
# fetch dataset 
bike_sharing = fetch_ucirepo(id=275) 
  
# data (as pandas dataframes) 
X = bike_sharing.data.features 
y = bike_sharing.data.targets 
  
# metadata 
print(bike_sharing.metadata) 
  
# variable information 
print(bike_sharing.variables) 


# COMMAND ----------

# MAGIC %md ## Load the dataset
# MAGIC The dataset is from the [UCI Machine Learning Repository](http://archive.ics.uci.edu/ml/datasets/Bike+Sharing+Dataset) and is provided with Databricks Runtime. The dataset includes information about bicycle rentals from the Capital bikeshare system in 2011 and 2012. 
# MAGIC
# MAGIC Load the data using the CSV datasource for Spark, which creates a [Spark DataFrame](http://spark.apache.org/docs/latest/sql-programming-guide.html).  

# COMMAND ----------

df = spark.read.csv("/databricks-datasets/bikeSharing/data-001/hour.csv", header="true", inferSchema="true")
# The following command caches the DataFrame in memory. This improves performance since subsequent calls to the DataFrame can read from memory instead of re-reading the data from disk.
df.cache()

# COMMAND ----------

# MAGIC %md #### Data description
# MAGIC
# MAGIC The following columns are included in the dataset:
# MAGIC
# MAGIC **Index column**:
# MAGIC * instant: record index
# MAGIC
# MAGIC **Feature columns**:
# MAGIC * dteday: date
# MAGIC * season: season (1:spring, 2:summer, 3:fall, 4:winter)
# MAGIC * yr: year (0:2011, 1:2012)
# MAGIC * mnth: month (1 to 12)
# MAGIC * hr: hour (0 to 23)
# MAGIC * holiday: 1 if holiday, 0 otherwise
# MAGIC * weekday: day of the week (0 to 6)
# MAGIC * workingday: 0 if weekend or holiday, 1 otherwise
# MAGIC * weathersit: (1:clear, 2:mist or clouds, 3:light rain or snow, 4:heavy rain or snow)  
# MAGIC * temp: normalized temperature in Celsius  
# MAGIC * atemp: normalized feeling temperature in Celsius  
# MAGIC * hum: normalized humidity  
# MAGIC * windspeed: normalized wind speed  
# MAGIC
# MAGIC **Label columns**:
# MAGIC * casual: count of casual users
# MAGIC * registered: count of registered users
# MAGIC * cnt: count of total rental bikes including both casual and registered

# COMMAND ----------

# MAGIC %md Call `display()` on a DataFrame to see a sample of the data. The first row shows that 16 people rented bikes between midnight and 1am on January 1, 2011. 

# COMMAND ----------

display(df)

# COMMAND ----------

print("The dataset has %d rows." % df.count())

# COMMAND ----------

# MAGIC %md ## Preprocess data
# MAGIC This dataset is well prepared for machine learning algorithms. The numeric input columns (temp, atemp, hum, and windspeed) are normalized, categorial values (season, yr, mnth, hr, holiday, weekday, workingday, weathersit) are converted to indices, and all of the columns except for the date (`dteday`) are numeric.
# MAGIC
# MAGIC The goal is to predict the count of bike rentals (the `cnt` column). Reviewing the dataset, you can see that some columns contain duplicate information. For example, the `cnt` column equals the sum of the `casual` and `registered` columns. You should remove the `casual` and `registered` columns from the dataset. The index column `instant` is also not useful as a predictor.
# MAGIC
# MAGIC You can also delete the column `dteday`, as this information is already included in the other date-related columns `yr`, `mnth`, and `weekday`. 

# COMMAND ----------

df = df.drop("instant").drop("dteday").drop("casual").drop("registered")
display(df)

# COMMAND ----------

# MAGIC %md Print the dataset schema to see the type of each column.

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md #### Split data into training and test sets
# MAGIC
# MAGIC Randomly split data into training and test sets. By doing this, you can train and tune the model using only the training subset, and then evaluate the model's performance on the test set to get a sense of how the model will perform on new data. 

# COMMAND ----------

# Split the dataset randomly into 70% for training and 30% for testing. Passing a seed for deterministic behavior
train, test = df.randomSplit([0.7, 0.3], seed = 0)
print("There are %d training examples and %d test examples." % (train.count(), test.count()))

# COMMAND ----------

# MAGIC %md #### Visualize the data
# MAGIC You can plot the data to explore it visually. The following plot shows the number of bicycle rentals during each hour of the day.  As you might expect, rentals are low during the night, and peak at commute hours.  
# MAGIC
# MAGIC To create plots, call `display()` on a DataFrame in Databricks and click the plot icon below the table.
# MAGIC
# MAGIC To create the plot shown, run the command in the following cell. The results appear in a table. From the drop-down menu below the table, select "Line". Click **Plot Options...**. In the dialog, drag `hr` to the **Keys** field, and drag `cnt` to the **Values** field. Also in the **Keys** field, click the "x" next to `<id>` to remove it. In the **Aggregation** drop down, select "AVG". 

# COMMAND ----------

display(train.select("hr", "cnt"))

# COMMAND ----------

# MAGIC %md ## Train the machine learning pipeline
# MAGIC
# MAGIC Now that you have reviewed the data and prepared it as a DataFrame with numeric values, you're ready to train a model to predict future bike sharing rentals. 
# MAGIC
# MAGIC Most MLlib algorithms require a single input column containing a vector of features and a single target column. The DataFrame currently has one column for each feature. MLlib provides functions to help you prepare the dataset in the required format.
# MAGIC
# MAGIC MLlib pipelines combine multiple steps into a single workflow, making it easier to iterate as you develop the model. 
# MAGIC
# MAGIC In this example, you create a pipeline using the following functions:
# MAGIC * `VectorAssembler`: Assembles the feature columns into a feature vector.
# MAGIC * `VectorIndexer`: Identifies columns that should be treated as categorical. This is done heuristically, identifying any column with a small number of distinct values as categorical. In this example, the following columns are considered categorical: `yr` (2 values), `season` (4 values), `holiday` (2 values), `workingday` (2 values), and `weathersit` (4 values).
# MAGIC * `SparkXGBRegressor`: Uses the [SparkXGBRegressor](https://xgboost.readthedocs.io/en/stable/python/python_api.html#xgboost.spark.SparkXGBRegressor) estimator to learn how to predict rental counts from the feature vectors.
# MAGIC * `CrossValidator`: The XGBoost regression algorithm has several hyperparameters. This notebook illustrates how to use [hyperparameter tuning in Spark](https://spark.apache.org/docs/latest/ml-tuning.html). This capability automatically tests a grid of hyperparameters and chooses the best resulting model.
# MAGIC
# MAGIC For more information:  
# MAGIC [VectorAssembler](https://spark.apache.org/docs/latest/ml-features.html#vectorassembler)  
# MAGIC [VectorIndexer](https://spark.apache.org/docs/latest/ml-features.html#vectorindexer)  

# COMMAND ----------

# MAGIC %md The first step is to create the VectorAssembler and VectorIndexer steps. 

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler, VectorIndexer
import mlflow
mlflow.xgboost.autolog()

# Remove the target column from the input feature set.
featuresCols = df.columns
featuresCols.remove('cnt')

# vectorAssembler combines all feature columns into a single feature vector column, "rawFeatures".
vectorAssembler = VectorAssembler(inputCols=featuresCols, outputCol="rawFeatures")

# vectorIndexer identifies categorical features and indexes them, and creates a new column "features". 
vectorIndexer = VectorIndexer(inputCol="rawFeatures", outputCol="features", maxCategories=4)

# COMMAND ----------

# MAGIC %md Next, define the model. To use distributed training, set `num_workers` to the number of Spark tasks you want to concurrently run during training xgboost model. To use the all Spark task slots, set `num_workers=sc.defaultParallelism`.

# COMMAND ----------

from xgboost.spark import SparkXGBRegressor

# The next step is to define the model training stage of the pipeline. 
# The following command defines a XgboostRegressor model that takes an input column "features" by default and learns to predict the labels in the "cnt" column.
# Set `num_workers` to the number of Spark tasks you want to concurrently run during training xgboost model.
xgb_regressor = SparkXGBRegressor(num_workers=sc.defaultParallelism, label_col="cnt", missing=0.0)
 

# COMMAND ----------

# MAGIC %md
# MAGIC fs.log_model(
# MAGIC             model=xgb_model,
# MAGIC             artifact_path=f"{model_name}_artifact_path",
# MAGIC             flavor=mlflow.xgboost,
# MAGIC             training_set = training_set ,
# MAGIC             registered_model_name = model_name
# MAGIC         ) 

# COMMAND ----------

# MAGIC %md The third step is to wrap the model you just defined in a `CrossValidator` stage. `CrossValidator` calls the XgboostRegressor estimator with different hyperparameter settings. It trains multiple models and selects the best one, based on minimizing a specified metric. In this example, the metric is [root mean squared error (RMSE)](https://en.wikipedia.org/wiki/Root-mean-square_deviation).
# MAGIC

# COMMAND ----------

from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator

# Define a grid of hyperparameters to test:
#  - maxDepth: maximum depth of each decision tree 
#  - maxIter: iterations, or the total number of trees 
paramGrid = ParamGridBuilder()\
  .addGrid(xgb_regressor.max_depth, [2, 5])\
  .addGrid(xgb_regressor.n_estimators, [10, 100])\
  .build()

# Define an evaluation metric.  The CrossValidator compares the true labels with predicted values for each combination of parameters, and calculates this value to determine the best model.
evaluator = RegressionEvaluator(metricName="rmse",
                                labelCol=xgb_regressor.getLabelCol(),
                                predictionCol=xgb_regressor.getPredictionCol())

# Declare the CrossValidator, which performs the model tuning.
cv = CrossValidator(estimator=xgb_regressor, evaluator=evaluator, estimatorParamMaps=paramGrid)

# COMMAND ----------

# MAGIC %md Create the pipeline.

# COMMAND ----------

from pyspark.ml import Pipeline
pipeline = Pipeline(stages=[vectorAssembler, vectorIndexer, cv])

# COMMAND ----------

# MAGIC %md Train the pipeline.
# MAGIC
# MAGIC Now that you have set up the workflow, you can train the pipeline with a single call.  
# MAGIC When you call `fit()`, the pipeline runs feature processing, model tuning, and training and returns a fitted pipeline with the best model it found.
# MAGIC This step takes several minutes.

# COMMAND ----------

pipelineModel = pipeline.fit(train)

# COMMAND ----------

# MAGIC %md ## Make predictions and evaluate results
# MAGIC
# MAGIC The final step is to use the fitted model to make predictions on the test dataset and evaluate the model's performance. The model's performance on the test dataset provides an approximation of how it is likely to perform on new data. For example, if you had weather predictions for the next week, you could predict bike rentals expected during the next week.
# MAGIC
# MAGIC Computing evaluation metrics is important for understanding the quality of predictions, as well as for comparing models and tuning parameters.

# COMMAND ----------

# MAGIC %md The `transform()` method of the pipeline model applies the full pipeline to the input dataset. The pipeline applies the feature processing steps to the dataset and then uses the fitted Xgbosot Regressor model to make predictions. The pipeline returns a DataFrame with a new column `predictions`.

# COMMAND ----------

predictions = pipelineModel.transform(test)

# COMMAND ----------

display(predictions.select("cnt", "prediction", *featuresCols))

# COMMAND ----------

# MAGIC %md A common way to evaluate the performance of a regression model is the calculate the [root mean squared error (RMSE)](https://spark.apache.org/docs/latest/mllib-evaluation-metrics.html#regression-model-evaluation). The value is not very informative on its own, but you can use it to compare different models. `CrossValidator` determines the best model by selecting the one that minimizes RMSE. 

# COMMAND ----------

rmse = evaluator.evaluate(predictions)
print("RMSE on our test set: %g" % rmse)

# COMMAND ----------

# MAGIC %md You can also plot the results, as you did the original dataset. In this case, the hourly count of rentals shows a similar shape.

# COMMAND ----------

display(predictions.select("hr", "prediction"))

# COMMAND ----------

# MAGIC %md ## Save and reload the model

# COMMAND ----------

# MAGIC %sh
# MAGIC rm -rf /dbfs/tmp/xgboost/pipeline_001
# MAGIC rm -rf /dbfs/tmp/xgboost/pipelineModel_001

# COMMAND ----------

# Save the pipeline that created the model
pipeline.save('/tmp/xgboost/pipeline_001')

# Save the model itself
pipelineModel.save('/tmp/xgboost/pipelineModel_001')

# COMMAND ----------

# Load the pipeline
loaded_pipeline = Pipeline.load('/tmp/xgboost/pipeline_001')

# COMMAND ----------

# Load and use the model
from pyspark.ml import PipelineModel

loaded_pipelineModel = PipelineModel.load('/tmp/xgboost/pipelineModel_001')

# To represent new data, use the first 3 rows of the test dataset
new_data = test.limit(3)

# Make predictions with the loaded model
new_preds = loaded_pipelineModel.transform(new_data)
display(new_preds.select("cnt", "prediction", *featuresCols))
