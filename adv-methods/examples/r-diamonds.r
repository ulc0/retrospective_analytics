# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC https://docs.databricks.com/sparkr/tutorials/using-glm.html#load-diamonds-data-and-split-into-training-and-test-sets

# COMMAND ----------

require(SparkR)
require

# Read diamonds.csv dataset as SparkDataFrame
diamonds <- read.df("/databricks-datasets/Rdatasets/data-001/csv/ggplot2/diamonds.csv",
                  source = "com.databricks.spark.csv", header="true", inferSchema = "true")
diamonds <- withColumnRenamed(diamonds, "", "rowID")

# Split data into Training set and Test set
trainingData <- sample(diamonds, FALSE, 0.7)
testData <- except(diamonds, trainingData)

# Exclude rowIDs
trainingData <- trainingData[, -1]
testData <- testData[, -1]

print(count(diamonds))
print(count(trainingData))
print(count(testData))

# COMMAND ----------

head(trainingData)
# Family = "gaussian" to train a linear regression model
lrModel <- glm(price ~ ., data = trainingData, family = "gaussian")

# Print a summary of the trained model
summary(lrModel)
# Generate predictions using the trained model
predictions <- predict(lrModel, newData = testData)

# View predictions against mpg column
display(select(predictions, "price", "prediction"))

# COMMAND ----------

# Generate predictions using the trained model
predictions <- predict(lrModel, newData = testData)

# View predictions against mpg column
display(select(predictions, "price", "prediction"))

# COMMAND ----------

errors <- select(predictions, predictions$price, predictions$prediction, alias(predictions$price - predictions$prediction, "error"))
display(errors)

# Calculate RMSE
head(select(errors, alias(sqrt(sum(errors$error^2 , na.rm = TRUE) / nrow(errors)), "RMSE")))

# COMMAND ----------

# Subset data to include rows where diamond cut = "Premium" or diamond cut = "Very Good"
trainingDataSub <- subset(trainingData, trainingData$cut %in% c("Premium", "Very Good"))
testDataSub <- subset(testData, testData$cut %in% c("Premium", "Very Good"))

# COMMAND ----------

# Family = "binomial" to train a logistic regression model
logrModel <- glm(cut ~ price + color + clarity + depth, data = trainingDataSub, family = "binomial")

# Print summary of the trained model
summary(logrModel)

# COMMAND ----------

# Generate predictions using the trained model
predictionsLogR <- predict(logrModel, newData = testDataSub)

# View predictions against label column
display(select(predictionsLogR, "label", "prediction"))

# COMMAND ----------

errorsLogR <- select(predictionsLogR, predictionsLogR$label, predictionsLogR$prediction, alias(abs(predictionsLogR$label - predictionsLogR$prediction), "error"))
display(errorsLogR)
