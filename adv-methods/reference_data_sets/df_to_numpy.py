# Databricks notebook source
# MAGIC %md
# MAGIC ### non pandas df to numpy for pymc Data constructor
# MAGIC `pyspark.pandas` `to_numpy()` is memory intensive and all [computation is in driver node](https://spark.apache.org/docs/3.2.1/api/python/reference/pyspark.pandas/api/pyspark.pandas.DataFrame.to_numpy.html)
# MAGIC
# MAGIC Alternative 1: spark.ml.linalg 
# MAGIC * sparkTable --> VectorAssembler --> to_numpy()
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### [pyshark VectorAssembler](https://pyshark.com/vectorassembler-in-pyspark/)

# COMMAND ----------

df = spark.createDataFrame(
    [
        (20, 1, 2, 22000),
        (25, 2, 3, 30000),
        (36, 12, 6, 70000),
    ],
    ["Age", "Experience", "Education", "Salary"]
)
df.show()

# COMMAND ----------


from pyspark.ml.feature import VectorAssembler
vecAssembler = VectorAssembler(outputCol="features")
vecAssembler.setInputCols(["Age", "Experience", "Education"])
farray=vecAssembler.transform(df).select("features")
print(farray["features"]) #collect())

# COMMAND ----------


from pyspark.ml.feature import VectorAssembler
import pyspark.sql.functions as F
assembler = VectorAssembler(
    inputCols=["Age", "Experience", "Education"],
    outputCol="features")
output = assembler.transform(df).select("features")



# COMMAND ----------
print(output)

from pyspark.ml.linalg import DenseVector
# import toArray

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler,Normalizer
# COMMAND ----------

from pyspark.ml.linalg import Vectors
from pyspark.ml.functions import vector_to_array
from pyspark.mllib.linalg import Vectors as OldVectors

# COMMAND ----------

# [Next Matrix to Array](https://api-docs.databricks.com/python/pyspark/latest/api/pyspark.ml.linalg.Matrix.html?highlight=numpy) 

