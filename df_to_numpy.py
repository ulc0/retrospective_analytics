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

from pyspark.ml.linalg import DenseVector
# import toArray
array=output.withColumn("array",DenseVector.toArray(F.col("features")))

# COMMAND ----------

npoutput=output.select("features").collect()
print((npoutput))

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler,Normalizer
Vector = VectorAssembler(inputCols=['Age','Experience'], outputCol="Vector_AE").transform(df)

# COMMAND ----------

from pyspark.ml.linalg import Vectors
from pyspark.ml.functions import vector_to_array
from pyspark.mllib.linalg import Vectors as OldVectors
df = spark.createDataFrame([
    (Vectors.dense(1.0, 2.0, 3.0), OldVectors.dense(10.0, 20.0, 30.0)),
    (Vectors.sparse(3, [(0, 2.0), (2, 3.0)]),
     OldVectors.sparse(3, [(0, 20.0), (2, 30.0)]))],
    ["vec", "oldVec"])
df.show()
df1 = df.select(vector_to_array("vec").alias("vec"),
                vector_to_array("oldVec").alias("oldVec"))
df1.collect()
df1.show()


df2 = df.select(vector_to_array("vec", "float64").alias("vec"),
                vector_to_array("oldVec", "float64").alias("oldVec"))
df2.collect()
df2.show()


df1.schema.fields


df2.schema.fields

# COMMAND ----------

# [Next Matrix to Array](https://api-docs.databricks.com/python/pyspark/latest/api/pyspark.ml.linalg.Matrix.html?highlight=numpy) 

