// Databricks notebook source
// MAGIC %md
// MAGIC
// MAGIC # Graph Analysis with GraphFrames
// MAGIC This notebook goes over basic graph analysis using [the GraphFrames package available on spark-packages.org](http://spark-packages.org/package/graphframes/graphframes). The goal of this notebook is to show you how to use GraphFrames to perform graph analysis. You're going to be doing this with Bay area bike share data from [Kaggle](https://www.kaggle.com/benhamner/sf-bay-area-bike-share/downloads/sf-bay-area-bike-share.zip).
// MAGIC
// MAGIC ## Graph Theory and Graph Processing
// MAGIC Graph processing is an important aspect of analysis that applies to a lot of use cases. Fundamentally, graph theory and processing are about defining relationships between different nodes and edges. Nodes or vertices are the units while edges are the relationships that are defined between those.
// MAGIC
// MAGIC Some business use cases could be to look at the central people in social networks (identifying who is most popular in a group of friends), the importance of papers in bibliographic networks (determining which papers are most referenced), and ranking web pages.
// MAGIC
// MAGIC ## Graphs and Bike Trip Data
// MAGIC As mentioned, in this example you'll be using Bay area bike share data. The way you're going to orient your analysis is by making every vertex a station and each trip will become an edge connecting two stations. This creates a *directed* graph.
// MAGIC
// MAGIC ## Requirements
// MAGIC This notebook requires Databricks Runtime for Machine Learning.
// MAGIC
// MAGIC **Further Reference:**
// MAGIC * [Graph Theory on Wikipedia](https://en.wikipedia.org/wiki/Graph_theory)

// COMMAND ----------

// MAGIC %md ### Create DataFrames

// COMMAND ----------

val bikeStations = spark.sql("SELECT * FROM station_csv")
val tripData = spark.sql("SELECT * FROM trip_csv")

// COMMAND ----------

display(bikeStations)

// COMMAND ----------

display(tripData)

// COMMAND ----------

// MAGIC %md It can often times be helpful to look at the exact schema to ensure that you have the right types associated with the right columns.

// COMMAND ----------

bikeStations.printSchema()
tripData.printSchema()

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Imports
// MAGIC You're going to need to import several things before you can continue. You're going to import a variety of SQL functions that are going to make working with DataFrames much easier and you're going to import everything that you're going to need from GraphFrames.

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import org.graphframes._

// COMMAND ----------

// MAGIC %md
// MAGIC ### Build the graph
// MAGIC Now that you've imported your data, you're going to need to build your graph. To do so you're going to do two things. You are going to build the structure of the vertices (or nodes) and you're going to build the structure of the edges. What's awesome about GraphFrames is that this process is incredibly simple. All that you need to do get the distinct **id** values in the Vertices table and rename the start and end stations to **src** and **dst** respectively for your edges tables. These are required conventions for vertices and edges in GraphFrames.

// COMMAND ----------

val stationVertices = bikeStations
  .distinct()

val tripEdges = tripData
  .withColumnRenamed("start_station_name", "src")
  .withColumnRenamed("end_station_name", "dst")

// COMMAND ----------

display(stationVertices)

// COMMAND ----------

display(tripEdges)

// COMMAND ----------

// MAGIC %md Now you can build your graph. 
// MAGIC
// MAGIC You're also going to cache the input DataFrames to your graph.

// COMMAND ----------

val stationGraph = GraphFrame(stationVertices, tripEdges)

tripEdges.cache()
stationVertices.cache()

// COMMAND ----------

println("Total Number of Stations: " + stationGraph.vertices.count)
println("Total Number of Trips in Graph: " + stationGraph.edges.count)
println("Total Number of Trips in Original Data: " + tripData.count)// sanity check

// COMMAND ----------

// MAGIC %md
// MAGIC ### Trips From station to station
// MAGIC One question you might ask is what the most common destinations in the dataset are for a given starting location. You can do this by performing a grouping operation and adding the edge counts together. This will yield a new graph except each edge will now be the sum of all of the semantically same edges. Think about it this way: you have a number of trips that are the exact same from station A to station B, and you want to count those up.
// MAGIC
// MAGIC The following query identifies the most common station to station trips and prints out the top 10.

// COMMAND ----------

// MAGIC %md
// MAGIC

// COMMAND ----------

val topTrips = stationGraph
  .edges
  .groupBy("src", "dst")
  .count()
  .orderBy(desc("count"))
  .limit(10)

display(topTrips)

// COMMAND ----------

// MAGIC %md You can see above that a given vertex being a Caltrain station seems to be significant. This makes sense train riders might need a way to get to their final destination after riding the train.

// COMMAND ----------

// MAGIC %md 
// MAGIC ### In degrees and out degrees
// MAGIC Remember that in this instance you've got a directed graph. That means that your trips are directional - from one location to another. Therefore you get access to a wealth of analysis that you can use. You can find the number of trips that go into a specific station and leave from a specific station.
// MAGIC
// MAGIC You can sort this information and find the stations with lots of inbound and outbound trips. Check out this definition of [Vertex Degrees](http://mathworld.wolfram.com/VertexDegree.html) for more information.
// MAGIC
// MAGIC Now that you've defined that process, go ahead and find the stations that have lots of inbound and outbound traffic.

// COMMAND ----------

val inDeg = stationGraph.inDegrees
display(inDeg.orderBy(desc("inDegree")).limit(5))

// COMMAND ----------

val outDeg = stationGraph.outDegrees
display(outDeg.orderBy(desc("outDegree")).limit(5))

// COMMAND ----------

// MAGIC %md One interesting follow up question you could ask is what is the station with the highest ratio of in degrees but fewest out degrees. As in, what station acts as almost a pure trip sink. A station where trips end at but rarely start from.

// COMMAND ----------

val degreeRatio = inDeg.join(outDeg, inDeg.col("id") === outDeg.col("id"))
  .drop(outDeg.col("id"))
  .selectExpr("id", "double(inDegree)/double(outDegree) as degreeRatio")

degreeRatio.cache()
  
display(degreeRatio.orderBy(desc("degreeRatio")).limit(10))

// COMMAND ----------

// MAGIC %md 
// MAGIC You can do something similar by getting the stations with the lowest in degrees to out degrees ratios, meaning that trips start from that station but don't end there as often. This is essentially the opposite of what you have above.

// COMMAND ----------

display(degreeRatio.orderBy(asc("degreeRatio")).limit(10))

// COMMAND ----------

// MAGIC %md The conclusions of what you get from the above analysis should be relatively straightforward. If you have a higher value, that means many more trips come into that station than out, and a lower value means that many more trips leave from that station than come into it.
