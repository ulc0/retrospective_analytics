# Databricks notebook source
# MAGIC %md
# MAGIC import os
# MAGIC import sys
# MAGIC import glob
# MAGIC from os.path import abspath
# MAGIC os.environ['SPARK_HOME'] = 'C:\spark-3.1.2-bin-hadoop3.2'
# MAGIC os.environ['JAVA_HOME'] = 'C:\Program Files\Java\jdk1.8.0_201'
# MAGIC os.environ['HADOOP_HOME'] = 'C:\spark-3.1.2-bin-hadoop3.2'
# MAGIC spark_python = os.path.join(os.environ.get('SPARK_HOME',None),'python')
# MAGIC py4j = glob.glob(os.path.join(spark_python,'lib','py4j-*.zip'))[0]
# MAGIC graphf = glob.glob(os.path.join(spark_python,'graphframes.zip'))[0]
# MAGIC sys.path[:0]=[spark_python,py4j]
# MAGIC sys.path[:0]=[spark_python,graphf]
# MAGIC os.environ['PYTHONPATH']=py4j+os.pathsep+graphf
# MAGIC import findspark
# MAGIC findspark.init()
# MAGIC findspark.find()
# MAGIC from pyspark.sql import SparkSession
# MAGIC spark = SparkSession.builder.appName("Spark Examples").enableHiveSupport().getOrCreate()

# COMMAND ----------

from graphframes import *

# COMMAND ----------

import networkx as nx

# COMMAND ----------

import matplotlib.pyplot as plt

# COMMAND ----------

vertices = spark.createDataFrame([
    ("Alice", 45),
    ("Jacob", 43),
    ("Roy", 21),
    ("Ryan", 49),
    ("Emily", 24),
    ("Sheldon", 52)],
    ["id", "age"]
)

# COMMAND ----------

vertices.show()

# COMMAND ----------

edges = spark.createDataFrame([("Sheldon", "Alice", "Sister"),
                              ("Alice", "Jacob", "Husband"),
                              ("Emily", "Jacob", "Father"),
                              ("Ryan", "Alice", "Friend"),
                              ("Alice", "Emily", "Daughter"),
                              ("Alice", "Roy", "Son"),
                              ("Jacob", "Roy", "Son")],
                             ["src", "dst", "relation"])

# COMMAND ----------

edges.show()

# COMMAND ----------

family_tree = GraphFrame(vertices, edges)

# COMMAND ----------

type(family_tree)

# COMMAND ----------

type(family_tree.vertices)

# COMMAND ----------

type(family_tree.edges)

# COMMAND ----------

vertices.count()

# COMMAND ----------

vertices.sort(vertices.age.asc()).show()

# COMMAND ----------

vertices.groupBy().min("age").show()

# COMMAND ----------

# the function will plot the source and destination nodes and connect them by meand of undirected line
def plot_undirected_graph(edge_list):
    # edge list contains infor,ation about edges which have clear direction
    # hoowever we will diregard the the direction in this function
    # the first thing is to plot the figure
    plt.figure(figsize=(9,9))
    # we instantiate a networkx graoh object.
    # nx. graoh represents an undirected graph.
    gplot=nx.Graph()
    for row in edge_list.select("src", "dst").take(1000):
        gplot.add_edge(row["src"], row["dst"])
    nx.draw(gplot, with_labels=True, font_weight="bold", node_size=3500)

# COMMAND ----------

plot_undirected_graph(family_tree.edges)

# COMMAND ----------

# the function will plot the source and destination nodes and connect them by meand of undirected line
def plot_directed_graph(edge_list):
    plt.figure(figsize=(9,9))
    gplot=nx.DiGraph()
    edge_labels = {}
    for row in edge_list.select("src", "dst", "relation").take(1000):
        gplot.add_edge(row["src"], row["dst"])
        edge_labels[(row["src"], row["dst"])] = row["relation"]
    pos = nx.spring_layout(gplot)
    nx.draw(gplot, pos, with_labels=True, font_weight="bold", node_size=3500)
    nx.draw_networkx_edge_labels(gplot, pos, edge_labels=edge_labels, font_color="green", font_size=11, font_weight="bold")

# COMMAND ----------

plot_directed_graph(family_tree.edges)

# COMMAND ----------

tree_degree = family_tree.degrees
tree_degree.show()

# COMMAND ----------

degree_edges = edges.filter(("src = 'Alice' or dst = 'Alice'"))
degree_edges.show()

# COMMAND ----------

plot_directed_graph(degree_edges)

# COMMAND ----------

tree_inDegree = family_tree.inDegrees
tree_inDegree.show()

# COMMAND ----------

indegree_edges = edges.filter(("dst = 'Jacob'"))
plot_directed_graph(indegree_edges)

# COMMAND ----------

tree_outDegree = family_tree.outDegrees
tree_outDegree.show()
