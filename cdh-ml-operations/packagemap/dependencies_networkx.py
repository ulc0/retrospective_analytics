# Databricks notebook source
# MAGIC %md
# MAGIC Future (visualize GraphFrames)[https://www.databricks.com/blog/2016/03/16/on-time-flight-performance-with-graphframes-for-apache-spark.html]
# MAGIC Future https://blog.devgenius.io/graph-modeling-in-pyspark-using-graphframes-part-1-e7cb42099182

# COMMAND ----------

#%pip install networkx  numpy==1.22 setuptools 
#%pip install git+https://github.com/mikedewar/d3py
%pip install pipgrip

# COMMAND ----------

import pipgrip
pbtree=pipgrip("pymc-bart")

# COMMAND ----------

# MAGIC %sh
# MAGIC #pip install pipgrip
# MAGIC pipgrip --tree-ascii

# COMMAND ----------

# MAGIC %md
# MAGIC #import pkg_resources
# MAGIC import networkx as nx
# MAGIC ##from pyvis.network import Network
# MAGIC import matplotlib.pyplot as plt
# MAGIC import matplotlib.image as mpimg
# MAGIC #import d3py
# MAGIC
# MAGIC NODE_SIZE_FACTOR=10 # size of node is getting smaller with depth. This is the size factor
# MAGIC
# MAGIC def build_dependencies_graph(pkg_name, depth):
# MAGIC     G = nx.DiGraph(arrows=True)
# MAGIC         
# MAGIC     def recurse(pkg_name, depth):
# MAGIC         if depth == 0:
# MAGIC             return
# MAGIC         pkg = pkg_resources.get_distribution(pkg_name)
# MAGIC         print(pkg)
# MAGIC         for i in pkg.requires():
# MAGIC             if not i.key in G:
# MAGIC                 G.add_node(i.key, specs=i.specs, size=NODE_SIZE_FACTOR*depth)
# MAGIC             G.add_edge(pkg_name, i.key, physics=True, arrowStrikethrough=True)
# MAGIC             recurse(i.key, depth-1)
# MAGIC     
# MAGIC     pkg = pkg_resources.get_distribution(pkg_name)
# MAGIC     for i in pkg.requires():
# MAGIC         G.add_node(i.key, specs=i.specs, size=NODE_SIZE_FACTOR*depth)
# MAGIC         recurse(i.key, depth-1)
# MAGIC         
# MAGIC     return G
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

 pkg = pkg_resources.get_distribution("scipy")
for requirement in pkg_resources.working_set.resolve(
pkg_resources.parse_requirements(requirements)
):
    yield Requirement(requirement)

# COMMAND ----------

  g = build_dependencies_graph("scipy", depth=10)
  print(g)
 ## net = Network(height = 1500, width = 2000, directed=True, notebook=True,cdn_resources='in_line')
 ## net.from_nx(g)
 ## net.write_html("this.html",local=True,notebook=True)
 


# COMMAND ----------

# MAGIC %md
# MAGIC #convert from `networkx` to a `pydot` graph
# MAGIC pydot_graph = nx.drawing.nx_pydot.to_pydot(g)
# MAGIC
# MAGIC # render the `pydot` by calling `dot`, no file saved to disk
# MAGIC png_str = pydot_graph.create_png(prog='dot')
# MAGIC
# MAGIC # treat the DOT output as an image file
# MAGIC sio = io.BytesIO()
# MAGIC sio.write(png_str)
# MAGIC sio.seek(0)
# MAGIC img = mpimg.imread(sio)
# MAGIC
# MAGIC # plot the image
# MAGIC imgplot = plt.imshow(img, aspect='equal')
# MAGIC plt.show()

# COMMAND ----------


#nx.draw(g)
nx.draw_networkx(g, with_labels=True)
plt.savefig("filename.png")
