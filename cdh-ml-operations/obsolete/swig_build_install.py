# Databricks notebook source
# MAGIC %sh
# MAGIC ls ../packages/sdist
# MAGIC cd ../packages/src
# MAGIC #ls ../sdist
# MAGIC cp ../sdist/swig-4.1.1.tar.gz .
# MAGIC tar -m -xvf swig-4.1.1.tar.gz

# COMMAND ----------

# MAGIC %sh
# MAGIC cd src/swig-4.1.1
# MAGIC cat configure

# COMMAND ----------

# DBTITLE 1,h
# MAGIC %sh
# MAGIC cd src/swig-4.1.1
# MAGIC ./configure
# MAGIC make
# MAGIC make install
