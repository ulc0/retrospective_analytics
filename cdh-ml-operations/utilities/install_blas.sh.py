# Databricks notebook source
# MAGIC %md
# MAGIC cd /dbfs/packages/sdist
# MAGIC wget http://www.netlib.org/blas/blas-3.11.0.tgz
# MAGIC  apt-get install libblas-dev liblapack-dev

# COMMAND ----------

# MAGIC %sh
# MAGIC OMP_NUM_THREADS=1 python check_blas.py -q
# MAGIC OMP_NUM_THREADS=2 python check_blas.py -q
