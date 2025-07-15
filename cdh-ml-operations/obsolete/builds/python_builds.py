# Databricks notebook source
# MAGIC %sh
# MAGIC cd /dbfs/FileStore
# MAGIC #mkdir pythonlibs
# MAGIC cd pythonlibs
# MAGIC #git clone https://github.com/aseyboldt/sunode
# MAGIC #wget https://anaconda.org/conda-forge/sunode/0.4.0/download/linux-64/sunode-0.4.0-py39h0502df0_1.tar.bz2
# MAGIC bzip2 -vd sunode-0.4.0-py39h0502df0_1.tar.bz2
# MAGIC tar xvf sunode-0.4.0-py39h0502df0_1.tar.bz2
# MAGIC ls
