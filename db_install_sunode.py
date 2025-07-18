# Databricks notebook source
# MAGIC %sh
# MAGIC apt update
# MAGIC apt install libsundials-dev

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /Volumes/edav_dev_cdh_test/dev_cdh_ml_test/compute/source/
# MAGIC wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
# MAGIC sha256sum Miniconda3-latest-Linux-x86_64.sh
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /Volumes/edav_dev_cdh_test/dev_cdh_ml_test/compute/source/
# MAGIC ls *.sh
# MAGIC bash Miniconda3-latest-Linux-x86_64.sh -b
# MAGIC conda -help
# MAGIC #git clone https://github.com/pymc-devs/sunode
# MAGIC #wget https://github.com/pymc-devs/sunode/archive/refs/heads/master.zip
# MAGIC # unzip master.zip
# MAGIC cd sunode-master
# MAGIC conda install --only-deps sunode
# MAGIC pip install -e .

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /Volumes/edav_dev_cdh_test/dev_cdh_ml_test/compute/source/
# MAGIC #git clone https://github.com/pymc-devs/sunode
# MAGIC #wget https://github.com/pymc-devs/sunode/archive/refs/heads/master.zip
# MAGIC # unzip master.zip
# MAGIC cd sunode-master
# MAGIC conda install --only-deps sunode
# MAGIC pip install -e .
