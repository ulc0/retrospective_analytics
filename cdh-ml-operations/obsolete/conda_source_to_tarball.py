# Databricks notebook source
# MAGIC %sh
# MAGIC python --version
# MAGIC pwd

# COMMAND ----------

# MAGIC %sh
# MAGIC cd ../conda_sources
# MAGIC for i in *.bz2; do bunzip2 -d $i; done
# MAGIC #for i in *.tar; do tar -xvf $i; done
# MAGIC #bunzip2 -d /Workspace/Repos/ulc0@cdc.gov/cdh-ml-operations/conda_sources/sunode-0.4.0-py310hc515079_1.tar.bz2 
# MAGIC dir
# MAGIC # | tar xvf  --touch 
# MAGIC # | gzip -v9 > sunode-0.4.0-py39h5f960c0_1.tar.gz

# COMMAND ----------

# MAGIC %sh
# MAGIC cd ../conda_sources
# MAGIC pip install --upgrade pip
# MAGIC for srcfile in *.tar.gz; do pip install --no-deps $srcfile; done
