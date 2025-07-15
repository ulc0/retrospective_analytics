# Databricks notebook source
# MAGIC %sh
# MAGIC sudo apt-get -y install python3.11-dev
# MAGIC sudo add-apt-repository ppa:ubuntugis/ppa
# MAGIC sudo apt-get -y update
# MAGIC sudo apt-get -y install gdal-bin
# MAGIC sudo apt-get -y install libgdal-dev
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC ogrinfo --version

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt-get -y install libgdal-dev
# MAGIC export CPLUS_INCLUDE_PATH=/usr/include/gdal
# MAGIC export C_INCLUDE_PATH=/usr/include/gda

# COMMAND ----------

# MAGIC %pip install GDAL==3.6.4
