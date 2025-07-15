# Databricks notebook source
# MAGIC %sh
# MAGIC ls ../sdist

# COMMAND ----------

# MAGIC %sh
# MAGIC cd ../sdist
# MAGIC #wget -c http://geolite.maxmind.com/download/geoip/database/GeoLite2-Country.tar.gz -O - | tar -xz 
# MAGIC #wget -O pymc.tar.gz -c https://github.com/pymc-devs/pymc/archive/refs/tags/v5.3.0.tar.gz 
# MAGIC #wget -O p-c https://github.com/pymc-devs/pymc/archive/refs/tags/v5.3.0.tar.gz -O - | tar -xzm -C -no-overwrite-dir  ./pymc/
# MAGIC #wget -O pytensor.tar.gz -c https://github.com/pymc-devs/pytensor/archive/refs/heads/master.tar.gz 
# MAGIC #wget -O pymc-bart.tar.gz -c https://github.com/pymc-devs/pymc-bart/archive/refs/heads/master.tar.gz 
# MAGIC wget -O sunode.tar.gz -c  https://github.com/pymc-devs/sunode/archive/refs/heads/master.tar.gz 
# MAGIC ls -ll

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -ll ../sdist/
# MAGIC ls -ll /dbfs/src/
# MAGIC unzip -Df ../sdist/sunode.zip -d /dbfs/src/
# MAGIC ls -ll -d /dbfs/src/

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /dbfs/src
# MAGIC ls -ll
# MAGIC cd sunode-master
# MAGIC #--pip install --no-deps -e sunode-master
# MAGIC #python -m pip install -U wheel setuptools
# MAGIC ls -ll
# MAGIC python setup.py sdist bdist_wheel
# MAGIC
