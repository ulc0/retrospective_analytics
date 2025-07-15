# Databricks notebook source
# MAGIC %md
# MAGIC pwd
# MAGIC date -d @1681149639000
# MAGIC date -d @1655929734000

# COMMAND ----------



# COMMAND ----------

# MAGIC %sh
# MAGIC # ML  0630-181015-713pi206
# MAGIC # GPU 0630-181430-yfox4zns
# MAGIC # JOB 1026-160804-q5ul8q0n
# MAGIC # dbfs:/mnt/cluster-logs/ml/0630-181015-713pi206
# MAGIC #/dbfs/cluster-logs/mljobs/1030-130156-qtifyrvq/init_scripts
# MAGIC #/dbfs/mnt/ml/cluster_logs/0630-181015-713pi206/init_scripts
# MAGIC #cd /dbfs/cluster-logs/mljobs #/0630-181015-713pi206/init_scripts #/0630-181015-713pi206_172_18_16_138
# MAGIC #cd ./1030-130156-qtifyrvq/init_scripts/1030-130156-qtifyrvq_172_18_16_16
# MAGIC #cd ./1030-130156-qtifyrvq/init_scripts
# MAGIC cd /dbfs/mnt/ml/cluster_logs/0630-181015-713pi206/init_scripts
# MAGIC cd ./0630-181015-713pi206_172_18_16_147
# MAGIC #gpu//0630-181430-yfox4zns/init_scripts/0630-181430-yfox4zns_172_18_16_117/
# MAGIC ls -ll  -R  #| grep pymc
# MAGIC cat * #.stdout.log
# MAGIC #cat  20231030_130553_01_init_mlflow.sh.stderr.log
# MAGIC #cat mount.err
# MAGIC #cat 20230830_140218_03_init_install_blas.sh.stderr.log
# MAGIC #cat 20230821_054202_02_init_pymc_bart.sh.stderr.log
# MAGIC #cat *.stderr.log #20230803_174535_03_init_scikit_survival*
# MAGIC #cat *0230720_200505_01_init_dbfsapi.sh.stderr.log
# MAGIC #ls -llR /dbfs/packages/wheels/sci*
# MAGIC #ls /dbfs/packages/wheels/scispacy-0.5.2-py3-none-any.whl

# COMMAND ----------

# MAGIC %sh
# MAGIC # dev cd /dbfs/mnt/cluster-logs/mlgpus/0427-171812-3nvg7jah/init_scripts/0427-171812-3nvg7jah_172_18_26_101
# MAGIC #prd 1030-124022-olaht78o
# MAGIC cd /dbfs/mnt/cluster-logs/mlgpu/0630-181430-yfox4zns/ #init_scripts/0630-181430-yfox4zns_172_18_16_177
# MAGIC ls -llR 
# MAGIC cat *scispacy*
# MAGIC #ls -lrt -d -1 "$PWD"/{*,.*}  
# MAGIC cat 20230724_190018_01_init_scispacy.sh.stderr.log
# MAGIC #ls -llR /dbfs/packages/wheels/sci*
# MAGIC #ls /dbfs/packages/wheels/scispacy-0.52-py3-none-any.whl

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /dbfs/init_scripts
# MAGIC cat init_dbfsapi.sh
# MAGIC cd /dbfs/mnt/cluster-logs/ml/0630-181015-713pi206/init_scripts/0630-181015-713pi206_172_18_16_153/
# MAGIC ls -ll
# MAGIC cat /dbfs/mnt/cluster-logs/ml/0630-181015-713pi206/init_scripts/0630-181015-713pi206_172_18_16_153/*.stderr.log
# MAGIC

# COMMAND ----------

dbutils.fs.ls("dbfs:/cluster-logs/mlgpu/0630-181430-yfox4zns/init_scripts/0630-181430-yfox4zns_172_18_16_168/")

# COMMAND ----------

dbutils.fs.head("dbfs:/cluster-logs/mlgpu/0630-181430-yfox4zns/init_scripts/0630-181430-yfox4zns_172_18_16_168/20240130_144505_03_init_mlflow.sh.stderr.log")
