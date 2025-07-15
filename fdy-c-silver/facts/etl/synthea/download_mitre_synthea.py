# Databricks notebook source
# MAGIC %md
# MAGIC https://synthea.mitre.org/downloads

# COMMAND ----------

# MAGIC %md
# MAGIC cd /Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data
# MAGIC mkdir mitre_synthea
# MAGIC cd mitre_synthea
# MAGIC wget https://mitre.box.com/shared/static/3bo45m48ocpzp8fc0tp005vax7l93xji.gz --no-check-certificate 
# MAGIC #gunzip 3bo45m48ocpzp8fc0tp005vax7l93xji.gz
# MAGIC ls -ll

# COMMAND ----------

# MAGIC %md
# MAGIC cd /Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data
# MAGIC #mkdir mitre_synthea
# MAGIC cd mitre_synthea
# MAGIC #wget https://mitre.box.com/shared/static/3bo45m48ocpzp8fc0tp005vax7l93xji.gz --no-check-certificate 
# MAGIC #gunzip 3bo45m48ocpzp8fc0tp005vax7l93xji.gz
# MAGIC ls -ll

# COMMAND ----------

# MAGIC %md
# MAGIC the coherent database
# MAGIC https://mitre.box.com/shared/static/j0mcu7rax187h6j6gr74vjto8dchbmsp.zip
# MAGIC
# MAGIC Walonoski J, Hall D, Bates KM, Farris MH, Dagher J, Downs ME, Sivek RT, Wellner B, Gregorowicz A, Hadley M, Campion FX, Levine L, Wacome K, Emmer G, Kemmer A, Malik M, Hughes J, Granger E, Russell S. The “Coherent Data Set”: Combining Patient Data and Imaging in a Comprehensive, Synthetic Health Record. Electronics. 2022; 11(8):1199. https://doi.org/10.3390/electronics11081199

# COMMAND ----------

# MAGIC %md
# MAGIC cd /Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data
# MAGIC mkdir mitre_coherent
# MAGIC cd mitre_coherent
# MAGIC wget https://mitre.box.com/shared/static/j0mcu7rax187h6j6gr74vjto8dchbmsp.zip --no-check-certificate 
# MAGIC #unzip j0mcu7rax187h6j6gr74vjto8dchbmsp.zip
# MAGIC ls -ll
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC cd /Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data
# MAGIC mkdir mitre_sample
# MAGIC cd mitre_sample
# MAGIC wget https://synthetichealth.github.io/synthea-sample-data/downloads/latest/synthea_sample_data_csv_latest.zip --no-check-certificate 
# MAGIC #gunzip 3bo45m48ocpzp8fc0tp005vax7l93xji.gz
# MAGIC ls -ll

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data
# MAGIC #mkdir mitre_sample
# MAGIC cd mitre_sample
# MAGIC unzip synthea_sample_data_csv_latest.zip 
# MAGIC ls -ll

# COMMAND ----------

# MAGIC
# MAGIC %sh
# MAGIC cd /Volumes/edav_dev_cdh_test/dev_cdh_ml_test/data
# MAGIC cd mitre_coherent
# MAGIC unzip j0mcu7rax187h6j6gr74vjto8dchbmsp.zip
# MAGIC ls -ll
