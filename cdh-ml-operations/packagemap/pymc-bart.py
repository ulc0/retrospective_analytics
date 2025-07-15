# Databricks notebook source
# MAGIC %sh
# MAGIC pip install --upgrade pip --quiet
# MAGIC pip install pipdeptree --quiet
# MAGIC pip install pycairo
# MAGIC

# COMMAND ----------

# MAGIC %sh
# MAGIC pipdeptree -p mleap -r

# COMMAND ----------

# MAGIC %sh
# MAGIC pipdeptree -p pymc,pytensor,pymc-bart,sunode

# COMMAND ----------

# MAGIC %sh
# MAGIC pipdeptree -p scikit-learn -r
