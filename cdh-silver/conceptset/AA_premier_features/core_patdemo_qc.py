# Databricks notebook source
# MAGIC %sql
# MAGIC use cdh_premier;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT i_o_ind,count(pat_key) as num_encounters,sum(cast((adm_mon<>disc_mon) as INTEGER)) as cross_month_encounters
# MAGIC from patdemo
# MAGIC group by i_o_ind
# MAGIC --where adm_mon<>disc_mon
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select i_o_ind,sequence,count(sequence) as visits,min(los), max(los), avg(los) from
# MAGIC (Select (cast(disc_mon_seq as INTEGER)) as sequence, los,i_o_ind
# MAGIC from patdemo)
# MAGIC where sequence>3
# MAGIC group by i_o_ind,sequence
# MAGIC order by i_o_ind,sequence
# MAGIC
# MAGIC
