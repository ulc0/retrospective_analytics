# Databricks notebook source
#from shared.etl_utils import table_tb

# COMMAND ----------


core_fact=['patientplanofcare','patientproblem', 'patientimmunization', 'patientmedication', 'patientprocedure', 'visitdiagnosis','patientlaborder','patientresultobservation']
complex_fact=['patientnote','patientnoteproblem','patientnoteprocedure', 'patientnoteresultobservation']


# COMMAND ----------


def table_tb(tablename,schema="cdh_premier_v2"):
    byte_size = spark.sql(f"describe detail {schema}.{tablename}").select("sizeInBytes").collect()
    byte_size = (byte_size[0]["sizeInBytes"])
    kb_size = byte_size/1024
    mb_size = kb_size/1024
    gb_size = mb_size/1024
    tb_size = gb_size #/1024
    print(f"{tablename} {tb_size}")
    return tb_size

core_size=0
cmp_size=0
dim_size=0
for tbl in complex_fact:
    cmp_size+=table_tb(tbl,'cdh_abfm_phi')
for tbl in core_fact:
    core_size+=table_tb(tbl,'cdh_abfm_phi')
#for tbl in dim:
#    dim_size+=table_tb(tbl)
total_size=core_size+cmp_size+dim_size



# COMMAND ----------

print(f"Core Size: {core_size} GB, {core_size/total_size*100}%")
print(f"Complex Size: {cmp_size} GB, {cmp_size/total_size*100}%")
print(f"Total Size: {cmp_size+core_size} GB, {(cmp_size+core_size)/total_size*100}%")
print(f"Dimensions Size: {dim_size} GB, {dim_size/total_size*100}%")


# COMMAND ----------


patient_size=table_tb("fact_person","cdh_abfm_phi_ra")
encounter_size=table_tb("note_bronze","cdh_abfm_phi_ra")
fact_size=patient_size+encounter_size
print(f"Fact Table: {fact_size} GB")
print(f"Reduction: {100-(fact_size/(core_size+cmp_size)*100)}%")
