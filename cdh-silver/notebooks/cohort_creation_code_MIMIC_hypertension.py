# Databricks notebook source
import pandas as pd
from pyspark.sql.window import Window
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, when, count, col, desc, asc, row_number, concat_ws, coalesce,to_timestamp,regexp_replace, to_date, date_format, concat, explode, rand
import pyspark.sql.functions as F
from functools import reduce
from pyspark.sql import DataFrame
from datetime import datetime

spark.conf.set('spark.sql.shuffle.partitions',7200*4)

# COMMAND ----------

catalog_name = 'edav_dev_cdh'
dbutils.widgets.text("source_table", defaultValue = "fact_patient")
dbutils.widgets.text("source_schema", defaultValue="cdh_mimic_ra")

args = dbutils.widgets.getAll()

# COMMAND ----------

input_table = spark.table(f"{catalog_name}.{args['source_schema']}.{args['source_table']}")



# COMMAND ----------

# ICD-9 codes for HT https://www.health.ny.gov/health_care/medicaid/redesign/dsrip/vbp_library/docs/htn.pdf

# Hypertension
HT = ['I10', 'I110', 'I119', 'I12', 'I120', 'I129', 'I13', 'I130', 'I131', 'I1310', 'I1311', 'I132', 'I15', 'I150', 'I151', 'I152', 'I158','I159', 'I16', 'I160', 'I161', 'I169', '4011', '4019', '40210', '40290', '40511', '40519', '40591', '40599', '4041', '4049'] 

# upper respiratory track infection ICD9, unspecified https://www2.gov.bc.ca/assets/gov/health/practitioner-pro/medical-services-plan/diag-codes_respiratory.pdf

URTI = ['J069', '4659']

# diabetes codes ICD-9 https://www.health.ny.gov/health_care/medicaid/redesign/dsrip/vbp_library/docs/diabetes.pdf

DM = [ 'E109','E119','E1010','E1101','2501','2500','E0800', 'E0801', 'E0810', 'E0811', 'E0821', 'E0822', 'E0829', 'E08311',    'E08319','E083211', 'E083212', 'E083213', 'E083219', 'E083291', 'E083292', 'E083293', 'E083299', 'E083311', 'E083312', 'E083313',    'E083319', 'E083391', 'E083392', 'E083393', 'E083399', 'E083411', 'E083412', 'E083413', 'E083419', 'E083491', 'E083492', 'E083493','E083499','E083511','E083512','E083513','E083519','E083521','E083522','E083523','E083529','E083531','E083532','E083533','E083539','E083541','E083542','E083543','E083549','E083551','E083552','E083553','E083559','E083591','E083592','E083593','E083599','E0836','E0837X1','E0837X2','E0837X3','E0837X9','E0839','E0840','E0841','E0842','E0843','E0844','E0849','E0851','E0852','E0859','E08610','E08618','E08620','E08621','E08622','E08628','E08630','E08638','E08641','E08649','E0865','E0869','E088','E089','E0900','E0901','E0910','E0911','E0921','E0922','E0929','E09311','E09319','E093211','E093212','E093213','E093219','E093291','E093292','E093293','E093299','E093311','E093312','E093313','E093319','E093391','E093392','E093393','E093399','E093411','E093412','E093413','E093419','E093491','E093492','E093493','E093499','E093511','E093512','E093513','E093519','E093521','E093522','E093523','E093529','E093531','E093532','E093533','E093539','E093541','E093542','E093543','E093549','E093551','E093552','E093553','E093559','E093591','E093592','E093593','E093599','E0936','E0937X1','E0937X2','E0937X3','E0937X9','E0939','E0940','E0941','E0942','E0943','E0944','E0949','E0951','E0952','E0959','E09610','E09618','E09620','E09621','E09622','E09628','E09630','E09638','E09641','E09649','E0965','E0969','E098','E099','E1010','E1011','E1021','E1022','E1029','E10311','E10319','E103211','E103212','E103213','E103219','E103291','E103292','E103293','E103299','E103311','E103312','E103313','E103319','E103391','E103392','E103393','E103399','E103411','E103412','E103413','E103419','E103491','E103492','E103493','E103499','E103511','E103512','E103513','E103519','E103521','E103522','E103523','E103529','E103531','E103532','E103533','E103539','E103541','E103542','E103543','E103549','E103551','E103552','E103553','E103559','E103591','E103592','E103593','E103599','E1036','E1037X1','E1037X2','E1037X3','E1037X9','E1039','E1040','E1041','E1042','E1043','E1044','E1049','E1051','E1052','E1059','E10610','E10618','E10620','E10621','E10622','E10628','E10630','E10638','E10641','E10649','E1065','E1069','E108','E109','E1100','E1101','E1110','E1111','E1121','E1122','E1129','E11311','E11319','E113211','E113212','E113213','E113219','E113291','E113292','E113293','E113299','E113311','E113312','E113313','E113319','E113391','E113392','E113393','E113399','E113411','E113412','E113413','E113419','E113491','E113492','E113493','E113499','E113511','E113512','E113513','E113519','E113521','E113522','E113523','E113529','E113531','E113532','E113533','E113539','E113541','E113542','E113543','E113549','E113551','E113552','E113553','E113559','E113591','E113592','E113593','E113599','E1136','E1137X1','E1137X2','E1137X3','E1137X9','E1139','E1140','E1141','E1142','E1143','E1144','E1149','E1151','E1152','E1159','E11610','E11618','E11620','E11621','E11622','E11628','E11630','E11638','E11641','E11649','E1165','E1169','E118','E119','E1300','E1301','E1310','E1311','E1321','E1322','E1329','E13311','E13319','E133211','E133212','E133213','E133219','E133291','E133292','E133293','E133299','E133311','E133312','E133313','E133319','E133391','E133392','E133393','E133399','E133411','E133412','E133413','E133419','E133491','E133492','E133493','E133499','E133511','E133512','E133513','E133519','E133521','E133522','E133523','E133529','E133531','E133532','E133533','E133539','E133541','E133542','E133543','E133549','E133551','E133552','E133553','E133559','E133591','E133592','E133593','E133599','E1336','E1337X1','E1337X2','E1337X3','E1337X9','E1339','E1340','E1341','E1342','E1343','E1344','E1349','E1351','E1352','E1359','E13610','E13618','E13620','E13621','E13622','E13628','E13630','E13638','E13641','E13649','E1365','E1369','E138','E139','O240','O2401','O24011','O24012','O24013','O24019','O2402','O2403','O241','O2411','O24111','O24112','O24113','O24119','O2412','O2413','O243','O2431','O24311','O24312','O24313','O24319','O2432','O2433','O248','O2481','O24811','O24812','O24813','O24819','O2482','O2483','O249','O2491','O24911','O24912','O24913','O24919','O2492','O2493', '24960', '25060', '25061', '3572', '24950', '25050', '25051', '36201', '36202', '36203', '36204', '36205', '36206', '36207', '24970', '25070', '25071', '24940', '25040', '25041', '24980', '24990', '25080', '25081', '25090', '25091', '36641', 'v4585', 'v5391', 'v6546', 'v5867', '24900', '25001', '25000']

# note: look at this paper https://www.ncbi.nlm.nih.gov/pmc/articles/PMC7192440/ seems that MIMIC has more records in heart or cardiac relaetd events. Makes sense since it seems to be focused on inpatient, ED data

# COMMAND ----------

## Creating sample cohort for OKR 11/22/2024

# COMMAND ----------

hypertension_cohort_temp = (
    input_table
    .select("person_id","code")
    .where(col("code").isin(HT))
    .select("person_id")
    .dropDuplicates()
)
hypertension_cohort_temp.cache()
display(hypertension_cohort_temp)



# COMMAND ----------

non_hypertension_cohort_temp = (
    input_table
    .select("person_id","code")    
    .join(
        hypertension_cohort_temp,
        on="person_id",
        how="leftanti"
    )  
    .select("person_id")
    .dropDuplicates()
    .limit(hypertension_cohort_temp.count())
)




# COMMAND ----------

cohort = (
    hypertension_cohort_temp
    .select("person_id")
    .unionByName(non_hypertension_cohort_temp)
)

display(cohort)

# COMMAND ----------

catalog = 'edav_dev_cdh'
output_schema= 'cdh_mimic_exploratory'
output_table = 'hypertension_cohort'

(
    cohort
    .write    
    .mode('overwrite')
    .saveAsTable(f"{catalog}.{output_schema}.{output_table}")
)



# COMMAND ----------

display(
    cohort
    .groupBy("person_id")
    .count()
    .orderBy("count", ascending = False)
)
