# Databricks notebook source
import pandas as pd
from pyspark.sql.window import Window
from datetime import date
from pyspark.sql import SparkSession
from pyspark.sql.functions import isnan, when, count, col, desc, asc, row_number, concat_ws, coalesce,to_timestamp,regexp_replace, to_date, date_format, concat, explode
import pyspark.sql.functions as F
from functools import reduce
from pyspark.sql import DataFrame

spark.conf.set('spark.sql.shuffle.partitions',7200*4)

# COMMAND ----------

#dbutils.widgets.text("output_table","edav_prd_cdh.cdh_abfm_phi_exploratory.note_silver_cdh",)
#stbl=dbutils.widgets.get("output_table")
dbutils.widgets.text("experiment_id",defaultValue="846061366474489")
EXPERIMENT_ID=dbutils.widgets.get("experiment_id")


# COMMAND ----------

import os
os.environ['PYSPARK_PIN_THREAD']='False'

import mlflow
import mlflow.data
from mlflow.data.pandas_dataset import PandasDataset
mlflow.autolog()

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

admission_sdf = spark.table("edav_dev_cdh.cdh_mimic.admissions")
d_hcpcs_sdf = spark.table("edav_dev_cdh.cdh_mimic.d_hcpcs")
d_icd_diagnoses_sdf = spark.table("edav_dev_cdh.cdh_mimic.d_icd_diagnoses")
d_icd_procedures_sdf = spark.table("edav_dev_cdh.cdh_mimic.d_icd_procedures")
discharge_sdf = spark.table("edav_dev_cdh.cdh_mimic.discharge")
discharge_detail_sdf = spark.table("edav_dev_cdh.cdh_mimic.discharge_detail")
hcpc_events_sdf = spark.table("edav_dev_cdh.cdh_mimic.hcpcsevents")
procedures_icd_sdf = spark.table("edav_dev_cdh.cdh_mimic.procedures_icd")

# COMMAND ----------

additinal_diab_codes = list(d_icd_diagnoses_sdf.where("lower(long_title) like '%diabetes%'").toPandas()["icd_code"])
DM += additinal_diab_codes

# COMMAND ----------


condition = (F.when(F.col("diag_code").isin(DM), "DM")
                .when(F.col("diag_code").isin(URTI), "URTI")
                .when(F.col("diag_code").isin(HT), "HT")
                .otherwise(None))

visit_test = (
    procedures_icd_sdf
    .withColumnRenamed("icd_code", "diag_code")
    .withColumn("diagnosis_label", condition
                )
    .withColumnRenamed("subject_id","person_id")
    .select("person_id","diagnosis_label","hadm_id")
    .withColumnRenamed("hadm_id","hadm_id_diagnosis")
    )

display( # they could have both diabetes and hypertension
    visit_test
    .select("person_id","diagnosis_label")
    .distinct()
    .groupBy("diagnosis_label")
    .count()
)

visit_test.select("person_id").where("diagnosis_label = 'DM' or diagnosis_label  = 'HT'").distinct().count()

# COMMAND ----------

# MAGIC %md ## Cleaning functions

# COMMAND ----------

def cleanDate(date):
    return F.to_date(F.date_format(date, "yyyy-MM-dd"), "yyyy-MM-dd")

# COMMAND ----------

def deleteMarkup(colNote):        
     rft = regexp_replace(colNote, r"\\[a-z]+(-?\d+)?[ ]?|\\'([a-fA-F0-9]{2})|[{}]|[\n\r]|\r\n?","") 
     xlm = regexp_replace(rft, r"<[^>]*>|&[^;]+;", "")
     font_header1 = regexp_replace(xlm, "Tahoma;;", "")
     font_header2 = regexp_replace(font_header1, "Arial;Symbol;| Normal;heading 1;", "")
     font_header3 = regexp_replace(font_header2, "Segoe UI;;", "")
     font_header4 = regexp_replace(font_header3, "MS Sans Serif;;", "")
     return font_header4       


# COMMAND ----------


def find_partial_matches(df, words_to_match, columns_to_search):
    regex_pattern = "|".join(words_to_match)
    #conditions = [F.col(col_name).rlike(pattern) for col_name in df.columns for pattern in regex_pattern]
    conditions = [F.col("concat_col_temp").rlike(pattern) for pattern in regex_pattern]
    concat_col = (
        df.select("patientuid", "clean_date", *columns_to_search,
                  concat(*[ concat(F.lit(" "),(F.lower(F.col(column)))) for column in columns_to_search],F.lit(" ")).alias("concat_col_temp")
                  )
        .where(reduce ( lambda a , b: a | b, conditions))
        )
    return concat_col


# COMMAND ----------

# MAGIC %md # Visit, Visit diagnosis, patient problem

# COMMAND ----------

procedures_icd_sdf = spark.table("edav_dev_cdh.cdh_mimic.procedures_icd")
admissions_sdf = spark.table("edav_dev_cdh.cdh_mimic.admissions") 

display(procedures_icd_sdf)
display(admissions_sdf)

# COMMAND ----------


condition = (
    F.when(F.col("diag_code").isin(HT), "HT")
    .otherwise(None)
)

visit_test = (
    procedures_icd_sdf
    .withColumnRenamed("icd_code", "diag_code")
    .withColumn("diagnosis_label", condition
                )        
    .withColumn("exposed", F.when(F.col("diagnosis_label").isNotNull(),F.lit(1)).otherwise(F.lit(0)))
    .withColumn("diagnosis_date_e", 
                F.when(F.col("diagnosis_label").isNotNull(), F.date_format("chartdate","yyyy-MM"))                
                )
    .withColumn("diagnosis_date_ne", 
                F.when(F.col("diagnosis_label").isNull(), F.date_format("chartdate","yyyy-MM"))                
                )  
    .withColumnRenamed("chartdate","diagnosis_date")
    .join(
        admissions_sdf.select("subject_id","race","insurance"), 
        ['subject_id'],
        'inner'
    )
    .dropDuplicates()
    .withColumnRenamed("subject_id","person_id")
    .select("person_id","diagnosis_label","diagnosis_date","exposed","diagnosis_date_e","diagnosis_date_ne","insurance","race")
)

display(visit_test) # format date

display( # they could have both diabetes and hypertension
    visit_test
    .select("person_id","diagnosis_label")
    .distinct()
    .groupBy("diagnosis_label")
    .count()
)

# COMMAND ----------

# finding who was ever exposed and getting their index date, for this specific condition the data scientist should verify if patient gets treated in multiple (distinct) practices
# in this case they just go to one practice

ever_exposed = (
    visit_test
    .where("exposed == 1 ")
    .orderBy("person_id", "diagnosis_date")   
    .dropDuplicates(['person_id']) 
)

display(ever_exposed)



# COMMAND ----------

non_exposed = (
    visit_test
    .where("exposed == 0 ")
    .join(
        ever_exposed.select("person_id"),
        ['person_id'],
        'leftanti'
    )
    .orderBy("person_id", "diagnosis_date")   
    .dropDuplicates(['person_id'])
    .orderBy("person_id", "diagnosis_date")
    .withColumn(
        "rand_rn",
        F.row_number().over(Window.partitionBy("person_id")          
                            .orderBy("diagnosis_date")
                            .orderBy(F.rand(seed=42))
        ),
    )
    .filter("rand_rn == 1") # selectring random encounter that would be used for matching    
    .drop("rand_rn")
    
)

display(non_exposed)
non_exposed.cache()

# COMMAND ----------

display(ever_exposed)

# COMMAND ----------

exposed_type = (
    ever_exposed
        .select(
            F.col("person_id").alias("case_id"),
            "race",          
            "insurance",
            "diagnosis_date_e",             
            F.row_number().over(
                Window.partitionBy("race", "insurance","diagnosis_date_e").orderBy("person_id")
            ).alias("case_rn")
        )
    .withColumnRenamed("insurance", "insurance_case")
    .withColumnRenamed("race", "race_case")
)



# COMMAND ----------

# Assigns a unique value to assigned_person_number for every id in controls that the same age and gender, since we are matchig by exposure month and the month in which the pregnancy started, we know the "unexposed" are align with the exposed cases 
unexposed_person_number = (    
    non_exposed    
        .select(
            F.col("person_id").alias("person_id"),
            "race",          
            "insurance",
            "diagnosis_date_ne",    
            F.row_number().over(                
                Window.partitionBy("race", "insurance","diagnosis_date_ne").orderBy(F.expr("uuid()"))
            ).alias("assigned_person_number")
        )
    .withColumnRenamed("race", "race_control")
    .withColumnRenamed("insurance", "insurance_control")    
)

display(unexposed_person_number)

# COMMAND ----------

display(exposed_type)
display(unexposed_person_number)

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ## Run case control

# COMMAND ----------

CASE_CONTROL_RATIO = 1 # 2:1

# for each covariate (age, gender, region), it assigns the first CASE_CONTROL_RATIO persons to the first case_rn
# the second CASE_CONTROL_RATIO to the second case_rn, etc.
# Since for each covariate each person gets a unique assigned_person_number, no person can end up in two groups.

exposed_unexposed_out = (
    exposed_type
        .join(
            unexposed_person_number,
            on=(
                (unexposed_person_number.race_control == exposed_type.race_case) &
                (unexposed_person_number.insurance_control == exposed_type.insurance_case) &
                (unexposed_person_number.diagnosis_date_ne   == exposed_type.diagnosis_date_e)  &                                          
                
                             
                (
                    unexposed_person_number.assigned_person_number
                    .between(CASE_CONTROL_RATIO*(F.col("case_rn") - 1)+1, CASE_CONTROL_RATIO*F.col("case_rn"))
                )
            ),
            how="inner"
        )
        .orderBy("case_id", "person_id")
)

eu_checker = (
    exposed_unexposed_out.groupBy("case_id")
).agg(
    F.max(F.col("assigned_person_number")).alias("number_of_exposed")    
).agg(
    F.min(F.col("number_of_exposed")).alias("max_min_num_exposed")
)



# COMMAND ----------

display(exposed_unexposed_out)
display(eu_checker)

# COMMAND ----------

hvid_to_remove_exposed_unexposed = (
    exposed_unexposed_out
    .groupBy("case_id")
    .agg(
        F.collect_list("person_id").alias("n_unexposed"),        
    )
    .withColumn("array_size",
             F.size("n_unexposed")
               )
    .filter(F.col("array_size") < CASE_CONTROL_RATIO)        
)

exposed_remove = hvid_to_remove_exposed_unexposed.select(F.col("case_id").alias("person_id"))
control_remove = hvid_to_remove_exposed_unexposed.select(F.col("n_unexposed").getItem(0).alias("person_id"))
hvid_remove = exposed_remove.unionByName(control_remove)

display(hvid_to_remove_exposed_unexposed)

# if this query returns empy then we had enough controls assigned to the cases
display(hvid_remove)

# COMMAND ----------

# MAGIC %md
# MAGIC Up to this point we have the ids of the cases and the controls. We need to:
# MAGIC - Match back these id to get the exposure dates
# MAGIC - Select the relevant note using a threshold for a look back period that we will use to bring the notes

# COMMAND ----------

# retrieving the information from the controls that were matched, we do not need to do this for the cases.

non_exposed_matched = (
    exposed_unexposed_out.select("person_id","diagnosis_date_ne")
    .join(
        non_exposed.alias("ne"), 
        (exposed_unexposed_out.person_id == F.col("ne.person_id")) & (exposed_unexposed_out.diagnosis_date_ne ==  F.col("ne.diagnosis_date_ne"))        
    )
    .drop("diagnosis_date_ne")
    .drop(exposed_unexposed_out.person_id )
)

display(non_exposed_matched)
non_exposed_matched.count()


# COMMAND ----------

display(non_exposed_matched.drop("diagnosis_date_ne","diagnosis_date_e"))
display(ever_exposed.drop("diagnosis_date_ne","diagnosis_date_e"))

# COMMAND ----------

all_cohort = (
    non_exposed_matched.drop("diagnosis_date_ne","diagnosis_date_e")
    .unionByName(ever_exposed.drop("diagnosis_date_ne","diagnosis_date_e"))    
).select("person_id",F.col("exposed").alias("diagnosed"))

display(all_cohort)

# COMMAND ----------

"""
all_cohort_notes = (
    all_cohort
    .join(patient_notes,         
          all_cohort.patientuid ==  patient_notes.patientuid
          , 'left'          
          )
    .drop(patient_notes.patientuid) 
    .withColumn("date_diff_diagnosis_note",                
                F.datediff(F.col("diagnosis_date"),F.col("note_date"))
                )
    .where(F.col("date_diff_diagnosis_note").between(1,30)) # one month before diagnosis, we are looking these notes for additional symtoms    
)
display(all_cohort_notes)
all_cohort_notes.count()
"""

# COMMAND ----------


DB_EXPLORATORY = 'edav_dev_cdh'
DB_NAME= 'cdh_mimic_exploratory'
TYPE_COHORT = 'hypertension_cohort'

(
    all_cohort
    .write    
    .mode('overwrite')
    .saveAsTable(f"{DB_EXPLORATORY}.{DB_NAME}.{TYPE_COHORT}")
)



# COMMAND ----------



# COMMAND ----------


