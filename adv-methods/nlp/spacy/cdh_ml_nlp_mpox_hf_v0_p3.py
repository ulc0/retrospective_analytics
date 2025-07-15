# Databricks notebook source
%pip install black tokenize-rt

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id",  dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-client-id"))
spark.conf.set("fs.azure.account.oauth2.client.secret", dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-client-secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint", dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-tenant-id-endpoint"))

  
#%pip install "tensorflow>=2.0.0"


# COMMAND ----------

#%pip install --upgrade tensorflow-hub

# COMMAND ----------

#%pip install transformers

# COMMAND ----------

#%pip install torch 

# COMMAND ----------

#%pip install torchvision

# COMMAND ----------

#%pip install transformers datasets

# COMMAND ----------

# %pip install Bio-Epidemiology-NER

# COMMAND ----------

#%pip install johnsnowlabs

# COMMAND ----------

#%pip install markupsafe==2.0.1

# COMMAND ----------

#%pip install spacy

# COMMAND ----------

#%pip install scispacy

# COMMAND ----------

#%pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.1/en_core_sci_scibert-0.5.1.tar.gz

# COMMAND ----------

#%pip install protobuf==3.20.* # debug PythonException: 'TypeError: Descriptors cannot not be created directly by downgrading

# COMMAND ----------

import sparknlp
from sparknlp.base import *
from sparknlp.annotator import *

from sparknlp.common import *
from sparknlp.pretrained import ResourceDownloader

# COMMAND ----------

#spark.sql("SHOW DATABASES").show(truncate=False)
import pandas as pd
from datetime import date
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession,DataFrame
#from pyspark.sql.functions import isnan, when, count, col, desc, asc, row_number, concat_ws, coalesce,to_timestamp,regexp_replace, to_date, date_format, concat
#from pyspark.sql.functions import pandas_udf
import pyspark.sql.functions as F
from pyspark.sql.window import Window

from functools import reduce


spark.conf.set('spark.sql.shuffle.partitions',7200*4)


# COMMAND ----------

# MAGIC %md ## Tables to explore in ABFM
# MAGIC
# MAGIC 1. visitdiagnosis
# MAGIC 2. patientsocialhistoryobservation
# MAGIC 3. patientnoteresultobservation
# MAGIC 4. patientnoteprocedure
# MAGIC 5. patientnoteproblem
# MAGIC 6. patientnote
# MAGIC 7. patientproblem

# COMMAND ----------

# MAGIC %run /CDH/Analytics/ABFM_PHI/Projects/cdh-abfm-phi-core/YYYY-MM-DD_abfm_template_notebooks/includes/0000-utils-high
# MAGIC def cleanDate(date):
# MAGIC     return F.to_date(F.date_format(date, "yyyy-MM-dd"), "yyyy-MM-dd")
# MAGIC

# COMMAND ----------

#visitDiag = spark.table("cdh_abfm_phi.visitdiagnosis")
visit_diagnosis_cleaned = load_visitdiagnosis_table().where(F.col("encounterdate").between(study_period_starts,study_period_ends))
patientData = load_patient_table()

visit  = load_visit_table() #spark.table("cdh_abfm_phi.visit")
#pxSocHis = spark.table("cdh_abfm_phi.patientsocialhistoryobservation")
pxResultObs = spark.table("cdh_abfm_phi.patientresultobservation")
p#xNoteResultObs = spark.table("cdh_abfm_phi.patientnoteresultobservation")

#pxProcedure =spark.table("cdh_abfm_phi.patientprocedure")
pxProm = load_patientproblem_table() #spark.table("cdh_abfm_phi.patientproblem")
#pxNoteProb = spark.table("cdh_abfm_phi.patientnoteproblem")

#pxNote = spark.table("cdh_abfm_phi.patientnote")
pxImunizaion = load_patientimmunization_table() # spark.table("cdh_abfm_phi.patientimmunization")



# COMMAND ----------

words_to_match_mpox = (
    [
    'monkeypox', 'monkypox', 'monkey pox', 
    'monkepox pox', 'monky pox', 'monkey-pox', 
    'monke-pox', 'monky-pox', 'orthopox',
    'orthopoxvirus', 'parapoxvirus',
    'ortho pox', 'poxvi', 'monkeypox', 
    'mpox', 'm-pox', 'mpx'
    ]
)

mpox_codes = ( [
            "B04", "359811007", "359814004", "59774002", "1003839",
            "1004340", "05900", "059", "414015000",  "B0869", "B0860" ,
           # "59010"
           ] 
          )

STI_visit_codes = (
    [
        'A510',	'A511',	'A512',	'A5131',	'A5132',	'A5139',	'A5141',	'A5142',	'A5143',	'A5144',	'A5145',	'A5146',	'A5149',	'A515',	'A519',	'A5200',	'A5201',	'A5202',	'A5203',	'A5204',	'A5205',	'A5206',	'A5209',	'A5210',	'A5211',	'A5212',	'A5213',	'A5214',	'A5215',	'A5216',	'A5217',	'A5219',	'A522',	'A523',	'A5271',	'A5272',	'A5273',	'A5274',	'A5275',	'A5276',	'A5277',	'A5278',	'A5279',	'A528',	'A529',	'A530',	'A539',	'A5400',	'A5401',	'A5402',	'A5403',	'A5409',	'A541',	'A5421',	'A5422',	'A5423',	'A5424',	'A5429',	'A5440',	'A5441',	'A5442',	'A5443',	'A5449',	'A545',	'A546',	'A5481',	'A5482',	'A5483',	'A5484',	'A5485',	'A5489',	'A549',	'A55',	'A5600',	'A5601',	'A5602',	'A5609',	'A5611',	'A5619',	'A562',	'A563',	'A564',	'A568',	'A57',	'A58',	'A5900',	'A5901',	'A5902',	'A5903',	'A5909',	'A598',	'A599',	'A6000',	'A6001',	'A6002',	'A6003',	'A6004',	'A6009',	'A601',	'A609',	'A630',	'A638',	'A64',	'B0081',	'B150',	'B159',	'B160',	'B161',	'B162',	'B169',	'B170',	'B1710',	'B1711',	'B172',	'B178',	'B179',	'B180',	'B181',	'B182',	'B188',	'B189',	'B190',	'B1910',	'B1911',	'B1920',	'B1921',	'B199',	'B20',	'B251',	'B2681',	'B581',	'K7010',	'K7011',	'Z21'

    ]
)

NYC_STI = ( [
            'Z113',	'Z114',	'Z202',	'Z206'
           ] 
          )

CCRS_econ= ( [ # this would go after flagging the STI visits
            'Z550',	'Z551',	'Z552',	'Z553',	'Z554',	'Z555',	'Z558',	'Z559',	'Z560',	'Z561',	'Z562',	'Z563',	'Z564',	'Z565',	'Z566',	'Z5681',	'Z5682',	'Z5689',	'Z569',	'Z570',	'Z571',	'Z572',	'Z5731',	'Z5739',	'Z574',	'Z575',	'Z576',	'Z577',	'Z578',	'Z579',	'Z586',	'Z590',	'Z5900',	'Z5901',	'Z5902',	'Z591',	'Z592',	'Z593',	'Z594',	'Z5941',	'Z5948',	'Z595',	'Z596',	'Z597',	'Z598',	'Z59811',	'Z59812',	'Z59819',	'Z5982',	'Z5986',	'Z5987',	'Z5989',	'Z599',	'Z600',	'Z602',	'Z603',	'Z604',	'Z605',	'Z608',	'Z609',	'Z620',	'Z621',	'Z6221',	'Z6222',	'Z6229',	'Z623',	'Z626',	'Z62810',	'Z62811',	'Z62812',	'Z62813',	'Z62819',	'Z62820',	'Z62821',	'Z62822',	'Z62890',	'Z62891',	'Z62898',	'Z629',	'Z630',	'Z631',	'Z6331',	'Z6332',	'Z634',	'Z635',	'Z636',	'Z6371',	'Z6372',	'Z6379',	'Z638',	'Z639',	'Z640',	'Z641',	'Z644',	'Z650',	'Z651',	'Z652',	'Z653',	'Z654',	'Z655',	'Z658',	'Z659',
           ] 
          )

codes = mpox_codes+STI_visit_codes+NYC_STI


study_period_starts = '2022-04-01'
study_period_ends = '2023-01-01'

# COMMAND ----------


#display(patientData)

# COMMAND ----------

# MAGIC %sql 
# MAGIC describe detail cdh_abfm_phi.visit

# COMMAND ----------

# MAGIC %md ## Cleaning functions

# COMMAND ----------


def find_partial_matches(df, words_to_match, columns_to_search):
    regex_pattern = "|".join(words_to_match)
    #conditions = [F.col(col_name).rlike(pattern) for col_name in df.columns for pattern in regex_pattern]
    conditions = [F.col("concat_col_temp").rlike(pattern) for pattern in regex_pattern]
    concat_col = (
        df.select("patientuid", "clean_date", *columns_to_search,
                  F.concat(*[ F.concat(F.lit(" "),(F.lower(F.col(column)))) for column in columns_to_search],F.lit(" ")).alias("concat_col_temp")
                  )
        .where(reduce ( lambda a , b: a | b, conditions))
        )
    return concat_col


# COMMAND ----------

# MAGIC %md # Visit diagnosis

# COMMAND ----------

display(visit_diagnosis_cleaned)

# COMMAND ----------

# MAGIC %md Finding all potential population

# COMMAND ----------

visits_mpox = (
    visit_diagnosis_cleaned
    .select("patientuid", 
            F.col("encounterdate").alias("mpox_encounter"),
            F.col("encounterdiagnosiscode").alias("diag_or_proc_code"),
            F.col("encounterdiagnosistext"),
            F.col("practiceid"),
            (F.when(F.col("encounterdiagnosiscode").isin(mpox_codes),1)
             .otherwise(0)).alias("case_encounter"), 
            )
    .select("patientuid","mpox_encounter","diag_or_proc_code").where("case_encounter = 1")
    .sort("patientuid","mpox_encounter") 
    .groupBy("patientuid")
    .agg(
        (F.collect_list("mpox_encounter").getItem(0)).alias("mpox_index"),
        F.collect_list("diag_or_proc_code").alias("diag_codes")
    )
    .select("patientuid","mpox_index")   
)

#display(visits_mpox)

# COMMAND ----------

visits_STI = (
    visit_diagnosis_cleaned
    .select("patientuid", 
            F.col("encounterdate").alias("STI_encounter"),
            F.col("encounterdiagnosiscode").alias("diag_or_proc_code"),
            F.col("encounterdiagnosistext"),
            F.col("practiceid"),
            (F.when(F.col("encounterdiagnosiscode").isin(codes),1)
             .otherwise(0)).alias("STI_visit"), 
            )
    .select("patientuid","STI_encounter","diag_or_proc_code").where("STI_visit = 1")
    .sort("patientuid","STI_encounter")
    .dropDuplicates()    
)

#display(visits_STI)
#visits_STI.select("patientuid").where("diag_or_proc_code = 'B04'").dropDuplicates().count()
#visits_STI.count()


# COMMAND ----------

visits_STI.select("patientuid").where(F.col("diag_or_proc_code").isin(mpox_codes)).dropDuplicates().count()

# COMMAND ----------

visit_STI_record = (
    visits_STI.select("patientuid", "STI_encounter")
    .join(
        visit_diagnosis_cleaned
        .select("patientuid", 
            F.col("encounterdate").alias("service_date"),
            F.col("encounterdiagnosiscode").alias("diag_or_proc_code"),
            F.col("encounterdiagnosistext").alias("problemcomment"),
            F.col("practiceid"),            
            ),
        ['patientuid']
        )
    .where(F.abs(F.datediff(F.col("STI_encounter"),F.col("service_date")))<=365)
    #.where(F.abs(F.datediff(F.col("STI_encounter"),F.col("service_date")))<=30)
    .withColumn("STI_other_visit",
                F.when(F.col("diag_or_proc_code").isin(codes),1).otherwise(0) # check with lara if this definition would make sense or if I should match the dates
                )
    .withColumn("mpox_visit",
                F.when(F.col("diag_or_proc_code").isin(mpox_codes),1).otherwise(0)
                )
    .withColumn("socio_economic_char",
                F.when(F.col("diag_or_proc_code").isin(CCRS_econ),1).otherwise(0)
                )
    .withColumn("mpox_date",
                F.when(F.col("mpox_visit")==1, F.col("service_date")).otherwise(None)
                )
    .sort("patientuid","service_date")
    .where("STI_other_visit = 1 or mpox_visit = 1")
    .dropDuplicates()
    .drop("service_date")
    #.groupBy("patientuid")
    #.agg(
    #    F.array_distinct(F.collect_list("service_date")).alias("service_dates"),
    #    F.array_distinct(F.collect_list("diag_or_proc_code")).alias("dx_px_s"),
    #    F.array_distinct(F.collect_list("problemcomment")).alias("diagnosistext"),
    #    F.array_distinct(F.collect_list("practiceid")).alias("practiceids"),
    #    F.array_distinct(F.collect_list("STI_other_visit")).alias("STI_other_visit"),
    #    F.array_distinct(F.collect_list("mpox_visit")).alias("mpox_visits"),
    #    F.array_distinct(F.collect_list("mpox_date")).alias("mpox_dates"),
    #    F.array_distinct(F.collect_list("socio_economic_char")).alias("socio_economic_chars")
    #)
    #.filter(array_contains(col("some_arr"), "one"))  
)

#display(visit_STI_record)
#visit_STI_record.select("patientuid").dropDuplicates().count()

# COMMAND ----------

# MAGIC %md # patientproblem and patientptoblemnote

# COMMAND ----------

#display(pxProm)

# COMMAND ----------

patientProb_filtered =   (
     pxProm
        .select(
            "patientuid",
            cleanDate("documentationdate").alias("STI_encounter"),
            F.col("problemcode").alias("diag_or_proc_code"),
            (concat_ws('_', "problemstatuscode","problemcomment", "problemtext")).alias("target_col")
            )
        .withColumn("diag_or_proc_code", F.regexp_replace("diag_or_proc_code",'\.',''))
        .withColumn("STI_visit",
                   F.when((F.col("diag_or_proc_code").isin(codes)) & (F.col("STI_encounter") >= study_period_starts)
                   ,1).otherwise(0)                   
                   )
        .where("STI_visit = 1")
        .select("patientuid","STI_encounter","diag_or_proc_code")
        .sort("patientuid","STI_encounter")
        .dropDuplicates()        
        #.groupBy("patientuid")
        #.agg(
        #    F.collect_list("index_date_temp").getItem(0).alias("index_date"),
        #F.collect_set("diag_or_proc_code").alias("index_diag"),
        #)   
        
        #.sort("patientuid","clean_date")
        )
        # need to add conditions to characterize mpox and STI independently
    
#display(patientProb_filtered.where("target_col like '%pox%'"))
#display(patientProb_filtered)

# COMMAND ----------

patientProblemRecord = (
    patientProb_filtered.select("patientuid", "STI_encounter")
    .join(
        pxProm
        .select("patientuid", 
            cleanDate(F.col("documentationdate")).alias("service_date"),
            (F.regexp_replace("problemcode",'\.','')).alias("diag_or_proc_code"),
            F.col("problemcomment"),
            F.col("practiceid"),            
            ),
        ['patientuid']
        )
    #.where(F.abs(F.datediff(F.col("STI_encounter"),F.col("service_date")))<=30)
    .where(F.abs(F.datediff(F.col("STI_encounter"),F.col("service_date")))<=365)
    .withColumn("STI_other_visit",
                F.when(F.col("diag_or_proc_code").isin(codes),1).otherwise(0) # check with lara if this definition would make sense or if I should match the dates
                )
    .withColumn("mpox_visit",
                F.when(F.col("diag_or_proc_code").isin(mpox_codes),1).otherwise(0)
                )
    .withColumn("socio_economic_char",
                F.when(F.col("diag_or_proc_code").isin(CCRS_econ),1).otherwise(0)
                )
    .withColumn("mpox_date",
                F.when(F.col("mpox_visit")==1, F.col("service_date")).otherwise(None)
                )
    .sort("patientuid","service_date")
    .where("STI_other_visit = 1 or mpox_visit = 1")
    .drop("service_date")
    #.groupBy("patientuid","index_date")
    #.agg(
    #    F.array_distinct(F.collect_list("service_date")).alias("service_dates"),
    #    F.array_distinct(F.collect_list("diag_or_proc_code")).alias("dx_px_s"),
    #    F.array_distinct(F.collect_list("problemcomment")).alias("diagnosistext"),
    #    F.array_distinct(F.collect_list("practiceid")).alias("practiceids"),
    #    F.array_distinct(F.collect_list("STI_other_visit")).alias("STI_other_visit"),
    #    F.array_distinct(F.collect_list("mpox_visit")).alias("mpox_visits"),
    #    F.array_distinct(F.collect_list("mpox_date")).alias("mpox_dates"),
    #    F.array_distinct(F.collect_list("socio_economic_char")).alias("socio_economic_chars")
    #)
    #.filter(array_contains(col("some_arr"), "one"))  
)

#display(patientProblemRecord)
#patientProblemRecord.select("patientuid").dropDuplicates().count()

# COMMAND ----------

# putting together Visit_STI_record and patientProblemRecord

targetPopulation = (
    visit_STI_record.unionByName(patientProblemRecord).dropDuplicates()
)
targetPopulation.cache()
#display(targetPopulation)

# COMMAND ----------

targetPopulation.select("patientuid").where("mpox_visit = 1").dropDuplicates().count()

# COMMAND ----------

# MAGIC %md # Case control matching

# COMMAND ----------

display(targetPopulation)
targetPopulation.select("patientuid").dropDuplicates().count()

# COMMAND ----------

case_contol =  (
    targetPopulation
    .sort("patientuid","STI_encounter")
    .groupBy("patientuid","STI_encounter")
    .agg(
        #F.array_distinct(F.collect_list("STI_encounter")).alias("STI_encounters"),
        F.array_distinct(F.collect_list("diag_or_proc_code")).alias("dx_px_s"),
        F.array_distinct(F.collect_list("problemcomment")).alias("diagnosistext"),
        F.array_distinct(F.collect_list("practiceid")).alias("practiceids"),
        (F.max("STI_other_visit")).alias("STI_other_visits"),
        (F.max("mpox_visit")).alias("exposed"),
        (F.min("mpox_date")).alias("mpox_dates"),
        (F.max("socio_economic_char")).alias("socio_economic_chars")
    )
    .select("patientuid","STI_encounter","diagnosistext","practiceids","STI_other_visits", "exposed", "mpox_dates", "socio_economic_chars")
    .join(
        patientData.select("patientuid","statecode","age_group","gender"), 
        ['patientuid'],'left'
        )
    .dropDuplicates()
)
#display(case_contol)
#patientData.printSchema()


# COMMAND ----------

# MAGIC %md ## selecting data set

# COMMAND ----------

df_exposed = (
    case_contol
    .where("exposed = 1")
    .withColumn("mpox_exposure_month_e", F.date_format("mpox_dates","yyyy-MM"))
    .withColumn("rn", F.row_number().over(Window.partitionBy("patientuid").orderBy("STI_encounter"))
                )
    .filter("rn=1")
    .drop("rn")
    
)

#display(df_exposed.where("patientuid = 'CCB68260-540C-4CC9-84D4-2E1DA3C2CD70' "))

# COMMAND ----------

df_non_exposed = (
    case_contol    
    .withColumn("mpox_dates",
               F.date_format(F.to_date(F.col("STI_encounter"),'yyyy-MM-dd'),"yyyy-MM-dd")
               )
    .withColumn(
        "rand_rn",
        F.row_number().over(Window.partitionBy("patientuid")          
                            .orderBy("proxy_exposure_day")
                            .orderBy(F.rand(seed=42))
        ),
    )
    .where("exposed = 0")
    .filter("rand_rn = 1")
    .drop("rand_rn")
    .withColumn("mpox_exposure_month_ne",
                F.date_format(F.to_date(F.col("STI_encounter"),'yyyy-MM-dd'),"yyyy-MM")
    )
    .drop("proxy_exposure_day")
    .sort("patientuid", "STI_encounter")    
)

#display(df_non_exposed)
#print(df_non_exposed.count())
#print(df_non_exposed.select("patientuid").dropDuplicates().count())

# COMMAND ----------

exposed_type = (
    df_exposed
        .select(
            F.col("patientuid").alias("case_id"),
            "age_group",          
            "gender",
            "mpox_exposure_month_e",             
            F.row_number().over(
                Window.partitionBy("age_group", "gender","mpox_exposure_month_e").orderBy("patientuid")
            ).alias("case_rn")
        )
    .withColumnRenamed("age_group", "age_group_case")
    .withColumnRenamed("gender", "gender_case")
    
)
# Assigns a unique value to assigned_person_number for every id in controls that the same age and gender, since we are matchig by exposure month and the month in which the pregnancy started, we know the "unexposed" are align with the exposed cases 
unexposed_person_number = (    
    df_non_exposed    
        .select(
            F.col("patientuid").alias("person_id"),
            "age_group",          
            "gender",
            "mpox_exposure_month_ne",  
            F.row_number().over(
                Window.partitionBy("age_group", "gender","mpox_exposure_month_ne").orderBy(F.expr("uuid()"))
            ).alias("assigned_person_number")
        )
    .withColumnRenamed("age_group", "age_group_control")
    .withColumnRenamed("gender", "gender_control")    
)

# COMMAND ----------

print(exposed_type.count())
print(exposed_type.select("case_id").dropDuplicates().count())

# COMMAND ----------

display(exposed_type.where("patientuid = 'CCB68260-540C-4CC9-84D4-2E1DA3C2CD70'"))


# COMMAND ----------

CASE_CONTROL_RATIO = 3 # 2:1

# for each covariate (age, gender, region), it assigns the first CASE_CONTROL_RATIO persons to the first case_rn
# the second CASE_CONTROL_RATIO to the second case_rn, etc.
# Since for each covariate each person gets a unique assigned_person_number, no person can end up in two groups.

exposed_unexposed_out = (
    exposed_type
        .join(
            unexposed_person_number,
            on=(
                (unexposed_person_number.age_group_control == exposed_type.age_group_case) &
                (unexposed_person_number.gender_control == exposed_type.gender_case) &
                (unexposed_person_number.mpox_exposure_month_ne   == exposed_type.mpox_exposure_month_e)  &                                          
                # mathcing on the mpox, gender and touch point for controls on the same month
                             
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

# COMMAND ----------

display(exposed_unexposed_out.orderBy("case_rn", "assigned_person_number", "case_id","person_id"))
display(eu_checker)

# COMMAND ----------

matched_temp = exposed_unexposed_out.groupBy("case_id").count().filter("count > 2").drop("count")
display(matched_temp)
matched_temp.count()

# COMMAND ----------

#### check data and start analyzing with transformers

# COMMAND ----------


temp1 = (
    exposed_unexposed_out
    .select(F.col("case_id").alias("patientuid"), "age_group_case", "gender_case","mpox_exposure_month_e")
    .withColumn("mpox_exposed", F.lit(1))    
    .join(df_exposed, ["patientuid","mpox_exposure_month_e"])
    .dropDuplicates()
)

#each of these should have a join to retrieve the infection date

temp2 = (
    exposed_unexposed_out
    .select(F.col("person_id").alias("patientuid"), "age_group_case", "gender_case", "mpox_exposure_month_ne")
    .withColumn("mpox_exposed", F.lit(0))
    .join(df_non_exposed, ["patientuid","mpox_exposure_month_ne"])
    .dropDuplicates()
)

case_cohort_matched = (
    temp1
    .withColumnRenamed("mpox_exposure_month_e","mpox_exposure_month")
    .unionByName(temp2
                 .withColumnRenamed("mpox_exposure_month_ne", "mpox_exposure_month")
                 )
    .withColumn("mpox_dates", F.to_date(F.col("mpox_dates"),"yyyy-MM-dd"))
)

#display(case_cohort_matched)

# should get 555 with all information including mpox date

# COMMAND ----------

# MAGIC %md # Patient note problem

# COMMAND ----------

# MAGIC %md # Patient note problem
# MAGIC pxNoteProbFiltered = (
# MAGIC     pxNoteProb
# MAGIC     .select(
# MAGIC         "*",
# MAGIC         (cleanDate("encounterdate")).alias("index_date_temp"),
# MAGIC         (concat_ws("_","note","group1","group2","group3","group4")).alias("target_col"), 
# MAGIC         (F.regexp_replace("group1",'\.','')).alias("diag_or_proc_code")      
# MAGIC     )
# MAGIC     #.withColumn("STI_visit",
# MAGIC     #            F.when(
# MAGIC     #                ((F.col("diag_or_proc_code").isin(codes)) & (F.col("index_date_temp") >= study_period_starts)) |                
# MAGIC     #                ((F.col("target_column").isin(codes)) & (F.col("index_date_temp") >= study_period_starts))   |
# MAGIC     #                ((F.col("target_column").rlike(words_to_match_mpox)) & (F.col("index_date_temp") >= study_period_starts))
# MAGIC     #               ,1)
# MAGIC     #            .otherwise(0)
# MAGIC     #)
# MAGIC     #.where("STI_visit = 1")
# MAGIC     .where(
# MAGIC             (F.col("diag_or_proc_code").isin(codes) & (F.col("index_date_temp") >= study_period_starts)) |
# MAGIC             (F.col("target_col").rlike("|".join(words_to_match_mpox)) & (F.col("index_date_temp") >= study_period_starts))
# MAGIC             )
# MAGIC     
# MAGIC     
# MAGIC )
# MAGIC #display(pxNoteProbFiltered)
# MAGIC

# COMMAND ----------

# MAGIC %md # patientnote

# COMMAND ----------

# MAGIC %md # Patient note problem
# MAGIC pxNote_cleanded = (
# MAGIC     pxNote.select("*",
# MAGIC                   (F.concat_ws(" ", 
# MAGIC                              F.col("sectionname"),
# MAGIC                              F.col("note"), 
# MAGIC                              F.col("group1"), 
# MAGIC                              F.col("group2"), 
# MAGIC                              F.col("group3"), 
# MAGIC                              F.col("group4"))).alias("target_col_temp"),
# MAGIC                   (cleanDate(F.col("encounterdate"))).alias("clean_date")
# MAGIC                   
# MAGIC                   )
# MAGIC     .withColumn("target_col", F.lower(deleteMarkup("target_col_temp")))
# MAGIC     .where(                      
# MAGIC            
# MAGIC             
# MAGIC              (F.col("clean_date") >= study_period_starts)
# MAGIC              )
# MAGIC             
# MAGIC             .select("patientuid", "clean_date", "target_col")
# MAGIC )
# MAGIC
# MAGIC #display(pxNote_cleanded)

# COMMAND ----------

######################################################
######################################################
# need to put the 'note' tables together and the pxproblem tables together with a left join so we have diagnosis, dates and patient notes
######################################################
######################################################

# COMMAND ----------

# MAGIC %md # Patient note problem
# MAGIC pxNote_filtered = (
# MAGIC     pxNote
# MAGIC     .select(
# MAGIC         "patientuid",        
# MAGIC         (cleanDate(F.col("encounterdate"))).alias("note_date"),
# MAGIC          F.lower(deleteMarkup((concat_ws(" ", "note")))).alias("target_col")
# MAGIC     )
# MAGIC     .where(
# MAGIC         (F.col("note_date") >= study_period_starts)
# MAGIC         )
# MAGIC )
# MAGIC
# MAGIC display(pxNote_filtered)

# COMMAND ----------

# MAGIC %md #patientnoteresultobservation 

# COMMAND ----------

# MAGIC %md # Patient note problem
# MAGIC pxResObs = spark.table("cdh_abfm_phi.patientnoteresultobservation")

# COMMAND ----------

# MAGIC %md # Patient note problem
# MAGIC pxResObsFilter = (
# MAGIC     pxResObs
# MAGIC     .select(
# MAGIC         "patientuid",
# MAGIC         cleanDate(F.col("encounterdate")).alias("note_date"),
# MAGIC         F.lower(deleteMarkup("note")).alias("target_col")
# MAGIC     )
# MAGIC     .where(                        
# MAGIC              (F.col("note_date") >= study_period_starts)
# MAGIC              
# MAGIC             )    
# MAGIC )
# MAGIC
# MAGIC #display(pxResObsFilter)

# COMMAND ----------

# MAGIC %md # Patient note problem
# MAGIC patient_notes = (
# MAGIC     pxNote_filtered.unionByName(pxResObsFilter)
# MAGIC     .dropDuplicates()
# MAGIC     .withColumnRenamed("target_col", "notes")
# MAGIC     
# MAGIC )
# MAGIC #patient_notes.cache()
# MAGIC #display(patient_notes)

# COMMAND ----------

case_cohort_matched.printSchema()
patient_notes.printSchema()

# COMMAND ----------

display(case_cohort_matched)

# COMMAND ----------

# MAGIC %md # Patient note problem
# MAGIC case_cohort_notes = (
# MAGIC     case_cohort_matched.join(patient_notes, ['patientuid'], 'left')
# MAGIC     .withColumn("note_date_diff", (F.abs(F.datediff(F.col("mpox_dates"), F.col("note_date")))))
# MAGIC     .where(
# MAGIC         "note_date_diff <= 30"
# MAGIC         )
# MAGIC )
# MAGIC
# MAGIC #display(case_cohort_notes)

# COMMAND ----------

# MAGIC %md # Patient note problem
# MAGIC case_cohort_notes.select("patientuid").dropDuplicates().count() # 22 patients with mpox do not have notes within 30 days before of after diagnosis...
# MAGIC
# MAGIC # there are 185 patients with a mpox diagnosis or pox live virus (555 including the controls)
# MAGIC
# MAGIC # out of those 163 have a patient note of more than 50 words, 
# MAGIC

# COMMAND ----------

# Patient note problem
DB_EXPLORATORY = 'cdh_abfm_phi_exploratory'
DB_NAME= 'mpoxProject_data_set_9_9_23'
USER = 'ml'

(
    case_cohort_notes
    .write
    .format('parquet')
    .mode('overwrite')
    .saveAsTable(f"{DB_EXPLORATORY}.{USER}_{DB_NAME}")
)


# COMMAND ----------

# MAGIC %md # Patient note problem
# MAGIC case_cohort_notes.count()

# COMMAND ----------

print(f"{DB_EXPLORATORY}.{DB_NAME}_{USER}")

# COMMAND ----------

collected_notes = spark.read.table(f"{DB_EXPLORATORY}.{DB_NAME}_{USER}")

display(collected_notes)

# COMMAND ----------


### END OF EDA and cohort definition

# COMMAND ----------

# DBTITLE 1,a
s = list(set(collected_notes.columns) - {'notes'}-{'note_date'}) 

collected_notes_2 = (
    collected_notes
    .sort("note_date")
    .groupBy(s)
    .agg(
        F.collect_list("note_date").alias("note_dates"),
        F.collect_list("notes").alias("notes"),
    )
)

display(collected_notes_2)

# COMMAND ----------

# display(collected_notes_2.select("patientuid", "exposed", "STI_encounter","age_group", "mpox_dates", "gender","statecode", "notes"))

# COMMAND ----------

#collected_notes_2.select("patientuid").where("exposed = 1").count()

# COMMAND ----------

#%sql 

#select * from cdh_abfm_phi_exploratory.mpox_notes_txt sort by patient_id

# COMMAND ----------

# MAGIC %md ##  Testing splitted tables 

# COMMAND ----------

root = 'cdh_abfm_phi_exploratory'


listTables = [
    'ml_patientnote',
    'ml_patientnote_htm',
    'ml_patientnote_mpox',
    'ml_patientnote_rtf',
    'ml_patientnote_utf',
    'ml_patientnote_xml'
]



# COMMAND ----------

spark.table(f'{root}.ml_patientnote_mpox').where("mpox_notes= True").count()

# COMMAND ----------

print('ml_patientnote')
spark.table(f'{root}.ml_patientnote').display()
print('ml_patientnote_htm')
spark.table(f'{root}.ml_patientnote_htm').display()
print('ml_patientnote_mpox')
spark.table(f'{root}.ml_patientnote_mpox').where("mpox_notes= True").display()
# Provider id 1814 seems to have an html format but the spliting function does not take it because it starts with plain text, think of using some other way to identify and classify such as <div> or </div> or <div (something)>

# test person_id 7747D1FB-04AA-48F4-A6A7-E239DE03211C for uniqueness of note, two records have the same note 

#411

print('ml_patientnote_rtf')
spark.table(f'{root}.ml_patientnote_rtf').display()
print('ml_patientnote_utf')
spark.table(f'{root}.ml_patientnote_utf').display()
print('ml_patientnote_xml')
spark.table(f'{root}.ml_patientnote_xml').display()


# COMMAND ----------

# DBTITLE 1,Exposed cohort
#cohort_exposed_sti = (
#    visits_STI.join(patient_notes, ['patientuid'], 'left')
#    .where(
#        F.datediff(visits_STI.STI_encounter, patient_notes.note_date).between(-15,15)
#        ) 
        #& 
        #(F.length(F.col("notes")) >= 50)
#)




#display(cohort_exposed_sti)

# COMMAND ----------

#cohort_exposed_sti.select("patientuid").dropDuplicates().count()

# COMMAND ----------

#db = "cdh_abfm_phi_exploratory"
#version_run = "STI_cohort"

#spark.sql("""DROP TABLE IF EXISTS cdh_abfm_phi_exploratory_run9_{}""".format(version_run))
#cohort_exposed_sti.write.mode("overwrite").format("parquet").saveAsTable(f"{db}.run9_{version_run}")

# COMMAND ----------

#f"{db}.run9_{version_run}"

# COMMAND ----------



# COMMAND ----------

# MAGIC %md # Testing Pretrained models

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ## Biomedical-ner-all

# COMMAND ----------

from transformers import pipeline
from transformers import AutoTokenizer, AutoModelForTokenClassification

tokenizer = AutoTokenizer.from_pretrained("d4data/biomedical-ner-all")
model = AutoModelForTokenClassification.from_pretrained("d4data/biomedical-ner-all")

pipe = pipeline("ner", model=model, tokenizer=tokenizer, aggregation_strategy="simple") # pass device=0 if using gpu



# COMMAND ----------

out = pipe("""["Patient here for annual wellness visit. Patient last visit july 25, 2022. Patient blood test may 2022. Patient home exercise no just walking 1 hour 2 x a week. Patient no exposure to HIV, no IV drug use no. Patient never smoked no. Patient denies alcohol intake no. Patient denies depression no. Patient last bone density dexa scan never. Patient has problem on bowel movement constipated worse past 6 months. patient failed on otc colace, metamucil, senokot. Patient last colonoscopy 2022 january colon polyp repeat in 3-5 years dr cremoninn. Patient last eye exam november 2021 no glaucoma. Patient flu shot october 2021. Patient pneumonia shot 2019 and 2020. Patient last prostate exam october 2021. patient covid x 2, 2021 booster september 2021 and march 2022. monkeypox vaccine august 5 #1. patient had covid + july 20, 2022. patient stopped metformin stopped metformin 6 months or sometimes with heavy meal."]""")
out

# COMMAND ----------

model.config.id2label

# COMMAND ----------

model.config

# COMMAND ----------

model

# COMMAND ----------

out = pipe(
"""ent recommendations from CDC, pre-exposure immunization is not recommended at this time for persons who do not have significant occupational exposure to monkeypox or monkeypox-infected individuals, even in the setting of significant - Pt inquires about vaccination to prevent monkeypox transmission.'""")

out

# COMMAND ----------


cleanedNotes_transformers_mpox = (
    spark.table(f'{root}.ml_patientnote_mpox').withColumn("note_datetime", cleanDate("note_datetime"))#.where("mpox_notes= True")
)


# COMMAND ----------

cleanedNotes_transformers_mpox.display()

# COMMAND ----------

selected_notes = (
    cleanedNotes_transformers_mpox.select("person_id","note_text","provider_id","mpox_notes", "note_datetime").dropDuplicates()
    .where("mpox_notes = true")
    .groupBy("person_id","note_datetime")
    .agg(
        F.collect_list("note_text").alias("daily_notes"),
        F.collect_list("provider_id").alias("providers"),
    )
)

display(selected_notes)

# COMMAND ----------

selected_notes_pdf = selected_notes.toPandas()
selected_notes_pdf

# COMMAND ----------

from Bio_Epidemiology_NER.bio_recognizer import ner_prediction

# COMMAND ----------

doc = selected_notes_pdf.iloc[7,2]

# COMMAND ----------

doc_string = ' '.join([str(elem) for elem in doc])
doc_string

# COMMAND ----------

type(doc_string)

# COMMAND ----------

temp_output = ner_prediction(corpus = doc_string, compute = 'cpu')
temp_output

# COMMAND ----------

# MAGIC %md ## Spacy

# COMMAND ----------

# sample notes

note0 = ["""Note:_<p>Willica presents today for a follow-up. She is currently working at Henderson. Her EKG was normal. She notes that she has been feeling tired lately. She notes that she is cold a lot. She notes that her heart rate is slightly elevated, and at first it was 100 beats a minute, and then when she checked it again, it was around 90 bpm. She reports that she thought she had monkeypox. She notes that bumps popped up a couple of weeks ago on her hands, face, and foot, but they went away. She notes that she still has a period, and it is heavy the first day, but the other days are normal. She denies any chance of pregnancy since her husband is getting a vasectomy. She notes that there have been no changes with her period lately. She reports that her blood pressure is normal. She is currently taking Acetazolamide. She notes that she is getting an EGD tomorrow. She notes that she has a history of cedar tumors, which are controlled. She reports that her dose of Acetazolamide has not changed recently, and she has always been on 2 twice a day. Her lipids back in January were normal, and her sugar was normal. She is not fasting right now. She had a screening test for diabetes less than a year ago, and it came back normal. She notes that her mother stresses her out before bedtime. Her dad helps take the pressure off of her that her mom puts on her. She states that her mom is not acting better. She states that she has not had her flu shot. </p>"""] 

note1 = ["Doubt monkey pox, or zoster.|Hope is not mrsa|Cover with keflex and rto 3-4 d if not responding|9/16/22  Small infection is gone.  Lump itself is not tender now, but same size.  We will refer to surgery for their opinion"]

note2 = ["""RESPIRATORY: normal breath sounds with no rales, rhonchi, wheezes or rubs; CARDIOVASCULAR: normal rate; rhythm is regular; no systolic murmur; GASTROINTESTINAL: nontender; normal bowel sounds; LYMPHATICS: no adenopathy in cervical, supraclavicular, axillary, or inguinal regions; BREAST/INTEGUMENT: no rashes or lesions; MUSCULOSKELETAL: normal gait pain with range of motion in Left wrist ormal tone; NEUROLOGIC: appropriate for age; Lab/Test Results: X-RAY INTERPRETATION: ORTHOPEDIC X-RAY: Left wrist(AP view): (+) fracture: of the distal radius"""]

sequences = note0 + note1 + note2

# COMMAND ----------

sequences_str = ' '.join(str(n) for n in sequences)

# COMMAND ----------

import scispacy
import spacy

from scispacy.linking import EntityLinker

nlp = spacy.load("en_core_sci_scibert")
text = """
#Myeloid derived suppressor cells (MDSC) are immature 
#myeloid cells with immunosuppressive activity. 
#They accumulate in tumor-bearing mice and humans 
#with different types of cancer, including hepatocellular 
#carcinoma (HCC).
#"""
doc = nlp(sequences_str)

print(list(doc.sents))

# COMMAND ----------

for ent in doc.ents:
    print(ent.text, ent.start_char, ent.end_char, ent.label_)

# COMMAND ----------

nlp.get_pipe("ner").labels

# COMMAND ----------

nlp.add_pipe("scispacy_linker", config={"resolve_abbreviations": True, "linker_name": "umls"})

doc = nlp("Spinal and bulbar muscular atrophy (SBMA) is an \
           inherited motor neuron disease caused by the expansion \
           of a polyglutamine tract within the androgen receptor (AR). \
           SBMA can be caused by this easily.")

doc


# COMMAND ----------

# MAGIC %md Testing the following task
# MAGIC
# MAGIC 1. Zero shot classification
# MAGIC 2. Name entity recognition
# MAGIC 3. Sentiment analysis
# MAGIC 4. Text classification
# MAGIC

# COMMAND ----------



# COMMAND ----------

from transformers import AutoTokenizer, AutoModel
tokenizer = AutoTokenizer.from_pretrained("sarahmiller137/distilbert-base-uncased-ft-ncbi-disease")
model = AutoModel.from_pretrained("sarahmiller137/distilbert-base-uncased-ft-ncbi-disease")


# COMMAND ----------

# MAGIC %md ## Clinical bert 

# COMMAND ----------

# MAGIC %md ### Testing for alternative zero shot (facebook/bart-large-mnli)

# COMMAND ----------

# MAGIC %md ## GatorTron-0G

# COMMAND ----------

# MAGIC %md ## PHS-BERT

# COMMAND ----------

# MAGIC %md ## DIstilled BERT

# COMMAND ----------



# COMMAND ----------

# MAGIC %md # Additional elements

# COMMAND ----------

# MAGIC %md ## Large tables for Kate - mpox

# COMMAND ----------

#display(pxNote)

pxNote_large_mpox = (
    pxNote.select("patientuid","encounterdate","note")
    .where((F.col("note").rlike("|".join(words_to_match_mpox)))
    )

    .groupBy("patientuid","encounterdate")
    .agg(
        F.collect_list("note").alias("notes")
    )
)

#display(pxNote_large_mpox.where("patientuid = '0061c058-eb95-4225-a1ab-91d3b650d6ce'"))
#pxNote_large_mpox.count()

# COMMAND ----------

#display(pxResObs)

pxResObs_large_mpox = (
    pxResObs.select("patientuid","encounterdate","note")
    .where((F.col("note").rlike("|".join(words_to_match_mpox)))
    )

    .groupBy("patientuid","encounterdate")
    .agg(
        F.collect_list("note").alias("notes")
    )
)
#display(pxResObs_large_mpox)
#pxResObs_large_mpox.count()


# COMMAND ----------

mpox_notes = pxResObs_large_mpox.unionByName(pxNote_large_mpox)
db = "cdh_abfm_phi_exploratory"
version_run1 = "mpox_large"

# COMMAND ----------



# COMMAND ----------

#spark.sql("""DROP TABLE IF EXISTS cdh_abfm_phi_exploratory_run9_{}""".format(version_run1))
#mpox_notes.write.mode("overwrite").format("parquet").saveAsTable(f"{db}.run9_{version_run1}")

# COMMAND ----------

#spark.table(f"{db}.run9_{version_run1}").display()
#spark.table(f"{db}.run9_{version_run1}").count()

# COMMAND ----------

# MAGIC %md ## All text table

# COMMAND ----------

pxNote_large = (
    pxNote.select("patientuid","encounterdate","note")
    .groupBy("patientuid","encounterdate")
    .agg(
        F.collect_list("note").alias("notes")
    )
)

#display(pxNote_large)
#pxNote_large.count()

# COMMAND ----------

pxNote_large = (
    pxResObs.select("patientuid","encounterdate","note")
    .groupBy("patientuid","encounterdate")
    .agg(
        F.collect_list("note").alias("notes")
    )
)
#display(pxNote_large)
#pxNote_large.count()

# COMMAND ----------

notes = pxNote_large.unionByName(pxNote_large)
db = "cdh_abfm_phi_exploratory"
version_run2 = "large"

# COMMAND ----------



# COMMAND ----------

#spark.sql("""DROP TABLE IF EXISTS cdh_abfm_phi_exploratory_run9_{}""".format(version_run2))
#notes.write.mode("overwrite").format("parquet").saveAsTable(f"{db}.run9_{version_run2}")

# COMMAND ----------

#spark.table(f"{db}.run9_{version_run2}").display()
#spark.table(f"{db}.run9_{version_run2}").count()


# COMMAND ----------

f"{db}.run9_{version_run2}"

# COMMAND ----------

# MAGIC %md single word array

# COMMAND ----------


db = "cdh_abfm_phi_exploratory"
version_run3 = "short_notes"

# COMMAND ----------

single_word = (
    spark.table(f"{db}.run9_{version_run2}")
    .withColumn("size", F.size("notes"))
    .where("size > 10")
)
    
#display(
#    single_word
#)

# COMMAND ----------

#spark.sql("""DROP TABLE IF EXISTS cdh_abfm_phi_exploratory_run9_{}""".format(version_run3))
#single_word.write.mode("overwrite").format("parquet").saveAsTable(f"{db}.run9_{version_run3}")

# COMMAND ----------

#f"{db}.run9_{version_run3}"

# COMMAND ----------

#single_word.count()

# COMMAND ----------

#display(spark.table("cdh_abfm_phi_exploratory.ml_fs_abfm_notes_rtf"))

# COMMAND ----------

#%sql 
#select encounterdate,note,sectionname,group1,group2,group3,group4 
#from cdh_abfm_phi.patientnote 
#where patientuid="E7C169BB-09B5-44AB-814B-E6502ED8C190"

# COMMAND ----------

#%sql 
#select encounterdate,	note_DEID
#from cdh_abfm_lds.patientnote 
#where patientuid="E7C169BB-09B5-44AB-814B-E6502ED8C190"

# COMMAND ----------



# COMMAND ----------

#%sql 
#select person_id, note_datetime, note_title,  provider_id, note_document  from cdh_abfm_phi_exploratory.ml_fs_abfm_notes_utf
