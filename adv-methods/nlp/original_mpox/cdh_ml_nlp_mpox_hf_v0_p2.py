# Databricks notebook source
spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type",
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id",  dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-client-id"))
spark.conf.set("fs.azure.account.oauth2.client.secret", dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-client-secret"))
spark.conf.set("fs.azure.account.oauth2.client.endpoint", dbutils.secrets.get(scope="dbs-scope-prod-kv-CDH", key="cdh-adb-tenant-id-endpoint"))



# COMMAND ----------

# MAGIC %md
# MAGIC import sparknlp
# MAGIC from sparknlp.base import *
# MAGIC from sparknlp.annotator import *
# MAGIC
# MAGIC from sparknlp.common import *
# MAGIC from sparknlp.pretrained import ResourceDownloader

# COMMAND ----------


import pandas as pd
from datetime import date
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession,DataFrame
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

from shared.abfm_phi_utils_high import *
def cleanDate(date):
    return F.to_date(F.date_format(date, "yyyy-MM-dd"), "yyyy-MM-dd")


# COMMAND ----------

#visitDiag = spark.table("edav_prd_cdh.cdh_abfm_phi.visitdiagnosis")
visit_diagnosis_cleaned = load_visitdiagnosis_table().where(F.col("encounterdate").between(study_period_starts,study_period_ends))
patientData = load_patient_table()

visit  = load_visit_table() #spark.table("edav_prd_cdh.cdh_abfm_phi.visit")
#pxSocHis = spark.table("edav_prd_cdh.cdh_abfm_phi.patientsocialhistoryobservation")
pxResultObs = spark.table("edav_prd_cdh.cdh_abfm_phi.patientresultobservation")
p#xNoteResultObs = spark.table("edav_prd_cdh.cdh_abfm_phi.patientnoteresultobservation")

#pxProcedure =spark.table("edav_prd_cdh.cdh_abfm_phi.patientprocedure")
pxProm = load_patientproblem_table() #spark.table("edav_prd_cdh.cdh_abfm_phi.patientproblem")
#pxNoteProb = spark.table("edav_prd_cdh.cdh_abfm_phi.patientnoteproblem")

#pxNote = spark.table("edav_prd_cdh.cdh_abfm_phi.patientnote")
pxImunizaion = load_patientimmunization_table() # spark.table("edav_prd_cdh.cdh_abfm_phi.patientimmunization")



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
# MAGIC describe detail edav_prd_cdh.cdh_abfm_phi.visit

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
