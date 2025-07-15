# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC ## Contact Information
# MAGIC *Who to contact for questions about this notebook?*
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC - Author: Stacy Cobb, Stacey Adjei
# MAGIC - Email: [rvz6@cdc.gov, ltq3@cdc.gov](rvz6@cdc.gov, ltq3@cdc.gov) <!--- enter your email address in both the square brackets and the parens -->

# COMMAND ----------

# MAGIC %md
# MAGIC ## Project:
# MAGIC Premier COVID-19 data tracker routin csv file creation
# MAGIC ## Notebook Description:
# MAGIC Creates CSV file to upload on TEAMS or Sharepoint page to update COVID data tracker for bi-weekly data update 		
# MAGIC
# MAGIC --1. Make sure qt or qtr are updated as needed to read in and combine all updated files in both part 1 and 2
# MAGIC 1. Update the admit_date to ~2 months earlier than the release (so, July 2021 release would mean we'd display data through May 2021)---*this code is now automated to account for 2 month lag*
# MAGIC               

# COMMAND ----------

dbutils.widgets.text('final_result_table', 'cdh_premier_exploratory.COVID_DATA_TRACKER_UC_tyf7')
final_result_table = dbutils.widgets.get('final_result_table')



# COMMAND ----------

spark.conf.set('spark.databricks.delta.checkLatestSchemaOnRead',False)

# COMMAND ----------

patdemo=spark.read.table("cdh_premier_v2.patdemo")
paticd=spark.read.table("cdh_premier_v2.paticd_diag")
patbill=spark.read.table("cdh_premier_v2.patbill")
providers=spark.read.table("cdh_premier_v2.providers")
disstat=spark.read.table("cdh_premier_v2.disstat")
chgmstr=spark.read.table("cdh_premier_v2.chgmstr")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Begin Joins/ Table manipulations

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window


# COMMAND ----------

#Description - Query patdemo for unique fields, counts of prov_id and race
#Table - patdemo: Patient demographic and hospital encounter characteristics
#Fields: (All Patdemo Fields)
# - n_prov_id: Counts of provider ID's
# - n_race_rep: Counts of distinct races
new=(
    patdemo
    .groupBy("medrec_key","pat_key","disc_mon", "admit_date", "prov_id", "i_o_ind", "pat_type", "ms_drg", "ms_drg_mdc", "point_of_origin", "adm_type", "disc_status", "mart_status", "age", "gender", "race", "hispanic_ind","admphy_spec", "adm_phy", "attphy_spec", "att_phy", "std_payor", "los", "pat_charges", "pat_cost","pat_fix_cost", "pat_var_cost", "publish_type", "quarter")
    .agg(
    F.countDistinct("race").alias("n_race_rep")
    )
    
)

    


# COMMAND ----------

display(new)

# COMMAND ----------

icd=paticd.filter("icd_code in ('U07.1','B97.29')").distinct()
icd2 = (
    icd
    .withColumn("U07_1", F.when(F.col("icd_code")== 'U07.1', 1).otherwise(0))
    .withColumn("B97_29", F.when(F.col("icd_code") == 'B97.29', 1).otherwise(0))
    .filter("icd_pri_sec in ('P','S')")
    .distinct()
)
icd3=(
    icd2
    .select("pat_key", "U07_1", "B97_29", "icd_version", "icd_code", "icd_pri_sec", "icd_poa", "year", "quarter")
    .groupBy("pat_key", "icd_version", "icd_code", "icd_pri_sec", "icd_poa", "year", "quarter")
    .agg(
        F.max("U07_1").alias("yes_u071"),
        F.max("B97_29").alias("yes_b9729")
    )
)
diagnostic=new.join(icd3, "pat_key","inner").distinct()

# COMMAND ----------

display(diagnostic)

# COMMAND ----------

diagnostic.groupBy("yes_u071","yes_b9729").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create covid population

# COMMAND ----------

# Make sure we only have records with correct ICD codes
covid=(
    diagnostic
    .filter(( (F.col("disc_mon")>=2020204) & (F.col("yes_u071")==1) ) | ( (F.col("admit_date").isin('2020-02-01','2020-02-02','2020-02-03','2020-02-04',
'2020-02-05','2020-02-06','2020-02-07','2020-02-08','2020-02-09','2020-02-10','2020-02-11','2020-02-12','2020-02-13','2020-02-14','2020-02-15','2020-02-16','2020-02-17','2020-02-18','2020-02-19','2020-02-20','2020-02-21','2020-02-22','2020-02-23','2020-02-24','2020-02-25','2020-02-26','2020-02-27','2020-02-28','2020-02-29','2020-03-01','2020-03-02','2020-03-03','2020-03-04','2020-03-05','2020-03-06','2020-03-07','2020-03-08','2020-03-09','2020-03-10','2020-03-11','2020-03-12','2020-03-13','2020-03-14','2020-03-15','2020-03-16','2020-03-17','2020-03-18','2020-03-19','2020-03-20','2020-03-21','2020-03-22','2020-03-23','2020-03-24','2020-03-25','2020-03-26','2020-03-27','2020-03-28','2020-03-29','2020-03-30','2020-03-31','2020-04-01','2020-04-02','2020-04-03','2020-04-04','2020-04-05','2020-04-06','2020-04-07','2020-04-08','2020-04-09','2020-04-10','2020-04-11','2020-04-12','2020-04-13','2020-04-14','2020-04-15','2020-04-16','2020-04-17','2020-04-18','2020-04-19','2020-04-20','2020-04-21','2020-04-22','2020-04-23','2020-04-24','2020-04-25','2020-04-26','2020-04-27','2020-04-28','2020-04-29','2020-04-30'
)) & (F.col("disc_mon").isin(2020103,2020204)) & (F.col("yes_b9729")==1)))
    .distinct()
)

covidFinal=(
    covid
    .join(providers, "prov_id","left")
    .distinct()
    .join(disstat,"disc_status","left")
    .distinct()
    .withColumn("inpatient",F.when((F.col("pat_type")==8) & (F.col("I_O_IND")=='I'),1).otherwise(0))
    .withColumn("death",F.when(F.col("disc_status").isin(20,40,41,42),1).otherwise(0))
)



# COMMAND ----------

#covidFinal.groupBy("inpatient").count().show()

# COMMAND ----------

#covidFinal.groupBy("death").count().show()
#covidFinal.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add charge master

# COMMAND ----------

covid_chgmstr =( 
    chgmstr
    .filter( (F.col("clin_sum_code").isin(110102)) | (F.col("clin_dtl_code")== 410412946570007))
)

# COMMAND ----------

# Inner join patbill with chgmstr on stg_chg_code
#redoing logic of code related to patbill macros since tables are in database and there is no need to collect them

patbill_new=(
    patbill
    #.filter("(year = '2020' and quarter in c('1','2','3','4')) or (year = '2021' and quarter in c('1','2','3', '4')) or (year = '2022' and quarter in c('1'))") #removing old logic so we are not limiting data
    .filter("year>=2020")
    .join(covid_chgmstr.select("std_chg_code", "clin_sum_code", "clin_dtl_code"),"std_chg_code","inner")
)


patbill_severe=(
    patbill_new
    .withColumn("icu",F.when(F.col("clin_sum_code")== 110102, 1).otherwise(0))
    .withColumn("vent",F.when(F.col("clin_dtl_code")== 410412946570007, 1).otherwise(0))
)

severity=(
    patbill_severe
    .groupBy("pat_key")
    .agg(
        F.max("icu").alias("ICU_flag"),
        F.max("vent").alias("vent_flag")
    )
)



# COMMAND ----------

# MAGIC %md
# MAGIC ## Add severity measures

# COMMAND ----------

# Left join covidFinal and severity on pat_key and create patient level flags for vent, inpatient and death
analysis=(
    covidFinal
    .join(severity, "pat_key","left")
    .withColumn("ICU_flag",F.coalesce(F.col("ICU_flag"),F.lit(0)))
    .withColumn("vent_flag",F.coalesce(F.col("vent_flag"),F.lit(0)))
    .groupBy("pat_key", "medrec_key", "disc_mon", "admit_date", "prov_id", "i_o_ind", "pat_type", "ms_drg", "ms_drg_mdc", "point_of_origin", "adm_type", "disc_status", "mart_status", "age", "gender", "race", "hispanic_ind", "admphy_spec", "adm_phy", "attphy_spec", "att_phy", "std_payor", "los", "pat_charges", "pat_cost", "pat_fix_cost", "pat_var_cost", "publish_type", "n_race_rep", "icd_version", "icd_code", "icd_pri_sec", "icd_poa", "year", "yes_u071", "yes_b9729", "urban_rural", "teaching", "beds_grp", "prov_region", "prov_division", "cost_type", "disc_status_desc")
    .agg(
        F.max("ICU_flag").alias("ICU_pat_flag"),
        F.max("vent_flag").alias("vent_pat_flag"),
        F.max("inpatient").alias("inpatient_pat_flag"),
        F.max("death").alias("death_pat_flag")
    )
    .distinct()
)

#changed the logic here to not include the flags in the groupby statement that I am also aggregating , i.e. inpatient, death, icu_flag, vent_flag

# COMMAND ----------

# Keep only patients with clin_sum_codes of 110108/110102 and also patients with Room and Board
cc=(
    chgmstr
    .filter( (F.col("clin_sum_code").isin(110102)) & (F.col("sum_dept_desc")=='ROOM AND BOARD'))
)

# Inner join patbill with cc on std_chg_code
cc_all=(
    patbill
    .join(cc, "std_chg_code","inner")
    .distinct()
)

# Collect days in ICU and first day of ICU per patient
cc_all2=(
    cc_all
    .groupBy("pat_key")
    .agg(F.countDistinct("serv_date").alias("icudays"),
         F.min("serv_date").alias("firsticuday")
        )
    .distinct()
)    


    
    
    

# COMMAND ----------

#cc_all2.count()

# COMMAND ----------

# Left join covidAnalysis with cc_all2 on pay_key
serv_day=(
    analysis
    .join(cc_all2, "pat_key","left")
    .distinct()
)

# Only clin_dtl_codes of 410412946570007
cc_m =(
    chgmstr
    .filter("clin_dtl_code = 410412946570007")
)

# Inner join patbill with cc_m on std_chg_code
cc_all_m =(
    patbill
    .join(cc_m, "std_chg_code","inner")
)

# Collect days in ventillation and first vent day per patient
cc_all_m2=( 
    cc_all_m
    .groupBy("pat_key")
    .agg(F.countDistinct("serv_date").alias("ventdays"),
         F.min("serv_date").alias("firstventday")
         )
    .distinct()
)    
        
# Left join serv_day with cc_all_m2 on pat_key
vent_day=(
    serv_day
    .join(cc_all_m2,"pat_key","left")
    .distinct()
)


# COMMAND ----------

#cc_all_m2.agg(F.min("ventdays"),F.mean("ventdays"),F.max("ventdays")).show()

# COMMAND ----------

#vent_day.count()

# COMMAND ----------

covid_analysis_olddef=(
    vent_day
    .filter("inpatient_pat_flag=1")
)
#changed logic to filter on inpatient_pat_flag instead of inpatient since the flag was on aggregated on and no longer is in dataframe 

# COMMAND ----------

#covid_analysis_olddef.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add demographic group variables
# MAGIC

# COMMAND ----------

# Create age categories
covid_analysis_olddef=(
    covid_analysis_olddef
    .withColumn("agecat6",
                F.when(F.col("age") < 0, "unknown")
                .when(F.col("age") < 18, "0-17")
                .when(F.col("age") < 50, "18-49")
                .when(F.col("age") < 65, "50-64")
                .when(F.col("age") < 75, "65-74")
                .when(F.col("age") < 85, "75-84")
                .when(F.col("age") >=85 , "85+")
                .otherwise("unknown")
               )
      .withColumn("agecat4",
                F.when(F.col("age") < 0, "unknown")
                .when(F.col("age") < 18, "0-17")
                .when(F.col("age") < 25, "18-24")
                .when(F.col("age") < 40, "25-39")
                .when(F.col("age") < 50, "40-49")
                .when(F.col("age") < 65, "50-64")
                .when(F.col("age") < 75, "65-74")
                .when(F.col("age") >=75 , "75+")
                .otherwise("unknown")
               )
    .withColumn("raceeth2",
                F.when(F.col("hispanic_ind")=="U", "Unknown")
                .when(F.col("hispanic_ind")=="Y", "Hispanic")
                .when( (F.col("race")=='B') & (F.col("hispanic_ind")=='N'),"NH Black")
                .when( (F.col("race")=="W") & (F.col("hispanic_ind")=='N'), "NH White")
                .when( (F.col("race")=="A" ) & (F.col("hispanic_ind")=='N'), "NH Asian")
                .when( (F.col("race")=="O" )& (F.col("hispanic_ind")=='N'), "Other")
                .when( (F.col("race")=="U" ) & (F.col("hispanic_ind")=='N'), "Unknown")
                .when( F.col("n_race_rep")==1, "Unknown")
               )
    .withColumn("adm_Year",
                F.substring(F.col("admit_date"),pos=0,len=4),
               )
    .withColumn("adm_Month",
               F.substring(F.col("admit_date"),pos=6,len=2)
               )
      )
#check this 



# COMMAND ----------

display(covid_analysis_olddef)

# COMMAND ----------

#covid_analysis_olddef.groupBy("inpatient_pat_flag").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Filter out data lag 
# MAGIC
# MAGIC Exclude the last 2 months of data due to incompleteness 

# COMMAND ----------

#remove last 2 months of data set 
#.filter(F.col('effectivedate').cast('date')<=F.to_date(F.current_date()))
covid_cohort_final=(
    covid_analysis_olddef
    .withColumn("adm_date",F.to_date(F.concat(F.col("adm_Year"),F.lit('-'),F.col("adm_Month"),F.lit("-01"))))
    .withColumn("current_date",F.current_date())
    .withColumn("months2",F.add_months(F.col("current_date"),-2))
    .filter( (F.col("admit_date")>="2020-03-01") & (F.col("adm_date")<= F.col("months2")))
    .orderBy("medrec_key","admit_date")
    .withColumn("first",F.row_number().over(Window.partitionBy("medrec_key").orderBy(F.col("medrec_key"),F.col("admit_date"))))
)

covid_cohort_final_first=(
    covid_cohort_final
    .filter(F.col("first")==1)
    .distinct()
)

#removed logic of filtering inpatient again bc it has already been done once 

# COMMAND ----------

#print current months2 date
#filtered_date=
covid_cohort_final.select("months2").distinct().show()
#filtered_date=covid_cohort_final.select("months2").distinct()
#print("The most recent filtered date is " str(filtered_date))

# COMMAND ----------

#covid_cohort_final.count()

# COMMAND ----------

#covid_cohort_final_first.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upper case all column names 

# COMMAND ----------

#for col in covid_cohort_final_first.columns:
#    covid_cohort_final_first = covid_cohort_final_first.withColumnRenamed(col, col.upper())
    

# COMMAND ----------

covid_cohort_final_first.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ## Select final data frame 

# COMMAND ----------

covid_cohort_final_sdf=(
    covid_cohort_final_first
    .withColumnRenamed("year","diag_year")
    .withColumnRenamed("adm_Year","Year")
    .withColumnRenamed("adm_Month","month")
    #.withColumn("Quater",F.lit("NA"))
    #.withColumn("new_ICU_flag",F.lit("NA"))
    #.withColumn("new_vent_flag",F.lit("NA"))
    #.withColumn("NIPPV_flag",F.lit("NA"))
    #.withColumn("ARespFAil_Flag",F.lit("NA"))
    #.withColumn("LR_NIPPV_flag",F.lit("NA"))
    #.withColumn("new_icu_pat_flag",F.lit("NA"))
    #.withColumn("new_vent_pat_flag",F.lit("NA"))
    #.withColumn("NIPPV_pat_flag",F.lit("NA"))
    #.withColumn("ARespFail_pat_flag",F.lit("NA"))
    #.withColumn("LR_NIPPV_pat_flag",F.lit("NA"))
    #.withColumn("New_firsticuday",F.lit("NA"))
    #.withColumn("new_ventdays",F.lit("NA"))
    #.withColumn("new_firstventday",F.lit("NA"))
    #.withColumn("new_icudays",F.lit("NA"))
    #.withColumn("new_icu_all_flag",F.lit("NA"))
    #.withColumn("new_vent_all_flag",F.lit("NA"))
    #.withColumn("Illness_Severity_Grade",F.lit("NA"))
    #.withColumn("Illness_Severity",F.lit("NA"))
    .withColumn("inpatient", F.col("INPATIENT_PAT_FLAG"))
    .withColumn("death",F.col("DEATH_PAT_FLAG"))
    .withColumn("ICU_flag",F.col("ICU_PAT_FLAG"))
    .withColumn("vent_flag",F.col("VENT_PAT_FLAG"))
    .select('PAT_KEY','MEDREC_KEY','DISC_MON','ADMIT_DATE', 'AGE', 'GENDER','RACE','HISPANIC_IND',  'yes_u071','yes_b9729', 'inpatient','ICU_pat_flag', 'vent_pat_flag', 'inpatient_pat_flag','death_pat_flag', 'raceeth2', 'month','Year','agecat6','agecat4','death','ICU_flag','vent_flag')
)

display(covid_cohort_final_sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Check min and max dates 

# COMMAND ----------

import datetime 
date_times=(
    covid_cohort_final_sdf
    .agg(F.min("ADMIT_DATE"),
         F.max("ADMIT_DATE"),
         F.min("DISC_MON"),
         F.max("DISC_MON")
        )
)

cnt_records=covid_cohort_final_sdf.count()

display(date_times)
print("Current number of records on "+ str(datetime.datetime.now()) + " is " +str(cnt_records))

previous_run=spark.read.table('cdh_premier_exploratory.COVID_DATA_TRACKER').count()
print("Previous number of records on was " +str(previous_run))

# COMMAND ----------

# MAGIC %md
# MAGIC ##write final dataframe
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Changes compared to v1
# MAGIC - No need to write to volumes. We will do that automatically once premier is refreshed
# MAGIC - Use parameters (see first command in this notebook) - in this case the final result table. This allows the default to still write to your table of choice when you are debugging but when it runs automatically, we write and extract from a different table and you do not get into permission issues
# MAGIC

# COMMAND ----------

# DBTITLE 1,1. Write the table to the volume
# (covid_cohort_final_sdf
# .coalesce(1)
# .write
# .mode('overwrite')
# .option('header', True)
# .option('sep', ',')
# .csv('/Volumes/edav_prd_cdh/cdh_premier_exploratory/covid_data_tracker/data')
# )



# COMMAND ----------

# DBTITLE 1,2.  (optional) Save result as a delta table
(covid_cohort_final_sdf
.coalesce(1)
.write
# .format('csv')
# .mode('overwrite')
# .option('header', True)
# .option('sep', ',')
.saveAsTable(final_result_table)
)
#covid_cohort_final_sdf.write.mode("overwrite").saveAsTable("cdh_premier_exploratory.COVID_DATA_TRACKER")
