# Databricks notebook source
# MAGIC %md 
# MAGIC
# MAGIC ## Contact Information
# MAGIC *Who to contact for questions about this notebook?*
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC - Author: Stacy Cobb
# MAGIC - Email: [rvz6@cdc.gov](rvz6@cdc.gov) <!--- enter your email address in both the square brackets and the parens -->

# COMMAND ----------

# MAGIC %md
# MAGIC This Premier notebook is intended to define the standard definition of commonly used variables in Premier for analysis. The purpose is to bring consistency across analyses. 
# MAGIC
# MAGIC Note: Some code is written in sql and some in pyspark. 

# COMMAND ----------

# MAGIC %md
# MAGIC #Definitions

# COMMAND ----------

# MAGIC %md
# MAGIC ##inpatient

# COMMAND ----------

# MAGIC %md
# MAGIC ##expired

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE OR REPLACE TEMP VIEW PATDEMO as
# MAGIC select *,
# MAGIC case when DISC_STATUS in (20,40,41,42) then 1 else 0 end as EXPIRED ,
# MAGIC case when PAT_TYPE=8 and I_O_IND='I' then 1 else 0 end as INPATIENT,
# MAGIC case 
# MAGIC     when age<5 then '00-04'
# MAGIC     when age>=5 and age<=11 then '05-11'
# MAGIC     when age>11 and age<=17 then '12-17'
# MAGIC     when age>17 and age<=49 then '18-49'
# MAGIC     when age>49 and age<=64 then '50-64'
# MAGIC     when age>64 and age<=74 then '65-74'
# MAGIC     when age>74 and age<=84 then '75-84'
# MAGIC     when age>84 and age<=89 then '85-89'
# MAGIC     else '>90'
# MAGIC     end as COVID_AGECAT,
# MAGIC case 
# MAGIC   when hispanic_ind="U" then "Unknown"
# MAGIC   when hispanic_ind="Y" then "Hispanic"
# MAGIC   when race='B' and hispanic_ind='N' then "NH Black"
# MAGIC   when race="W" and hispanic_ind="N" then "NH White"
# MAGIC   when race="A" and hispanic_ind="N" then "NH Asian"
# MAGIC   when race="O" and hispanic_ind="N" then "Other"
# MAGIC   when race="U" and hispanic_ind="N" then "Unknown"
# MAGIC   end as RACEETH2
# MAGIC
# MAGIC from edav_prd_cdh.cdh_premier_v2.patdemo
# MAGIC

# COMMAND ----------

patdemo =  patdemo.withColumn(
        "INPATIENT",
        F.when((F.col('PAT_TYPE') == 8) & (F.col('I_O_IND') == 'I') , 1)
        .otherwise(0)
    ).withColumn("ED",
                F.when(F.col('PAT_TYPE') == 28,1).otherwise(0)
               ).withColumn(
        "EXPIRED",
        F.when(F.col('DISC_STATUS').isin(20,40,41,42),1)
        .otherwise(0))
    .withColumn(
        "age_group",
        F.when(F.col("age") < 0, "unknown")
        .when(F.col("age") <18 , "00-17")
        .when(F.col("age") < 64, "18-65")
        .otherwise("65+") 
    )
    .withColumn(
      "RACEETH2",
      F.when(F.col("hispanic_ind")=="U", "Unknown")
      .when(F.col("hispanic_ind")=="Y", "Hispanic")
      .when( (F.col("race")=='B') & (F.col("hispanic_ind"=='N'),"NH Black"))
      .when( (F.col("race")=='W') & (F.col("hispanic_ind"=='N'),"NH White"))
      .when( (F.col("race")=='A') & (F.col("hispanic_ind"=='N'),"NH Asian"))
      .when( (F.col("race")=='O') & (F.col("hispanic_ind"=='N'),"Other"))
      .when( (F.col("race")=='U') & (F.col("hispanic_ind"=='N'),"Unknown"))
    )

# COMMAND ----------

# MAGIC %md 
# MAGIC ## COVID-19 
# MAGIC
# MAGIC In Premier, COVID-19 can be defined based on lab results, snomed codes, or diagnosis codes. When defining based on diagnosis codes, the corresponding admission and discharge months should be considered.
# MAGIC
# MAGIC The definition of COVID-19 will vary depending on the project leads. Labs and codes can be used interchangeably or simulataneously. 

# COMMAND ----------

from pyspark.sql.functions import lit


icd_desc=spark.read.table("edav_prd_cdh.cdh_premier_v2.icdcode")
icd_code=spark.read.table("edav_prd_cdh.cdh_premier_v2.paticd_diag")
patlabres=spark.read.table("edav_prd_cdh.cdh_premier_v2.lab_res")
genlab=spark.read.table("edav_prd_cdh.cdh_premier_v2.genlab")
patdemo=spark.read.table("edav_prd_cdh.cdh_premier_v2.patdemo")

patdemo=patdemo.withColumnRenamed("PAT_KEY","pat_key_demo")


# COMMAND ----------

#look at diagnostic pcr tests
#####COVID TESTS#####
#look at diagnostic PCR tests (PRIMARY) and positive result 
#display(patlabres)
covid_lab=patlabres.filter("LAB_TEST_DESC in ('SARS coronavirus 2 RNA:PrThr:Pt:Respiratory:Ord:Probe.amp.tar','SARS coronavirus 2 RNA:PrThr:Pt:XXX:Ord:Probe.amp.tar')").withColumn("type",lit("pcr_primary"))
#covid_lab=patlabres.filter("LAB_TEST_DESC in ('SARS coronavirus 2 RNA:PrThr:Pt:Respiratory:Ord:Probe.amp.tar','SARS coronavirus 2 #RNA:PrThr:Pt:XXX:Ord:Probe.amp.tar') and observation='positive'").withColumn("type",lit("pcr_primary"))
display(covid_lab)

# COMMAND ----------




#Dataframe for LOINC codes for PCR Tests and lab_test_result in ()
covid_loinc_pcr = genlab.filter("lab_test_loinc_code in ('94500-6','94845-5', '94534-5', '94756-4', '94757-2', '94559-2', '94533-7', '94502-2', '92142-9', '92141-1', '94309-2', '94307-6', '94308-4', '68993-5', '95406-5', '95409-9', '96091-4', '95425-5', '94758-0', '95423-0', '96448-6') and lab_test_result='Detected'").withColumn("type",lit("loinc_pcr"))

#Dataframe for LOINC codes for PCR and Antigen Tests (last 3 are antigen)
covid_loinc_antig = genlab.filter("lab_test_loinc_code in ('96119-3', '97097-0', '95209-3') and lab_test_result='Detected'").withColumn("type",lit("loinc_antig"))


#Dataframe for Snomed codes (description)
covid_snomed = genlab.filter("specimen_source_snomed_desc = ('SARS-CoV-2 (COVID-19) Qualitative PCR') or specimen_source_snomed_code in ('NCVSD') ").withColumn("type",lit("snomed"))

#icd codes depending on admission month 
covid_dx=icd_code.join(patdemo,icd_code.PAT_KEY==patdemo.pat_key_demo,"inner").filter("ICD_PRI_SEC in ('P','S') and ((DISC_MON>=2020204 and ICD_CODE='U07.1') or (ADM_MON in (2020102, 2020103, 2020204) and DISC_MON in (2020103, 2020204) and ICD_CODE='B97.29')) ").withColumn("type",lit("dx_code"))



# COMMAND ----------

# MAGIC %md 
# MAGIC ### Venn diagram showing overlap in covid definitions  
# MAGIC
# MAGIC Joshua B. has mentioned that we should only consider overlap between patients where both the diagnosis codes and a microbiology lab facility is available at the facility when comparing the two. This will display a more consistent overlap within like facilities.  Also, snomed codes and antigen codes do not return any results 

# COMMAND ----------

# MAGIC %md
# MAGIC #%pip install matplotlib-venn
# MAGIC from matplotlib_venn import venn2, venn2_circles, venn2_unweighted, venn3_unweighted 
# MAGIC from matplotlib_venn import venn3, venn3_circles 
# MAGIC from matplotlib import pyplot as plt
# MAGIC
# MAGIC v=venn3(subsets=(50051,199,8,2256522,403061,6,8),set_labels=('lab_pcr','loinc_pcr','dx_codes'))
# MAGIC plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ICU 
# MAGIC  
# MAGIC

# COMMAND ----------


chgmstr=spark.read.table("edav_prd_cdh.cdh_premier_v2.chgmstr")


ICU=chgmstr.filter("clin_sum_code in ('110108','110102')")
print(ICU)

# COMMAND ----------

# MAGIC %md
# MAGIC ## IMV 

# COMMAND ----------

vent=chgmstr.filter("clin_dtl_code in ('410412946570007')")
print(vent)

# COMMAND ----------

# MAGIC %md
# MAGIC # Functions
# MAGIC
# MAGIC 1) Create covid population from diagnosis codes only  (reduce to one row per encounter)
# MAGIC 2) Create covid population from diagnosis codes, labs, snomed  (reduce to one row per encounter)  
# MAGIC
# MAGIC Give user the option to extract only the 1st encounter or all covid encounters 

# COMMAND ----------

#build covid population 
#def load_covid_population (dataset):
    
