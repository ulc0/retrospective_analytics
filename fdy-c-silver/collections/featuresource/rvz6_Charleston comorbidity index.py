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
# MAGIC ## Problem:
# MAGIC
# MAGIC *Define the Charleston Comorbidity index score for each patient.*
# MAGIC
# MAGIC "Each condition from the Charlson score is assigned a score when computing the weighted Charlson index, irrespectively of the coding system utilised. In particular, diabetes with complications, hemiplegia/paraplegia, renal disease, and malignancies are assigned a score of 2; moderate/severe liver disease is assigned a score of 3; metastatic solid tumour and AIDS/HIV are assigned a score of 6; the remaining comorbidities are assigned a score of 1. comorbidity allows the option of applying a hierarchy of comorbidities should a more severe version be present: by choosing to do so (and that is the default behaviour of comorbidity) a type of comorbidity is never computed more than once for a given patient."
# MAGIC
# MAGIC Reference:  
# MAGIC <https://mran.microsoft.com/snapshot/2020-04-15/web/packages/comorbidity/vignettes/comorbidityscores.html>
# MAGIC

# COMMAND ----------

covid=spark.read.table("cdh_premier_exploratory.rvz6_covid_cohort_outpatient_ED_clinic")
icd=spark.read.table("cdh_premier.paticd_diag")
providers=spark.read.table("cdh_premier.providers")


# COMMAND ----------

import pandas as pd
from dateutil.parser import parse
import pyspark.sql.window as W
import pyspark.sql.functions as F

# COMMAND ----------

covid_dx=(
    spark.sql("select distinct diag.pat_key,diag.icd_code,diag.icd_pri_sec,diag.icd_poa, regexp_replace(diag.ICD_CODE, '\\\.', '') as icd_code_new, cv.* from cdh_premier.paticd_diag diag inner join cdh_premier_exploratory.rvz6_covid_cohort_outpatient_ED_clinic cv on (cv.covid_pat_key=diag.pat_key) "
        )
)

# COMMAND ----------

unexpos_dx=(
    spark.sql("select distinct diag.pat_key,diag.icd_code,diag.icd_pri_sec,diag.icd_poa, regexp_replace(diag.ICD_CODE, '\\\.', '') as icd_code_new, cv.* from cdh_premier.paticd_diag diag inner join cdh_premier_exploratory.rvz6_unexposed_cohort_outpatient_ED_clinic cv on (cv.pat_key_demo=diag.pat_key) "
        )
)

# COMMAND ----------

display(covid_dx)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Methods

# COMMAND ----------

# MAGIC %md
# MAGIC ### Collect all icd codes at encounter visit in an array 

# COMMAND ----------


lst=(
    covid_dx
    .groupBy("pat_key", "prov_id")
    .agg(
      F.collect_set("icd_code_new").alias("icd_codes")
    )

)
display(lst)

# COMMAND ----------

unexp_lst=(
    unexpos_dx
    .groupBy("pat_key", "prov_id")
    .agg(
      F.collect_set("icd_code_new").alias("icd_codes")
    )

    
)
display(unexp_lst)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create diagnosis code mapping

# COMMAND ----------

MI_lst=['I21','I22','I252']
CHF_lst=['I43','I50','I099','I110','I130','I132','I255','I420','I425','I426','I427','I428','I429','P290']
PVD_lst=['I70','I71','I731','I738','I739','I771','I790','I792','K551','K558','K559','Z958','Z959']
CVD_lst=['G45','G46','I60','I61','I62','I63','I64','I65','I66','I67','I68','I69','H340']
DEMENTIA_lst=['F00','F01','F02','F03','G30','F051','G311']
CPD_lst=['J40','J41','J42','J43','J44','J45','J46','J47','J60','J61','J62','J63','J64','J65','J66','J67','I278','I279','J684','J701','J703'] 
CTD_lst=['M05','M32','M33','M34','M06','M315','M351','M353','M360']
PUD_lst=['K25','K26','K27','K28']
MLD_lst=['B18','K73','K74','K700','K701','K702','K703','K709','K717','K713','K714','K715','K760','K762','K763','K764','K768','K769','Z944']
DIABETES_wo_C=['E100','E101','E106','E108','E109','E110','E111','E116','E118','E119','E120','E121','E126','E128','E129','E130','E131','E136','E138','E139','E140','E141','E146','E148','E149']
DIABETES_w_C=['E102','E103','E104','E105','E107','E112','E113','E114','E115','E117','E122','E123','E124','E125','E127','E132','E133','E134','E135','E137','E142','E143','E144','E145','E147']
PARA_HEMI_lst=['G81','G82','G041','G114','G801','G802','G830','G831','G832','G833','G834','G839']
RENAL_lst=['N18','N19','N052','N053','N054','N055','N056','N057','N250','I120','I131','N032','N033','N034','N035','N036','N037','Z490','Z491','Z492','Z940','Z992']
CANCER_lst=['C00','C01','C02','C03','C04','C05','C06','C07','C08','C09','C10','C11',
                         'C12','C13','C14','C15','C16','C17','C18','C19','C20','C21','C22','C23',
                         'C24','C25','C26','C30','C31','C32','C33','C34','C37','C38','C39','C40',
                         'C41','C43','C45','C46','C47','C48','C49','C50','C51','C52','C53','C54',
                         'C55','C56','C57','C58','C60','C61','C62','C63','C64','C65','C66','C67',
                         'C68','C69','C70','C71','C72','C73','C74','C75','C76','C81','C82','C83',
                         'C84','C85','C88','C90','C91','C92','C93','C94','C95','C96','C97']
SLD_lst=['K704','K711','K721','K729','K765','K766','K767','I850','I859','I864','I982']
META_CARC_lst=['C77','C78','C79','C80']
HIV_lst=['B20','B21','B22','B24']

# COMMAND ----------

mapping_dictionary = {
'MI': ('I21','I22','I252'),
'CHF':('I43','I50','I099','I110','I130','I132','I255','I420','I425','I426','I427','I428','I429','P290'),
'PVD':('I70','I71','I731','I738','I739','I771','I790','I792','K551','K558','K559','Z958','Z959'),
'CVD':('G45','G46','I60','I61','I62','I63','I64','I65','I66','I67','I68','I69','H340'),
'DEMENTIA':('F00','F01','F02','F03','G30','F051','G311'),
'CPD':('J40','J41','J42','J43','J44','J45','J46','J47','J60','J61','J62','J63','J64','J65','J66','J67','I278','I279','J684','J701','J703'), 
'CTD':('M05','M32','M33','M34','M06','M315','M351','M353','M360'),
'PUD':('K25','K26','K27','K28'),
'MLD':('B18','K73','K74','K700','K701','K702','K703','K709','K717','K713','K714','K715','K760','K762','K763','K764','K768','K769','Z944'),
'DIABETES_wo_C':('E100','E101','E106','E108','E109','E110','E111','E116','E118','E119','E120','E121','E126','E128','E129','E130','E131','E136','E138','E139','E140','E141','E146','E148','E149'),
'DIABETES_w_C':('E102','E103','E104','E105','E107','E112','E113','E114','E115','E117','E122','E123','E124','E125','E127','E132','E133','E134','E135','E137','E142','E143','E144','E145','E147'),
'PARA_HEMI':('G81','G82','G041','G114','G801','G802','G830','G831','G832','G833','G834','G839'),
'RENAL':('N18','N19','N052','N053','N054','N055','N056','N057','N250','I120','I131','N032','N033','N034','N035','N036','N037','Z490','Z491','Z492','Z940','Z992'),
'CANCER':('C00','C01','C02','C03','C04','C05','C06','C07','C08','C09','C10','C11',
                         'C12','C13','C14','C15','C16','C17','C18','C19','C20','C21','C22','C23',
                         'C24','C25','C26','C30','C31','C32','C33','C34','C37','C38','C39','C40',
                         'C41','C43','C45','C46','C47','C48','C49','C50','C51','C52','C53','C54',
                         'C55','C56','C57','C58','C60','C61','C62','C63','C64','C65','C66','C67',
                         'C68','C69','C70','C71','C72','C73','C74','C75','C76','C81','C82','C83',
                         'C84','C85','C88','C90','C91','C92','C93','C94','C95','C96','C97'),
'SLD':('K704','K711','K721','K729','K765','K766','K767','I850','I859','I864','I982'),
'META_CARC':('C77','C78','C79','C80'),
'AIDS_HIV':('B20','B21','B22','B24')
                }

# COMMAND ----------

#test code 
lst_flags=(
    lst
    .withColumn("MI",
                F.when(
                    F.size(
                        F.array_intersect(
                            F.col('icd_codes'), 
                            F.array(*[F.lit(item) for item in CHF])
                        )
                    )>0,1)
                .otherwise(0)
               )
               
)
display(lst_flags)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create flags

# COMMAND ----------

#create comorbidity flags the long way without using a function 


covid_flags=(
    lst
    .withColumn("MI",
               F.when(
                    F.size(
                        F.array_intersect(
                            F.col('icd_codes'), 
                            F.array(*[F.lit(item) for item in MI_lst])
                        )
                    )>0,1)
                .otherwise(0)
               )
    .withColumn("CHF",
               F.when(
                    F.size(
                        F.array_intersect(
                            F.col('icd_codes'), 
                            F.array(*[F.lit(item) for item in CHF_lst])
                        )
                    )>0,1)
                .otherwise(0)
               )
    .withColumn("PVD",
               F.when(
                    F.size(
                        F.array_intersect(
                            F.col('icd_codes'), 
                            F.array(*[F.lit(item) for item in PVD_lst])
                        )
                    )>0,1)
                .otherwise(0)
               )
    .withColumn("CVD",
               F.when(
                    F.size(
                        F.array_intersect(
                            F.col('icd_codes'), 
                            F.array(*[F.lit(item) for item in CVD_lst])
                        )
                    )>0,1)
                .otherwise(0)
               )
    .withColumn("DEMENTIA",
               F.when(
                    F.size(
                        F.array_intersect(
                            F.col('icd_codes'), 
                            F.array(*[F.lit(item) for item in DEMENTIA_lst])
                        )
                    )>0,1)
                .otherwise(0)
               )
    .withColumn("CPD",
              F.when(
                    F.size(
                        F.array_intersect(
                            F.col('icd_codes'), 
                            F.array(*[F.lit(item) for item in CPD_lst])
                        )
                    )>0,1)
                .otherwise(0)
               )
    .withColumn("CTD",
               F.when(
                    F.size(
                        F.array_intersect(
                            F.col('icd_codes'), 
                            F.array(*[F.lit(item) for item in CTD_lst])
                        )
                    )>0,1)
                .otherwise(0)
               )
    .withColumn("PUD",
               F.when(
                    F.size(
                        F.array_intersect(
                            F.col('icd_codes'), 
                            F.array(*[F.lit(item) for item in PUD_lst])
                        )
                    )>0,1)
                .otherwise(0)
               )
    .withColumn("MLD",
               F.when(
                    F.size(
                        F.array_intersect(
                            F.col('icd_codes'), 
                            F.array(*[F.lit(item) for item in MLD_lst])
                        )
                    )>0,1)
                .otherwise(0)
               )
    .withColumn("DIABETES_wo_C",
               F.when(
                    F.size(
                        F.array_intersect(
                            F.col('icd_codes'), 
                            F.array(*[F.lit(item) for item in DIABETES_wo_C])
                        )
                    )>0,1)
                .otherwise(0)
               )
    .withColumn("DIABETES_w_C",
               F.when(
                    F.size(
                        F.array_intersect(
                            F.col('icd_codes'), 
                            F.array(*[F.lit(item) for item in DIABETES_w_C])
                        )
                    )>0,1)
                .otherwise(0)
               )
    .withColumn("PARA_HEMI",
               F.when(
                    F.size(
                        F.array_intersect(
                            F.col('icd_codes'), 
                            F.array(*[F.lit(item) for item in PARA_HEMI_lst])
                        )
                    )>0,1)
                .otherwise(0)
               )
    .withColumn("RENAL",
               F.when(
                    F.size(
                        F.array_intersect(
                            F.col('icd_codes'), 
                            F.array(*[F.lit(item) for item in RENAL_lst])
                        )
                    )>0,1)
                .otherwise(0)
               )
        .withColumn("CANCER",
               F.when(
                    F.size(
                        F.array_intersect(
                            F.col('icd_codes'), 
                            F.array(*[F.lit(item) for item in CANCER_lst])
                        )
                    )>0,1)
                .otherwise(0)
               )
        .withColumn("SLD",
               F.when(
                    F.size(
                        F.array_intersect(
                            F.col('icd_codes'), 
                            F.array(*[F.lit(item) for item in SLD_lst])
                        )
                    )>0,1)
                .otherwise(0)
               )
        .withColumn("META_CARC",
               F.when(
                    F.size(
                        F.array_intersect(
                            F.col('icd_codes'), 
                            F.array(*[F.lit(item) for item in META_CARC_lst])
                        )
                    )>0,1)
                .otherwise(0)
               )
        .withColumn("AIDS_HIV",
               F.when(
                    F.size(
                        F.array_intersect(
                            F.col('icd_codes'), 
                            F.array(*[F.lit(item) for item in HIV_lst])
                        )
                    )>0,1)
                .otherwise(0)
               )

)
display(covid_flags)

# COMMAND ----------

#create comorbidity flags using a function 


def flags_ftn(data, name, mapp):
    result=(
        data
        .withColumn(name,
                    F.when(
                        F.size(
                            F.array_intersect(
                                F.col('icd_codes'), 
                                F.array(*[F.lit(item) for item in mapp])
                            )
                        )>0,1)
                    .otherwise(0)
                   )
    )
    return result 

unexp_flags=unexp_lst  
for i,j in mapping_dictionary.items():
    unexp_flags=flags_ftn(unexp_flags, i,j)

# COMMAND ----------

covid_flags=lst  
for i,j in mapping_dictionary.items():
    covid_flags=flags_ftn(covid_flags, i,j)
    
    
unexp_flags=unexp_lst  
for i,j in mapping_dictionary.items():
    unexp_flags=flags_ftn(unexp_flags, i,j)

# COMMAND ----------

display(unexp_flags)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Assign scores  
# MAGIC Each condition from the Charlson score is assigned a score when computing the weighted Charlson index, irrespectively of the coding system utilised. In particular, diabetes with complications, hemiplegia/paraplegia, renal disease, and malignancies are assigned a score of 2; moderate/severe liver disease is assigned a score of 3; metastatic solid tumour and AIDS/HIV are assigned a score of 6; the remaining comorbidities are assigned a score of 1

# COMMAND ----------

lst_flags.columns

# COMMAND ----------

#weights=[mi = 1,chf = 1,pvd = 1,cvd = 1,dementia = 1,cpd = 1,ctd = 1,pud = 1,mld = 1,diabetes_wo_c = 1,diabetes_w_c = 2,para_hemi = 2,renal = 2,cancer = 2,sld = 3,meta_carc = 6,aids_hiv = 6]

#newdf = df.withColumn('total', sum(df[col] for col in df.columns))

covid_scores=(
    covid_flags
    .withColumn("CC_score",
         (F.col("MI")+F.col("CHF")+F.col("PVD")+F.col("CVD")+F.col("DEMENTIA")+F.col("CPD")+ F.col("CTD")+F.col("PUD")+ F.col("MLD")+F.col("DIABETES_wo_C")+ F.col("DIABETES_w_C")*2+F.col("PARA_HEMI")*2+F.col("RENAL")*2+F.col("CANCER")*F.lit(2)+ F.col("SLD")*3 + F.col("META_CARC")*6+ F.col("AIDS_HIV")*6)
               )
)


unexposed_scores=(
    unexp_flags
    .withColumn("CC_score",
         (F.col("MI")+F.col("CHF")+F.col("PVD")+F.col("CVD")+F.col("DEMENTIA")+F.col("CPD")+ F.col("CTD")+F.col("PUD")+ F.col("MLD")+F.col("DIABETES_wo_C")+ F.col("DIABETES_w_C")*2+F.col("PARA_HEMI")*2+F.col("RENAL")*2+F.col("CANCER")*F.lit(2)+ F.col("SLD")*3 + F.col("META_CARC")*6+ F.col("AIDS_HIV")*6)
               )
)



#display(covid_scores)
#display(unexposed_scores)

# COMMAND ----------

#write out covid population with missing features to complete adjusted model
print(covid_scores.count())
print(unexposed_scores.count())

final_CC_table=(
    covid_scores
    .unionAll(unexposed_scores)
    .join(providers.select("PROV_ID","URBAN_RURAL","PROV_REGION", "PROV_DIVISION"), "PROV_ID","left")
)
#print(final_CC_table.count())

#10279802


# COMMAND ----------

final_CC_table.write.mode("overwrite").saveAsTable("edav_prd_cdh.cdh_sandbox.rvz6_PCC_outpatient_scores")

