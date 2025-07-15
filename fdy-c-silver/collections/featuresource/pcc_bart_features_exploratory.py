# Databricks notebook source
REF_DATA="edav_prd_cdh.cdh_ml_test"

# COMMAND ----------



featurePrimitiveDict={

"ICD_MHCONDS":['F41.1', 'F432.3', 'F33.1', 'F43.22', 'F43.20', 'F41.9', 
                         'F43.10', 'F43.21', 'F32.12', 'Z63.0', 'F33.0', 'F90.2', 
                         'F32.1', 'F34.1', 'F90.0', 'F33.2', 'F32.0', 'F43.25', 'F41.0', 'F84.0',] ,
"ICD_COVID" : [
        "U07.1",
        "U07.2",
        "U09.9",
        "B34.2",
        "B97.21",
        "B97.29",
        "J12.81",
        "J12.82",
        "M35.81",
        "Z86.16",
        "B94.8",
        "B94.9",   ],
"CPT_COVID" : [        #unchanged from HV
        "Q02.40",
        "Q02.43",
        "Q02.44",
        "M02.40",
        "M02.41",
        "M02.43",
        "M02.44",
        "Q02.45",
        "M02.45",
        "M02.46",
        "Q02.47",
        "M02.47",
        "M02.48",
        "Q02.22",
        "M02.22",
        "M02.23",
        "J02.48",
    ],


"NDC_COVID" : [ 
    #not used in Premier
    "61755004202",
    "61755004502",
    "61755003502",
    "61755003608",
    "61755003705",
    "61755003805",
    "61755003901",
    "61755002501",
    "61755002701",
    "00002791001",
    "00002795001",
    "00002791001",
    "00002795001",
    "00173090186",
    "00002758901",
    "00069108530",
    "00069108506",
    "00006505506",
    "00006505507",
    "61958290202",
    "61968290101",
],
"LOINC_COVIDTEST":[
'101289-7',
'94306-8',
'94309-2',
'94500-6',
'94531-1',
'94565-9',
'94660-8',
'94745-7',
'94746-5',
'94759-8',
'94819-0',
'94822-4',
'94845-5',
'95406-5',
'95424-8',
'95608-6',
'95826-4',
'96797-6',
'96829-7',
'96897-4',
],

}


# COMMAND ----------

# MAGIC %md
# MAGIC #### Setups
# MAGIC #### From util/conditions.py
# MAGIC "LOINCDESC_COVID" : [
# MAGIC         "SARS coronavirus 2 RNA:PrThr:Pt:Respiratory:Ord:Probe.amp.tar",
# MAGIC         "SARS coronavirus 2 RNA:PrThr:Pt:XXX:Ord:Probe.amp.tar",
# MAGIC     ],  
# MAGIC
# MAGIC LOINC_CODE	NEW_LOINC_CODE	LOINC_DESC  
# MAGIC 101289-7		SARS coronavirus 2 RNA:PrThr:Pt:Thrt:Ord:Non-probe.amp.tar  
# MAGIC 94306-8		SARS coronavirus 2 RNA panel:-:Pt:XXX:-:Probe.amp.tar  
# MAGIC 94309-2		SARS coronavirus 2 RNA:PrThr:Pt:XXX:Ord:Probe.amp.tar  
# MAGIC 94500-6		SARS coronavirus 2 RNA:PrThr:Pt:Respiratory:Ord:Probe.amp.tar  
# MAGIC 94531-1		SARS coronavirus 2 RNA panel:-:Pt:Respiratory:-:Probe.amp.tar  
# MAGIC 94565-9		SARS coronavirus 2 RNA:PrThr:Pt:Nph:Ord:Non-probe.amp.tar  
# MAGIC 94660-8		SARS coronavirus 2 RNA:PrThr:Pt:Ser/Plas:Ord:Probe.amp.tar  
# MAGIC 94745-7		SARS coronavirus 2 RNA:ThreshNum:Pt:Respiratory:Qn:Probe.amp.tar  
# MAGIC 94746-5		SARS coronavirus 2 RNA:ThreshNum:Pt:XXX:Qn:Probe.amp.tar  
# MAGIC 94759-8		SARS coronavirus 2 RNA:PrThr:Pt:Nph:Ord:Probe.amp.tar  
# MAGIC 94819-0		SARS coronavirus 2 RNA:LnCnc:Pt:XXX:Qn:Probe.amp.tar  
# MAGIC 94822-4		SARS coronavirus 2 RNA:PrThr:Pt:Saliva:Ord:Sequencing  
# MAGIC 94845-5		SARS coronavirus 2 RNA:PrThr:Pt:Saliva:Ord:Probe.amp.tar  
# MAGIC 95406-5		SARS coronavirus 2 RNA:PrThr:Pt:Nose:Ord:Probe.amp.tar  
# MAGIC 95424-8		SARS coronavirus 2 RNA:PrThr:Pt:Respiratory:Ord:Sequencing  
# MAGIC 95608-6		SARS coronavirus 2 RNA:PrThr:Pt:Respiratory:Ord:Non-probe.amp.tar  
# MAGIC 95826-4		SARS coronavirus 2 RNA panel:-:Pt:Saliva:-:Probe.amp.tar  
# MAGIC 96797-6		SARS coronavirus 2 RNA:PrThr:Pt:Oropharyngeal wash:Ord:Probe.amp.tar  
# MAGIC 96829-7		SARS coronavirus 2 RNA:PrThr:Pt:XXX^Donor:Ord:Probe.amp.tar  
# MAGIC 96897-4		SARS coronavirus 2 RNA panel:-:Pt:Oropharyngeal wash:-:Probe.amp.tar  

# COMMAND ----------

# This should be cleaned up and standardized once edav_dev_edav_prd_cdh.cdh_test.dev_edav_prd_cdh.cdh_ml_test has a standarizaed CCSR list maintained
# Pattern follows for other aggregators. GROUPING_CONCEPT_FEATURE
featureCCSRDict={"CCSR_ICD_CCSRCOVID1" : ["END005",
        "CIR013",
        "RSP010",
        "NVS014",
        "NEO059",
        "NEO064",
        "CIR005",
        "NVS018",
        "CIR015",
        "RSP002",
        "INF002",
        ],
"CCSR_ICD_CCSRCOVID2" : ["END005",
        "RSP010",
        "NVS014",
        "CIR005",
        "CIR015",
        "RSP002",],


}


# COMMAND ----------



drugNames={
"DRUGNAME_COVID" : [
    #unchanged from HV
    "CASIRIVIMAB",
    "IMDEVIMAB",
    "ETESEVIMAB",
    "BAMLANIVIMAB",
    "SOTROVIMAB",
    "BEBTELOVIMAB",
    "PAXLOVID",
    "MOLNUPIRAVIR",
    "REMDESIVIR",
       "PAXLOVID", 
         "MOLNUPIR",
         "EVUSHELD",
         "TIXAGEVIMAB",
         "CILGAVIMAB",
         "BEBTELOVIMA",
         "SOTROVIMAB",
         "BAMLANIVIMAB",
         "ETESEVIMAB",
         "REGEN-COV",
         "CASIRIVIMAB",
         "IMDEVIMAB",
         "DEXAMETHASONE",
         "TOFACITINIB", 
         "TOCILIZUMAB",
         "SARILUMAB",
         "BARICITINIB",
         "REMDESIVIR",
         ],

}


# COMMAND ----------

import pandas as pd
import pyspark as ps
plist=[]
for ft,ft_list in featurePrimitiveDict.items():
    tbl=pd.DataFrame(ft_list,columns=['concept_code'])
    ct,fn=ft.split('_')
    tbl['feature_code']=fn
    tbl['vocabulary_id']=ct
    plist=plist+tbl.values.tolist()
fp=spark.createDataFrame(pd.DataFrame(plist,columns=['concept_code','feature','vocabulary_id']))

# COMMAND ----------

ft_name=f"{REF_DATA}.ml_feature_table"
ft=spark.table(ft_name)
ft.union(fp).dropDuplicates()
fp.write.mode("overwrite").format("delta").saveAsTable(ft_name)

# COMMAND ----------

#this can be a dictionary of dictionaries, or sometihng
ltables={
    "CCSR":"ccsr_icd_crossreference",
}
lcolumns={
    "ICD":"ICD-10-CM_Code",
}
ccolumn={
    "CCSRS":"CCSR_Category",
}


# COMMAND ----------

#ccsrs=spark.table(f"{REF_DATA}.ccsr_icd_codes")
for feature_code,ccsrList in featureCCSRDict.items():
    lt,ct,fn=feature_code.split("_")
    ftable=ltables[lt]
    concept_column=lcolumns[ct]
    vlist="|".join(ccsrList)
    catcol=ccolumn[lt]
    sqlstring=f"select distinct `{concept_column}` as concept_code, '{ct}' as vocabulary_id,'{fn}' as feature from {REF_DATA}.{ftable} where `{catcol}` RLIKE '{vlist}'"
    print(sqlstring) 
    values=spark.sql(sqlstring)
    display(values)
    ft_name=f"{REF_DATA}.ml_feature_table"
    ft=spark.table(ft_name)
    ft.union(values).dropDuplicates()
    fp.write.mode("overwrite").format("delta").saveAsTable(ft_name)
display(fp)

