# Databricks notebook source
# MAGIC %md
# MAGIC # Create Charlson Comorbidity Index
# MAGIC
# MAGIC This notebook generates CCI scores based on ICD diagnoses codes

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2022-09-08

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook Configuration

# COMMAND ----------

#import pyspark sql functions with alias
import pyspark.sql.functions as F
import pyspark.sql.window as W

#import specific functions from pyspark sql types
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

from operator import add
from functools import reduce

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Definitions and Filters

# COMMAND ----------

#%run ../includes/0000-hv-filters

# COMMAND ----------

# MAGIC %run ./includes/0000-GLOBAL-NAMES

# COMMAND ----------

# MAGIC %md 
# MAGIC #### CCI Mapping

# COMMAND ----------

# Define the charlson_comorbidity index dict
# MAPPING DEFINED HERE

mapping = dict()

# Charlson score, ICD10
tmpn = "charlson_icd10_quan"
mapping[tmpn] = dict()
mapping[tmpn]["ami"] = (
    "I21",
    "I22",
    "I252",
)
mapping[tmpn]["chf"] = (
    "I099",
    "I110",
    "I130",
    "I132",
    "I255",
    "I420",
    "I425",
    "I426",
    "I427",
    "I428",
    "I429",
    "I43",
    "I50",
    "P290",
)
mapping[tmpn]["pvd"] = (
    "I70",
    "I71",
    "I731",
    "I738",
    "I739",
    "I771",
    "I790",
    "I792",
    "K551",
    "K558",
    "K559",
    "Z958",
    "Z959",
)
mapping[tmpn]["cevd"] = (
    "G45",
    "G46",
    "H340",
    "I60",
    "I61",
    "I62",
    "I63",
    "I64",
    "I65",
    "I66",
    "I67",
    "I68",
    "I69",
)
mapping[tmpn]["dementia"] = (
    "F00",
    "F01",
    "F02",
    "F03",
    "F051",
    "G30",
    "G311",
)
mapping[tmpn]["copd"] = (
    "I278",
    "I279",
    "J40",
    "J41",
    "J42",
    "J43",
    "J44",
    "J45",
    "J46",
    "J47",
    "J60",
    "J61",
    "J62",
    "J63",
    "J64",
    "J65",
    "J66",
    "J67",
    "J684",
    "J701",
    "J703",
)
mapping[tmpn]["rheumd"] = (
    "M05",
    "M06",
    "M315",
    "M32",
    "M33",
    "M34",
    "M351",
    "M353",
    "M360",
)
mapping[tmpn]["pud"] = (
    "K25",
    "K26",
    "K27",
    "K28",
)
mapping[tmpn]["mld"] = (
    "B18",
    "K700",
    "K701",
    "K702",
    "K703",
    "K709",
    "K713",
    "K714",
    "K715",
    "K717",
    "K73",
    "K74",
    "K760",
    "K762",
    "K763",
    "K764",
    "K768",
    "K769",
    "Z944",
)
mapping[tmpn]["diab"] = (
    "E100",
    "E101",
    "E106",
    "E108",
    "E109",
    "E110",
    "E111",
    "E116",
    "E118",
    "E119",
    "E120",
    "E121",
    "E126",
    "E128",
    "E129",
    "E130",
    "E131",
    "E136",
    "E138",
    "E139",
    "E140",
    "E141",
    "E146",
    "E148",
    "E149",
)
mapping[tmpn]["diabwc"] = (
    "E102",
    "E103",
    "E104",
    "E105",
    "E107",
    "E112",
    "E113",
    "E114",
    "E115",
    "E117",
    "E122",
    "E123",
    "E124",
    "E125",
    "E127",
    "E132",
    "E133",
    "E134",
    "E135",
    "E137",
    "E142",
    "E143",
    "E144",
    "E145",
    "E147",
)
mapping[tmpn]["hp"] = (
    "G041",
    "G114",
    "G801",
    "G802",
    "G81",
    "G82",
    "G830",
    "G831",
    "G832",
    "G833",
    "G834",
    "G839",
)
mapping[tmpn]["rend"] = (
    "I120",
    "I131",
    "N032",
    "N033",
    "N034",
    "N035",
    "N036",
    "N037",
    "N052",
    "N053",
    "N054",
    "N055",
    "N056",
    "N057",
    "N18",
    "N19",
    "N250",
    "Z490",
    "Z491",
    "Z492",
    "Z940",
    "Z992",
)
mapping[tmpn]["canc"] = (
    "C00",
    "C01",
    "C02",
    "C03",
    "C04",
    "C05",
    "C06",
    "C07",
    "C08",
    "C09",
    "C10",
    "C11",
    "C12",
    "C13",
    "C14",
    "C15",
    "C16",
    "C17",
    "C18",
    "C19",
    "C20",
    "C21",
    "C22",
    "C23",
    "C24",
    "C25",
    "C26",
    "C30",
    "C31",
    "C32",
    "C33",
    "C34",
    "C37",
    "C38",
    "C39",
    "C40",
    "C41",
    "C43",
    "C45",
    "C46",
    "C47",
    "C48",
    "C49",
    "C50",
    "C51",
    "C52",
    "C53",
    "C54",
    "C55",
    "C56",
    "C57",
    "C58",
    "C60",
    "C61",
    "C62",
    "C63",
    "C64",
    "C65",
    "C66",
    "C67",
    "C68",
    "C69",
    "C70",
    "C71",
    "C72",
    "C73",
    "C74",
    "C75",
    "C76",
    "C81",
    "C82",
    "C83",
    "C84",
    "C85",
    "C88",
    "C90",
    "C91",
    "C92",
    "C93",
    "C94",
    "C95",
    "C96",
    "C97",
)
mapping[tmpn]["msld"] = (
    "I850",
    "I859",
    "I864",
    "I982",
    "K704",
    "K711",
    "K721",
    "K729",
    "K765",
    "K766",
    "K767",
)
mapping[tmpn]["metacanc"] = (
    "C77",
    "C78",
    "C79",
    "C80",
)
mapping[tmpn]["aids"] = (
    "B20",
    "B21",
    "B22",
    "B24",
)

# COMMAND ----------

mapping

# COMMAND ----------

# set weights
mapping_keys = list(mapping[tmpn].keys())

weights = {
    'ami':1,
    'chf':1,
    'pvd':1,
    'cevd':1,
    'dementia':1,
    'copd':1,
    'rheumd':1,
    'pud':1,
    'mld':1,
    'diab':1,
    'diabwc':2,
    'hp':2,
    'rend':2,
    'canc':2,
    'msld':3,
    'metacanc':6,
    'aids':6
}

# COMMAND ----------

cci_list = [(k,_v) for k,v in mapping[tmpn].items() for _v in v]

# COMMAND ----------

cci_list

# COMMAND ----------

cci_df = spark.createDataFrame(cci_list, schema="`comorbidity` STRING, `icd_code` STRING")

# COMMAND ----------

cci_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Collect Data

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### long_df

# COMMAND ----------

spark.table(f'{DB_WRITE}.{LONG_OUTPUT_NAME}').show()

# COMMAND ----------

spark.table(f'{DB_WRITE}.{MATCHES_UID_TABLE}').show()

# COMMAND ----------

# From twj8_medrec_covid_ccsr
# Filter
# - NOT covid = 1 and covid = 0
# - age 18-64
# - gender M,F
# - days_enrolled >= 1
# - adm_date-index_date between -365,-7
# Select medrec, age, icd_code, adm_date
# join to twj8_uid_case_control, inner
long_df = (
    spark
    .table(
        f'{DB_WRITE}.{LONG_OUTPUT_NAME}'
    )
    .filter(
        ~((F.col('covid_indicator') == 1) & (F.col('covid') == 0))
        & F.col('age').between(18,64)
        & F.col('gender').isin('M','F')
        & (F.col('days_enrolled') >= 1)
        & F.datediff('adm_date','index_date').between(-365,-7)
    )
    .select(
        'medrec_key',
        'age',
        'icd_code',
        'adm_date'
    )
    .join(
        F.broadcast(spark.table(f'{DB_WRITE}.{MATCHES_UID_TABLE}')),
        'medrec_key',
        'inner'
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### long_df_scored

# COMMAND ----------

mapping_keys

# COMMAND ----------

cci_df.show()

# COMMAND ----------

# Use long_df
# join cci_df, left on long_df.icd_code startswith cci_df.icd_code
# groupby medrec_key, age
# pivot on comorbidity
# agg
# - count icd_code
# create cci_subscore1
# create cci_subscore2
# create cci_subscore3
# create cci_score1 as cci_subscore1 + cci_subscore2
# create cci_score2 as cci_subscore1 + cci_subscore2 + cci_subscore3
long_df_scored = (
    
    long_df
        
    .join(
        F.broadcast(cci_df),
        [
            long_df['icd_code'].startswith(cci_df['icd_code'])
        ],
        'left'
    )
    .drop(
        cci_df['icd_code']
    )
    .groupBy(
        'medrec_key',
        'age'
    )
    .pivot(
        'comorbidity',
        mapping_keys      
    )
    .agg(
        F.count('icd_code')
    )
    
    .withColumn(
        'cci_subscore1',
        reduce(
            add,
            [
                F.when(
                    F.col(c).isNotNull(),
                    1*weights[c]
                )
                .otherwise(0) for c in mapping_keys if c not in ['diab','diabwc','mld','msld','canc','metacanc']
            ]
        )
    )
    
    .withColumn(
        'cci_subscore2',
        reduce(
            add,
            [
                F.when(
                    F.col(b).isNotNull(),
                    1*weights[b]
                )
                .when(
                    F.col(b).isNull() & F.col(a).isNotNull(),
                    1*weights[a]
                ) 
                .otherwise(0) for a,b in [('diab','diabwc'), ('mld','msld'), ('canc','metacanc')]
            ]
        )

    )
    
    .withColumn(
        'age_subscore3',
#         F.floor(
#             (
#                 # If age minus 40 is negative, then set to 0
#                 F.greatest(
                    
#                     # If age is >= 80, then set age to 80
#                     F.least(
#                         F.col('age'),
#                         F.lit(80).cast('integer')
#                     ) - 40,
#                     F.lit(0).cast('integer')
#                 ) 
#             )/10
#         )
        F.when(
            F.col('age').between(0,49),
            0
        )
        .when(
            F.col('age').between(50,59),
            1
        )
        .when(
            F.col('age').between(60,69),
            2
        )
        .when(
            F.col('age').between(70,79),
            3
        )
        .when(
            F.col('age') >= 80,
            4
        )
    )
    
    .select(
        '*',
        (F.col('cci_subscore1') + F.col('cci_subscore2')).alias('cci_score'),
        (F.col('cci_subscore1') + F.col('cci_subscore2') + F.col('age_subscore3')).alias('cci_score_age'),
        
    )
    
    .select(
        'medrec_key',
        'cci_score',
        'cci_score_age'
    )

)

# COMMAND ----------

display(long_df_scored)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write Table

# COMMAND ----------

# write table
# f'{DB_WRITE}.{CCI_TABLE_NAME}'

# COMMAND ----------

# spark.sql(f'DROP TABLE IF EXISTS {DB_WRITE}.{CCI_TABLE_NAME}')

# COMMAND ----------

# write table
# spark configuration
spark.conf.set('spark.sql.shuffle.partitions', 1600)
spark.conf.set('spark.databricks.adaptive.autoOptimizeShuffle.enabled', False)
spark.conf.set('spark.databricks.queryWatchdog.enabled', False)
spark.conf.set('spark.sql.files.maxPartitionBytes', 1024 * 1024 * 64)

(
    long_df_scored
    .write
    .format('parquet')
    .mode('overwrite')
    .saveAsTable(f'{DB_WRITE}.{CCI_TABLE_NAME}')
)

# COMMAND ----------

spark.sql(f'CONVERT TO DELTA {DB_WRITE}.{CCI_TABLE_NAME}')

# COMMAND ----------

spark.sql(f'OPTIMIZE {DB_WRITE}.{CCI_TABLE_NAME}')

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Footer

# COMMAND ----------

(
    spark
    .table(f"{DB_WRITE}.{CCI_TABLE_NAME}")
    .groupBy()
    .max()
    .display()
)

# COMMAND ----------

(
    spark
    .table(f"{DB_WRITE}.{CCI_TABLE_NAME}")
    .count()
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC The codes for the Charlson Score were adapted from the following resources:
# MAGIC
# MAGIC
# MAGIC <b> Coding Algorithms for Defining Comorbidities in ICD-9-CM and ICD-10 Administrative Data (Quan et al., 2005) </b>
# MAGIC
# MAGIC https://journals.lww.com/lww-medicalcare/pages/articleviewer.aspx?year=2005&issue=11000&article=00010&type=Fulltext
# MAGIC
# MAGIC <b> Updating and Validating the Charlson Comorbidity Index and Score for Risk Adjustment in Hospital Discharge Abstracts Using Data From 6 Countries (Quan et al., 2011) </b>
# MAGIC
# MAGIC https://academic.oup.com/aje/article/173/6/676/182985
# MAGIC
# MAGIC <b> comorbidipy </b>
# MAGIC
# MAGIC https://github.com/vvcb/comorbidipy/blob/main/comorbidipy/mapping.py
# MAGIC
# MAGIC > MIT License
# MAGIC
# MAGIC > Copyright (c) 2021, vvcb
# MAGIC
# MAGIC > Permission is hereby granted, free of charge, to any person obtaining a copy
# MAGIC of this software and associated documentation files (the "Software"), to deal
# MAGIC in the Software without restriction, including without limitation the rights
# MAGIC to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# MAGIC copies of the Software, and to permit persons to whom the Software is
# MAGIC furnished to do so, subject to the following conditions:
# MAGIC > The above copyright notice and this permission notice shall be included in all
# MAGIC copies or substantial portions of the Software.
# MAGIC > THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# MAGIC IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# MAGIC FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# MAGIC AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# MAGIC LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# MAGIC OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# MAGIC SOFTWARE.
