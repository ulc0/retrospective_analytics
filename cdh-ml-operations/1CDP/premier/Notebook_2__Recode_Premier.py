# Databricks notebook source
# MAGIC %md
# MAGIC ## Contact Information
# MAGIC *Who to contact for questions about this notebook?*
# MAGIC
# MAGIC <br>
# MAGIC
# MAGIC - Author: Stacey Adjei 
# MAGIC - Email: [Ltq3@cdc.gov](Ltq3@cdc.gov) <!--- enter your email address in both the square brackets and the parens -->

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recode the Premier Data

# COMMAND ----------

# MAGIC %md
# MAGIC This notebook loads the Premier data and recodes a subset of it according to the scheme specified in the coding file (`src/coding.py`). The AHA data should also be recoded according to the same scheme (accomplished in a different notebook).
# MAGIC
# MAGIC The notebook generates a simple CSV output file with the following contents:
# MAGIC
# MAGIC     * Header row (PROV_ID, URBRUR, TEACHING, BEDS, REGION, DIVISION, DISCHARGES)
# MAGIC     * Data rows, encoded, one row for each eligible Premier hospital

# COMMAND ----------

import os
import re
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from collections import Counter, defaultdict

# this file specifies the coding scheme, i.e. the variables and their categorical values
from src import coding as CODING

# functions used by both the AHA and Premier recoding notebooks
from src import recoding_common as COMMON

# COMMAND ----------

# MAGIC %md
# MAGIC ### User Params

# COMMAND ----------

# MAGIC %md
# MAGIC All user-adjustable parameters appear in the next code cell.

# COMMAND ----------

# use Premier data and weights from this year
YEAR = 2021

# this is a CSV file of the 2019 Premier data
PREMIER_FILE = 'premier_2021.csv'

# this file contains discharge counts for various provider ids and years
PREMIER_DISCHARGE_FILE = 'premier_discharge_counts_2021.csv'

# these columns contain numeric data
PREMIER_NUMERIC_COLS = [
    'c_INPATIENT', 'c_SAME_DAY_SURGERY', 'c_EMERGENCY', 'discharge_patdemo',
]

# numeric column descriptions, used for labeling plots
PREMIER_COL_DESC = {
    'c_INPATIENT'        : 'Inpatient Admissions',
    'c_SAME_DAY_SURGERY' : 'Outpatient Surgical Operations',
    'c_EMERGENCY'        : 'Emergency Department Visits',
    'discharge_patdemo'  : 'Total Discharges',
}

# COMMAND ----------

# MAGIC %md
# MAGIC ### End of user params

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load the Premier data files

# COMMAND ----------

raw_premier_df = pd.read_csv(PREMIER_FILE)

# DROP the single row with an unknown number of beds
raw_premier_df = raw_premier_df[~raw_premier_df['BEDS_GRP'].isin(['Unavailable'])]

print(raw_premier_df.shape)

# print the column names
print(raw_premier_df.columns)

# COMMAND ----------

# each row should have a unique provider id
assert len(raw_premier_df) == len(raw_premier_df['prov_id'].unique())

# COMMAND ----------

# load the premier discharge file
premier2_df = pd.read_csv(PREMIER_DISCHARGE_FILE)
premier2_df.shape

# print the column names
print(premier2_df.columns)

# COMMAND ----------

# extract discharge counts with provider IDs for the desired year
premier_yr_df = premier2_df.loc[premier2_df['year'] == YEAR]

# these IDs had weights for that year
year_id_set = (premier_yr_df['PROV_ID'].unique())

# count the number of provider IDs with no weights for that year
missing_list = []
for index, row in raw_premier_df.iterrows():
    prov_id = row['PROV_ID']
    if prov_id not in year_id_set:
        missing_list.append(prov_id)
        
num_missing = len(missing_list)
print('Year {0} has data for {1} provider IDs, {2} of these with weights.'.
      format(YEAR, len(raw_premier_df), len(year_id_set)))

# COMMAND ----------

# merge the raw and year dataframes on provider id
merged_df = pd.merge(raw_premier_df, premier_yr_df, on=['PROV_ID'])
merged_df.shape

# COMMAND ----------

# MAGIC %md
# MAGIC ## Recode Urban_Rural

# COMMAND ----------

recoded_df = merged_df.copy()

# expect this many rows in the recoded data
checksum = recoded_df.shape[0]

# COMMAND ----------

def recode_urban_rural(df):
    """
    Recode the URBAN_RURAL variable as specified in the coding file.
    """
    
    remap = {
        'URBAN' : CODING.URBRUR.URBAN.value,
        'RURAL' : CODING.URBRUR.RURAL.value
    }
    
    samples = df['URBAN_RURAL'].values
    
    new_values = []
    for s in samples:
        assert s in remap.keys()
        new_values.append(remap[s])
            
    var_name = CODING.Variables.URBRUR.name            
    COMMON.check_recode(new_values, CODING.URBRUR, checksum)
    df = df.assign(**{var_name:new_values})
    return df            

# COMMAND ----------

recoded_df = recode_urban_rural(recoded_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Recode TEACHING

# COMMAND ----------

def recode_teaching(df):
    """
    Recode the TEACHING variable as variable as specified in the coding file.
    """
    
    remap = {
        'NO'  : CODING.TEACHING.NO.value,
        'YES' : CODING.TEACHING.YES.value,
    }
    
    samples = df['TEACHING'].values
    
    new_values = []
    for s in samples:
        assert s in remap.keys()
        new_values.append(remap[s])

    var_name = CODING.Variables.TEACHING.name
    COMMON.check_recode(new_values, CODING.TEACHING, checksum)
    df = df.assign(**{var_name:new_values})
    return df        

# COMMAND ----------

recoded_df = recode_teaching(recoded_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Recode BEDS_GRP

# COMMAND ----------

def recode_beds(df):
    """
    Recode the BEDS_GRP variable as specified in the coding file.
    """
    
    remap = {
        '000-099' : CODING.BEDS.BEDS_000_099.value,
        '100-199' : CODING.BEDS.BEDS_100_299.value,
        '200-299' : CODING.BEDS.BEDS_100_299.value,
        '300-399' : CODING.BEDS.BEDS_300_PLUS.value,
        '400-499' : CODING.BEDS.BEDS_300_PLUS.value,
        '500+'    : CODING.BEDS.BEDS_300_PLUS.value,
    }
    
    samples = df['BEDS_GRP'].values
    
    new_values = []
    for s in samples:
        assert s in remap.keys()
        new_values.append(remap[s])
    
    var_name = CODING.Variables.BEDS.name
    COMMON.check_recode(new_values, CODING.BEDS, checksum)
    df = df.assign(**{var_name:new_values})    
    return df

# COMMAND ----------

recoded_df = recode_beds(recoded_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Recode PROV_REGION

# COMMAND ----------

def recode_prov_region(df):
    """
    Recode the PROV_REGION variable as specified in the coding file:
    """
    
    remap = {
        'MIDWEST'   : CODING.REGION.MIDWEST.value,
        'SOUTH'     : CODING.REGION.SOUTH.value,
        'NORTHEAST' : CODING.REGION.NORTHEAST.value,
        'WEST'      : CODING.REGION.WEST.value,
    }
    
    samples = df['PROV_REGION'].values
    
    new_values = []
    for s in samples:
        assert s in remap.keys()
        new_values.append(remap[s])
           
    var_name = CODING.Variables.REGION.name
    COMMON.check_recode(new_values, CODING.REGION, checksum)
    df = df.assign(**{var_name:new_values})    
    return df

# COMMAND ----------

recoded_df = recode_prov_region(recoded_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Recode PROV_DIVISION

# COMMAND ----------

def recode_prov_division(df):
    """
    Recode the PROV_DIVISION variable as specified in the CODING file.
    """
    
    remap = {
        'SOUTH ATLANTIC'       : CODING.DIVISION.DIV_SA.value,
        'EAST NORTH CENTRAL'   : CODING.DIVISION.DIV_ENC.value,
        'MIDDLE ATLANTIC'      : CODING.DIVISION.DIV_MA.value,
        'PACIFIC'              : CODING.DIVISION.DIV_P.value,
        'WEST SOUTH CENTRAL'   : CODING.DIVISION.DIV_WSC.value,
        'EAST SOUTH CENTRAL'   : CODING.DIVISION.DIV_ESC.value,
        'WEST NORTH CENTRAL'   : CODING.DIVISION.DIV_WNC.value,
        'MOUNTAIN'             : CODING.DIVISION.DIV_M.value,
        'NEW ENGLAND'          : CODING.DIVISION.DIV_NE.value,        
    }
    
    samples = df['PROV_DIVISION'].values
    
    new_values = []
    for s in samples:
        assert s in remap.keys()
        new_values.append(remap[s])
        
    var_name = CODING.Variables.DIVISION.name
    COMMON.check_recode(new_values, CODING.DIVISION, checksum)
    df = df.assign(**{var_name:new_values})    
    return df

# COMMAND ----------

recoded_df = recode_prov_division(recoded_df)
#recoded_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### Integer Variables

# COMMAND ----------

# MAGIC %md
# MAGIC #### Compute basic statistics for integer variables

# COMMAND ----------

info = {}
for col in PREMIER_NUMERIC_COLS:
    values = recoded_df[col].values
    
    data = values[~np.isnan(values)]
    minval = np.min(data)
    maxval = np.max(data)
    stddev = np.std(data)
    
    info[col] = (minval, maxval, stddev)
    
print('Data for each column: ')
for col,tup in info.items():
    minval, maxval, stddev = tup
    print('{0:>{4}} : min: {1:.2f}, max: {2:.2f}, stddev: {3:.2f}'.format(
        col, minval, maxval, stddev, len('c_SAME_DAY_SURGERY')))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Plot distributions for integer variables

# COMMAND ----------

PREMIER_REBIN_MAP = {
    # variable : (bin_width, minval, maxval, logscale_y, x_label)
    'c_INPATIENT'        : (1000, 0, 150000, True, PREMIER_COL_DESC['c_INPATIENT']),
    'c_SAME_DAY_SURGERY' : (1000, 0, 200000, True, PREMIER_COL_DESC['c_SAME_DAY_SURGERY']),
    'c_EMERGENCY'        : (1000, 0, 350000, True, PREMIER_COL_DESC['c_EMERGENCY']),
    'discharge_patdemo'  : (1000, 0, 50000, True, PREMIER_COL_DESC['discharge_patdemo'])
}

# COMMAND ----------

for var, tup in PREMIER_REBIN_MAP.items():
    bin_width, range_min, range_max, logscale_y, x_label = tup
    COMMON.rebin_and_update_df(recoded_df, var, bin_width, range_min, range_max, logscale_y, x_label)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Recode the integer variables

# COMMAND ----------

# MAGIC %md
# MAGIC The DISCHARGES varible is currently the only integer variable being used for weighting.

# COMMAND ----------

def recode_discharges(df, col):
    
    samples = df[col].values
    
    new_values = []
    for s in samples:
        if s < 1000:
            new_values.append(CODING.DISCHARGES.DSCH_LT_1K.value)
        elif s < 5000:
            new_values.append(CODING.DISCHARGES.DSCH_1K_5K.value)
        elif s < 10000:
            new_values.append(CODING.DISCHARGES.DSCH_5K_15K.value)
        else:
            new_values.append(CODING.DISCHARGES.DSCH_GT_15K.value)
            
    COMMON.check_recode(new_values, CODING.DISCHARGES, df.shape[0])
    var_name = CODING.Variables.DISCHARGES.name
    df = df.assign(**{var_name:new_values})
    return df

# COMMAND ----------

recoded_df = recode_discharges(recoded_df, 'discharge_patdemo')

# COMMAND ----------

# MAGIC %md
# MAGIC #### Write recoded Premier data to disk

# COMMAND ----------

# extract relevant fields only
var_names = ['PROV_ID'] + [k for k,v in CODING.VAR_NAME_MAP.items()]
premier_output_df = recoded_df[var_names]
print(premier_output_df.shape)
premier_output_file = 'premier_{0}_recoded.csv'.format(YEAR)
premier_output_df.to_csv(premier_output_file, index=False)
print('Wrote file "{0}"'.format(premier_output_file))

# COMMAND ----------

