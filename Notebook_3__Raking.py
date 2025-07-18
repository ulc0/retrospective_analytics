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
# MAGIC ## Iterative Proportional Fitting

# COMMAND ----------

# MAGIC %md
# MAGIC This notebook performs iterative proportional fitting ("raking") on variables relevant to the T10 weighting problem. Weights are computed for a source sample so that specified marginal distributions match those of a weighted target population. The raking procedure can use as few as two of the variables for weighting.
# MAGIC
# MAGIC The source and target datasets **must** obey an identical coding scheme. This notebook **assumes** that both the source and target datasets have been recoded accordingly.

# COMMAND ----------

# MAGIC %pip install ipfn

# COMMAND ----------



# COMMAND ----------

import os
import re
import sys
import math
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

sys.path.append('/Workspace/Users/ltq3@cdc.gov/Weight_test/Pericarditis and pericardial disease')

from tabulate import tabulate
from collections import Counter, defaultdict

from src import plots, pdf, models, raking

# coding file - ensure that the input files were generated with the selected coding scheme
from src import coding as CODING

# COMMAND ----------

# MAGIC %md
# MAGIC ### User Parameters

# COMMAND ----------

# MAGIC %md
# MAGIC These parameters control the raking process. These are the only variables in the notebook that users should modify.

# COMMAND ----------

##
# RAKING VARIABLES
#
# This data structure contains various identifiers for the variables to be raked.
# It is a list of three-component tuples for each variable.
#
# The left column specifies the variable type from the coding file.
#
# The middle column specifies the name of this variable in the SOURCE (Premier) file.
#
# The right column specifies the name of this variable in the TARGET (AHA) file.

RAKE_DATA = [
    # variable,                   # Premier file col name,                       # AHA file column name
    (CODING.Variables.URBRUR,     CODING.Variables.URBRUR.name,                  CODING.Variables.URBRUR.name),
    (CODING.Variables.TEACHING,   CODING.Variables.TEACHING.name,                CODING.Variables.TEACHING.name),
    (CODING.Variables.BEDS,       CODING.Variables.BEDS.name,                    CODING.Variables.BEDS.name),
    (CODING.Variables.REGION,     CODING.Variables.REGION.name,                  CODING.Variables.REGION.name),
    (CODING.Variables.DISCHARGES, CODING.Variables.DISCHARGES.name,              CODING.Variables.DISCHARGES.name),
]


#
# MIN_CELL_SIZE
#
#
# Any raking model that produces source variable marginal distributions with fewer
# than this many samples in a single category will be discarded.
#
# The purpose of this integer variable is to identify raking models that are likely to produce
# large maximum weights. Large values in the upper tail of the weight distribution tend to
# overweight relatively rare individuals, which causes them to represent an unrealistic percentage
# of the final weighted population.
#
# A suitable minimum value for this variable is 30.
MIN_CELL_SIZE = 30

#
# DROP_708
# 
# Whether to drop the entry at index 708 from the Premier data,
# which causes a large weight when using DISCHARGES as one of the variables.
# This only applies when using five variables to do the weighting.
#
# It is also dependent on the source and target datasets. If either of these
# change, this variable should be set to False because it may no longer apply.
# The largest-weighted hospitals are displayed in a table below, from which a
# determination can be made as to whether or not any outlier weights should
# be dropped.
DROP_708 = True

#
# DATA DIR
# 
# This is the name of the directory where the source and target RECODED input files
# can be found. It is assumed to be relative to the directory containing this notebook.
# This directory must exist. If the files are located in the current directory, use '.'
# as the value of this variable.
DATA_DIR = '.' # Note from Oscar. We may not need this based on our current DB environment


#
# TARGET_FILE_NAME
#
# The NAME of the recoded AHA file.
# This file name is assumed to be relative to DATA_DIR.
TARGET_FILE_NAME =  'aha_2021_recoded.csv'


#
# SOURCE_FILE_NAME
# 
# The NAME of the recoded source dataset.
# This file name is assumed to be relative to DATA_DIR.
SOURCE_FILE_NAME = 'premier_2021_recoded.csv'


#
# OUTPUT_FILE_NAME
# 
# A weighted version of the source file will be written to the file with this name.
# This name is relative to DATA_DIR.
OUTPUT_FILE_NAME = 'premier_2021_weighted_{0}vars.csv'.format(len(RAKE_DATA))

# COMMAND ----------

# MAGIC %md
# MAGIC ### End of User Parameters

# COMMAND ----------

# MAGIC %md
# MAGIC #### Consistency checks on user params

# COMMAND ----------

### Note thos validation makes sense in the case we have a pre especified structure 
# Validate raking requirements and file paths

if len(RAKE_DATA) < 2:
    raise SystemExit('Raking must be performed on two or more variables.')

if not isinstance(MIN_CELL_SIZE, int) or MIN_CELL_SIZE < 0:
    raise SystemExit('The minimum cell size must be a nonnegative integer.')

if not os.path.isdir(DATA_DIR):
    raise SystemExit(f'DATA_DIR not found: "{DATA_DIR}".')

source_filepath = os.path.join(DATA_DIR, SOURCE_FILE_NAME)
if not os.path.isfile(source_filepath):
    raise SystemExit(f'Source file "{source_filepath}" not found.')

target_filepath = os.path.join(DATA_DIR, TARGET_FILE_NAME)
if not os.path.isfile(target_filepath):
    raise SystemExit(f'Target file "{target_filepath}" not found.')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Setup some data structures

# COMMAND ----------

# map of the enumvar to (source, target) colum name tuple
RAKE_MAP = {enumvar:(source_col, target_col) for enumvar, source_col, target_col in RAKE_DATA}
        
# list of enumerated variables to be raked
RAKEVARS = [tup[0] for tup in RAKE_DATA]

# map of variable enum to source file col name
SOURCE_COL_MAP = {RAKEVARS[i]:RAKE_DATA[i][1] for i in range(len(RAKEVARS))}

# map of variable enum to PUMS file col name
TARGET_COL_MAP = {RAKEVARS[i]:RAKE_DATA[i][2] for i in range(len(RAKEVARS))}

# AHA data is unweighted
TARGET_WEIGHT_COL = None

# get the names of the variables, in order
RAKEVAR_NAMES = [enumvar.name for enumvar in RAKEVARS]
print('Variable order: {0}'.format(RAKEVAR_NAMES))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Choose raking models

# COMMAND ----------

# the problem dimension determines which raking models to use
dim = len(RAKEVARS)

if 2 == dim:
    MODELS = models.MODELS_2
elif 3 == dim:
    MODELS = models.MODELS_3
elif 4 == dim:
    MODELS = models.MODELS_4
elif 5 == dim:
    MODELS = models.MODELS_5
elif 6 == dim:
    MODELS = models.MODELS_6
elif 7 == dim:
    MODELS = models.MODELS_7
elif 8 == dim:
    MODELS = models.MODELS_8
elif 9 == dim:
    MODELS = models.MODELS_9
elif 10 == dim:
    MODELS = models.MODELS_10
else:
    # use 1D marginals only
    MODELS = [
        [[i] for i in range(dim)],
    ]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load the target (AHA) data

# COMMAND ----------

# recoded AHA file
print('Loading target file "{0}" ...'.format(target_filepath))
aha_df = pd.read_csv(target_filepath)
print('Raw target dataframe shape: {0}'.format(aha_df.shape))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load the source (Premier) data

# COMMAND ----------

print('Loading source file "{0}" ...'.format(source_filepath))
raw_source_df = pd.read_csv(source_filepath)
print('Raw source dataframe shape: {0}'.format(raw_source_df.shape))

# extract required cols
cols = ['PROV_ID'] + [c for c in SOURCE_COL_MAP.values()]
premier_df = raw_source_df[cols]

if len(RAKE_DATA) >= 5 and DROP_708:
    # drop the row at index 708
    premier_df = premier_df.drop([708])
    premier_df = premier_df.reset_index(drop=True)

print('Source dataframe shape: {0}'.format(premier_df.shape))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check for Premier records not in AHA

# COMMAND ----------

if 4 == len(RAKE_DATA):
    COLS = RAKEVAR_NAMES[:4]
elif 5 == len(RAKE_DATA):
    COLS = RAKEVAR_NAMES

for e in range(3, len(COLS)+1):
    
    s_df = premier_df[COLS[:e]]
    t_df = aha_df[COLS[:e]]

    s_tuples = list(s_df.itertuples(index=False, name=None))
    t_tuples = list(t_df.itertuples(index=False, name=None))

    diff_set = set(s_tuples) - set(t_tuples)

    for tup in diff_set:
        
        if 3 == len(COLS[:e]):
            check_df = aha_df.loc[
                (aha_df['URBRUR']   == tup[0])  &
                (aha_df['TEACHING'] == tup[1])  &
                (aha_df['BEDS']  == tup[2])
            ]
        elif 4 == len(COLS[:e]):
            pass
            check_df = aha_df.loc[
                (aha_df['URBRUR']   == tup[0])  &
                (aha_df['TEACHING'] == tup[1])  &
                (aha_df['BEDS']  == tup[2])  &
                (aha_df['REGION']   == tup[3])
            ]
        else:
            check_df = aha_df.loc[
                (aha_df['URBRUR']     == tup[0])  &
                (aha_df['TEACHING']   == tup[1])  &
                (aha_df['BEDS']    == tup[2])  &
                (aha_df['REGION']     == tup[3])  &
                (aha_df['DISCHARGES'] == tup[4])
            ]
            
        assert 0 == len(check_df)

    print('Variables {0}: '.format(COLS[:e]))
    print('Diff set contains {0} entries.'.format(len(diff_set)))

    s_ctr = Counter(s_tuples)
    t_ctr = Counter(t_tuples)

    tot = 0
    for tup in sorted(list(diff_set)):
        print('\t{0}: {1}'.format(tup, s_ctr[tup]))
        tot += s_ctr[tup]
    print('Total Premier hospitals not in AHA: {0}\n'.format(tot))
### note from Oscar - Output here differes from original markup, output was 54 before. However, the oiriginal data sources may be different (sharepoint files vs files gotten from run)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Build target sample arrays

# COMMAND ----------

def get_target_samples(target_df):
    """
    This function takes the target dataframe and builds three data structures used by the raking code:
    
    TARGET_SAMPLES : a list of np.array entries, one for each target variable column, taken in order
    
    TARGET_WEIGHT_COL : an np.array of the target weights
    
    TARGET_POPULATION : either the sum of the target weights, or the number of target rows if unweighted
    """
    
    # get the names of the target columns in the same order as the RAKEVARS array
    ordered_target_cols = [TARGET_COL_MAP[enumvar] for enumvar in RAKEVARS]
    print('Target columns, in order: {0}\n'.format(ordered_target_cols))

    # samples taken in order of the variables in VARIABLES
    TARGET_SAMPLES = [np.array(target_df[col].values) for col in ordered_target_cols]

    # set TARGET_WEIGHTS to None to rake to an unweighted target population
    if TARGET_WEIGHT_COL is not None:
        TARGET_WEIGHTS = np.array(target_df[TARGET_WEIGHT_COL].values)
        TARGET_POPULATION = np.sum(TARGET_WEIGHTS)
        #print('Target population (weighted): {0}'.format(TARGET_POPULATION))
    else:
        TARGET_WEIGHTS = None
        TARGET_POPULATION = len(TARGET_SAMPLES[0])    
        #print('Target population (unweighted): {0}'.format(TARGET_POPULATION))
        
    return TARGET_SAMPLES, TARGET_WEIGHTS, TARGET_POPULATION        

# COMMAND ----------

# MAGIC %md
# MAGIC #### Build source sample arrays

# COMMAND ----------

def get_source_samples(source_df):
    """
    This function takes the source dataframe and returns a data structure used by the raking code:
    
    SOURCE_SAMPLES: a list of np.array entries, one for each source variable column, taken in order
    """
    
    # get the names of the source dataframe columns in the order matching the variables
    ordered_source_cols = [SOURCE_COL_MAP[enumvar] for enumvar in RAKEVARS]
    print('Source columns, in order: {0}\n'.format(ordered_source_cols))

    # build a list of np.arrays containing the data for each col
    SOURCE_SAMPLES = [np.array(source_df[col].values) for col in ordered_source_cols]
    return SOURCE_SAMPLES

# COMMAND ----------

# MAGIC %md
# MAGIC #### Compute the number of bins required to hold each variable

# COMMAND ----------

# MAGIC %md
# MAGIC Categorical values for a given variable are **assumed** to be a contiguous block of integers starting at 0.

# COMMAND ----------

# bin counts in order of the variables
BIN_COUNTS = []

for enumvar in RAKEVARS:
    BIN_COUNTS.append(CODING.BIN_COUNTS[enumvar])

# maximum-length variable name, used for prettyprinting
maxlen = max([len(var_name) for var_name in RAKEVAR_NAMES])

print('Bin counts: ')
for i in range(len(RAKEVAR_NAMES)):
    print('{0:>{2}} : {1}'.format(RAKEVAR_NAMES[i], BIN_COUNTS[i], maxlen))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Raking in N Dimensions

# COMMAND ----------

# MAGIC %md
# MAGIC This code cell makes the call to the raking function. For each model that converges, the maximum weight, smallest marginal cell size, and the raking model index are saved as a tuple in the `data` list. This information is used below to build the result display table.

# COMMAND ----------

target_df = aha_df.copy()
source_df = premier_df.copy()

# get source and target sample arrays
TARGET_SAMPLES, TARGET_WEIGHTS, TARGET_POPULATION = get_target_samples(target_df)
SOURCE_SAMPLES = get_source_samples(source_df)

# list of (max_wt, smallest_cell_size, model_index) for each converged raking model
data = []

# map of model_index => weights for that model
weight_map = {}
for model_index, model in enumerate(MODELS):

    print('\n[{0}/{1}]\tModel {2}:\n'.format(model_index+1, len(MODELS), model))        

    raking_result_tuple = raking.rake(model,
                                      SOURCE_SAMPLES,
                                      TARGET_SAMPLES,
                                      TARGET_WEIGHTS,
                                      BIN_COUNTS,
                                      TARGET_POPULATION)

    if raking_result_tuple is None:
        print('\n*** No acceptable raking model was found. ***')
    else:
        weights_m, unraked_m, target_m, raked_m, fnorm_m, smallest_cell_m = raking_result_tuple
        if smallest_cell_m < MIN_CELL_SIZE:
            print('\tSource marginal cell size {0} less than MIN_CELL_SIZE {1}, discarding...'.
                  format(smallest_cell_m, MIN_CELL_SIZE))
        else: 
            wmax = np.max(weights_m)
            print('\tMax weight: {0:.3f}'.format(wmax))
            data.append( (wmax, smallest_cell_m, model_index))
            weight_map[model_index] = weights_m

# COMMAND ----------

# MAGIC %md
# MAGIC ### Score and rank the results

# COMMAND ----------

def scoring_metric(weights_m, avg_wt):
    """
    This function implements David Marker's modification of the variability score for
    the weight distribution.
    
    The basic idea is to score the results so that 1) weight distributions with larger
    values of the maximum weight or 2) a larger percentage of the weights in the upper
    tail of the distribution are penalized.
    
    This code sums the weights in the upper MAX_WT_FRAC percentage of the weight distribution.
    It normalizes this by the target population, then multiplies by the maximum weight to
    get the score.
    
    In general lower scores are better, but we have yet to find the perfect scoring system.
    """
    
    # will sum all weights >= this fraction * max_wt
    MAX_WT_FRAC = 0.9
    
    # maximum weight for this model
    wmax = np.max(weights_m)
    
    # cutoff value; compute the sum of all weights >= this cutoff
    w_cutoff = MAX_WT_FRAC * wmax
    
    samples = []
    for w in weights_m:
        if w >= w_cutoff:
            samples.append(w)
            
    w_sum = np.sum(samples)
    
    # w_sum represents this percent of the weighted total population
    pct = w_sum / TARGET_POPULATION
     
    # the score is the product of the max wt and the fraction above the cutoff, divided by the avg weight
    score = wmax * pct / avg_wt
    return score

# COMMAND ----------

# The average weight is used to standardize the scoring metric.
sample_count = len(SOURCE_SAMPLES[0])
avg_wt = TARGET_POPULATION / sample_count

# compute a score for each model
scores = []
for wmax, smallest_cell, model_index in data:
    model = MODELS[model_index]
    weights_m = weight_map[model_index]
    score = scoring_metric(weights_m, avg_wt)
    scores.append(score)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Display results for all converged models

# COMMAND ----------

# collect results in a new dataframe
result_df = pd.DataFrame(data=data, columns=['Max Weight', 'Min Cell', 'Model Index'])
result_df = result_df.assign(**{'Score':scores})
model_details = result_df['Model Index'].map(lambda x: MODELS[x])
result_df = result_df.assign(**{'Model':model_details})

# sort models by increasing max_weight
result_df = result_df.sort_values(by=['Max Weight'])
result_df = result_df.reset_index(drop=True)

# build display dataframe
sample_count = len(SOURCE_SAMPLES[0])
display_df = result_df[['Max Weight', 'Min Cell', 'Score', 'Model']]
print('Results: ')
print('Source samples: {0}, target population: {1}'.format(sample_count, TARGET_POPULATION))
print('Variables : {0}'.format(RAKEVAR_NAMES))
print(tabulate(display_df, headers = 'keys', tablefmt = 'psql', floatfmt=".3f"))

if 0 == len(result_df):
    raise SystemExit('*** No acceptable models were found. ***')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write result file

# COMMAND ----------

# MAGIC %md
# MAGIC The result file is a copy of the source data with ten weight columns appended. These weight columns contain the weights for the ten models that generated the smallest maximum weight values (i.e. the first ten models in the table above).
# MAGIC
# MAGIC The weight sets are numbered `Weights_1`, `Weights_2`, ..., `Weights_10`. The maximum weight increases with the index.

# COMMAND ----------

output_df = source_df.copy()
# append weight cols for the ten models with the smallest max weight
for index, row in result_df.iterrows():
    model_index = row['Model Index']
    weights = weight_map[model_index]
    output_df = output_df.assign(**{'Weights_{0}'.format(index+1):weights})
    if index+1 >= 10:
        break
    
#display(output_df)


# write output file
#output_sdf  = spark.createDataFrame(output_df)
#output_sdf.write.mode("overwrite").saveAsTable(f'edav_prd_cdh.cdh_premier_exploratory.{OUTPUT_FILE_NAME}')

# construct output file name from the source file name
output_file = os.path.join(DATA_DIR, OUTPUT_FILE_NAME)

# write output file
output_df.to_csv(output_file, index=False)
print('Wrote file "{0}".'.format(output_file))


# COMMAND ----------

output_df

# COMMAND ----------

f'edav_prd_cdh.cdh_premier_exploratory.{OUTPUT_FILE_NAME}'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Plots

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Selected components of the weight distributions

# COMMAND ----------

# MAGIC %md
# MAGIC For each model in the table above, taken in order, plot these components of the weight distribution:
# MAGIC * Maximum weight
# MAGIC * Fifth-largest weight (sort the weights, select the weight at the fifth position from the end)
# MAGIC * Twentieth-largest weight
# MAGIC * 99th percentile weight
# MAGIC * 95th percentile weight
# MAGIC * 90th percentile weight

# COMMAND ----------

wts_max = []
wts_m1  = []
wts_m2  = []
wts_99  = []
wts_95  = []
wts_90  = []   
    
for index, row in result_df.iterrows():    
    model_index = row['Model Index']
    weights_m = weight_map[model_index]    
    sorted_weights = sorted(weights_m)
  
    wts_max.append(sorted_weights[-1])
    wts_m1.append(sorted_weights[-5])
    wts_m2.append(sorted_weights[-20])
    wts_99.append(np.percentile(weights_m, 99))
    wts_95.append(np.percentile(weights_m, 95))
    wts_90.append(np.percentile(weights_m, 90))
    
plt.figure(figsize=(20,12))
plt.plot(wts_max, label='Wt max', marker='o')
plt.plot(wts_m1, label='Wt[-5]', marker='o')
plt.plot(wts_m2, label='Wt[-20]', marker='o')
plt.plot(wts_99, label='Wt 99%', marker='o')
plt.plot(wts_95, label='Wt 95%', marker='o')
plt.plot(wts_90, label='Wt 90%', marker='o')
plt.xlabel('Max Weight Rank', fontsize=16)
plt.ylabel('Weight Component', fontsize=16)
plt.title('Selected Weights vs. Max Weight Rank', fontsize=20)
plt.grid()
plt.legend(loc='upper left')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Percentage of the population in the tail of the weight distribution

# COMMAND ----------

# MAGIC %md
# MAGIC For each model in the table above, taken in order, compute the percentage of the population represented by these sets of the largest weights:
# MAGIC * Largest 8 weights
# MAGIC * Largest 16 weights
# MAGIC * Largest 32 weights
# MAGIC
# MAGIC Ideally the upper tail of the weight distribution should only contain a small percentage of the overall population. Large percentages in the upper tail can indicate that the maximum weights are unrepresentative of the population as a whole.

# COMMAND ----------

# N largest weights
lim_data = [
    (8, [], 'Top 8'),
    (16, [], 'Top 16'),
    (32, [], 'Top 32'),
]

for index, row in result_df.iterrows():
    model_index = row['Model Index']
    weights_m = weight_map[model_index]
    sorted_weights = sorted(weights_m)
    
    for lim, arr, label in lim_data:
        
        if lim > len(sorted_weights):
            break
        
        # the nth-from-the-max weight
        wn = sorted_weights[-lim]

        # extract all weights >= this value
        samples = []
        for w in weights_m:
            assert w >= 0
            if w >= wn:
                samples.append(w)    
             
        # the weighted population in the top N weights
        pop = np.sum(samples)
        # pct of the target population this represents
        pct = 100.0 * (pop / TARGET_POPULATION)
        arr.append(pct)

# COMMAND ----------

plt.figure(figsize=(20,12))
for lim, arr, label in lim_data:
    if lim > len(sorted_weights):
        continue
    plt.plot(arr, label=label, marker='o')
plt.xlabel('Max Weight Rank', fontsize=16)
plt.ylabel('Percentage', fontsize=16)
plt.title('Percentage of the Population Represented by the Max N Weights', fontsize=20)
plt.grid()
plt.legend(loc='upper left')
plt.show()
print('Top 8 percentage for best model: {0:.3f}.'.format(lim_data[0][1][0]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Percentage of the population greater than a cutoff weight

# COMMAND ----------

# MAGIC %md
# MAGIC For each model in the table above, taken in order, compute the percentage of the population greater than a cutoff weight. The cutoff values are stated as a percentage of the maximum weight:
# MAGIC * 90% of the maximum weight
# MAGIC * 85% of the maximum weight
# MAGIC * 80% of the maximum weight
# MAGIC * 75% of the maximum weight
# MAGIC
# MAGIC This plot give another perspective on the weights in the upper tail of the weight distribution.

# COMMAND ----------

# pct of population >= cutoff weight
max_data = [
    (90, [], '90%'),
    (85, [], '85%'),
    (80, [], '80%'),
    (75, [], '75%'),
]

for index, row in result_df.iterrows():
    model_index = row['Model Index']
    weights_m = weight_map[model_index]
    sorted_weights = sorted(weights_m)

    for pct, arr, label in max_data:
        wmax = np.max(weights_m)
        cutoff = pct * 0.01 * wmax
        # extract all weights >= cutoff
        samples = []
        for w in weights_m:
            if w >= cutoff:
                samples.append(w)
        
        # fraction of the population in these weights
        pop_fraction = np.sum(samples) / TARGET_POPULATION
        arr.append(pop_fraction * 100.0)        

# COMMAND ----------

plt.figure(figsize=(20,12))
for lim, arr, label in max_data:
    if lim > len(sorted_weights):
        continue
    plt.plot(arr, label=label, marker='o')
plt.xlabel('Max Weight Rank', fontsize=16)
plt.ylabel('Percentage', fontsize=16)
plt.title('Percentage of the Population Greater than the Stated Pct of WMax', fontsize=20)
plt.grid()
plt.legend(loc='upper right')
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Weight distribution for the model with the smallest maximum weight

# COMMAND ----------

# MAGIC %md
# MAGIC Plot a histogram of the weights for the model in the first row of the result table, which is the converged model with the smallest maximum weight.

# COMMAND ----------

best_model_index = result_df.iloc[[0]]['Model Index'][0]
best_model = str(result_df.iloc[[0]]['Model'][0])
best_weights = weight_map[best_model_index]

# COMMAND ----------

def plot_weight_distribution(plot_weights, title):
    
    max_wt = np.max(plot_weights)
    int_max_wt = int(max_wt + 1)
    
    # set the bin count from the max weight
    bins = [q for q in range(int_max_wt + 1)]
    
    fig = plt.figure(figsize=(16,8))
    plt.hist(plot_weights, bins=bins, edgecolor='k', alpha=0.5)
    plt.xlabel('Weight', fontsize=16)
    plt.ylabel('Count', fontsize=16)
    plt.title(title, fontsize=18)
    plt.yscale('log')
    plt.grid()
    plt.show()    

# COMMAND ----------

plot_weight_distribution(best_weights, 'Weight Distribution')
wts_lt_one = 0
for w in best_weights:
    if w < 1.0:
        wts_lt_one += 1
print('Number of weights in [0, 1): {0}.'.format(wts_lt_one))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.1: Check for zero-weight records

# COMMAND ----------

# MAGIC %md
# MAGIC Zero weights will be assigned to any source record with a combination of features that are **not** found in the target records.

# COMMAND ----------

# source data using "best" weight distribution
best_df = source_df.copy()
best_df = best_df.assign(**{'Weight':best_weights})
zero_df = best_df.loc[best_df['Weight'] == 0]
#display(zero_df) # commented by Oscar due to error in display
print('\nFound {0} zero-weight source records.'.format(len(zero_df)))

# drop the weight col, not needed for these checks
zero_df = zero_df.drop(columns=['Weight'])

# convert df to set of tuples
zero_wt_tup_set = set(zero_df.itertuples(index=False, name=None))

# drop the weight col from the pums df
if TARGET_WEIGHT_COL is not None:
    target_no_wt_df = target_df.drop(columns=[TARGET_WEIGHT_COL])
else:
    target_no_wt_df = target_df
target_tup_set = set(target_no_wt_df.itertuples(index=False, name=None))

# these sets should be disjoint, meaning that none of the zero wt tuples occur in the PUMS population
error_population = len(zero_wt_tup_set.intersection(target_tup_set))
    
# this number should be zero
print('Number of zero-weight source records found in the target population: {0}.'.format(error_population))

# COMMAND ----------

zero_df # added by Oscar to show empty df

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4.2: Find the source records with the largest weights

# COMMAND ----------

# MAGIC %md
# MAGIC The records with the largest weights indicate the combinations of characteristics that are **relatively** less common in the source population relative to the target population.

# COMMAND ----------

nonzero_weights = best_weights[best_weights > 0]
sorted_weights = sorted(nonzero_weights, reverse=True)
max_wt_df = best_df[best_df['Weight'].isin(sorted_weights[:10])]
max_wt_df = max_wt_df.sort_values(by=['Weight'], ascending=False)

# Drop the PROV_ID column for display. The AHA data license agreement
# states the following: "No hospital names or unique identifiers may
# be shared or exposed externally."
max_wt_df = max_wt_df.drop(columns=['PROV_ID'])

max_wt_df

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Univariate distributions for the model with the smallest maximum weight
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Compute the weighted source univariate distributions and compare with the target distributions. If the raking procedure was successful, the two distributions should be nearly identical.

# COMMAND ----------

# target pdfs
target_pdfs = {}
for q in range(len(RAKEVARS)):
    target_pdfs[q] = pdf.to_pdf(BIN_COUNTS[q], TARGET_SAMPLES[q], weights=TARGET_WEIGHTS)

# unraked source pdfs
source_unraked_pdfs = {}
for q in range(len(RAKEVARS)):
    source_unraked_pdfs[q] = pdf.to_pdf(BIN_COUNTS[q], SOURCE_SAMPLES[q], weights=None)

# raked source pdfs
source_raked_pdfs = {}
for q in range(len(RAKEVARS)):
    source_raked_pdfs[q] = pdf.to_pdf(BIN_COUNTS[q], SOURCE_SAMPLES[q], weights=best_weights)

print('Univariate Distributions, model {0}:\n'.format(best_model))

# display precision
P = 5
for q in range(0, len(source_unraked_pdfs)):
    print('{0}: '.format(RAKEVAR_NAMES[q]))
    print('\tUnweighted Premier PDF : {0}'.format(np.array_str(source_unraked_pdfs[q], precision=P)))
    print('\t  Weighted Premier PDF : {0}'.format(np.array_str(source_raked_pdfs[q],   precision=P)))
    print('\t               AHA PDF : {0}'.format(np.array_str(target_pdfs[q],         precision=P)))    
    
print('\nPDF diffs after raking for model {0}:\n'.format(best_model))   

# diff between raked and target pdfs
diffs = []
for q in range(len(RAKEVARS)):
    diff = abs(source_raked_pdfs[q] - target_pdfs[q])
    diffs.append(diff)
    print('{0:>{2}} : {1}'.format(RAKEVAR_NAMES[q], 
                                  np.array_str(diff, precision=5, suppress_small=True),
                                  maxlen))

# check the diff vectors for the presence of any diff > 0.01 (i.e. 1%)
all_ok = True
THRESHOLD = 0.01
for diff_vector in diffs:
    if np.any(diff_vector > THRESHOLD):
        all_ok = False

# sum of the weights
sum_of_weights = np.sum(best_weights)
print()
print('Min weight : {0:.3f}'.format(np.min(best_weights)))
print('Max weight : {0:.3f}'.format(np.max(best_weights)))
print()
print('Sum of the weights : {0:.3f}'.format(sum_of_weights))
print('  Population total : {0:.3f}'.format(TARGET_POPULATION))
print('        Difference : {0:.3f}'.format(abs(TARGET_POPULATION - sum_of_weights)))
print('\nRaked PDFs differ from target PDFs by less than {0}%: {1}'.format(int(THRESHOLD * 100),
                                                                           all_ok))

# COMMAND ----------

def plot_pdfs(source_unraked_pdfs, source_raked_pdfs, target_pdfs):
    """
    Plot the unraked source, raked source, and target univariate distributions.
    The raked source and target bars should be nearly identical in height.
    """
    
    # display precision
    P = 5
    
    num_pdfs = len(source_unraked_pdfs)
    assert len(source_raked_pdfs) == len(target_pdfs) == num_pdfs
    
    for q in range(0, num_pdfs):
        plots.triple_histogram_from_pdfs('{0}'.format(RAKEVAR_NAMES[q]), 
                                         source_unraked_pdfs[q], source_raked_pdfs[q], target_pdfs[q],
                                         labels=['Uweighted Premier', 'Weighted Premier', 'AHA'])
        print('Unweighted Premier PDF : {0}'.format(np.array_str(source_unraked_pdfs[q], precision=P)))
        print('  Weighted Premier PDF : {0}'.format(np.array_str(source_raked_pdfs[q],   precision=P)))
        print('               AHA PDF : {0}'.format(np.array_str(target_pdfs[q],         precision=P)))

# COMMAND ----------

plot_pdfs(source_unraked_pdfs, source_raked_pdfs, target_pdfs)

# COMMAND ----------

