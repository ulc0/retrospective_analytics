# Maximize Pandas output text width.
import pandas as pd
pd.set_option('display.max_colwidth', 200)

#ready for advanced plotting
import matplotlib.pyplot as plt
import numpy as np

#import pyspark sql functions with alias
import pyspark.sql.functions as F
import pyspark.sql.window as W

#import specific functions from pyspark sql types
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

import math

import plotly.express as px

# COMMAND ----------

import shared.abfm-utils as etl_utils
# COMMAND ----------

def prepare_geo_data(mdf, state_col, uid_col, states=True):
    
    total_rows = mdf.count()

    state_practice_counts = (
    mdf
    .transform(clean_state(state_col))
    .groupBy(state_col)
    .agg(
            F.countDistinct(uid_col).alias(f'unique_{uid_col}s'),
            F.count(uid_col).alias('table_rowcount')
           )
    ).toPandas()

    if states:
        state_practice_counts = state_practice_counts.loc[~state_practice_counts[state_col].isin(['PR','MP','VI','AS']),:]
    
    state_practice_counts['percent_total_rowcount'] = (state_practice_counts['table_rowcount'] / total_rows)*100

    state_practice_counts = state_practice_counts.dropna()
    
    return state_practice_counts

# COMMAND ----------

def simple_geoplot(df, location, factors):

    summary = df.describe()

    all_cbs = []
    all_cls = []
    all_cmaps = []

    factors = factors

    for factor in factors:
        
        factor_label = f"{factor.replace('_',' ').title()}"

        a = math.ceil(summary.loc['min',factor])
        b = math.ceil(summary.loc['25%',factor])
        c = math.ceil(summary.loc['50%',factor])
        d = math.ceil(summary.loc['75%',factor])
        e = math.ceil(summary.loc['max',factor])

        cl = [f"{a:,} - {b:,}",f"{(b+1):,} - {c:,}",f"{(c+1):,} - {d:,}",f"{(d+1):,} - {e:,}",]
        cb = [a,b,c,d,e]

        cm = [
        px.colors.sequential.Blues[8], 
        px.colors.sequential.Blues[4],
        px.colors.sequential.Reds[8], 
        px.colors.sequential.Reds[4]
        ]
        cmap = dict(zip(cl,cm))

        all_cbs.append(cb)
        all_cls.append(cl)
        all_cmaps.append(cmap)
        
        try:
            df[factor_label] = pd.cut(df[factor], bins=cb, labels=cl,include_lowest=True)
        
        except ValueError:
            a = summary.loc['min',factor]
            b = summary.loc['25%',factor]
            c = summary.loc['50%',factor]
            d = summary.loc['75%',factor]
            e = summary.loc['max',factor]

            cl = [f"{a:.2f} - {b:.2f}",f"{b:.2f} - {c:.2f}",f"{c:.2f} - {d:.2f}",f"{d:.2f} - {e:.2f}",]
            cb = [a,b,c,d,e]
            cm = [
            px.colors.sequential.Blues[8], 
            px.colors.sequential.Blues[4],
            px.colors.sequential.Reds[8], 
            px.colors.sequential.Reds[4]
            ]
            cmap = dict(zip(cl,cm))
            df[factor_label] = pd.cut(df[factor], bins=cb, labels=cl,include_lowest=True)

        fig = px.choropleth(
            df,
            hover_name=factor,
            width=800,
            height=400,
            locations=location,
            locationmode='USA-states',
            color=factor_label,
            scope='usa',
            category_orders={factor_label:cl},
            color_discrete_map=cmap
        )
        fig.show()

# COMMAND ----------

def cc_geoplot(df, location, factors, colors=None):

    summary = df.describe()

    all_cbs = []
    all_cls = []
    all_cmaps = []

    if not colors:
        c = [
        px.colors.sequential.Blues[8], 
        px.colors.sequential.Blues[4],
        px.colors.sequential.Reds[8], 
        px.colors.sequential.Reds[4]
        ]
    
    factors = factors

    for factor in factors:
        
        factor_label = f"{factor.replace('_',' ').title()}"

        a = math.ceil(summary.loc['min',factor])
        b = math.ceil(summary.loc['25%',factor])
        c = math.ceil(summary.loc['50%',factor])
        d = math.ceil(summary.loc['75%',factor])
        e = math.ceil(summary.loc['max',factor])

        cl = [f"{a:,} - {b:,}",f"{(b+1):,} - {c:,}",f"{(c+1):,} - {d:,}",f"{(d+1):,} - {e:,}",]
        cb = [a,b,c,d,e]

        cmap = dict(zip(cl,c))

        all_cbs.append(cb)
        all_cls.append(cl)
        all_cmaps.append(cmap)
        
        df[factor_label] = pd.cut(df[factor], bins=cb, labels=cl,include_lowest=True)

        fig = px.choropleth(
            df,
            width=800,
            height=400,
            locations=location,
            locationmode='USA-states',
            color=factor_label,
            scope='usa',
            category_orders={factor_label:cl},
            color_discrete_map=cmap
        )
        fig.show()
