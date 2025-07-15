# COMMAND ----------

# Maximize Pandas output text width.
# import pandas as pd
# pd.set_option('display.max_colwidth', 200)

#ready for advanced plotting
# import matplotlib.pyplot as plt
# import numpy as np

#import pyspark sql functions with alias
import pyspark.sql.functions as F
import pyspark.sql.window as W

#import specific functions from pyspark sql types
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, ArrayType

#needed to create maps/dictionary for spark dataframe
from itertools import chain
from functools import reduce

# COMMAND ----------

def _expand_map(custom_dict):
    """Return a dictionary where desired output values can also serve as keys."""
    
    # Create a key from the the dict value and use itself as the value 
    labbr_abbr = {str.lower(v): v for k, v in custom_dict.items()}

    # Add new keys to custom dict
    custom_dict.update(labbr_abbr)
    
    return custom_dict

# COMMAND ----------

states_dict = {
    'alabama':'AL',
    'alaska':'AK',
    'arizona':'AZ',
    'arkansas':'AR',
    'california':'CA',
    'colorado':'CO',
    'connecticut':'CT',
    'delaware':'DE',
    'florida':'FL',
    'georgia':'GA',
    'hawaii':'HI',
    'idaho':'ID',
    'illinois':'IL',
    'indiana':'IN',
    'iowa':'IA',
    'kansas':'KS',
    'kentucky':'KY',
    'louisiana':'LA',
    'maine':'ME',
    'maryland':'MD',
    'massachusetts':'MA',
    'michigan':'MI',
    'minnesota':'MN',
    'mississippi':'MS',
    'missouri':'MO',
    'montana':'MT',
    'nebraska':'NE',
    'nevada':'NV',
    'new hampshire':'NH',
    'new jersey':'NJ',
    'new mexico':'NM',
    'new york':'NY',
    'north carolina':'NC',
    'north dakota':'ND',
    'ohio':'OH',
    'oklahoma':'OK',
    'oregon':'OR',
    'pennsylvania':'PA',
    'rhode island':'RI',
    'south carolina':'SC',
    'south dakota':'SD',
    'tennessee':'TN',
    'texas':'TX',
    'utah':'UT',
    'vermont':'VT',
    'virginia':'VA',
    'washington':'WA',
    'west virginia':'WV',
    'wisconsin':'WI',
    'wyoming':'WY',
    'district of columbia':'DC',
    'puerto rico':'PR',
    'virgin islands':'VI',
    'american samoa':'AS'
}

states_dict = _expand_map(states_dict)

# COMMAND ----------

def _clean_input(uinput, remove_space_char=True):
    """Lowercase input and remove hyphens, periods, and optionally space characters."""
    
    if type(uinput) == str:
        uinput_cleaned = uinput.lower().replace('-','').replace('.','')

        if remove_space_char:
            uinput_cleaned = uinput_cleaned.replace(' ','')

    else:
        uinput_cleaned = F.regexp_replace(F.lower(uinput),r"[-.]","")
        
        if remove_space_char:
            uinput_cleaned = F.regexp_replace(uinput_cleaned, r"[\s]",'')
            
    return uinput_cleaned

# COMMAND ----------

def percent_full(df):
    """Return a dataframe with the percent of non-null values in dataframe."""
    
    _df = df.select([(F.count(F.when(F.col(c).isNotNull(), c))/df.count()*100).alias(c) for c in df.columns])

    return _df

# COMMAND ----------

def percent_not_empty(df):
    """Return a dataframe with the percent of non-null and non-blank values in dataframe."""
    
    c1 = lambda x: F.col(x).isNotNull()
    c2 = lambda x: F.length(F.col(x)) == 0
    
    denom = df.count()

    _df = df.select([(F.count(F.when(c1(c) & ~c2(c), c))/denom*100).alias(c) for c in df.columns])

    return _df

# COMMAND ----------

def column_profile(df):
    """Return a two-row dataframe with the min and max values for every column in the input dataframe."""
    
    _dfmin = df.select([F.min(c).alias(c) for c in df.columns])
    _dfmax = df.select([F.max(c).alias(c) for c in df.columns])
    
    _df = reduce(lambda x,y: x.unionByName(y), [_dfmin, _dfmax])
    
    return _df

# COMMAND ----------

def clean_gender(col_name='gender'):
    """Return a dataframe with the specified gender column coded as M, F, or U"""

    col_name = col_name
    
    def _(input_df):
        df = input_df.withColumn(
            col_name,
            F.when(F.lower(F.col(col_name)).isin('m','male'),'M')
            .when(F.lower(F.col(col_name)).isin('f','female'),'F')
            .otherwise('U')
        )
        return df

    return _

# COMMAND ----------

def clean_state(col_name='service_location_state'):
    """Return a dataframe where state names and state codes are converted to two-letter abbreviations."""
        
    col_name = col_name
    
    def _(input_df):
    
        mapping_expr = F.create_map([F.lit(_clean_input(x)) for x in chain(*states_dict.items())])

        df = input_df.withColumn(col_name, F.upper(mapping_expr[_clean_input(F.col(col_name))]))
        
        
        return df
    
    return _

# COMMAND ----------

def assign_regions(col_name='service_location_state'):
    """Return a dataframe with US census region appended."""
    #https://www2.census.gov/geo/pdfs/maps-data/maps/reference/us_regdiv.pdf

    col_name = col_name
    
    def _(input_df):
    
        df = input_df.withColumn(
            'region',
            F.when(input_df[col_name].isin('WA','OR','ID','MT','WY','CA','NV','UT','CO','AZ','NM', 'HI', 'AK'),'west')
            .when(input_df[col_name].isin('ND','MN','WI','MI','SD','NE','KS','IA','MO','IL','IN','OH'),'midwest')
            .when(input_df[col_name].isin('OK','TX','AR','LA','KY','TN','MS','AL','WV','MD','DE','DC','VA','NC','SC','GA','FL'),'south')
            .when(input_df[col_name].isin('ME','VT','NH','MA','CT','RI','NY','PA','NJ'),'northeast')
        )
        return df
    
    return _

# COMMAND ----------

def clean_date(col_name='dob'):
    """Return a dataframe where specified column is truncated to YYYY-MM-DD format"""
        
    col_name = col_name
    
    def _(input_df):

        df = input_df.withColumn(col_name, F.substring(col_name,1,10))
        
        return df
    
    return _

# COMMAND ----------

def clean_zipcode(col_name='zipcode'):
    """Modify the selected column and return a dataframe that has a 5 digit zipcode."""
        
    col_name = col_name
    
    def _(input_df):

        df = (
            input_df
            .withColumn(
                col_name, 
                F.when(
                    (F.length(col_name) > 4),
                    F.substring(col_name,1,5)
                )
            )
        )
        
        return df
    
    return _

# COMMAND ----------

def create_age(col_name='dob', vs_column=None):
    """Calculate age and age-group and return a dataframe."""
        
    col_name = col_name
    
    def _(input_df):
        
        if vs_column:
            df = input_df.withColumn(
            'age',
            (F.datediff(
              vs_column,
              F.to_date(col_name)
            )/365.25).cast('int')
          ) 
            
        elif vs_column is None:
            df = input_df.withColumn(
        'age',
        (F.datediff(
          F.current_date(),
          F.to_date(col_name)
        )/365.25).cast('int')
      )     
        
        df = df.withColumn(
    "age_group",
    F.when(F.col("age") < 0, "unknown")
    .when(F.col("age") < 18, "0-17")
    .when(F.col("age") < 50, "18-49")
    .when(F.col("age") < 65, "50-64")
    .when(F.col("age") < 75, "65-74")
    .when(F.col("age") < 85, "75-84")
    .when(F.col("age") < 90, "85-89")
    .when(F.col("age") >= 90, ">90")
    .otherwise("unknown")
  )  
        
        return df
    
    return _

# COMMAND ----------

pp_category_dict = {
    '1':'1',
    '2':'2',
    '3':'3',
    'snomedct':'3',
    'icd9cm':'1',
    'icd10cm':'2',
    '2.16.840.1.113883.6.96':'3',
    '2.16.840.1.113883.6.90':'2',
    '9':'1',
    '10':'2'
}

pp_category_dict = _expand_map(pp_category_dict)

# COMMAND ----------

def clean_category_code(col_name='problemcategory'):
    """Map values in column to either 1, 2, or 3 (ICD9,ICD10,SNOMED) and return a dataframe."""
        
    col_name = col_name
    
    def _(input_df):
    
        mapping_expr = F.create_map([F.lit(_clean_input(x)) for x in chain(*pp_category_dict.items())])

        df = input_df.withColumn(col_name, mapping_expr[_clean_input(F.col(col_name))])
        
        return df
    
    return _

# COMMAND ----------

starts_with_from_list = lambda i,j: reduce(
    lambda x,y: x|y, [F.col(i).startswith(s) for s in j], F.lit(False)
)

# COMMAND ----------

rlike_from_list = lambda i,j: reduce(
    lambda x,y: x|y, [F.col(i).rlike(s) for s in j], F.lit(False)
)

# COMMAND ----------

language_dict = {
    'english':'english',
    'spanish':'spanish',
#     'vietnamese':'vietnamese',
    'eng':'english',
#     'yiddish':'yiddish',
#     'chinese':'chinese',
#     'armenian':'armenian',
#     'tagalog':'taglog',
    'en':'english',
    'spanish; castillian':'spanish',
}

# COMMAND ----------

def clean_languagetext(col_name='languagetext'):
    """Return one of English, Spanish, or Other/unknown."""
        
    col_name = col_name
    
    def _(input_df):
    
        mapping_expr = F.create_map([F.lit(_clean_input(x, remove_space_char=False)) for x in chain(*language_dict.items())])

        df = input_df.withColumn(col_name, mapping_expr[_clean_input(F.col(col_name), remove_space_char=False)])
        
        df = df.withColumn(col_name, F.when(F.col(col_name).isNull(), "Unknown/Missing").otherwise(F.initcap(F.col(col_name))))
        
        return df
    
    return _
