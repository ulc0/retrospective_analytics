#test comment from qxv3
#this doesn't work. Can't have second parameter in `renameColumns`

from schema import *

from pyspark.sql import DataFrame

def transform(self, f):
    return f(self)

DataFrame.transform = transform

def renameColumns(df,rename_dict):
     from pyspark import col
     return df.select([col(c).alias(rename_dict.get(c, c)) for c in df.columns])


df_renamed = spark.sql('select * from patdemo limit 10000') \
.transform(renameColumns,patdemo.t_dict)

print(df_renamed)