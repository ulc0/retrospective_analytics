from itertools import chain
from typing import Dict
from pyspark.sql import DataFrame, Column
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, IntegerType

def ColumnOrName(inColumn:"ColumnOrName"):
    if isinstance(inColumn, str):
        column, name = F.col(inColumn), inColumn
    elif isinstance(inColumn, Column):
        column, name = inColumn, inColumn.__str__()
    else:
        raise ValueError("text parameter needs to be Column or string type")
    return column, name

#suitable for transform
def map_column_values(df:DataFrame, map_dict:Dict, scolumn:"ColumnOrName", ncolumn:"ColumnOrName")->DataFrame:  
    """Handy method for mapping column values from one value to another
    Args:
        df (DataFrame): Dataframe to operate on 
        map_dict (Dict): Dictionary containing the values to map from and to
        scolumn (ColumnOrName): The column containing the values to be mapped
        ncolumn (ColumnOrName, optional): The name of the column to store the mapped values in. 
                                    If not specified the values will be stored in the original column

    Returns:
        DataFrame

    """
    column, _ = ColumnOrName(scolumn)
    new_column,_ = ColumnOrName(ncolumn)
    spark_map = F.create_map([F.lit(x) for x in chain(*map_dict.items())])
    return df.withColumn(new_column or column, spark_map[df[column]])