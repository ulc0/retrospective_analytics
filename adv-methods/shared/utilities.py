# Conver Premier times to actual times
# - dataset
# - time col
# - 
import datetime
import pandas as pd
import numpy as np
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column
#from pyspark.sql.types import StringType, IntegerType
#import re

def explode_column(dataframe: DataFrame, text: "ColumnOrName") -> DataFrame:
    if isinstance(text, str):
        column, name = F.col(text), text
    elif isinstance(text, Column):
        column, name = text, text.__str__()
    else:
        raise ValueError("text parameter needs to be Column or string type")

    return dataframe.withColumn(
        name+'_exploded',
        F.explode(column)
    )

def samevalue_column(dataframe: DataFrame, booleanCol: "ColumnOrName", col1: "ColumnOrName", col2: "ColumnOrName",) -> DataFrame:
    if isinstance(col1, str):
        column1, name1 = F.col(col1), col1
    elif isinstance(col1, Column):
        column1, name1 = col1, col1.__str__()
    else:
        raise ValueError("col1 parameter needs to be Column or string type")

    if isinstance(col2, str):
        column2, name2 = F.col(col2), col2
    elif isinstance(col2, Column):
        column2, name2 = col2, col2.__str__()
    else:
        raise ValueError("col2 parameter needs to be Column or string type")

    if isinstance(booleanCol, str):
        _, bname = F.col(booleanCol), booleanCol
    elif isinstance(booleanCol, Column):
        _, bname = col2, col2.__str__()
    else:
        raise ValueError("booleanCol parameter needs to be Column or string type")

    return dataframe.withColumn(
        bname,
        F.when(column1==column2,True).otherwise(False)
        )



class premier_time_converter:
    def __init__(self, patdemo, readmit):
        # initializing functions
        def prep_patdemo(patdemo):
            patdemo.columns = patdemo.columns.str.lower()
            patdemo = (patdemo[["pat_key", 
                                "disc_mon", 
                                "disc_mon_seq", 
                                "adm_mon", 
                                "los", 
                                "i_o_ind"]]
                .set_index("pat_key")
            )
            return patdemo
        def prep_readmit(readmit):
            readmit.columns = readmit.columns.str.lower()
            readmit = (readmit[["pat_key", 
                                "disc_mon", 
                                "disc_mon_seq", 
                                "days_from_prior", 
                                "days_from_index", 
                                "calc_los", 
                                "i_o_ind"]]
                .set_index("pat_key")
            )
            return readmit
        def join_enc_data(patdemo, readmit):
            out = (patdemo.
                    join(readmit, rsuffix="_r")
                    [["adm_mon", 
                    "disc_mon", 
                    "disc_mon_r", 
                    "disc_mon_seq",
                    "disc_mon_seq_r",
                    "days_from_prior",
                    "days_from_index",
                    "los",
                    "calc_los",
                    "i_o_ind",
                    "i_o_ind_r"
                    ]]
                )
            out["disc_mon_seq"] = out["disc_mon_seq"].astype("int")

            # out["dfp"] = int(out["days_from_prior"] or 0)
            out["dfi"] = out["days_from_index"].astype("int")
            out["los"] = out["los"].astype("int")
            out["calc_los"] = out["calc_los"].astype("int")
            return out

        # inital vars
        self.enc_data = join_enc_data(prep_patdemo(patdemo),
                                    prep_readmit(readmit))
        self.index_row = self.enc_data[self.enc_data["dfi"] == 0]
        self.dfi = self.enc_data["dfi"].tolist()
        self.calc_los = np.array(self.enc_data["calc_los"],
                            "timedelta64[D]")

        self.ind_adm = None
        self.ind_disc = None
        self.adm_dt = None
        self.disc_dt = None

        self.paticd = None
        self.patcpt = None
        self.patlabres = None
        self.patgenlab = None
        self.patbill = None 

    def get_enc_data(self):
        return self.enc_data
    def set_adm_dt(self):
        self.ind_adm = datetime.datetime.strptime(
            self.index_row["adm_mon"].str[0:4].iat[0] +
            self.index_row["adm_mon"].str[-2:].iat[0] +
            "01",
            "%Y%m%d"
            )
        self.ind_disc = self.ind_adm + datetime.timedelta(
            days=int(self.index_row["calc_los"].iat[0])
            )
        self.adm_dt = np.datetime64(self.ind_disc) + np.array(self.dfi, dtype="timedelta64[D]")
        self.disc_dt = self.adm_dt + self.calc_los

    def set_patbill(self, patbill):
        self.patbill = patbill    
    


