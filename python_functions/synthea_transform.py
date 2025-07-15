from functions.api import function  # , String
import transforms

#  from transforms.api import *

from pyspark.sql import functions as F

# from pyspark.sql.types import StructType,StructField, StringType, TimestampType


castdict = {
    "PATIENT": "patient_id",
    #    "ENCOUNTER":"observation_period_id",
    "START": "observation_datetime",
    "DATE": "observation_datetime",
    "CODE": "code",
    "BODYSITE_CODE": "code",
    "PROCEDURECODE": "code",
    "DIAGNOSIS1": "code",
    "SYSTEM": "vocabulary_id",
}
clist = list(castdict.keys())

vocabdict = {
    "observations": "LOINC",
    "immunizations": "CVX",
    "medications": "RXNORM",
}


@transforms
def synthea_transform(df, tbl):
    dfcols = list(df.columns)
    keep = [x for x in dfcols if x in clist]
    if len(keep) > 0:
        print(keep)
        vocab = "SNOMED"
        if "SYSTEM" not in keep:
            if tbl in vocabdict.keys():
                vocab = vocabdict[tbl]
                print(vocab)
            keep = keep + ["SYSTEM"]
        xdf = (
            df.withColumn("SYSTEM", F.lit(vocab))
            .select(*keep)
            .withColumnsRenamed(castdict)
            .withColumn("source_type_concept_id", F.lit(tbl))
            .distinct()
        )
    return xdf


# def my_function() -> String:
#    return "Hello World!"
