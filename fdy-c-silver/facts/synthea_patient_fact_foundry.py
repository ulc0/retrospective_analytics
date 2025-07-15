import pyspark.sql.functions as F
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from transforms.api import Input, Output, transform_df, TransformContext

# COMMAND ----------

# MAGIC %md
# MAGIC https://github.com/synthetichealth/synthea/wiki/CSV-File-Data-Dictionary
# https://www.palantir.com/docs/foundry/transforms-python/transforms-python-api#transform_df
# https://www.palantir.com/docs/foundry/transforms-python/transforms-python-api#transform

# this transform() ??

schema = StructType([
    StructField("observation_datetime", TimestampType(), True),
    StructField("person_id", StringType(), False),
    StructField("concept_code", StringType(), True),
    StructField("src_vocabulary_id", StringType(), True),
    StructField("src_table", StringType(), True),
])


@transform_df( output=Output("/DCIPHER/Training Playground/cdh_silver_synthea_10k/fact_person") )
def process(ctx: TransformContext, output: Output):
    tdict = {
        "allergies": "ri.foundry.main.dataset.0c58dace-96f4-4021-a5c4-f862189dfc0b",
        "careplans": "ri.foundry.main.dataset.5d9f86b7-25e7-46a7-b56a-d60efe03978d",
        "conditions": "ri.foundry.main.dataset.498f7a0a-26c9-4bcb-8af0-58377d662f4c",
        "encounters": "ri.foundry.main.dataset.661c5ad0-d6a2-4285-9346-0852a32f9282",
        "imaging_studies": "ri.foundry.main.dataset.ba9a6fe5-63fd-460c-a19c-2cefceeb8fcb",
        "immunizations": "ri.foundry.main.dataset.5deb41fc-b289-434e-bdd4-e877283d01b4",
        "medications": "ri.foundry.main.dataset.84eb847e-b312-4af7-b9b1-29c7c245c16b",
        "observations": "ri.foundry.main.dataset.84eb847e-b312-4af7-b9b1-29c7c245c16b",
        "patients": "ri.foundry.main.dataset.84eb847e-b312-4af7-b9b1-29c7c245c16b",
        "procedures": "ri.foundry.main.dataset.84eb847e-b312-4af7-b9b1-29c7c245c16b",
        "providers": "ri.foundry.main.dataset.84eb847e-b312-4af7-b9b1-29c7c245c16b",
        "supplies": "ri.foundry.main.dataset.84eb847e-b312-4af7-b9b1-29c7c245c16b",
    }

    castdict = {
        "PATIENT": "patient_id",
        "START": "observation_datetime",
        "DATE": "observation_datetime",
        "CODE": "concept_code",
        "BODYSITE_CODE": "concept_code",
        "PROCEDURECODE": "concept_code",
        "DIAGNOSIS1": "concept_code",
        "SYSTEM": "vocabulary_id",
    }
    clist = list(castdict.keys())
#    print(clist)

    vocabdict = {
        "observations": "LOINC",
        "immunizations": "CVX",
        "medications": "RXNORM",
    }

    fact_df = ctx.spark_session.createDataFrame([], schema)

    for tbl, rid in tdict:
        print(tbl)
        #    df=spark.table(f"{src_catalog}.{src_schema}.{src_prefix}_{tbl}")
        #    display(fname)
        # df = spark.read.options(header=True,).csv(fname)
        # display(df)
        #df = ctx.spark_session.read.options(header=True).csv(rid)
        df=Input(rid)
        dfcols = list(df.columns)
        # print(df.columns)
        #   print(dfcols)
        #   print(clist)
        #   keep=list(set(clist)&set(dfcols))
        keep = [x for x in dfcols if x in clist]
        if len(keep) > 0:  # == len(clist):
            # print(keep)
            vocab = "SNOMED"
            if "SYSTEM" not in keep:
                if tbl in vocabdict.keys():
                    vocab = vocabdict[tbl]
                    # print(vocab)
                keep = keep + ["SYSTEM"]
            # print(keep)
            xdf = (
                df.withColumn("SYSTEM", F.lit(vocab))
                .select(*keep)
                .withColumnsRenamed(castdict)
                .withColumn("source_table", F.lit(tbl))
                .distinct()
            )
            # display(xdf)
            # print(xdf.columns)
            fact_df = fact_df.union(xdf)
# print(f"Writing {tbl}")
    return fact_df
#    fact_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(f"edav_prd_cdh.cdh_ml.synthea_{tbl}")


"""

domains = [
    "Procedure",
    "Condition",
    "Drug",
    "Observation",
    "Specimen",
    "Visit",
    "Meas Value",
    "Type Concept",
]
dstr = ",".join(domains)
print(dstr)
concept = spark.sql(
    f"select concept_code,vocabulary_id,concept_id,domain_id from cdh_ml.concept where domain_id in
     ('Procedure','Condition','Drug','Observation','Specimen','Visit','Meas Value','Type Concept')
      order by concept_code, vocabulary_id"
)
display(concept)

# COMMAND ----------

fact_tbl = fact_df.join(concept, ["concept_code"], "left")
display(fact_df)

# COMMAND ----------

# quick QC

display(
    fact_tbl.groupBy(
        "src_table",
        "domain_id",
        "src_vocabulary_id",
        "vocabulary_id",
    )
    .count()
    .orderBy("src_table", "src_vocabulary_id")
)
# display(fact_tbl.where(isnull(fact_tbl.domain_id)))

# COMMAND ----------


fact_df.write.mode("overwrite").option("overwriteSchema", "true").format(
    "delta"
).saveAsTable(f"edav_prd_cdh.cdh_ml.synthea_fact_person")
"""
