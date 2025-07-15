import pytest
import sys
from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F

import cdh_featurization.markup_cleaner as muc

spark = SparkSession.builder.appName("text-xml-utils").getOrCreate()


def test_clean_xml():
    """
    We test utils.clean_xml with various XML strings
    """
    test_data = [
        Row(
            xml_data="""<?xml version="1.0" encoding="UTF-8"?><review><Category Code="42" Name="MyName"></Category></review>"""
        ),
        Row(
            xml_data="""<note><to>User</to><from>AI</from><heading>Reminder</heading><body>Don't forget to attend the meeting!</body></note>"""
        ),
        Row(
            xml_data="""<catalog><book id="bk101"><author>John Doe</author><title>XML Developer's Guide</title>"""
        ),
    ]

    expected_results = [
        "review Category Code 42 Name MyName",
        "note to User from AI heading Reminder body Don't forget to attend the meeting!",
        "catalog book id bk101 author John Doe title XML Developer's Guide"
    ]

    test_df = spark.createDataFrame(test_data)
    column = test_df["xml_data"]

    xml_cleaned = muc.clean_xml(test_df["xml_data"])

    # Another function handles whitespace, so we do it here for our tests
    xml_cleaned = F.regexp_replace(xml_cleaned, r"\s{2,}", " ")
    
    test_df = test_df.withColumn("results", xml_cleaned)
        
    for index, row in enumerate(test_df.select("results").collect()):
        error_msg = f"Test {index} failed. ACTUAL:: {row.results} != EXPECTED:: {expected_results[index]}"
        assert row.results == expected_results[index], error_msg
