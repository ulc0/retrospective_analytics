import pytest
import sys
from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import StringType, IntegerType
import re
import note.text_process as tp

spark = SparkSession.builder.appName("text-process-utils").getOrCreate()


def test_setLowercase():
    """
    We test text_process.setLowercase with multiple strings
    """
    test_data = [
        Row(
            text="Encouraged and discussed advanced directives. H: Advance Directives X: 06/09/21 Follow Up  Annually and PRN"
        ),
        Row(
            text="ASKING FOR BLOOD WORK WHICH SHE JUST HAD IT DONE THIS MORNING ORDER MAMMO FOR OCTOBER"
        ),
    ]
    test_df = spark.createDataFrame(test_data)

    expected_data = [
        Row(
            text="encouraged and discussed advanced directives. h: advance directives x: 06/09/21 follow up  annually and prn"
        ),
        Row(
            text="asking for blood work which she just had it done this morning order mammo for october"
        ),
    ]
    expected_df = spark.createDataFrame(expected_data)

    result_df = tp.setLowercase(test_df, "text")

    result = [row["text"] for row in result_df.collect()]
    expected = [row["text"] for row in expected_df.collect()]

    assert result == expected


def test_trim_string():
    """
    We test text_process.trim_string with multiple strings
    """
    test_data = [
        Row(
            text=" we can call and schedule that, but I hate to make her wait until tomorrow with CMR gets in.&#160; Unless its okay to wait.&#160;"
        ),
        Row(
            text=" TIMES A DAY AND HE IS NOT ON INSULIN.&#160; HE HAS NO IDEA WHAT SHE TOLD THEM.&#160; "
        ),
        Row(
            text="I WOULD SEND FOR METER, TEST STRIPS AND LANCETS TO THE PHARMACY OF HIS CHOICE.&#160; JMT</span></font></div></body></html> "
        ),
    ]
    test_df = spark.createDataFrame(test_data)

    expected_data = [
        Row(
            text="we can call and schedule that, but I hate to make her wait until tomorrow with CMR gets in.&#160; Unless its okay to wait.&#160;"
        ),
        Row(
            text="TIMES A DAY AND HE IS NOT ON INSULIN.&#160; HE HAS NO IDEA WHAT SHE TOLD THEM.&#160;"
        ),
        Row(
            text="I WOULD SEND FOR METER, TEST STRIPS AND LANCETS TO THE PHARMACY OF HIS CHOICE.&#160; JMT</span></font></div></body></html>"
        ),
    ]
    expected_df = spark.createDataFrame(expected_data)

    result_df = tp.trim_string(test_df, "text")

    result = [row["text"] for row in result_df.collect()]
    expected = [row["text"] for row in expected_df.collect()]

    assert result == expected


def test_replace_nonbreaking_space():
    """
    We test text_process.replace_nonbreaking_space with multiple strings containing nonbreaking spaces
    """
    test_data = [
        Row(
            text="Flonase, Levaquin and Prednisone 20 mg one tablet daily for five days is given.&#160; Follow up earlier if needed otherwise as per scheduled appointment"
        ),
        Row(text="SIGNED BY John Smith &#160;"),
    ]
    test_df = spark.createDataFrame(test_data)

    expected_data = [
        Row(
            text="Flonase, Levaquin and Prednisone 20 mg one tablet daily for five days is given.  Follow up earlier if needed otherwise as per scheduled appointment"
        ),
        Row(text="SIGNED BY John Smith  "),
    ]
    expected_df = spark.createDataFrame(expected_data)

    result_df = tp.replace_nonbreaking_space(test_df, "text")

    result = [row["text"] for row in result_df.collect()]
    expected = [row["text"] for row in expected_df.collect()]

    assert result == expected


def test_replace_ampersand():
    """
    We test text_process.replace_ampersand with multiple strings containing ampersands
    """
    test_data = [
        Row(
            text='evening. &#160; </span></div><div align="left"><span style=" font-size:10pt"><b>Follow Up</b>:&#160; PRN </span></div><div align="left">Refer to Hand Center for complete extraction&amp;if cyst returns or does not completely drain.'
        ),
        Row(
            text="20 minutes spent with patient, greater than 50% of the office visit was dedicated to counseling, reviewing tests &amp; labs, treatment options and follow up"
        ),
    ]
    test_df = spark.createDataFrame(test_data)

    expected_data = [
        Row(
            text='evening. &#160; </span></div><div align="left"><span style=" font-size:10pt"><b>Follow Up</b>:&#160; PRN </span></div><div align="left">Refer to Hand Center for complete extraction and if cyst returns or does not completely drain.'
        ),
        Row(
            text="20 minutes spent with patient, greater than 50% of the office visit was dedicated to counseling, reviewing tests  and  labs, treatment options and follow up"
        ),
    ]
    expected_df = spark.createDataFrame(expected_data)

    result_df = tp.replace_ampersand(test_df, "text")

    result = [row["text"] for row in result_df.collect()]
    expected = [row["text"] for row in expected_df.collect()]

    assert result == expected


def test_replace_paragraph_break():
    """
    We test text_process.replace_paragraph_break with multiple strings containing paragraph breaks
    """
    test_data = [
        Row(
            text="Federal Drive<br>Janice J Hessling MD,PhD<br>4380 Federal Dr, Ste 100<br>Greensboro, NC 27410-8149<br><br><p>"
        ),
        Row(
            text="<p>Flonase, Levaquin and Prednisone 20 mg one tablet daily for five days is given.&#160;</p>"
        ),
    ]
    test_df = spark.createDataFrame(test_data)

    expected_data = [
        Row(
            text="Federal Drive<br>Janice J Hessling MD,PhD<br>4380 Federal Dr, Ste 100<br>Greensboro, NC 27410-8149<br><br> ",
        ),
        Row(
            text=" Flonase, Levaquin and Prednisone 20 mg one tablet daily for five days is given.&#160; ",
        ),
    ]
    expected_df = spark.createDataFrame(expected_data)

    result_df = tp.replace_paragraph_break(test_df, "text")

    result = [row["text"] for row in result_df.collect()]
    expected = [row["text"] for row in expected_df.collect()]

    assert result == expected


def test_replace_pipes():
    """
    We test text_process.replace_pipes with multiple strings containing pipes
    """
    test_data = [
        Row(text="This|is|a|string|with|pipes"),
        Row(text="This||has||multiple|pipes"),
        Row(text="No pipes in this string"),
    ]
    test_df = spark.createDataFrame(test_data)

    expected_data = [
        Row(text="This is a string with pipes"),
        Row(text="This  has  multiple pipes"),
        Row(text="No pipes in this string"),
    ]
    expected_df = spark.createDataFrame(expected_data)

    result_df = tp.replace_pipes(test_df, "text")

    result = [row["text"] for row in result_df.collect()]
    expected = [row["text"] for row in expected_df.collect()]

    assert result == expected


def test_replace_multi_space():
    """
    We test text_process.replace_multi_space with multiple strings containing multispaces
    """
    test_data = [
        Row(
            text="faq/QFT<br>(This link is being provided for  informational/<br>educational purposes only.)"
        ),
        Row(text="SIGNED BY  Deanna M Twyman, RN, FNP-BC (DMT)"),
        Row(
            text="no weakness, no numbness, no tingling, no burning, no seizure no activity, no headache, no visual changes, no ataxia, no vertigo"
        ),
    ]
    test_df = spark.createDataFrame(test_data)

    expected_data = [
        Row(
            text="faq/QFT<br>(This link is being provided for  informational/<br>educational purposes only.)"
        ),
        Row(text="SIGNED BY  Deanna M Twyman, RN, FNP-BC (DMT)"),
        Row(
            text="no weakness, no numbness, no tingling, no burning, no seizure no activity, no headache, no visual changes, no ataxia, no vertigo"
        ),
    ]
    expected_df = spark.createDataFrame(expected_data)

    result_df = tp.replace_multi_space(test_df, "text")

    result = [row["text"] for row in result_df.collect()]
    expected = [row["text"] for row in expected_df.collect()]

    assert result == expected


def test_replace_multi_comma():
    """
    We test text_process.replace_multi_comma with multiple strings containing commas
    """

    test_data = [
        Row(
            text="Follow up earlier if needed,,otherwise as per scheduled appointment.</span></font>"
        ),
        Row(
            text='</span></div><div align "left">Refer to Hand Center for complete extraction if cyst returns,,or does not completely drain. </div>'
        ),
        Row(
            text="CISION SUBCUTANEOUS LESION: Procedure: FACE cleaned and draped,,Local infiltration done with 1% xylocaine."
        ),
    ]
    test_df = spark.createDataFrame(test_data)

    expected_data = [
        Row(
            text="Follow up earlier if needed, otherwise as per scheduled appointment.</span></font>"
        ),
        Row(
            text='</span></div><div align "left">Refer to Hand Center for complete extraction if cyst returns, or does not completely drain. </div>'
        ),
        Row(
            text="CISION SUBCUTANEOUS LESION: Procedure: FACE cleaned and draped, Local infiltration done with 1% xylocaine."
        ),
    ]
    expected_df = spark.createDataFrame(expected_data)

    result_df = tp.replace_multi_comma(test_df, "text")

    result = [row["text"] for row in result_df.collect()]
    expected = [row["text"] for row in expected_df.collect()]

    assert result == expected


def test_replace_hex_backspace():
    """
    We test text_process.replace_hex_backspace with multiple strings containing hex backspaces
    """
    test_data = [
        Row(text="Hex backspace \x08 is replaced with a space"),
        Row(text="This string also \x08 has a hex backspace"),
        Row(text="This string has no hex backspace"),
    ]
    test_df = spark.createDataFrame(test_data)

    expected_data = [
        Row(text="Hex backspace  is replaced with a space"),
        Row(text="This string also  has a hex backspace"),
        Row(text="This string has no hex backspace"),
    ]
    expected_df = spark.createDataFrame(expected_data)

    result_df = tp.replace_hex_backspace(test_df, "text")

    result = [row["text"] for row in result_df.collect()]
    expected = [row["text"] for row in expected_df.collect()]

    assert result == expected


def test_clean_xml():
    """
    We test text_process.clean_xml with various XML strings
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
        "catalog book id bk101 author John Doe title XML Developer's Guide",
    ]

    test_df = spark.createDataFrame(test_data)
    column = test_df["xml_data"]

    xml_cleaned = tp.clean_xml(test_df["xml_data"])

    # Another function handles whitespace, so we do it here for our tests
    xml_cleaned = F.regexp_replace(xml_cleaned, r"\s{2,}", " ")

    test_df = test_df.withColumn("results", xml_cleaned)

    for index, row in enumerate(test_df.select("results").collect()):
        error_msg = f"Test {index} failed. ACTUAL:: {row.results} != EXPECTED:: {expected_results[index]}"
        assert row.results == expected_results[index], error_msg


def test_count_words():
    """
    We test text_process.count_words with multiple strings
    """
    test_data = [
        Row(
            text="Office E/M services provided face to face john smith is a 46 year old male who presents for above"
        ),  # 18
        Row(
            text="CISION SUBCUTANEOUS LESION: Procedure: FACE cleaned and draped. Local infiltration done with 1% xylocaine"
        ),  # 13
    ]
    test_df = spark.createDataFrame(test_data)

    expected_data = [
        Row(
            text="Office E/M services provided face to face john smith is a 46 year old male who presents for above",
            word_count=18,
        ),
        Row(
            text="CISION SUBCUTANEOUS LESION: Procedure: FACE cleaned and draped. Local infiltration done with 1% xylocaine",
            word_count=13,
        ),
    ]
    expected_df = spark.createDataFrame(expected_data)

    result_df = tp.count_words(test_df, "text")

    result = [(row["text"], row["word_count"]) for row in result_df.collect()]
    expected = [(row["text"], row["word_count"]) for row in expected_df.collect()]

    assert result == expected


def test_replace_non_readable_characters():
    """
    We test text_process.replace_non_readable_characters with multiple strings containing non readable characters
    """
    test_data = [
        Row(
            text="Neurological: no weakness, no numbness, no tingling, no burning, no seizure no activity, no headache, no visual changes, no ataxia, no\x03vertigo"
        ),
        Row(
            text="CISION SUBCUTANEOUS LESION: Procedure: FACE\x05cleaned and draped. Local infiltration done with 1% xylocaine. ( 3.5 ) cm linear incision made over the lesion and deepened."
        ),
    ]
    test_df = spark.createDataFrame(test_data)

    expected_data = [
        Row(
            text="Neurological: no weakness, no numbness, no tingling, no burning, no seizure no activity, no headache, no visual changes, no ataxia, no vertigo"
        ),
        Row(
            text="CISION SUBCUTANEOUS LESION: Procedure: FACE cleaned and draped. Local infiltration done with 1% xylocaine. ( 3.5 ) cm linear incision made over the lesion and deepened."
        ),
    ]
    expected_df = spark.createDataFrame(expected_data)

    result_df = tp.replace_non_readable_characters(test_df, "text")

    result = [row["text"] for row in result_df.collect()]
    expected = [row["text"] for row in expected_df.collect()]

    assert result == expected


def test_clean_html_tags():
    """
    We test text_process.clean_html_tags with multiple html strings
    """
    test_data = [
        Row(
            text="""w-up in two to four weeks. </span></div><div align "left"><span style " font-size:11pt"><br /></span><span style " font-size:11pt">"""
        ),
        Row(text="<p>Provider<b>John Smith</b>DO</p>"),
    ]
    test_df = spark.createDataFrame(test_data)

    expected_data = [
        Row(text="w-up in two to four weeks.        "),
        Row(text=" Provider John Smith DO "),
    ]
    expected_df = spark.createDataFrame(expected_data)

    result_df = tp.clean_html_tags(test_df, "text")

    result = [row["text"] for row in result_df.collect()]
    expected = [row["text"] for row in expected_df.collect()]

    assert result == expected


def test_clean_rtf():
    """
    We test text_process.clean_rtf with an rtf string
    """
    test_data = [
        Row(text=r"{\rtf1\ansi\deff0 this is rtf {\fonttbl {\f0 Times New Roman;}}")
    ]
    test_df = spark.createDataFrame(test_data)

    expected_data = [Row(text="    this is rtf     ")]
    expected_df = spark.createDataFrame(expected_data)

    result_df = tp.clean_rtf(test_df, "text")

    result = [row["text"] for row in result_df.collect()]
    expected = [row["text"] for row in expected_df.collect()]

    assert result == expected


def test_cdh_proposed():
    """
    We test text_process.cdh_proposed with multiple types of encoded strings
    """
    test_data = [
        Row(
            text='<html><head><title></title><meta UserTagId="3" UserTagData="0"/></head><body><div align="left">'
        ),
        Row(text=None),
        Row(text="{\\rtf1\ansi\ansicpg1252{\fonttbl{\f0\fcharset1 MS Sans Serif;}}"),
        Row(
            text="<SOA>Medical problems to be addressed today include hypertension and type II diabetes and mixed hyperlipidemia"
        ),
        Row(text="<S:Envelope xmlns:S='http://schemas.xmlsoap.org/soap/envelope/"),
        Row(text="<CareData>Some more Health Related XML Content"),
        Row(
            text="Refer to Hand Center for complete extraction if cyst returns or does not completely drain"
        ),
    ]
    test_df = spark.createDataFrame(test_data)

    result_df = test_df.withColumn("classification", tp.cdh_proposed(F.col("text")))

    expected = [
        (
            '<html><head><title></title><meta UserTagId="3" UserTagData="0"/></head><body><div align="left">',
            "htm",
        ),
        (None, "unknown"),
        ("{\\rtf1\ansi\ansicpg1252{\fonttbl{\f0\fcharset1 MS Sans Serif;}}", "rtf"),
        (
            "<SOA>Medical problems to be addressed today include hypertension and type II diabetes and mixed hyperlipidemia",
            "sop",
        ),
        ("<S:Envelope xmlns:S='http://schemas.xmlsoap.org/soap/envelope/", "sop"),
        ("<CareData>Some more Health Related XML Content", "xml"),
        (
            "Refer to Hand Center for complete extraction if cyst returns or does not completely drain",
            "utf",
        ),
    ]

    result = [(row["text"], row["classification"]) for row in result_df.collect()]

    assert result == expected


def test_cdh_remove_html_tags():
    """
    We test text_process.remove_html_tags with multiple html strings
    """
    test_data = [
        Row(
            text="<p>ASKING FOR BLOOD WORK WHICH SHE JUST HAD IT DONE THIS MORNING ORDER MAMMO FOR<b>OCTOBER</b></p>",
            encoding="utf",
        ),
        Row(
            text="Trouble falling or staying asleep, or sleeping too much: Several Days Feeling tired or having little energy",
            encoding="utf",
        ),
    ]
    test_df = spark.createDataFrame(test_data)

    result_df = tp.cdh_remove_html_tags(test_df, "text", "encoding")

    expected_data = [
        Row(
            text="<p>ASKING FOR BLOOD WORK WHICH SHE JUST HAD IT DONE THIS MORNING ORDER MAMMO FOR<b>OCTOBER</b></p>",
            encoding="utf",
            clean_text=" ASKING FOR BLOOD WORK WHICH SHE JUST HAD IT DONE THIS MORNING ORDER MAMMO FOR OCTOBER  ",
        ),
        Row(
            text="Trouble falling or staying asleep, or sleeping too much: Several Days Feeling tired or having little energy",
            encoding="utf",
            clean_text="Trouble falling or staying asleep, or sleeping too much: Several Days Feeling tired or having little energy",
        ),
    ]
    expected_df = spark.createDataFrame(expected_data)

    result = [
        (row["text"], row["encoding"], row["clean_text"]) for row in result_df.collect()
    ]
    expected = [
        (row["text"], row["encoding"], row["clean_text"])
        for row in expected_df.collect()
    ]

    assert result == expected


def test_encoding_decider():
    """
    We test text_process.encoding_decider with multiple types of encoded strings
    """
    test_data = [
        Row(
            text='<html><head><title></title><meta UserTagId="3" UserTagData="0"/></head><body><div align="left">'
        ),
        Row(text="{\\rtf1\ansi\ansicpg1252{\fonttbl{\f0\fcharset1 MS Sans Serif;}}"),
        Row(
            text="<SOA>Medical problems to be addressed today include hypertension and type II diabetes and mixed hyperlipidemia"
        ),
        Row(text="<CHRT>Chart Data"),
        Row(
            text="Refer to Hand Center for complete extraction if cyst returns or does not completely drain"
        ),
        Row(text=None),
    ]
    test_df = spark.createDataFrame(test_data)

    result_df = tp.encoding_decider(test_df, "text", func=tp.cdh_proposed)

    expected_data = [
        Row(
            text='<html><head><title></title><meta UserTagId="3" UserTagData="0"/></head><body><div align="left">',
            encoding="htm",
        ),
        Row(
            text="{\\rtf1\ansi\ansicpg1252{\fonttbl{\f0\fcharset1 MS Sans Serif;}}",
            encoding="rtf",
        ),
        Row(
            text="<SOA>Medical problems to be addressed today include hypertension and type II diabetes and mixed hyperlipidemia",
            encoding="sop",
        ),
        Row(text="<CHRT>Chart Data", encoding="xml"),
        Row(
            text="Refer to Hand Center for complete extraction if cyst returns or does not completely drain",
            encoding="utf",
        ),
        Row(text=None, encoding="unknown"),
    ]
    expected_df = spark.createDataFrame(expected_data)

    result = [(row["text"], row["encoding"]) for row in result_df.collect()]
    expected = [(row["text"], row["encoding"]) for row in expected_df.collect()]

    assert result == expected