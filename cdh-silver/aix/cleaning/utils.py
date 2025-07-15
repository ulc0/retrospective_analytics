from pyspark.sql import DataFrame, Column
import pyspark.sql.functions as F

import cdh_featurization.cleaning.markup_cleaner as muc


def extract_note_text(dataframe: DataFrame, text: "ColumnOrName", encoding: "ColumnOrName") -> DataFrame:
    """
    Returns a dataframe with a 'clean_text' column, that removes tags and formatting for rtf, html, and
    xml text while leaving remaining text as is.

    Args:
        dataframe: DataFrame
        text: Column or name of column that contains text to be cleaned
        encoding: Column or name of column that states the encoding of the text
    """
    # Cast strings are column objects
    if isinstance(text, str):
        text = F.col(text)
    
    if isinstance(encoding, str):
        encoding = F.col(encoding)
    
    # We remove all the tags for html
    html_result = F.regexp_replace(
        string=text, 
        pattern=r"</?[^>]+>|<[^>]* /$|</[^>]*$",
        replacement=" "
    )

    xml_result = muc.clean_xml(text)
    
    # For rtf, we remove the control word and then braces
    rtf_result = F.regexp_replace(
        string=text, 
        pattern=r"\{\*?\\[^{}]+}|[{}]|\\\n?[A-Za-z]+\n?(?:-?\d+)?[ ]?", 
        replacement=" "
    )

    return dataframe.withColumn(
        "clean_text",
        F.when(encoding == "html", html_result)
        .when(encoding == "xml", xml_result)
        .when(encoding == "rtf", rtf_result)
        .otherwise(text)
    )
