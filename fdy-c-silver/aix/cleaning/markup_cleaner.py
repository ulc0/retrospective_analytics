from typing import Callable
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import StringType


def markup_decider(text: Column) -> Column:
    """
    Decides if a string is a member of the RTF, XML or HTML family
    of languages. Defaults to UTF but does not validate.
    """
    decider = (
        F.when(text.isNull(), "unknown")
        .when(text.startswith("{\\rtf"), "rtf")
        .when(text.startswith("<?xml"), "xml")
        .when(text.contains("<html>"), "html")
        .otherwise("utf")
    )
    return decider


def encoding_decider(dataframe: DataFrame, text: "ColumnOrName", func=markup_decider) -> DataFrame:
    """
    Returns a dataframe with an 'encoding" column, that contains the label of from a decider
    provided by func.

    Args:
        dataframe: DataFrame
        text: Column or name of column that contains text
        func: A Callable that decides membership of a text column
    """
    # If given name, cast to Column
    if isinstance(text, str):
        text = F.col(text)
    
    decider = func(text)

    return dataframe.withColumn("encoding", decider)


def clean_xml(text_column: Column) -> Column:
    """
    Cleans up xml text by removing xml declaration, tags, and attributes
    """
    # 1. remove xml declaration
    cleaned_col = F.regexp_replace(string=text_column, pattern=r"<\?xml.*?\?>", replacement="")

    # 2. Extract tags and attributes (removing angle brackets and slashes)
    cleaned_col = F.regexp_replace(string=cleaned_col, pattern=r"<([^/][^>]+)>", replacement="$1 ")

    # 3. Remove closing tags
    cleaned_col = F.regexp_replace(string=cleaned_col, pattern=r"</[^>]+>", replacement=" ")

    # 4. Cleaning up tags and attributes to format
    cleaned_col = F.regexp_replace(string=cleaned_col, pattern=r"\=\"", replacement=" ")
    cleaned_col = F.regexp_replace(string=cleaned_col, pattern=r"\"", replacement=" ")
    
    # Remove trailing or leading whitespace and return
    return F.trim(cleaned_col)
from typing import Callable
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import StringType


def html_decider(text: Column) -> Column:
    """
    Decides if a string is a member of the HTML family. This function
    also considers malformed HTML strings as HTML.
    """
    # Some XML examples contain HTML and project team wants them treated like HTML
    # Check the first 50 characters for optional whitespace, optional xml tag, and <html> declarations
    html_declarations = r"(<html\b|<!DOCTYPE html\b|<head\b|<body\b|<div\b|<table\b|<span\b)"
    xml_with_html = (
        text
        .substr(1, 50)
        .rlike(rf"^\s*(<\?xml.*?\??)?\s*{html_declarations}")
    )

    # Most of the malformed examples end with </html>, so this
    # may be sufficient
    malformed_html_1 = text.endswith("</html>")

    return xml_with_html | malformed_html_1


def xml_decider(text: Column) -> Column:
    """
    Decides if a string is a member of the XML family.
    """
    # Check first 50 characters optional whitespace, XML declaration and no HTML declaration
    xml_sans_html = text.substr(1, 50).rlike(r"^\s*<\?xml.*?\?>((?!<html\b).)*$")
    return xml_sans_html


def markup_decider(text: Column) -> Column:
    """
    Decides if a string is a member of the RTF, XML or HTML family
    of languages. Defaults to UTF but does not val"idate.
    """
    decider = (
        F.when(text.isNull(), "unknown")
        .when(text.startswith("{\\rtf"), "rtf")
        .when(html_decider(text), "html")
        .when(xml_decider(text), "xml")
        .otherwise("utf")
    )
    return decider


def encoding_decider(dataframe: DataFrame, text: "ColumnOrName", func=markup_decider) -> DataFrame:
    """
    Returns a dataframe with an 'encoding" column, that contains the label of from a decider
    provided by func.

    Args:
        dataframe: DataFrame
        text: Column or name of column that contains text
        func: A Callable that decides membership of a text column
    """
    # If given name, cast to Column
    if isinstance(text, str):
        text = F.col(text)
    
    decider = func(text)

    return dataframe.withColumn("encoding", decider)


def clean_xml(text_column: Column) -> Column:
    """
    Cleans up xml text by removing xml declaration, tags, and attributes
    """
    # 1. remove xml declaration
    cleaned_col = F.regexp_replace(string=text_column, pattern=r"<\?xml.*?\?>", replacement="")

    # 2. Extract tags and attributes (removing angle brackets and slashes)
    cleaned_col = F.regexp_replace(string=cleaned_col, pattern=r"<([^/][^>]+)>", replacement="$1 ")

    # 3. Remove closing tags
    cleaned_col = F.regexp_replace(string=cleaned_col, pattern=r"</[^>]+>", replacement=" ")

    # 4. Cleaning up tags and attributes to format
    cleaned_col = F.regexp_replace(string=cleaned_col, pattern=r"\=\"", replacement=" ")
    cleaned_col = F.regexp_replace(string=cleaned_col, pattern=r"\"", replacement=" ")
    
    # Remove trailing or leading whitespace and return
    return F.trim(cleaned_col)
