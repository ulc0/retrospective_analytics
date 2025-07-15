# Databricks notebook source
from typing import Callable
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import StringType

# COMMAND ----------

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

# COMMAND ----------

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

# COMMAND ----------

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

    xml_result = clean_xml(text)
    
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

# COMMAND ----------

def name_to_column(name: str) -> Column:
    """
    Converts a string to a column object.
    """
    if isinstance(name, str):
        column = F.col(name)
    elif isinstance(name, Column):
        column = name
    else:
        raise ValueError("text parameter needs to be Column or string type")

    return column

# COMMAND ----------

def apply_func(
    dataframe: DataFrame,
    text: "ColumnOrName", 
    func: Callable,
    name: str=None, 
    **kwargs
    ) -> DataFrame:
    """
    Applies a provided function with kwargs to a column in a dataframe.
    """
    column = name_to_column(text)

    # If no name supplied, replace column in dataframe
    # str(column) returns Column<'name'>, we use [8:-2] to get the name only, can't run Java/Scala code
    name = str(column)[8:-2] if name is None else name

    return dataframe.withColumn(name, func(column, **kwargs))
