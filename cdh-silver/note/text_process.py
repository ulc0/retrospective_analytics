import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import StringType, IntegerType
import re

def setLowercase(dataframe: DataFrame, text: "ColumnOrName") -> DataFrame:
    if isinstance(text, str):
        column, name = F.col(text), text
    elif isinstance(text, Column):
        column, name = text, text.__str__()
    else:
        raise ValueError("text parameter needs to be Column or string type")

    return dataframe.withColumn(
        name,
        F.lower(column)
    )


def trim_string(dataframe: DataFrame, text: "ColumnOrName") -> DataFrame:
    if isinstance(text, str):
        column, name = F.col(text), text
    elif isinstance(text, Column):
        column, name = text, text.__str__()
    else:
        raise ValueError("text parameter needs to be Column or string type")

    return dataframe.withColumn(
        name,
       F.trim(column)
    )


def replace_nonbreaking_space(dataframe: DataFrame, text: "ColumnOrName") -> DataFrame:
    if isinstance(text, str):
        column, name = F.col(text), text
    elif isinstance(text, Column):
        column, name = text, text.__str__()
    else:
        raise ValueError("text parameter needs to be Column or string type")

    return dataframe.withColumn(
        name,
        F.regexp_replace(column, "&#160;", " ")
    )


def replace_ampersand(dataframe: DataFrame, text: "ColumnOrName") -> DataFrame:
    if isinstance(text, str):
        column, name = F.col(text), text
    elif isinstance(text, Column):
        column, name = text, text.__str__()
    else:
        raise ValueError("text parameter needs to be Column or string type")

    return dataframe.withColumn(
        name,
        F.regexp_replace(column, "&amp;", " and ")
    )


def replace_paragraph_break(dataframe: DataFrame, text: "ColumnOrName") -> DataFrame:
    if isinstance(text, str):
        column, name = F.col(text), text
    elif isinstance(text, Column):
        column, name = text, text.__str__()
    else:
        raise ValueError("text parameter needs to be Column or string type")

    return dataframe.withColumn(
        name,
       F.regexp_replace( F.regexp_replace(column, "<p>", " "),"</p>"," ")
    )


def replace_pipes(dataframe: DataFrame, text: "ColumnOrName") -> DataFrame:
    if isinstance(text, str):
        column, name = F.col(text), text
    elif isinstance(text, Column):
        column, name = text, text.__str__()
    else:
        raise ValueError("text parameter needs to be Column or string type")
    return dataframe.withColumn(
        name,
        F.regexp_replace(column, r"\|", " ")
    )


def replace_multi_space(dataframe: DataFrame, text: "ColumnOrName") -> DataFrame:
    if isinstance(text, str):
        column, name = F.col(text), text
    elif isinstance(text, Column):
        column, name = text, text.__str__()
    else:
        raise ValueError("text parameter needs to be Column or string type")

    return dataframe.withColumn(
        name,
        F.regexp_replace(column, r"/\s\s+/g", " ")
    )


def replace_multi_comma(dataframe: DataFrame, text: "ColumnOrName") -> DataFrame:
    if isinstance(text, str):
        column, name = F.col(text), text
    elif isinstance(text, Column):
        column, name = text, text.__str__()
    else:
        raise ValueError("text parameter needs to be Column or string type")

    return dataframe.withColumn(
        name,
        F.regexp_replace(column, ",,", ", ")
    )


def replace_hex_backspace(dataframe: DataFrame, text: "ColumnOrName") -> DataFrame:
    if isinstance(text, str):
        column, name = F.col(text), text
    elif isinstance(text, Column):
        column, name = text, text.__str__()
    else:
        raise ValueError("text parameter needs to be Column or string type")

    return dataframe.withColumn(
        name,
        F.regexp_replace(column, r"\x08 ", " ")
    )


def replace_non_readable_characters(dataframe: DataFrame, text: "ColumnOrName") -> DataFrame:
    if isinstance(text, str):
        column, name = F.col(text), text
    elif isinstance(text, Column):
        column, name = text, text.__str__()
    else:
        raise ValueError("text parameter needs to be Column or string type")

    # Define a regex pattern to match only readable characters (printable ASCII characters)
    not_readable_regex = "[^\\x20-\\x7E]"

    return dataframe.withColumn(
        name,
        F.regexp_replace(column, not_readable_regex, " ")
    )


def clean_html_tags(dataframe: DataFrame, text: "ColumnOrName") -> DataFrame:
    if isinstance(text, str):
        column, name = F.col(text), text
    elif isinstance(text, Column):
        column, name = text, text.__str__()
    else:
        raise ValueError("text parameter needs to be Column or string type")

    html_pattern = r"</?[^>]+>|<[^>]* /$|</[^>]*$"

    return dataframe.withColumn(
        name,
        F.regexp_replace(column, html_pattern, " ")
    )

def clean_rtf(dataframe: DataFrame, text: "ColumnOrName") -> DataFrame:
    if isinstance(text, str):
        column, name = F.col(text), text
    elif isinstance(text, Column):
        column, name = text, text.__str__()
    else:
        raise ValueError("text parameter needs to be Column or string type")

    rtf_pattern = r"\{\*?\\[^{}]+}|[{}]|\\\n?[A-Za-z]+\n?(?:-?\d+)?[ ]?"
    return dataframe.withColumn(
        name,
        F.regexp_replace(column, rtf_pattern, " ")
    )


def clean_xml(text_column: Column) -> Column:
    """
    Cleans up xml text by removing xml declaration, tags, and attributes
    """
    # 1. remove xml declaration
    cleaned_col = F.regexp_replace(
        string=text_column, pattern=r"<\?xml.*?\?>", replacement=""
    )

    # 2. Extract tags and attributes (removing angle brackets and slashes)
    cleaned_col = F.regexp_replace(
        string=cleaned_col, pattern=r"<([^/][^>]+)>", replacement="$1 "
    )

    # 3. Remove closing tags
    cleaned_col = F.regexp_replace(
        string=cleaned_col, pattern=r"</[^>]+>", replacement=" "
    )

    # 4. Cleaning up tags and attributes to format
    cleaned_col = F.regexp_replace(string=cleaned_col, pattern=r"\=\"", replacement=" ")
    cleaned_col = F.regexp_replace(string=cleaned_col, pattern=r"\"", replacement=" ")

    # Remove trailing or leading whitespace and return
    return F.trim(cleaned_col)


def count_words(dataframe: DataFrame, text: "ColumnOrName") -> DataFrame:
    if isinstance(text, str):
        column, name = F.col(text), text
    elif isinstance(text, Column):
        column, name = text, text.__str__()
    else:
        raise ValueError("text parameter needs to be Column or string type")

    return dataframe.withColumn(
        "word_count",
        (F.size(F.split(F.col(text), " "))-1)
    )

    
def cdh_extract_note_text(
    dataframe: DataFrame, text: "ColumnOrName", encoding: "ColumnOrName"
) -> DataFrame:
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

    # Define a regex pattern to match only readable characters (printable ASCII characters)
    text = replace_non_readable_characters(text)

    # We remove all the tags for html and xml <--- might want to keep XML tags
    html_result = clean_html_tags(text)

    # For rtf, we remove the control word and then braces
    rtf_result = clean_rtf(text)

    # removing xml declaration, tags, and attributes
    xml_result = clean_xml(text)

    return dataframe.withColumn(
        "clean_text",
        F.when(encoding == "htm", html_result)
        .when(encoding == "xml", xml_result)
        .when(encoding == "rtf", rtf_result)
        .otherwise(text),
    )


def cdh_remove_partial_tag(
    dataframe: DataFrame, text: "ColumnOrName", encoding: "ColumnOrName"
) -> DataFrame:
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

    # We remove all the partial tags  <--- might want to keep XML tags
    close_tag_result = F.right(
        text,
        (
            F.length(text)
            - (
                F.locate(F.lit('">'), text)
                - (F.contains(text, F.lit('">')).cast(IntegerType()))
            )
        ),
    )

    return dataframe.withColumn(
        "clean_text", F.when(encoding == "xml", text).otherwise(close_tag_result)
    )


def cdh_remove_html_tags(dataframe: DataFrame, text: "ColumnOrName", encoding: "ColumnOrName") -> DataFrame:
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
    # Define a regex pattern to match only readable characters (printable ASCII characters)
   
    html_pattern= r"</?[^>]+>|<[^>]* /$|</[^>]*$"
    html_pattern=r"<[^>]*>|&[^;]+;"
    html_pattern=r"<[^>]+>" # <[^>]*>


    # We remove all the tags for html and xml <--- might want to keep XML tags
    html_result = F.regexp_replace(
        string=text,
        pattern=html_pattern,
        replacement=" "
    )
    return dataframe.withColumn(
        "clean_text",
        F.when(encoding == "xml", text)
        .otherwise(html_result)
    )


def cdh_proposed(text: Column) -> Column:
    decider = (
        F.when(text.contains('<html>'), "htm")
        .otherwise(F.when(text.isNull(), "unknown")
                    .when(text.startswith("{\\rtf"), "rtf")
                    .when(text.startswith("<SOA"), "sop")
                    .when(text.startswith("<S:"), "sop")
                    .when(text.startswith("<CHRT"), "xml")
                    .when(text.startswith("<Care"), "xml")
                    .otherwise("utf")
                    )
    )
    return decider


def encoding_decider(
    dataframe: DataFrame, text: "ColumnOrName", func=cdh_proposed
) -> DataFrame:
    """
    Returns a dataframe with an 'encoding" column, that contains the label from a decider
    provided by func.

    Args:
        dataframe: DataFrame
        text: Column or name of column that contains text
        func: A Callable that decides membership of a text column
    """
    # If given name, cast to Column
    if isinstance(text, str):
        text = F.col(text)

    decider = F.lower(func(text))

    return dataframe.withColumn("encoding", decider)
