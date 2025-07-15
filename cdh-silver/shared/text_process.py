# rtf works from striprtf.striprtf import rtf_to_text
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import StringType, IntegerType
import re

def cdh_proposed(text: Column) -> Column:
    decider = (
        F.when(text.contains('<html>'), "htm")
        .otherwise(F.when(text.isNull(), "unknown")
                    .when(text.startswith("{\\rtf"), "rtf")
            #        .when(text.startswith("<?xml"), "xml")
                    .when(text.startswith("<SOA"), "sop") 
                    .when(text.startswith("<S:"), "sop") 
                    .when(text.startswith("<CHRT"), "xml")
                    .when(text.startswith("<Care"), "xml")
                    .otherwise("utf")
                    )
    )
    return decider

def encoding_decider(dataframe: DataFrame, text: "ColumnOrName", func=cdh_proposed) -> DataFrame:
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
    
    decider = F.lower(func(text))

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
#https://github.com/StefanoTrv/simple_icd_10/blob/master/simple_icd_10.py
def xadd_dot_to_code(code):

    if len(code)>3 and code[3]!=".":
        return code[:3]+"."+code[3:]
    else:
        return code

# COMMAND ----------
def icddot(dataframe: DataFrame, text:"ColumnOrName") -> DataFrame:
    # Cast strings are column objects
    if isinstance(text, str):
        text = F.col(text)
    tlen=F.len(text)
    if tlen>3 and F.substr(text,3)!=".":
        text=F.concat_ws(".",F.substring(text,3),F.substring(text,tlen-4) )
    return dataframe

def cdh_extract_note_text(dataframe: DataFrame, text: "ColumnOrName", encoding: "ColumnOrName") -> DataFrame:
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
    not_readable_regex = "[^\\x20-\\x7E]"
    text=F.regexp_replace(text,not_readable_regex," ")    
    
    html_pattern= r"</?[^>]+>|<[^>]* /$|</[^>]*$"
    html_pattern=r"<[^>]*>|&[^;]+;"
    html_pattern=r"<[^>]+>"
    html_pattern= r"</?[^>]+>|<[^>]* /$|</[^>]*$"   

    # We remove all the tags for html and xml <--- might want to keep XML tags
    html_result = F.regexp_replace(
        string=text, 
        pattern=html_pattern,
        replacement=""
    )
    # Original
    rtf_pattern=r"\{\*?\\[^{}]+}|[{}]|\\\n?[A-Za-z]+\n?(?:-?\d+)?[ ]?"
    # For rtf, we remove the control word and then braces
    rtf_result = F.regexp_replace(
        string=text, 
        pattern=rtf_pattern, 
        replacement=""
    )
    
    xml_result = clean_xml(text)

    return dataframe.withColumn(
        "clean_text",
        F.when(encoding == "htm", html_result)
        .when(encoding == "xml", xml_result)
        .when(encoding == "rtf", rtf_result)
        .otherwise(text)
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

#substring_index(df.text, '-', 1)

def cdh_remove_partial_tag(dataframe: DataFrame, text: "ColumnOrName", encoding: "ColumnOrName") -> DataFrame:
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
   
    # We remove all the partial tags  <--- might want to keep XML tags
    # right(note_text,length(note_text)-locate('">',note_text)-cast(contains(note_text,'">') as INT))
    close_tag_result = F.right(text,(F.length(text)-(F.locate(F.lit('">'),text)-(F.contains(text,F.lit('">')).cast(IntegerType()) ) ) ) )
    #F.right(text,(F.length(text)-F.substring_index(text, '">', 1)),)   
    return dataframe.withColumn(
        "clean_text",
        F.when(encoding == "xml", text)
        .otherwise(close_tag_result)
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

def replace_pipes(dataframe: DataFrame, text: "ColumnOrName") -> DataFrame:
    if isinstance(text, str):
        column, name = F.col(text), text
    elif isinstance(text, Column):
        column, name = text, text.__str__()
    else:
        raise ValueError("text parameter needs to be Column or string type")
    return dataframe.withColumn(
        name,
        F.regexp_replace(column, r"/\|/g", " ")
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

def count_words(dataframe: DataFrame, text: "ColumnOrName") -> DataFrame:
    if isinstance(text, str):
        column, name = F.col(text), text
    elif isinstance(text, Column):
        column, name = text, text.__str__()
    else:
        raise ValueError("text parameter needs to be Column or string type")
#size(split(col("assigned_products"), r"\+")) - 1)
    return dataframe.withColumn(
        "word_count",
        (F.size(F.split(F.col(text), " "))-1)
    )

