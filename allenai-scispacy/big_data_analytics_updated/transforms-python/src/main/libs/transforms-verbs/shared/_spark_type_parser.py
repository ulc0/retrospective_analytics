#  (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
#  This was copied from the Apache/Spark github repo on the 18th of March 2022 by @kfjensen an
#  then altered to fit our more niche need.
#  https://github.com/apache/spark/blob/f84018a4810867afa84658fec76494aaae6d57fc/python/pyspark/sql/types.py#L972

from parsimonious.grammar import Grammar
from parsimonious.nodes import NodeVisitor
from pyspark.sql import types as T


# @TODO(kfjensen) Add new spark types as they become part of spark.
_atomic_types = [
    T.StringType,
    T.BinaryType,
    T.BooleanType,
    T.DecimalType,
    T.FloatType,
    T.DoubleType,
    T.ByteType,
    T.ShortType,
    T.IntegerType,
    T.LongType,
    T.DateType,
    T.TimestampType,
    T.NullType,
]
_all_atomic_types = dict((t.typeName(), t) for t in _atomic_types)
# Special cases which are not covered by typeName().
_all_atomic_types["bigint"] = T.LongType
_all_atomic_types["smallint"] = T.ShortType
_all_atomic_types["tinyint"] = T.ByteType
_all_atomic_types["int"] = T.IntegerType


class IniVisitor(NodeVisitor):
    def visit_type(self, node, visited_children):
        return visited_children[0]

    def visit_map_type(self, node, visited_children):
        """Returns the overall output."""
        return T.MapType(visited_children[2], visited_children[4])

    def visit_array_type(self, node, visited_children):
        """Makes a dict of the section (as key) and the key/value pairs."""
        return T.ArrayType(visited_children[2])

    def visit_decimal_type(self, node, visited_children):
        """Makes a dict of the section (as key) and the key/value pairs."""
        return T.DecimalType(
            int(visited_children[1].text), int(visited_children[3].text)
        )

    def visit_atomic_type(self, node, visited_children):
        """Gets the section name."""
        return _all_atomic_types[node.text]()

    def generic_visit(self, node, visited_children):
        """The generic visit method."""
        return visited_children or node


def _parse_datatype_string(e):
    e = e.lower()
    e = e.replace(" ", "")
    # @TODO(kfjensen) Add support for struct types.
    grammar = Grammar(
        r"""
        type = (map_type / array_type / decimal_type / atomic_type)
        map_type     = "map" sub_open type delimeter type sub_close
        array_type   = "array" sub_open type sub_close
        decimal_type = ~"(decimal|dec)\(" ~"\d+" delimeter ~"-?\d+" ~"\)"
        atomic_type  = ~"[a-z]+"
        delimeter = ~","
        sub_open  = ~"<"
        sub_close = ~">"
        """
    )

    tree = grammar.parse(e)

    iv = IniVisitor()
    out = iv.visit(tree)
    return out
