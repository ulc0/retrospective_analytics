#  (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.

from datetime import datetime, timedelta


def comparison_value_cross_dataset(value):
    return {
        "type": "crossDataset",
        "crossDataset": value,
    }


def comparison_value_literal(value):
    return {
        "type": "literal",
        "literal": {
            "type": "any",
            "any": value,
        },
    }


def timestamp_comparison_value(value: datetime):
    assert isinstance(value, datetime)
    return {"type": "timestamp", "timestamp": value.isoformat(timespec="milliseconds")}


def offset_comparison_value(value: timedelta):
    assert isinstance(value, timedelta)
    return int(value.total_seconds())


def comparison_value_column(name: str):
    return {"type": "column", "column": column(name)}


def comparison_value_null():
    return {
        "type": "null",
        "null": {},
    }


def column(name: str):
    return {"type": "name", "name": name}


def as_comparison_operator(operation):
    if operation.__name__ == "gt":
        return "GREATER_THAN"
    if operation.__name__ == "ge":
        return "GREATER_THAN_OR_EQUALS"
    if operation.__name__ == "lt":
        return "LESS_THAN"
    if operation.__name__ == "le":
        return "LESS_THAN_OR_EQUALS"
    if operation.__name__ == "eq":
        return "EQUALS"
    if operation.__name__ == "ne":
        return "NOT_EQUALS"

    raise TypeError("Unsupported operation: " + operation.__name__)
