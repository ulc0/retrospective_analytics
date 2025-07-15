# Copyright 2020 Palantir Technologies
import re


def _insert_regex(pattern, value, test):
    """Searches in a string for any matched pattern and inserts a value in the middle"""
    start_idx = 0
    returned_col = ""
    for group in re.finditer(pattern, test):
        returned_col += (
            test[start_idx : group.start() + 1]
            + value
            + test[group.start() + 1 : group.end()]
        )
        start_idx = group.end()
    returned_col += test[start_idx:]
    return returned_col


def snake_case(input_string: str) -> str:
    no_garbage = re.sub("(^([^a-zA-Z0-9]))|([^a-z0-9_A-Z/])", "", input_string)
    number_by_letter = _insert_regex("[0-9][a-zA-Z]", "_", no_garbage)
    camel = _insert_regex("[a-z][^a-z_/]|[A-Z][0-9]", "_", number_by_letter)
    no_repeat = re.sub("[_]{2}", "_", camel)
    return no_repeat.lower()
