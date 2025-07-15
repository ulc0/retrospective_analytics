#  (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
from typing import Union, List


def flatten(list_of_lists):
    return [item for items in list_of_lists for item in items]


def unique(elements, key):
    return list({key(element): element for element in elements}.values())


def as_list(value: Union[object, List[object]]):
    return [value] if type(value) is not list else value
