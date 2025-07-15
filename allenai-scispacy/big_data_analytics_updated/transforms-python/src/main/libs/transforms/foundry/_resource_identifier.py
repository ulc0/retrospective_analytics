# Copyright 2017 Palantir Technologies, Inc.
"""Module for dealing with ResourceIdentifiers.

Copied almost verbatim from:
https://github.com/palantir/resource-identifier/blob/0.5.0/src/main/java/com/palantir/ri/ResourceIdentifier.java
"""
import re

RID_CLASS = "ri"
SEPARATOR = "."

SERVICE_REGEX = "([a-z][a-z0-9\\-]*)"
INSTANCE_REGEX = "([a-z0-9][a-z0-9\\-]*)?"
TYPE_REGEX = "([a-z][a-z0-9\\-]*)"
LOCATOR_REGEX = "([a-zA-Z0-9_\\-\\.]+)"

SERVICE_PATTERN = re.compile(SERVICE_REGEX)
INSTANCE_PATTERN = re.compile(INSTANCE_REGEX)
TYPE_PATTERN = re.compile(TYPE_REGEX)
LOCATOR_PATTERN = re.compile(LOCATOR_REGEX)

#: Creates a Pattern in form of ri.<service>.<instance>.<type>.<locator>
SPEC_PATTERN = re.compile(
    RID_CLASS
    + "\\."
    + SERVICE_REGEX
    + "\\."
    + INSTANCE_REGEX
    + "\\."
    + TYPE_REGEX
    + "\\."
    + LOCATOR_REGEX
)


def is_valid(rid: str) -> bool:
    """Check if the input string is a valid resource identifier.

    Args:
        rid (str): The input string the check

    Returns:
        bool: True iff the input satisfies the defined SPEC_PATTERN.
    """
    return SPEC_PATTERN.match(rid) is not None
