# Copyright 2018 Palantir Technologies, Inc.

# Constants
SCHEMA_VERSION = "http://json-schema.org/draft-04/schema#"
FOUNDRY_KEY = "$foundry"
DATASET_LOCATOR = {"type": "datasetLocator", "datasetLocator": {}}

# Types
RID = {
    "type": "string",
    # https://github.com/palantir/resource-identifier
    "pattern": "ri.([a-z][a-z0-9\\-]*).([a-z0-9][a-z0-9\\-]*)?.([a-z][a-z0-9\\-]*).([a-zA-Z0-9_\\-\\.]+)",
}

# "The most basic schema is a blank JSON object, which constrains nothing, allows anything, and describes nothing"
BASE_SCHEMA = {"$schema": SCHEMA_VERSION}

# Foundry dataset
DATASET_SCHEMA = {
    "type": "object",
    "properties": {"datasetRid": RID},
    "required": ["datasetRid"],
    FOUNDRY_KEY: DATASET_LOCATOR,
}


def base():
    """Returns base json schema."""
    return dict(BASE_SCHEMA)
