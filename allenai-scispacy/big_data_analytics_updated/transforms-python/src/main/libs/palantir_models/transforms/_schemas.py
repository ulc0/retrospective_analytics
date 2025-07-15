#  (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
"""Constants used within transforms."""

from transforms._json_schema import DATASET_LOCATOR, FOUNDRY_KEY

# https://github.com/palantir/resource-identifier
RID_PATTERN = "ri.([a-z][a-z0-9\\-]*).([a-z0-9][a-z0-9\\-]*)?.([a-z][a-z0-9\\-]*).([a-zA-Z0-9_\\-\\.]+)"
SEMVER_PATTERN = "^(0|[1-9]d*).(0|[1-9]d*).(0|[1-9]d*)$"
RID_OR_SEMVER = {"type": "string", "pattern": "(" + RID_PATTERN + "|" + SEMVER_PATTERN + ")"}
RID = {"type": "string", "pattern": RID_PATTERN}
# we use datasetRid for legacy reasons, identifiers in jobspecs use the dataset terminology
DATASET_RID = "datasetRid"

# Used within transforms-python at CI time to validate that the schema provided is correct
MODEL_SCHEMA = {
    "type": "object",
    "properties": {
        "alias": {"type": "string"},
        "modelRid": RID,
        "modelVersion": RID_OR_SEMVER,
        "useSidecar": {"type": "boolean"},
    },
    "required": ["alias"],
    FOUNDRY_KEY: {
        "type": "modelLocatorSpec",
        "modelLocatorSpec": {},
    },
}

LANGUAGE_MODEL_SCHEMA = {
    "type": "object",
    "properties": {DATASET_RID: RID},
    "required": [DATASET_RID],
    FOUNDRY_KEY: DATASET_LOCATOR,
}
