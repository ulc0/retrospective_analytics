# Copyright 2024 Palantir Technologies, Inc.

# we use camel-case naming convention as these will be serialized for Java consumption
INCREMENTAL_INPUTS = "incrementalInputs"
INCREMENTAL_OUTPUTS = "incrementalOutputs"
INCREMENTAL_OPTIONS = "incrementalOptions"
INPUT = "input"
OUTPUT = "output"

CONSENSUS_ON_NEW_BUILD = "CONSENSUS_OF_NEW_BUILD"
CONSENSUS_ON_INCREMENTAL = "CONSENSUS_ON_INCREMENTAL"
NO_CONSENSUS_ON_PRIOR_BUILD = "NO_CONSENSUS_ON_PRIOR_BUILD"
INCREMENTAL_INPUT_RESOLUTIONS = "incrementalInputResolutions"
CONSENSUS_ON_INCOMPATIBLE_BUILD = "CONSENSUS_ON_INCOMPATIBLE_BUILD"
OUTPUT_HAS_BEEN_ALTERED = "OUTPUT_HAS_BEEN_ALTERED"
OUTCOME = "outcome"
BRANCH = "branch"
DATASET_LOCATOR = "datasetLocator"
DATASET_PROPERTIES = "datasetProperties"
DATASET_RID = "datasetRid"
PRIOR_INCR_TXN_RID = "priorIncrementalTransactionRid"

SEMANTIC_VERSION_TYPE = "incremental-semantic-version"
SEMANTIC_VERSION_RID = "ri.transforms.python.semantic.version"
SEMANTIC_VERSION_NAME = "semantic_version"
DATASET_LOCATOR_SPEC = "datasetLocator"
DATASET_PROPERTIES = "datasetProperties"
SEMANTIC_VERSION_PROPERTY = "semanticVersion"
