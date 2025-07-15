# Copyright 2024 Palantir Technologies, Inc.

from . import _param
from ._abstract_incremental_params import (
    AbstractNonCatalogIncrementalCompatibleInput,
    AbstractNonCatalogIncrementalCompatibleTransformInput,
)
from ._incremental_constants import (
    DATASET_LOCATOR_SPEC,
    DATASET_PROPERTIES,
    SEMANTIC_VERSION_PROPERTY,
    SEMANTIC_VERSION_RID,
    SEMANTIC_VERSION_TYPE,
)


class IncrementalSemanticVersion(
    _param.FoundryInputParam, AbstractNonCatalogIncrementalCompatibleInput
):
    def __init__(self, semantic_version):
        super().__init__(
            [SEMANTIC_VERSION_RID],
            type=SEMANTIC_VERSION_TYPE,
            properties={SEMANTIC_VERSION_PROPERTY: str(semantic_version)},
        )
        self._semantic_version = semantic_version

    @property
    def json_value(self):
        return {"rid": SEMANTIC_VERSION_RID}

    @staticmethod
    def instance(context, json_value):
        input_spec = context.input_specs.get((SEMANTIC_VERSION_RID, None))
        semantic_version = input_spec[DATASET_LOCATOR_SPEC][DATASET_PROPERTIES].get(
            SEMANTIC_VERSION_PROPERTY
        )
        return IncrementalSemanticVersionParam(semantic_version)

    def get_incremental_options(self, allow_retention):
        return {}


class IncrementalSemanticVersionParam(
    AbstractNonCatalogIncrementalCompatibleTransformInput
):
    def __init__(self, semantic_version) -> None:
        self._semantic_version = semantic_version

    @property
    def rid(self) -> str:
        return SEMANTIC_VERSION_RID

    def was_change_incremental(self, incremental_input_resolution) -> (bool, str):
        prior_sem_ver = self._get_prior_sem_ver(incremental_input_resolution)
        if prior_sem_ver != self._semantic_version:
            not_incremental_reason = f"""Semantic version has changed from {prior_sem_ver} to {self._semantic_version},
            cannot run incrementally."""
            return (False, not_incremental_reason)
        return (True, "")

    def _get_prior_sem_ver(self, incremental_input_resolution):
        resolution_type = incremental_input_resolution["type"]
        if resolution_type == "newInput":
            # a new input implies the prior semantic version was "1"
            return "1"
        if resolution_type == "incrementalInput":
            details = incremental_input_resolution["incrementalInput"]["details"]
            if details["currentSemanticVersion"] != self._semantic_version:
                # sanity check
                raise ValueError(
                    "Mismatch between expected and returned current semantic version"
                )
            return details["priorSemanticVersion"]
        raise ValueError("Semantic version resolution malformed")

    def get_incremental(self, incremental_input_resolution):
        return self

    def get_non_incremental(self):
        return self
