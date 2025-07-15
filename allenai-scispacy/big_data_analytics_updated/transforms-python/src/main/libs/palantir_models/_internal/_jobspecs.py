import logging
from typing import List, Optional

log = logging.getLogger("palantir_models")


RESOLVED_MODEL_VERSION_RID = "modelVersionRid"
RESOLVED_SIDECAR_PORT = "sidecarPort"
SIDECAR_LOCATION = "sidecarLocation"
PENDING_MODEL_VERSION_RID = "pendingModelVersionRid"
STAGED_EXPERIMENTS = "stagedExperimentRids"


def _get_model_input_dataset_properties(input_specs, model_rid, input_branch=None):
    model_input_spec = input_specs.get((model_rid, input_branch))
    if not model_input_spec:
        return None
    return model_input_spec["datasetLocator"]["datasetProperties"]


def _get_latest_model_version_rid_from_dataset_properties(
    auth_header, executable_service, model_rid, dataset_properties, input_branch=None
):
    if not dataset_properties:
        raise KeyError(f"Input spec for model {model_rid} branch {input_branch} not found")
    try:
        model_version_rid = dataset_properties[RESOLVED_MODEL_VERSION_RID]
    except KeyError as key_error:
        raise Exception(f"There are no successfully built model versions for model rid {model_rid}") from key_error
    # Check to see if this model_version_rid is containerized.
    # If it is raise an exception: a user must specify a model version if they wish to load a containerized model.
    maybe_context = executable_service.get_containerized_representation_context(
        auth_header, model_rid, model_version_rid
    )
    if maybe_context:
        raise Exception(
            "model_version required to be specified within ModelInput as a rid or semver for containerized models"
        )
    return model_version_rid


def _maybe_get_port_from_dataset_properties(dataset_properties) -> Optional[int]:
    if not dataset_properties:
        return None
    try:
        return int(dataset_properties[RESOLVED_SIDECAR_PORT])
    except KeyError:
        return None


def _get_sidecar_location_from_dataset_properties(dataset_properties) -> str:
    return dataset_properties[SIDECAR_LOCATION]


def _get_model_version_rid_from_context_output_specs(output_specs, model_rid):
    try:
        return output_specs.get(model_rid)["datasetLocator"]["datasetProperties"][PENDING_MODEL_VERSION_RID]
    except KeyError as key_error:
        raise Exception(f"Resolved model version rid not found in output spec for model rid {model_rid}") from key_error


def _get_staged_experiment_rids_from_output_specs(output_specs, model_rid) -> List[str]:
    try:
        return output_specs.get(model_rid)["datasetLocator"]["datasetProperties"][STAGED_EXPERIMENTS]
    except KeyError:
        log.warning(f"stagedExperimentRids key missing from output spec for model rid {model_rid}")
        return []
