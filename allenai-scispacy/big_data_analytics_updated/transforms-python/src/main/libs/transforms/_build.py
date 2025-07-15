# Copyright 2017 Palantir Technologies, Inc.
# pylint: disable=pointless-statement
import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Tuple,
    Union,
    cast,
)

from transforms._code_checks import _run_checks, abort_build_if_failed_expectations
from transforms._job_spec_utils import (
    _DatasetParamsMap,
    _InputSpecsMap,
    _OutputSpecsMap,
    extract_input_specs,
    extract_output_specs,
    get_dataset_param_branches,
)
from transforms.api import (
    Check,
    FoundryOutputParam,
    Input,
    Output,
    ParamContext,
    Transform,
    TransformContext,
)
from transforms.api._incremental_constants import (
    DATASET_LOCATOR,
    DATASET_PROPERTIES,
    PRIOR_INCR_TXN_RID,
)
from transforms.api._incremental_semantic_version import IncrementalSemanticVersion
from transforms.api._param import DataframeParamInstance, FoundryInputParam
from transforms.api._transform import AbortJobException, TransformOutput
from transforms.foundry.services import EventLogger

from ._results import AbortResult, SuccessResult

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

    from transforms._shrinkwrap import Shrinkwrap
    from transforms.foundry.connectors import FoundryConnector


log = logging.getLogger(__name__)


class JobExecution(object):
    """Class for wrapping up a job execution."""

    FOUNDRY_INPUT_TYPE = "foundry"

    def __init__(
        self,
        foundry: "FoundryConnector",
        transform: Transform,
        shrinkwrap: "Shrinkwrap",
    ) -> None:
        self._foundry = foundry
        self._transform = transform
        self._shrinkwrap = shrinkwrap

        self._catalog = foundry._service_provider.catalog()
        self._event_logger = foundry._event_logger

    def _create_fake_raw_input(
        self, transform_input: Input, input_specs: _InputSpecsMap
    ) -> Dict[str, str]:
        # Back compatibility for jobspecs with no parameters

        # Resolve the transform input alias into a rid
        input_rid = self._shrinkwrap.rid_for_alias(cast(str, transform_input.alias))

        # Now we have the rid, we can get the Foundry Build ResolvedInputSpec
        input_spec = input_specs.get((input_rid, None))

        if not input_spec:
            raise KeyError(
                f"Input rid {input_rid} for {transform_input} not found in job spec"
            )

        log.info("Raw parameter for Input not found, using datasetRid: %s", input_rid)
        return {"datasetRid": input_rid}

    def _create_fake_raw_output(
        self, transform_output: Output, output_specs: _OutputSpecsMap
    ) -> Dict[str, str]:
        # Back compatibility for jobspecs with no parameters

        # Resolve the transform output alias into a rid
        output_rid = self._shrinkwrap.rid_for_alias(cast(str, transform_output.alias))

        # Now we have the rid, we can get the Foundry Build ResolvedOutputSpec
        output_spec = output_specs.get(output_rid)

        if not output_spec:
            raise KeyError(
                f"Output rid {output_rid} for {transform_output} not found in job spec"
            )

        log.info("Raw parameter for Output not found, using datasetRid: %s", output_rid)
        return {"datasetRid": output_rid}

    def _build_parameters(
        self,
        transform: Transform,
        branch: str,
        input_specs: _InputSpecsMap,
        output_specs: _OutputSpecsMap,
        raw_parameters: Dict[str, Any],
        param_overrides: Dict[str, Any],
        job_rid: str,
    ) -> Dict[str, Any]:
        context = ParamContext(
            self._foundry, input_specs, output_specs, branch, job_rid
        )

        build_parameters: Dict[str, Any] = {}
        for name, param in transform.parameters.items():
            raw_parameter = raw_parameters.get(name)
            if not raw_parameter:
                # Back compatibility for jobspecs with no parameters. This must be an input or output.
                if isinstance(param, Input):
                    raw_parameter = self._create_fake_raw_input(param, input_specs)
                elif isinstance(param, Output):
                    raw_parameter = self._create_fake_raw_output(param, output_specs)
                # TODO(mbakovic): Support optional parameters
                elif isinstance(param, IncrementalSemanticVersion):
                    # if there is an incremental semantic version input but it does not exist in the job spec,
                    # then we check if the semantic verison is 1. If so, we assume that the compiled version
                    # of the compute method did not support semantic version, so we drop it.
                    if param._semantic_version != 1:
                        raise KeyError(
                            "Incremental semantic version expected but not found. "
                            f"Found parameters: {raw_parameters.keys()}"
                        )
                    continue
                else:
                    raise KeyError(
                        f"Parameter {name} expected but not found. Found parameters: {raw_parameters.keys()}"
                    )
            # TODO(mbakovic): Verify parameter value using json schema here
            override = param_overrides.get(name)
            if override:
                build_parameters[name] = param.instance(
                    context, raw_parameter, override
                )
            else:
                build_parameters[name] = param.instance(context, raw_parameter)
            log.info("Build parameters: %s", build_parameters)
        return build_parameters

    def provenance_records(
        self, input_specs: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """Construct a set of provenance records for the given input specs."""
        # Only include foundry input types until arbitrary inputs are supported
        return [
            self._build_input_provenance_record(input_spec)
            for input_spec in input_specs
            if input_spec["inputType"] == JobExecution.FOUNDRY_INPUT_TYPE
        ]

    def _build_input_provenance_record(
        self, input_spec: Dict[str, Any]
    ) -> Dict[str, Any]:
        props = input_spec[DATASET_LOCATOR][DATASET_PROPERTIES]
        prov = {
            "datasetRid": input_spec[DATASET_LOCATOR]["datasetRid"],
            "schemaVersionId": props.get("schemaVersionId"),
            "schemaBranchId": props.get("schemaBranchId"),
        }

        # Transaction range may be None if there are no committed transactions in the range.
        transaction_range = self._catalog.get_dataset_view_range(
            self._foundry.auth_header,
            input_spec[DATASET_LOCATOR]["datasetRid"],
            props.get("endTransactionRid"),
            start_txrid=props.get("startTransactionRid"),
        )

        if transaction_range is not None:
            (start_txrid, end_txrid) = transaction_range
            prov["transactionRange"] = {
                "startTransactionRid": start_txrid,
                "endTransactionRid": end_txrid,
            }

        return prov

    @staticmethod
    def _get_registration_and_version_id(
        output_specs: List[Dict[str, Any]],
    ) -> Tuple[Union[str, None], Union[str, None]]:
        """
        Returns `(version_rid, job_rid, version_id)` if present.
        """

        for spec in output_specs:
            if spec["outputType"] == "data-health":
                registration_rid: str = spec[DATASET_LOCATOR]["datasetRid"]
                output_metadata: Dict[str, Any] = (
                    spec["outputMetadata"] if "outputMetadata" in spec else {}
                )
                version_id: Union[str, None] = output_metadata.get("versionId", None)
                return registration_rid, version_id

        return None, None

    def _execute_code_checks_on_transform_items(
        self,
        registration_rid: str,
        job_rid: str,
        transform_items: Iterable[Tuple[str, Union[Input, Output]]],
        parameters: Dict[str, Any],
        params_to_df: Dict[str, Callable[[], "DataFrame"]],
    ) -> None:
        dataframe_and_checks_tuples: List[Tuple[DataFrame, List[Check]]] = [
            (parameters[name].dataframe(), titem.checks)
            for name, titem in transform_items
            if len(titem.checks) > 0
        ]

        if not dataframe_and_checks_tuples:
            return

        results_dict = _run_checks(dataframe_and_checks_tuples, params_to_df)
        self._foundry._service_provider.code_checks_result().publish_results(
            self._foundry.auth_header,
            registration_rid,
            job_rid,
            results_dict["check_results"],
        )

        abort_build_if_failed_expectations(results_dict)

    def _execute_preconditions(
        self,
        transform: Transform,
        parameters: Dict[str, Any],
        registration_rid: str,
        job_rid: str,
    ):
        params_to_df = self._get_params_to_df(parameters)
        return self._execute_code_checks_on_transform_items(
            registration_rid,
            job_rid,
            transform.inputs.items(),
            parameters,
            params_to_df,
        )

    def _execute_postconditions(
        self,
        transform: Transform,
        parameters: Dict[str, Any],
        registration_rid: str,
        job_rid: str,
    ):
        params_to_df = self._get_params_to_df(parameters, include_outputs=True)
        return self._execute_code_checks_on_transform_items(
            registration_rid,
            job_rid,
            transform.outputs.items(),
            parameters,
            params_to_df,
        )

    def _get_params_to_df(
        self, parameters: Dict[str, Any], include_outputs: bool = False
    ):
        params_to_df: Dict[str, Callable[[], "DataFrame"]] = {}
        for name, parameter in parameters.items():
            is_transform_param = self._is_input_parameter(name) or (
                include_outputs and self._is_output_parameter(name)
            )
            is_dataset_param = isinstance(parameter, DataframeParamInstance)
            if is_transform_param and is_dataset_param:
                # We pass the method instead of the full dataframe to avoid calling the dataframe method
                # unless we really need to.
                params_to_df[name] = parameter.dataframe
        return params_to_df

    # pylint: disable=too-many-branches,too-many-statements
    def run(
        self,
        input_specs: List[Dict[str, Any]],
        output_specs: List[Dict[str, Any]],
        branch: str,
        incremental_resolution_result: Dict[str, Any],
        job_rid: str,
        **kwargs: Any,
    ) -> Dict[str, Any]:
        """Run the given ExecutionRequest."""
        if not output_specs:
            # It's not valid for a build2 job to have zero output specs.
            raise ValueError("Transforms with zero outputs are not supported.")

        # Since we get these values out of the `kwargs` dict, and `kwargs` has type `Any`, these
        # also need to be typed as any without a `cast` or `assert` (@ccabo 2023.10.05)
        compute_function: Union[Callable[..., Any], Any] = kwargs.get(
            "compute_function"
        )
        params: Union[Dict[str, Any], Any] = kwargs.get("parameters", {})
        param_overrides: Union[Dict[str, Any], Any] = kwargs.get(
            "parameter_overrides", {}
        )
        environment: Union[Dict[str, Any], Any] = kwargs.get("environment", {})

        # Convert params to dataset_rid -> [branch1, branch2, ...] dictionaries
        dataset_param_branches: _DatasetParamsMap = get_dataset_param_branches(params)
        # Convert input specs to (rid, branch) -> spec dictionaries
        input_specs_map: _InputSpecsMap = extract_input_specs(
            input_specs, dataset_param_branches
        )
        # Convert output specs to rid -> spec dictionaries
        output_specs_map: _OutputSpecsMap = extract_output_specs(output_specs)

        log.info("Got input specs: %s", str(input_specs_map))
        log.info("Got output specs: %s", str(output_specs_map))
        log.info("Got compute function: %s", compute_function)
        log.info("Got parameters: %s", params)
        log.info("Got parameter overrides: %s", param_overrides)
        log.info("Got environment: %s", environment)
        log.info("Got incremental resolution: %s", incremental_resolution_result)

        log.info("Running build for transform %s", self._transform)

        parameters = self._build_parameters(
            self._transform,
            branch,
            input_specs_map,
            output_specs_map,
            params,
            param_overrides,
            job_rid,
        )
        log.info("Constructed parameters: %s", parameters)

        registration_rid, version_id = self._get_registration_and_version_id(
            output_specs
        )

        transform_context = TransformContext(
            self._foundry,
            parameters=params,
            environment=environment,
        )

        # Force load spark session from foundry connector proxy before user code is run.
        # Prevents user code from initializing its own spark session that lacks foundry connector configuration.
        transform_context.spark_session

        def log_progress(state: str) -> None:
            self._event_logger.emit_progress_event(
                state, job_rid, registration_rid, version_id
            )

        if registration_rid:
            log_progress(EventLogger.PRECONDITIONS_START)
            self._execute_preconditions(
                self._transform, parameters, registration_rid, job_rid
            )
            log_progress(EventLogger.PRECONDITIONS_END)

        if incremental_resolution_result:
            catalog_prior_output_txns = {
                rid: spec[DATASET_LOCATOR][DATASET_PROPERTIES].get(
                    PRIOR_INCR_TXN_RID, None
                )
                for rid, spec in output_specs_map.items()
            }
            catalog_prior_output_txns = {
                rid: txn
                for rid, txn in catalog_prior_output_txns.items()
                if txn is not None
            }
            kwargs = {
                "incremental_resolution_result": incremental_resolution_result,
                "catalog_prior_output_txns": catalog_prior_output_txns,
                "ctx": transform_context,
            }
        else:
            kwargs = {"ctx": transform_context}

        try:
            self._transform.compute(**kwargs, **parameters)
        except AbortJobException:
            log.info("Job was aborted")
            return AbortResult()

        if self._should_abort_job(incremental_resolution_result, parameters):
            log.info(
                "Job was aborted because all outputs were Catalog and each output was aborted. This happens "
                "when using incremental v2 in order to maintain incrementality"
            )
            return AbortResult()

        if registration_rid:
            log_progress(EventLogger.POSTCONDITIONS_START)
            self._execute_postconditions(
                self._transform, parameters, registration_rid, job_rid
            )
            log_progress(EventLogger.POSTCONDITIONS_END)

        output_parameters: Dict[str, Any] = {}
        for parameter_name, parameter in parameters.items():
            if self._is_output_parameter(parameter_name):
                output_parameters[parameter_name] = parameter

        log.info("Filtered to output parameters: %s", output_parameters)
        return SuccessResult(outputs=output_parameters)

    def _is_output_parameter(self, parameter_name: str) -> bool:
        return self._is_param_of_type(parameter_name, FoundryOutputParam)

    def _is_input_parameter(self, parameter_name: str) -> bool:
        return self._is_param_of_type(parameter_name, FoundryInputParam)

    def _is_param_of_type(self, parameter_name: str, requested_type: type) -> bool:
        return parameter_name in self._transform.parameters and (
            isinstance(self._transform.parameters[parameter_name], requested_type)
        )

    def _should_abort_job(self, incremental_resolution_result, parameters):
        # if we are running incremental v2, all outputs are Catalog, and all outputs have been aborted, we want
        # to abort the job for the user as well. This is a bit hacky, but will make people happy and save us a
        # lot of heartache
        if not incremental_resolution_result:
            return False

        all_outputs = [
            parameter
            for (parameter_name, parameter) in parameters.items()
            if self._is_output_parameter(parameter_name)
        ]

        all_outputs_are_catalog = all(
            [isinstance(output, TransformOutput) for output in all_outputs]
        )

        if not all_outputs_are_catalog:
            return False

        all_outputs_are_aborted = all([output._aborted for output in all_outputs])
        return all_outputs_are_aborted
