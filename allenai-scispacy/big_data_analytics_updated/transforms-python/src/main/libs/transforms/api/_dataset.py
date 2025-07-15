from typing import TYPE_CHECKING, Any, Dict, List, Mapping, Optional, Union

from transforms import _json_schema
from transforms.api._checks import Check
from transforms.api._param import UnmarkingDef  # noqa # pylint: disable=unused-import
from transforms.api._param import (
    FoundryInputParam,
    FoundryOutputParam,
    Markings,
    OrgMarkings,
)
from transforms.api._utils import _as_list
from transforms.foundry import _resource_identifier

if TYPE_CHECKING:
    from transforms.api._transform import TransformOutput
    from transforms.foundry.connectors import FoundryConnector


class Input(FoundryInputParam):
    def __init__(
        self,
        alias: Optional[str] = None,
        branch: Optional[str] = None,
        description: Optional[str] = None,
        stop_propagating: Optional[Markings] = None,
        stop_requiring: Optional[OrgMarkings] = None,
        checks: Union[Check, List[Check], None] = None,
        failure_strategy: Optional[str] = None,
    ) -> None:
        """Specification of a transform dataset input.

        Args:
            alias (str, optional): Dataset rid or the absolute Compass path of the dataset.
                If not specified, parameter is unbound.
            branch (str, optional): Branch name to resolve the input dataset to. If not specified, resolved at
                build-time.
            description (str, optional): Input description.
            stop_propagating (Markings, optional): Security markings to stop propagating from this.
            stop_requiring (OrgMarkings, optional): Org markings to assume on this input.
            checks (List[Check], Check): One or more :class:`Check` objects.
            failure_strategy (str, optional): strategy in case the input fails to update.
                Must be either 'continue' or 'fail'. If not specified, defaults to 'fail'.

        .. versionchanged:: 1.53.0
            Now subclasses :class:`~transforms.api.FoundryInputParam`
        """
        super().__init__(
            [alias] if alias else [],
            branch=branch,
            description=description,
            failure_strategy=failure_strategy,
            stop_propagating=stop_propagating,
            stop_requiring=stop_requiring,
        )
        self._schema.update(_json_schema.DATASET_SCHEMA)
        self.alias = alias
        self.branch = branch
        self.checks: List[Check] = _as_list(checks)
        self.failure_strategy = failure_strategy

    @property
    def json_value(self) -> Union[Dict[str, Any], None]:
        val: Dict[str, Any] = {}
        if self.alias is not None:
            val["datasetRid"] = self.alias
        if self._branch is not None:
            val["branch"] = self._branch
        if self.failure_strategy is not None:
            val["inputFailureStrategy"] = self.failure_strategy
        return val or None

    @staticmethod
    def instance(context, json_value: Mapping[str, Any]):
        return _create_input_from_raw(
            context.foundry_connector, context.input_specs, json_value
        )


class InputSet(FoundryInputParam):
    def __init__(
        self, aliases: Optional[List[str]] = None, description: Optional[str] = None
    ) -> None:
        """Specification of a list of transform inputs.

        Args:
            aliases (list of str, optional): List of dataset rids or the absolute Compass paths of the datasets.
                If not specified, parameter is unbound. Empty list is not allowed.
            description (str, optional): Input set description. NOTE: This feature is not implemented yet.

        .. versionadded:: 1.53.0
        """
        super().__init__(aliases or [], description=description)
        if aliases == []:
            raise ValueError("Aliases list must be non-empty")
        self._schema.update(
            {"type": "array", "items": _json_schema.DATASET_SCHEMA, "uniqueItems": True}
        )
        self.aliases = aliases

    @property
    def json_value(self) -> Optional[List[Dict[str, str]]]:
        return (
            [{"datasetRid": alias} for alias in self.aliases]
            if self.aliases is not None
            else None
        )

    @staticmethod
    def instance(context, json_value):
        input_set = []
        for dataset in json_value:
            input_set.append(
                _create_input_from_raw(
                    context.foundry_connector, context.input_specs, dataset
                )
            )
        return input_set


class Output(FoundryOutputParam):
    def __init__(
        self,
        alias: Optional[str] = None,
        sever_permissions: bool = False,
        description: Optional[str] = None,
        checks: Union[Check, List[Check], None] = None,
    ) -> None:
        """Specification of a transform output.

        Args:
            alias (str, optional): Dataset rid or the absolute Compass path of the dataset.
                If not specified, parameter is unbound.
            sever_permissions (bool, optional): If true, sever's the permissions of the dataset from the
                permissions of its inputs. Ignored if parameter is unbound.
            description (str, optional): Output description. NOTE: This feature is not implemented yet.
            checks (List[Check], Check): One or more :class:`Check` objects.

        .. versionchanged:: 1.53.0
            Now subclasses :class:`~transforms.api.FoundryOutputParam`
        """

        super().__init__([alias] if alias else [], description=description)
        self._schema.update(_json_schema.DATASET_SCHEMA)
        self.alias = alias
        self.sever_permissions = sever_permissions
        self.checks: List[Check] = _as_list(checks)

    @property
    def json_value(self):
        return {"datasetRid": self.alias} if self.alias is not None else None

    @staticmethod
    def instance(context, json_value):
        return _create_output_from_raw(
            context.foundry_connector, context.output_specs, context.branch, json_value
        )

    def output_results(self, instance, **kwargs):
        # pylint: disable=import-outside-toplevel
        from transforms.api._transform import TransformOutput

        if not isinstance(instance, TransformOutput):
            raise TypeError(
                f"Instance must be a TransformOutput. Instead found type {type(instance)}."
            )
        return (
            {
                instance.rid: {
                    "jobResult": {
                        "additionalTransactionRecordProperties": {},
                        "provenance": {
                            "provenanceRecords": kwargs.get("provenance_records")
                        },
                        "schemaVersionId": instance.schema_version_id,
                        "notModified": instance._aborted,
                    },
                    "datasetProperties": {
                        "schemaVersionId": instance.schema_version_id
                    },
                }
            }
            if self.alias is not None
            else None
        )


class OutputSet(FoundryOutputParam):
    def __init__(self, aliases=None, sever_permissions=False, description=None):
        """Specification of a list of transform outputs.

        Args:
            aliases (list of str, optional): List of dataset rids or the absolute Compass paths of the datasets.
                If not specified, parameter is unbound. Empty list is not allowed.
            sever_permissions (bool, optional): If true, sever's the permissions of the datasets from the
                permissions of their inputs. Ignored if parameter is unbound.
            description (str, optional): Output description. NOTE: This feature is not implemented yet.

        .. versionadded:: 1.53.0
        """
        super().__init__(aliases or [], description=description)
        if aliases == []:
            raise ValueError("Aliases list must be non-empty")
        self._schema.update(
            {"type": "array", "items": _json_schema.DATASET_SCHEMA, "uniqueItems": True}
        )
        self.aliases = aliases
        self.sever_permissions = sever_permissions

    @property
    def json_value(self):
        return (
            [{"datasetRid": alias} for alias in self.aliases]
            if self.aliases is not None
            else None
        )

    @staticmethod
    def instance(context, json_value):
        output_set = []
        for dataset in json_value:
            output_set.append(
                _create_output_from_raw(
                    context.foundry_connector,
                    context.output_specs,
                    context.branch,
                    dataset,
                )
            )
        return output_set

    def output_results(self, instance, **kwargs):
        if not isinstance(instance, list):
            raise TypeError(
                f"Instance must be a list of TransformOutputs. Instead found type {type(instance)}."
            )
        # pylint: disable=import-outside-toplevel
        from transforms.api._transform import TransformOutput

        for output in instance:
            if not isinstance(output, TransformOutput):
                raise TypeError(
                    f"Instance must be a list of TransformOutputs. List contained element of type {type(output)}."
                )
        return {
            output.rid: {
                "jobResult": {
                    "additionalTransactionRecordProperties": {},
                    "provenance": {
                        "provenanceRecords": kwargs.get("provenance_records")
                    },
                    "schemaVersionId": output.schema_version_id,
                    "notModified": output._aborted,
                },
                "datasetProperties": {"schemaVersionId": output.schema_version_id},
            }
            for output in instance
        }


def _create_input_from_raw(
    foundry_connector: "FoundryConnector", input_specs, raw: Dict[str, Any]
):
    input_rid = raw["datasetRid"]
    if not _resource_identifier.is_valid(input_rid):
        raise ValueError(
            f"Expected valid resource identifier in parameters but got {input_rid!r}. "
            "It might be because parameters are not resolved during CI. Check if 'transformsVersion' is correct."
        )

    input_branch = raw.get("branch", None)
    # if raw.get('branch') contains a value, this means the branch was set explicitly, will be correct in job spec
    # if this is None and not fake, no branch specified, branch should be under None
    input_spec = input_specs.get((input_rid, input_branch))
    if not input_spec:
        raise KeyError(
            f"Input spec for dataset {input_rid} branch {input_branch} not found"
        )

    input_type = input_spec.get("inputType")
    props = input_spec["datasetLocator"]["datasetProperties"]

    if input_type is not None and input_type == "table":
        return foundry_connector.input_table(
            rid=input_rid,
            to_version=props["version"]["resolved"],
            from_version=props.get("fromVersion"),
            branch=input_spec.get("branch"),
        )

    return foundry_connector.input(
        rid=input_rid,
        branch=props.get("schemaBranchId"),
        schema_version=props.get("schemaVersionId"),
        end_txrid=props.get("endTransactionRid"),
        start_txrid=props.get("startTransactionRid"),
    )


def _create_output_from_raw(
    foundry_connector: "FoundryConnector",
    output_specs: Dict[str, Any],
    branch,
    raw: Dict[str, Any],
) -> "TransformOutput":
    output_rid: Union[str, Any] = raw["datasetRid"]
    if not _resource_identifier.is_valid(output_rid):
        raise ValueError(
            f"Expected valid resource identifier in parameters but got {output_rid!r}. "
            "It might be because parameters are not resolved during CI. Check if 'transformsVersion' is correct."
        )
    output_spec = output_specs.get(output_rid)
    if not output_spec:
        raise KeyError(f"Output spec for dataset {output_rid} not found")

    return foundry_connector.output(
        rid=output_rid,
        branch=branch,
        txrid=output_spec["datasetLocator"]["datasetProperties"][
            "outputTransactionRid"
        ],
    )
