# Copyright 2018 Palantir Technologies, Inc.
import uuid
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from transforms import _json_schema
from transforms.api._utils import _as_list

if TYPE_CHECKING:
    from transforms.foundry.connectors import FoundryConnector


class Param(object):
    def __init__(self, description: Optional[str] = None):
        """Base class for any parameter taken by transform compute function.

        Args:
            description (str, optional): Parameter description to be added in json schema.

        .. versionadded:: 1.53.0
        """
        self._schema: Dict[str, Any] = _json_schema.base()
        self._schema.update({"description": description} if description else {})

    @property
    def schema(self):
        """Returns JSON schema for parameter of this type. Must return valid JSON schema."""
        return self._schema

    @property
    def json_value(self) -> Any:
        """Returns JSON value for this parameter to put in jobspec.

        If return value is None, parameter is considered unbound. If any transform's parameter is unbound,
        transform is considered to be unbound. For unbound transforms jobspec is not published.
        """
        return None

    @staticmethod
    def instance(context, json_value):  # pylint: disable=unused-argument
        """Creates parameter instance using raw JSON value from jobspec parameters and specific context.

        Return value is injected in transform compute function.

        Args:
            context (ParamContext): Context object with properties that might be required for creating an instance.
            json_value (any): Any raw value deserialized from jobspec parameters.
        """
        return json_value


class ParamContext(object):
    def __init__(
        self,
        foundry_connector: "FoundryConnector",
        input_specs,
        output_specs,
        branch: str,
        job_rid: Optional[str],
    ) -> None:
        """Context object injected in instance method of parameter.

        Args:
            foundry_connector (FoundryConnector): `transforms.foundry.connectors.FoundryConnector` object
            input_specs (dict): Map from rid to input spec entry from jobspec.
            output_specs (dict): Map from rid to output spec entry from jobspec.
            branch (str): Branch name this build is running on.
            job_rid (Optional[str]): The rid of the job.

        .. versionadded:: 1.53.0
        """
        self.foundry_connector = foundry_connector
        self.input_specs = input_specs
        self.output_specs = output_specs
        self.branch = branch
        self.job_rid = job_rid


class UnmarkingDef:
    def __init__(
        self, marking_ids: Union[List[str], str], on_branches: Union[List[str], str]
    ) -> None:
        """Base class for unmarking datasets configuration.

        Args:
            marking_ids (List[str], str): List of marking identifiers or single marking identifier.
            on_branches (List[str], str): Branch on which to apply unmarking.
        """
        self.marking_ids = _as_list(marking_ids)
        self.branches = _as_list(on_branches)


class Markings(UnmarkingDef):
    """Specification of a marking that stops propagating from input."""


class OrgMarkings(UnmarkingDef):
    """Specification of a marking that is no longer required on the output."""


class FoundryInputParam(Param):
    # pylint: disable=redefined-builtin
    def __init__(
        self,
        aliases: List[str],
        branch: Optional[str] = None,
        type: str = "foundry",
        description: Optional[str] = None,
        properties: Optional[Dict[Any, Any]] = None,
        failure_strategy: Optional[str] = None,
        stop_propagating: Optional[Markings] = None,
        stop_requiring: Optional[OrgMarkings] = None,
    ) -> None:
        """All aliases are resolved and added in input specs section of jobspec. Will not pass a defaultPath to graph
        dependency if the alias is a valid rid. Input params have a unique identifier, which is set if there is
        reference in job spec to input.

        Args:
            aliases (list of str): List of absolute Compass paths or dataset rids.
            branch (str, optional): Optional branch against which to resolve input transactions.
            properties (dict, optional): Optional properties that get written to build graph input reference.
            description (str, optional): Parameter description.
            type (str, optional): Input dataset type.
            failure_strategy (str, optional): Failure strategy to be used in case the input update fails.

        .. versionadded:: 1.53.0
        """
        super().__init__(description)
        self._aliases = aliases
        self._branch = branch
        self._type = type
        self._input_identifier = str(uuid.uuid1()).strip()
        self._properties = properties

        if stop_propagating is not None and not isinstance(stop_propagating, Markings):
            raise TypeError(
                f"stop_propagating must be of type Markings. Instead found type {type(stop_propagating)}."
            )
        if stop_requiring is not None and not isinstance(stop_requiring, OrgMarkings):
            raise TypeError(
                f"stop_requiring must be of type OrgMarkings. Instead found type {type(stop_requiring)}."
            )
        self.stop_propagating = stop_propagating
        self.stop_requiring = stop_requiring

        # These are coming from build2.api.InputFailureStrategy
        possible_strategies = set(["CONTINUE", "FAIL"])

        if (
            failure_strategy is not None
            and failure_strategy.upper() not in possible_strategies
        ):
            raise TypeError(
                "failure_strategy must be 'continue' or 'fail'."
                f"Usage: failure_strategy='continue', instead, given: "
                f"{failure_strategy}"
            )

        self._failure_strategy = failure_strategy


class FoundryOutputParam(Param):
    # pylint: disable=redefined-builtin
    def __init__(
        self,
        aliases: List[str],
        type: str = "foundry",
        description: Optional[str] = None,
        properties: Optional[Dict[Any, Any]] = None,
    ) -> None:
        """All aliases are resolved and added in output specs section of jobspec.  Will not pass a defaultPath to graph
        dependency if the alias is a valid rid.

        Args:
            aliases (list of str): List of absolute Compass paths or dataset rids.
            description (str, optional): Parameter description.
            properties (dict, optional): Optional properties that get written to build graph output reference.
            type (str, optional): Output dataset type.

        .. versionadded:: 1.53.0
        """
        super().__init__(description)
        self._aliases = aliases
        self._type = type
        self._properties = properties

    def output_results(self, _instance, **_kwargs):
        """Generates a map from output RIDs to TransformsOutputResult. Results are merged into a TransformJobResult.

        Args:
            instance: An object of the type generated by instance().
        """
        return None


class DataframeParamInstance(object):
    def dataframe(self):
        """Return a :class:`pyspark.sql.DataFrame` containing the full view of the dataset.

        Returns:
            pyspark.sql.DataFrame: The dataframe for the dataset.
        """
        raise NotImplementedError()
