# Copyright 2017 Palantir Technologies, Inc.
"""Connectors for interacting with Foundry from the transforms APIs.

A Connector can be used interactively to construct :class:`~transforms.api.TransformInput` and
:class:`~transforms.api.TransformOutput` objects and also to run a :class:`~transforms.api.Transform`.
"""
import abc
import logging
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, cast

from future import utils

from .. import _java_utils as _j
from .. import _utils, api
from . import _dfreader, _dfwriter, _foundryfs, _fsm, _fsreader, _fswriter, services

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from transforms.api import Transform


class _BaseConnector(utils.with_metaclass(abc.ABCMeta)):
    """Interface for building TransformInput and TransformOutput objects backed by different data stores."""

    @abc.abstractmethod
    def input(
        self,
        alias: Optional[str] = None,
        rid: Optional[str] = None,
        branch: Optional[str] = None,
        end_txrid: Optional[str] = None,
        start_txrid: Optional[str] = None,
        schema_version: Optional[str] = None,
    ):
        """Returns a :class:`~transforms.api.TransformInput` for the given parameters."""

    @abc.abstractmethod
    def output(
        self,
        alias: Optional[str] = None,
        rid: Optional[str] = None,
        branch: Optional[str] = None,
        txrid: Optional[str] = None,
        filesystem_id: Optional[str] = None,
    ):
        """Returns a :class:`~transforms.api.TransformOutput` for the given parameters."""

    @_utils.driver_only
    def run(self, transform: "Transform") -> Any:
        """Run the given :class:`~transforms.api.Transform` using the latest inputs and outputs.

        Args:
            transform (transforms.api.Transform): the transform to run.
        """
        ctx = api.TransformContext(self)
        tinputs = {
            name: self.input(alias=input_spec.alias)
            for name, input_spec in transform.inputs.items()
        }

        toutputs = {
            name: self.output(alias=output_spec.alias)
            for name, output_spec in transform.outputs.items()
        }

        return transform.compute(ctx, **dict(tinputs, **toutputs))


class FoundryConnector(_utils.SelectivePickleMixin, _BaseConnector):
    """Entry-point for accessing `Foundry` services.

    The `Foundry` object manages interactions with Foundry services by providing APIs for manipulating datasets.
    """

    # pylint: disable=super-init-not-called

    _PICKLE_FIELDS = [
        "_auth_header",
        "_filesystem_id",
        "_branches",
        "_service_provider",
        "_foundry_fs",
    ]

    _DFREADER_CLS = _dfreader.DataFrameReader
    _DFWRITER_CLS = _dfwriter.DataFrameWriter
    _FSREADER_CLS = _fsreader.FileSystemReader
    _FSWRITER_CLS = _fswriter.FileSystemWriter

    def __init__(
        self,
        service_config: Dict[str, Any],
        auth_header: str,
        filesystem_id: Optional[str] = None,
        fallback_branches: Optional[List[str]] = None,
        resolver: Optional[Callable[[str], str]] = None,
        temp_creds_token: Optional[str] = None,
        clone_hadoop_conf: bool = True,
    ) -> None:
        """
        Args:
            service_config (dict): A configuration dictionary conforming to the JSON spec in
                the Java class com.palantir.remoting.api.config.service.ServicesConfigBlock
            auth_header (str): The authorization string to use when connecting to foundry services.
            filesystem_id (str, optional): The backing filesystem to use.
            fallback_branches (List[str], optional): Fallback branches.
            resolver (Callable[[str], str], optional): Function for resolving a dataset alias into a rid.
                Defaults to resolving the alias as a Compass path.
            temp_creds_token (Optional[str]): The temporary credentials authorization token to use for FoundryFS.
            clone_hadoop_conf (Optional[bool]): A flag indicating whether the Hadoop configuration obtained from the
                SparkSession should be cloned before being passed to FoundryFS. This option will be removed in the
                future.
        """

        self._auth_header = auth_header
        self._filesystem_id = filesystem_id
        self._branches = fallback_branches or []
        self._service_provider = services.ServiceProvider(service_config)
        self._resolver = resolver or self._resolve_from_compass
        self._event_logger = services.EventLogger()

        self._foundry_fs = _foundryfs.FoundryFS(
            self._auth_header,
            self._service_provider,
            temp_creds_token,
            self.spark_session,
            clone_hadoop_conf,
        )

    @property
    def _jvm(self):
        return _j.get_or_create_gateway().jvm

    @property
    def auth_header(self):
        """str: The auth header used to contact Foundry."""
        return self._auth_header

    @property
    def fallback_branches(self):
        """List[str]: the fallback branches used to retrieve datasets."""
        return self._branches

    @property
    @_utils.driver_only
    def spark_session(self):
        """:class:`pyspark.sql.SparkSession`: Get a handle on the `SparkSession` created by `FoundrySparkManager`."""
        return self._foundry_spark_manager.spark_session  # pylint: disable=no-member

    @_utils.memoized_property
    @_utils.driver_only
    def _foundry_spark_manager(self):
        """:class:`transforms.foundry._fsm.FoundrySparkManager`: Return the wrapped spark manager."""
        return _fsm.FoundrySparkManager(self._service_provider, branches=self._branches)

    def _resolve_from_compass(self, path: str) -> str:
        """Resolve a dataset rid from its Compass path.

        Args:
            path (str): A Compass path

        Returns:
            str: The resource identifier at the given path

        Raises:
            :exc:`~exceptions.ValueError`: If a resource with the given path can not be found.
        """
        rid = self._service_provider.path().get_resource_by_path(self.auth_header, path)
        if not rid:
            raise ValueError(f"Could not find resource with path {path}")
        return rid

    @_utils.driver_only
    def _resolve_role(self, role: str) -> List[str]:
        return _j.get_or_create_gateway().entry_point.getRoleUris(role)

    @_utils.driver_only
    def input_table(self, rid, to_version, from_version=None, branch=None):
        """Construct a :class:`~transforms.api.TableTransformInput` from the given parameters.

        Args:
            rid (str): The resource identifier of the table.
            to_version (dict): The resolved toVersion of the table (generated by the
                Tables Input Manager), corresponding to the current view of the table.
            from_version (dict, optional): The resolved fromVersion of the table (generated
                by the Tables Input Manager), used as the previous view of the table when
                reading from it incrementally. If absent, the provided table does not
                support incremental reading.
            branch (str, optional): The branch of the table. Reads are done using only
                the properties above, so we do not need to select a fallback branch for
                dataframe reading as we do with datasets.

        Returns:
            transforms.api.TableTransformInput: An input object representing the requested table.
        """
        return api.TableTransformInput(
            rid,
            branch,
            _dfreader.TableDataFrameReader(self, rid, from_version, to_version),
        )

    @_utils.driver_only
    def input(
        self,
        alias: Optional[str] = None,
        rid: Optional[str] = None,
        branch: Optional[str] = None,
        end_txrid: Optional[str] = None,
        start_txrid: Optional[str] = None,
        schema_version: Optional[str] = None,
    ) -> api.TransformInput:
        """Construct a :class:`~transforms.api.TransformInput` from the given parameters.

        The `resource identifier` used to construct the :class:`~transforms.api.TransformInput` will be resolved
        from the given ``alias`` unless the ``rid`` parameter is passed.

        Args:
            alias (str, optional): The alias of the dataset.
            rid (str, optional): The resource identifier of the dataset.
            branch (str, optional): The branch from which to read the dataset. If not set the branch is
                chosen as the first branch in the `fallbacks` list that exists in the `Catalog`.
            end_txrid (str, optional): The end transaction of the view, if not set, defaults to the latest
                transaction on the given branch.
            start_txrid (str, optional): The starting transaction of the view.
            schema_version (str, optional): The schema version to use when reading, if not set, defaults to
                the latest schema version on the given branch.

        Returns:
            transforms.api.TransformInput: An input object representing the requested dataset.

        Raises:
            :exc:`~exceptions.ValueError`: If either the `alias` or `rid` (but not both) is not specified.
            :exc:`~exceptions.ValueError`: If a branch is not specified and a fallback branch cannot be
                found in the `Catalog`.
        """
        if not (alias or rid):
            raise ValueError("Must specify either alias or rid")
        rid = rid or self._resolver(cast(str, alias))

        if not branch:
            catalog_branches = self._service_provider.catalog().get_branches(
                self._auth_header, rid
            )
            branch = next((b for b in self._branches if b in catalog_branches), None)
            log.info(
                "branch not set, had branches: %s, got catalog_branches: %s, and picked branch: %s",
                self._branches,
                catalog_branches,
                branch,
            )

        return api.TransformInput(
            rid,
            branch,
            (start_txrid, end_txrid),
            self._DFREADER_CLS(self, rid, branch, schema_version),
            self._FSREADER_CLS(self, rid, branch),
        )

    # pylint: disable=arguments-differ
    @_utils.driver_only
    def output(
        self,
        alias: Optional[str] = None,
        rid: Optional[str] = None,
        branch: Optional[str] = None,
        txrid: Optional[str] = None,
        filesystem_id: Optional[str] = None,
        mode: str = "replace",
    ) -> api.TransformOutput:
        """Construct a :class:`~transforms.api.TransformOutput` from the given alias or rid.

        The `resource identifier` used to construct the :class:`transforms.api.TransformOutput` will be
        resolved from the given ``alias`` unless the ``rid`` parameter is passed.

        Args:
            alias (str, optional): The alias of the dataset.
            rid (str, optional): The resource identifier of the dataset.
            branch (str, optional): The branch to which to write the dataset. If not set the branch is chosen as the
                first branch in the `fallbacks` list.
            txrid (str, optional): The transaction into which data should be written.
            filesystem_id (str, optional): the filesystem in which to create the dataset if it doesn't already exist.
            mode (str, optional): The write mode, one of 'replace' or 'modify'. In modify mode anything written
                to the output is appended to the dataset. In replace mode, anything written to the output
                replaces the dataset.

        Returns:
            transforms.api.TransformOutput: An output object representing the requested dataset.

        Raises:
            :exc:`~exceptions.ValueError`: If either the `alias` or `rid` (but not both) is not specified.
        """

        if not (alias or rid):
            raise ValueError("Must specify either alias or rid")

        rid = rid or self._resolver(alias)

        branch = branch or next(iter(self._branches), None)
        if not branch:
            raise ValueError("No branch defined for output dataset.")

        return api.TransformOutput(
            rid,
            branch,
            txrid,
            self._DFREADER_CLS(self, rid, branch, schema_version=None),
            self._DFWRITER_CLS(self, rid, txrid, branch, filesystem_id),
            self._FSWRITER_CLS(self, rid, txrid),
            mode,
        )

    @_utils.driver_only
    def run(self, transform: "Transform") -> Any:
        ctx = api.TransformContext(self)
        tinputs = {
            name: self.input(alias=input_spec.alias)
            for name, input_spec in transform.inputs.items()
        }

        toutputs = {
            name: self.output(alias=output_spec.alias)
            for name, output_spec in transform.outputs.items()
        }

        return transform.compute(ctx, **dict(tinputs, **toutputs))
