# Copyright 2017 Palantir Technologies, Inc.
# pylint: disable=cyclic-import,  import-outside-toplevel
import collections
import inspect
import logging
import os
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Generator,
    Iterable,
    List,
    Optional,
    Tuple,
    Union,
)

from pyspark.sql import DataFrame, SparkSession

from transforms import _env_utils, _errors, _source_provenance, _tllv_hasher, _utils
from transforms.foundry import _dfreader, _dfwriter, _fsreader, _fswriter

from ._checks import Check
from ._dataset import Input, Output
from ._param import DataframeParamInstance, FoundryOutputParam, Param
from ._utils import _get_most_similar_to

if TYPE_CHECKING:
    import pandas

    from transforms.foundry._foundryfs import FoundryFS
    from transforms.foundry.connectors import FoundryConnector

log = logging.getLogger(__name__)


class Transform(object):
    """A callable object that describes single step of computation.

    A Transform consists of a number of parameters that subclass :class:`Param` class and a compute function.

    It is idiomatic to construct a Transform object using the provided decorators: :func:`transform`,
    :func:`transform_df`, and :func:`transform_pandas`.

    Note the original compute function is exposed via the Transform's ``__call__`` method.
    """

    def __init__(
        self,
        compute_func: Callable[..., Any],
        inputs: Optional[Dict[str, Input]] = None,
        outputs: Optional[Dict[str, Output]] = None,
        profile: Union[str, List[str], None] = None,
        parameters: Optional[Dict[str, Param]] = None,
        type_alias: Optional[str] = None,
    ) -> None:
        """
        Args:
            compute_func (Callable): The compute function to wrap.
            inputs (Dict[str, Input]): A dictionary mapping input names to :class:`Input` specs.
            outputs (Dict[str, Output]): A dictionary mapping output names to :class:`Output` specs.
            profile (str or List[str], optional): The Transforms profile(s) to use at runtime.
            parameters (Dict[str, Param]): A dictionary mapping parameter names to :class:`Param` specs or subclass.
            type_alias (str): The type alias to register for this transform.
        """
        # Wrap the compute function and store its properties
        self._compute_func = compute_func
        self.__name__ = compute_func.__name__
        self.__module__ = compute_func.__module__
        self.__doc__ = compute_func.__doc__

        self.inputs = inputs or {}
        self.outputs = outputs or {}
        self._profile = profile
        self._allowed_run_duration = None
        self.parameters = parameters or {}
        self.type_alias = type_alias
        self.incremental_resolution_request = None

        # Used to create string representation
        self._clean_reference = _format_reference(compute_func)
        self._output_aliases: Union[List[str], None] = None

        self._bound_transform = True
        self._reference = self._clean_reference

        # Determines whether the job will run using a user scoped token.
        self._run_as_user = False

        # For compatibility reasons, add inputs and outputs to parameters
        if not self.parameters and (self.inputs or self.outputs):
            self.parameters.update(self.inputs)
            self.parameters.update(self.outputs)

        for param in self.parameters.values():
            # If `hasattr(param, 'json_value')` is False, we'll raise an exception from validate()
            if hasattr(param, "json_value") and param.json_value is None:
                self._bound_transform = False

        # For compatibility reasons, append hash of all outputs to compute function reference.
        # Required to make compute function reference unique when using transform generation.
        if self._bound_transform:
            self._output_aliases = [
                x
                for param in self.parameters.values()
                if isinstance(param, FoundryOutputParam)
                for x in param._aliases
            ]

            if self._output_aliases:
                outputs_hash = _utils.hash_list_ignore_order(self._output_aliases)
                self._reference = f"{self._clean_reference}:{outputs_hash}"

        # Search for file/line where transform object is created or imported
        self.filename, self.lineno = _source_provenance.compute_source_provenance(
            compute_func
        )

        self._argspec = inspect.getfullargspec(compute_func)

        # Inspect the function to see if it accepts a ctx parameter
        self._use_context = "ctx" in self._argspec.args

    def validate(self) -> None:
        """Validations to be executed during checks time.

        These won't be executed during runtime, to improve performance and satisfy
        backwards compatibility during runtime upgrades.
        """
        if "ctx" in self.parameters:
            raise ValueError(
                f"'ctx' is a reserved parameter name, cannot use it as a parameter for {self}"
            )

        self._assert_parameter_types_are_correct()
        self._assert_check_names_are_unique()
        self._check_inputs_are_present_in_parameters()
        self._check_outputs_are_present_in_parameters()

    def _assert_parameter_types_are_correct(self) -> None:
        for name, tinput in self.inputs.items():
            if not isinstance(tinput, Input):
                raise ValueError(
                    f"Input {name!r} to transform {self} is not a transforms.api.Input"
                )
        for name, toutput in self.outputs.items():
            if not isinstance(toutput, Output):
                raise ValueError(
                    f"Output {name!r} of transform {self} is not a transforms.api.Output"
                )
        for name, param in self.parameters.items():
            if not isinstance(param, Param):
                raise ValueError(
                    f"Parameter {name!r} of transform {self} is not a transforms.api.Param"
                )
            if param.json_value is None:
                self._bound_transform = False

    def _assert_check_names_are_unique(self) -> None:
        checks = Transform._get_checks_in_items(
            list(self.inputs.values()) + list(self.outputs.values())
        )
        check_names_counter = collections.Counter([check.name for check in checks])
        for check_name in check_names_counter:
            if check_names_counter[check_name] > 1:
                raise ValueError("Check names must be unique: " + check_name)

    def _check_inputs_are_present_in_parameters(self) -> None:
        self._check_expected_params_are_present("input", self.inputs.keys())

    def _check_outputs_are_present_in_parameters(self) -> None:
        self._check_expected_params_are_present("output", self.outputs.keys())

    def _check_expected_params_are_present(
        self, params_designator: str, expected_params: Iterable[str]
    ) -> None:
        """
        Args:

        - `params_designator: Literal["input", "output"]`
        - `expected_params: Iterable[str]` - names of params which should be present
        """

        io_params = self.inputs.keys() | self.outputs.keys()
        unused_fn_params = set(self._argspec.args) - io_params

        for param in expected_params:
            if self._argspec.varkw is None and param not in self._argspec.args:
                error_message = (
                    f"{params_designator.capitalize()} '{param}' "
                    f"to transform {self._clean_reference} doesn't have a "
                    "corresponding parameter in the function's parameter "
                    f"list: {self._argspec.args}"
                )

                most_similar_fn_param = _get_most_similar_to(param, unused_fn_params)

                if not most_similar_fn_param and len(unused_fn_params) == 1:
                    most_similar_fn_param = unused_fn_params.pop()

                if most_similar_fn_param:
                    error_message += (
                        f"\n\nDid you mean to write '{most_similar_fn_param}' "
                        f"instead of '{param}'?"
                    )

                raise _errors.NoCorrespondingFunctionParameter(error_message)

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """Passthrough call to the underlying compute function."""
        try:
            return self._compute_func(*args, **kwargs)
        except Exception as e:
            setattr(e, "__transform_compute_error", True)
            raise

    def __repr__(self) -> str:
        # Format here is subject to change
        return f"Transform({self._clean_reference})<{', '.join(self._output_aliases or [])}>"

    @staticmethod
    def _get_checks_in_items(items: List[Union[Input, Output]]) -> List[Check]:
        """Get `Check` objects across `items`

        Returns:
            List[Check]: list of checks across all inputs and outputs
        """

        all_checks: List[Check] = []

        for item in items:
            checks = item.checks

            if not checks:
                continue

            all_checks.extend(checks)

        return all_checks

    @property
    def profile(self) -> Optional[str]:
        """The profile applied to this transform.

        Returns:
            str: The applied profile

        Raises:
            :exc:`~exceptions.ValueError`: If profiles is a list.
        """
        if isinstance(self._profile, list):
            raise ValueError(
                f"Multiple profiles {self._profile!r} applied to transform {self}"
            )
        return self._profile

    @profile.setter
    def profile(self, profile: Union[str, List[str], None]) -> None:
        self._profile = profile

    @property
    def profiles(self) -> List[str]:
        """List[str]: The profiles applied to this transform."""
        if isinstance(self._profile, list):
            return self._profile
        if self._profile:
            return [self._profile]
        return []

    @property
    def reference(self) -> str:
        """Reference to this transform, unique in pipeline.

        .. versionadded:: 1.53.0
        """
        return self._reference

    def compute(
        self, ctx: Optional["TransformContext"] = None, **kwargs: Any
    ) -> Dict[str, Any]:
        """Compute the transform with a context and instantiated parameters.

        Args:
            ctx (TransformContext, optional): A context object passed to the transform if it requests one.
            **kwargs (TransformInput or TransformOutput): The input, output and context objects
                to be passed by name into the compute function.

        Returns:
            dict of (str, TransformOutput): The output objects after running the transform.
        """
        if self._use_context:
            assert (
                ctx is not None
            ), "TransformContext not passed to transform that requires one"
            kwargs["ctx"] = ctx

        self(**kwargs)

        # TODO(mbakovic): This method shouldn't return any value #1231
        return {
            name: arg
            for name, arg in kwargs.items()
            if name in self.parameters
            and isinstance(self.parameters[name], FoundryOutputParam)
        }

    @_utils.memoized_property
    def version(self) -> str:
        """str: A string that is used to compare two versions of a transform when considering logic staleness.

        For example, a SQL transform may take the hash of the SQL query. Ideally the SQL query would be
        transformed into a format that yields the same version for transforms of equivalent semantic meaning. I.e.
        the SQL query `select A, B from foo;` should be the same version as the SQL query
        `select A, B from (select * from foo);`.

        If no version is specified then the version of the repository will be used.

        Raises:
            :exc:`~exceptions.ValueError`: If fails to compute object hash of compute function
        """
        if not _env_utils.PROJECT_SOURCE_DIR:
            raise ValueError(
                "PROJECT_SOURCE_DIR not in environment, required when computing transform's version"
            )
        try:
            return _tllv_hasher.hash_object(
                self._compute_func, os.path.abspath(_env_utils.PROJECT_SOURCE_DIR)
            )
        except _tllv_hasher.HashException as e:
            raise ValueError(
                f"Hash exception when computing transform's version: {e}"
            ) from e


class DataframeTransform(Transform):
    """A subclass of Transform that gives direct access to DataFrames.

    Only Input/Output parameters are currently supported for this type of transforms.
    """

    def __init__(self, compute_func: Callable[..., Any], output: Output, **kwargs: Any):
        """Take a single :class:`Output` spec and any number of :class:`Input` specs.

        Args:
            compute_func (function): The compute function to wrap.
            output (Output): The single output spec.
            **kwargs: Other kwargs to pass through to ``Transform``.
        """
        outputs = {"bound_output": output}
        super().__init__(compute_func, outputs=outputs, **kwargs)

    def compute(  # pylint: disable=arguments-differ
        self,
        ctx: Optional["TransformContext"] = None,
        bound_output: Optional["TransformOutput"] = None,
        **transform_inputs: "TransformInput",
    ) -> DataFrame:
        """Run the computation by unwrapping and wrapping the input and output ``DataFrame``s.

        Passes the input's `DataFrame` objects to the compute function before writing
        the output dataframe to the `TransformOutput`.

        Returns:
            pyspark.sql.DataFrame: The dataframe returned by the compute function.
        """

        kwargs: Dict[str, Union[DataFrame, TransformContext]] = {
            name: param.dataframe()
            if isinstance(param, DataframeParamInstance)
            else param
            for name, param in transform_inputs.items()
        }

        if self._use_context:
            assert (
                ctx is not None
            ), "TransformContext not passed to transform that requires one"
            kwargs["ctx"] = ctx

        output_df: Union[DataFrame, Any] = self(**kwargs)

        if not isinstance(output_df, DataFrame):
            raise _errors.InvalidTransformReturnType(
                f"Expected {self} to return a pyspark.sql.DataFrame, instead got {output_df}"
            )

        assert bound_output, "Output not passed to transform"
        bound_output.write_dataframe(output_df)
        return output_df

    def _check_outputs_are_present_in_parameters(self):
        pass  # no-op because there are no user-specified Output()-s


class PandasTransform(Transform):
    """A subclass of Transform that gives direct access to pandas.DataFrames.

    Only Input/Output parameters are currently supported for this type of transforms.
    """

    def __init__(self, compute_func: Callable[..., Any], output: Output, **kwargs: Any):
        """Take a single :class:`Output` spec and any number of :class:`Input` specs.

        Args:
            compute_func (function): The compute function to wrap.
            output (Output): The single output spec.
            **kwargs: Other kwargs to pass through to ``Transform``.
        """
        outputs = {"bound_output": output}
        super().__init__(compute_func, outputs=outputs, **kwargs)

    def compute(  # pylint: disable=arguments-differ
        self,
        ctx: Optional["TransformContext"] = None,
        bound_output: Optional["TransformOutput"] = None,
        **transform_inputs: "TransformInput",
    ) -> Any:
        """Run the computation by unwrapping and wrapping the input and output `DataFrame`s.

        Passes the input's `pandas.DataFrame` objects to the compute function before writing
        the output Pandas dataframe to the `TransformOutput`.

        Returns:
            pandas.DataFrame: The Pandas dataframe returned by the compute function.
        """

        kwargs: Dict[str, Union["pandas.DataFrame", "TransformContext"]] = {
            name: param.pandas() if isinstance(param, TransformInput) else param
            for name, param in transform_inputs.items()
        }

        if self._use_context:
            assert (
                ctx is not None
            ), "TransformContext not passed to transform that requires one"
            kwargs["ctx"] = ctx

        output_df = self(**kwargs)

        import pandas

        if not isinstance(output_df, pandas.DataFrame):
            raise _errors.InvalidTransformReturnType(
                f"Expected {self} to return a pandas.DataFrame, instead got {output_df}"
            )

        assert bound_output, "Output not passed to transform"
        bound_output.write_pandas(output_df)
        return output_df

    def _check_outputs_are_present_in_parameters(self):
        pass  # no-op because there are no user-specified Output()-s


class TransformInput(DataframeParamInstance):
    """The input object passed into Transform objects at runtime."""

    def __init__(
        self,
        rid: str,
        branch: str,
        txrange: Tuple[Optional[str], Optional[str]],
        dfreader: Optional[_dfreader.DataFrameReader],
        fsbuilder: Optional[_fsreader.FileSystemReader],
    ) -> None:
        self._rid = rid
        self._branch = branch
        self._start_txrid, self._end_txrid = txrange
        self._dfreader = dfreader
        self._fsbuilder = fsbuilder

    @property
    def rid(self) -> str:
        """str: the resource identifier of the dataset."""
        return self._rid

    @property
    def branch(self) -> str:
        """str: the branch of the dataset."""
        return self._branch

    @property
    def start_transaction_rid(self) -> Optional[str]:
        """str: the starting transaction of the input dataset."""
        return self._start_txrid

    @property
    def end_transaction_rid(self) -> Optional[str]:
        """str: the ending transaction of the input dataset."""
        return self._end_txrid

    @property
    def path(self) -> str:
        """str: The Compass path of the dataset."""
        return self._dfreader.path

    @property
    def column_typeclasses(self) -> Dict[str, str]:
        """Dict<str, str>: The column typeclasses of the dataset."""
        return self._dfreader.column_typeclasses

    @property
    def column_descriptions(self) -> Dict[str, str]:
        """Dict<str, str>: The column descriptions of the dataset."""
        return self._dfreader.column_descriptions

    def dataframe(self) -> DataFrame:
        """Return a :class:`pyspark.sql.DataFrame` containing the full view of the dataset.

        Returns:
            pyspark.sql.DataFrame: The dataframe for the dataset.
        """
        return self._dfreader.current((self._start_txrid, None, self._end_txrid))

    def pandas(self):
        """:class:`pandas.DataFrame`: A Pandas dataframe containing the full view of the dataset."""
        return self.dataframe().toPandas()

    def filesystem(self) -> "FileSystem":
        """Construct a `FileSystem` object for reading from `FoundryFS`.

        Returns:
            FileSystem: A `FileSystem` object for reading from `Foundry`.
        """
        return FileSystem(
            self._fsbuilder.current((self._start_txrid, None, self._end_txrid)),
            read_only=True,
        )


class TableTransformInput(TransformInput):
    """The input Table object passed into Transform objects at runtime."""

    def __init__(
        self,
        rid: str,
        branch: str,
        table_dfreader: _dfreader.TableDataFrameReader,
    ) -> None:
        super().__init__(
            rid=rid,
            branch=branch,
            txrange=(None, None),
            dfreader=None,
            fsbuilder=None,
        )
        self._table_dfreader = table_dfreader

    @property
    def start_transaction_rid(self) -> Optional[str]:
        raise _errors.NotSupportedForTables(
            "Cannot access 'start_transaction_rid' for virtual table inputs"
        )

    @property
    def end_transaction_rid(self) -> Optional[str]:
        raise _errors.NotSupportedForTables(
            "Cannot access 'end_transaction_rid' for virtual table inputs"
        )

    @property
    def path(self) -> str:
        return self._table_dfreader.path

    @property
    def column_typeclasses(self) -> Dict[str, str]:
        return {}

    @property
    def column_descriptions(self) -> Dict[str, str]:
        return {}

    def dataframe(self, options=None) -> DataFrame:
        """Return a :class:`pyspark.sql.DataFrame` containing the full view of the table.

        Args:
            options (dict, option): Additional Spark read options to pass when reading the table.

        Returns:
            pyspark.sql.DataFrame: The dataframe for the table.
        """
        return self._table_dfreader.current(options=options)

    def filesystem(self) -> "FileSystem":
        raise _errors.NotSupportedForTables(
            "Cannot access 'filesystem' for virtual table inputs"
        )


class TransformOutput(TransformInput):
    """The output object passed into Transform objects at runtime."""

    # Set the type explicitly since it is a subclass of the type in `TransformInput`
    _fsbuilder: _fswriter.FileSystemWriter

    def __init__(
        self,
        rid: str,
        branch: str,
        txrid: str,
        dfreader: _dfreader.DataFrameReader,
        dfwriter: _dfwriter.DataFrameWriter,
        fsbuilder: _fswriter.FileSystemWriter,
        mode: str = "replace",
    ) -> None:
        super().__init__(
            rid=rid,
            branch=branch,
            txrange=(None, txrid),
            dfreader=dfreader,
            fsbuilder=fsbuilder,
        )
        self._txrid = txrid
        self._dfwriter = dfwriter
        self._written = False
        self._aborted = False
        self._mode = mode
        self.set_mode(mode)

    @classmethod
    def from_transform_output(
        cls, instance: "TransformOutput", delegate: "TransformOutput"
    ) -> None:
        """Sets fields in a `TransformOutput` instance to the values from the delegate `TransformOutput`."""
        cls.__init__(
            instance,
            rid=delegate._rid,
            branch=delegate._branch,
            txrid=delegate._txrid,
            dfreader=delegate._dfreader,
            dfwriter=delegate._dfwriter,
            fsbuilder=delegate._fsbuilder,
            mode=delegate._mode,
        )

    @property
    def transaction_rid(self) -> str:
        return self._txrid

    @property
    def schema_version_id(self):
        return self._dfwriter.schema_version_id

    def write_dataframe(
        self,
        df: DataFrame,
        partition_cols: Optional[List[str]] = None,
        bucket_cols: Optional[List[str]] = None,
        bucket_count: Optional[int] = None,
        sort_by: Optional[List[str]] = None,
        output_format: Optional[str] = None,
        options: Optional[Dict[str, str]] = None,
        column_descriptions: Optional[Dict[str, str]] = None,
        column_typeclasses: Optional[Dict[str, List[Dict[str, str]]]] = None,
    ) -> None:
        """Write the given :class:`~pyspark.sql.DataFrame` to the datset.

        Args:
            df (pyspark.sql.DataFrame): The PySpark dataframe to write.
            partition_cols (List[str], optional): Column partitioning to use when writing data.
            bucket_cols (List[str], optional): The columns by which to bucket the data. Must be specified if
                bucket_count is given.
            bucket_count (int, optional): The number of buckets. Must be specified if bucket_cols is given.
            sort_by (List[str], optional): The columns by which to sort the bucketed data.
            output_format (str, optional): The output file format, defaults to 'parquet'.
            options (dict, optional): Extra options to pass through to
                ``org.apache.spark.sql.DataFrameWriter#option(String, String)``.
            column_descriptions (Dict[str, str], optional): Map of column names to their string descriptions.
                This map is intersected with the columns of the DataFrame,
                and must include descriptions no longer than 800 characters.
            column_typeclasses (Dict[str, List[Dict[str, str]]], optional): Map of column names to their column
                typeclasses. Each typeclass in the List is a Dict[str, str], where only two keys are valid:
                "name" and "kind", which each map to the corresponding string the user wants up to a maximum
                of 100 characters. An example `column_typeclasses` value would be `{"my_column": [{"name":
                "my_typeclass_name", "kind": "my_typeclass_kind"}]}`.
        """
        self._check_not_written("Cannot write to output more than once.")
        self._written = True
        self._dfreader.clear_cache()
        return self._dfwriter.write(
            df,
            partition_cols=partition_cols,
            bucket_cols=bucket_cols,
            bucket_count=bucket_count,
            sort_by=sort_by,
            output_format=output_format,
            options=options,
            column_descriptions=column_descriptions,
            column_typeclasses=column_typeclasses,
        )

    def write_pandas(self, pandas_df: "pandas.DataFrame"):
        """Write the given :class:`pandas.DataFrame` to the datset.

        Args:
            pandas_df (pandas.DataFrame): The dataframe to write.
        """
        self._check_not_written("Cannot write to output more than once.")
        self._written = True
        self._dfreader.clear_cache()
        return self._dfwriter.write_pandas(pandas_df)

    def filesystem(self) -> "FileSystem":
        """Construct a `FileSystem` object for writing to `FoundryFS`.

        Returns:
            FileSystem: A `FileSystem` object for writing to `Foundry`.
        """
        return FileSystem(
            self._fsbuilder.current((None, None, self._txrid)), read_only=False
        )

    def set_mode(self, mode: str) -> None:
        """Change the write mode of the dataset.

        Args:
            mode (str): The write mode, one of 'replace', 'modify' or 'append'. In modify mode anything written
                is appended to the dataset. In replace mode, anything written replaces the dataset.
                In append mode, anything written is appended to the dataset
                and will not override existing files.

        Note:
            The write mode cannot be changed after data has been written.

        .. versionadded:: 1.61.0
        """
        self._check_not_written("Cannot set write mode after writing.")
        self._dfreader.clear_cache()
        self._dfwriter.set_mode(mode)
        self._fsbuilder.set_mode(mode)
        self._mode = mode
        log.info(
            "Output %s write mode set to %s. Transaction rid: %s",
            self._rid,
            mode,
            self._txrid,
        )

    def abort(self) -> None:
        """Aborts all work on this output. Any work done on writers from this output before or after calling this
        method will be ignored.
        """
        log.info("Aborting transaction %s on output %s", self._txrid, self._rid)
        self._aborted = True

    def _check_not_written(self, msg: str) -> None:
        if self._written:
            raise ValueError(msg)


# TODO: we should use a proper `NamedTuple` here, but I am not sure if there are any backwards-
# compat concerns with that (@ccabo 2023.10.05)
class FileStatus(collections.namedtuple("FileStatus", ["path", "size", "modified"])):
    """A :class:`collections.namedtuple` capturing details about a `FoundryFS` file."""


class FileSystem(object):
    """A filesystem object for reading and writing raw dataset files."""

    def __init__(self, foundry_fs: "FoundryFS", read_only: bool = False) -> None:
        self._foundry_fs = foundry_fs
        self._read_only = read_only

    def open(self, path: str, mode: str = "r", **kwargs: Any):
        """Open a FoundryFS file in the given mode.

        Args:
            path (str): The logical path of the file in the dataset.
            mode (str): File opening mode, defaults to read.
            **kwargs: Remaining keyword args passed to :func:`io.open`.

        Returns:
            File: a Python file-like object attached to the stream.
        """
        if mode.startswith("w") and self._read_only:
            raise ValueError(
                "Cannot open a file for writing on a read-only file system"
            )
        return self._foundry_fs.open(path, mode=mode, **kwargs)

    def ls(
        self, glob: Optional[str] = None, regex: str = ".*", show_hidden: bool = False
    ) -> Generator[FileStatus, None, None]:
        """Recurses through all directories and lists all files matching the given patterns,
        starting from the root directory of the dataset.

        Args:
            glob (str, optional): A unix file matching pattern. Also supports globstar.
            regex (str, optional): A regex pattern against which to match filenames.
            show_hidden (bool, optional): Include hidden files, those prefixed with '.' or '_'.

        Yields:
            :class:`~transforms.api.FileStatus`: The logical path, file size (bytes), modified
            timestamp (ms since January 1, 1970 UTC)
        """
        for path, size, modified in self._foundry_fs.ls(
            glob=glob, regex=regex, show_hidden=show_hidden
        ):
            yield FileStatus(path, size, modified)

    def files(
        self,
        glob: Optional[str] = None,
        regex: str = r".*",
        show_hidden: bool = False,
        packing_heuristic: Optional[str] = None,
    ) -> DataFrame:
        """Create a :class:`~pyspark.sql.DataFrame` containing the paths accessible within this dataset.

        The :class:`~pyspark.sql.DataFrame` is partitioned by file size where each partition contains file paths
        whose combined size is at most ``spark.files.maxPartitionBytes`` bytes or a single file if that file is
        itself larger than ``spark.files.maxPartitionBytes``. The size of a file is calculated as its on-disk
        file size plus the ``spark.files.openCostInBytes``.

        Args:
            glob (str, optional): A unix file matching pattern. Also supports globstar.
            regex (str, optional): A regex pattern against which to match filenames.
            show_hidden (bool, optional): Include hidden files, those prefixed with '.' or '_'.
            packing_heuristic (str, optional): Specify a heuristic to use for bin-packing files into Spark partitions.
                Possible choices are 'ffd' (First Fit Decreasing) or 'wfd' (Worst Fit Decreasing). While 'wfd' tends to
                produce a less even distribution, it is much faster, so 'wfd' is recommended for datasets containing
                a very large number of files. If a heuristic is not specified, one will be selected automatically.

        Returns:
            pyspark.sql.DataFrame: DataFrame of (path, size, modified)
        """

        # external implementations for self._foundry_fs don't have **kwargs, so we need to ensure this doesn't
        # raise an error on unexpected argument
        supports_packing_heuristic = "packing_heuristic" in [
            x.name
            for x in inspect.signature(self._foundry_fs.files).parameters.values()
        ]

        if supports_packing_heuristic:
            return self._foundry_fs.files(
                glob=glob,
                regex=regex,
                show_hidden=show_hidden,
                packing_heuristic=packing_heuristic,
            )

        if packing_heuristic is not None:
            log.warning(
                "packing_heuristic parameter was passed but the filesystem used doesn't support it. The parameter"
                " will be ignored. Filesystem implementation: %s",
                self._foundry_fs.__class__,
            )

        return self._foundry_fs.files(glob=glob, regex=regex, show_hidden=show_hidden)

    @property
    def hadoop_path(self) -> str:
        """Fetches the hadoop path of the dataset which can be for code that requires direct hadoop IO.

        Returns:
            string: hadoop path of the dataset backing this FileSystem or None
        """
        return self._foundry_fs._root_path


class TransformContext(object):
    """Context object that can optionally be injected into the compute function of a transform."""

    # pylint: disable=dangerous-default-value
    def __init__(
        self,
        foundry_connector: "FoundryConnector",
        parameters: Optional[Dict[str, Any]] = None,
        environment: Dict[str, Any] = {},
    ) -> None:
        self._foundry = foundry_connector
        self._parameters = parameters or {}
        self._environment = environment

    def _args(self):
        return self._foundry, self._parameters, self._environment

    @property
    def auth_header(self) -> str:
        """str: The auth header used to run the transform."""
        return self._foundry.auth_header

    @property
    def fallback_branches(self) -> List[str]:
        """List[str]: The fallback branches configured when running the transform."""
        return self._foundry.fallback_branches

    @property
    def spark_session(self) -> SparkSession:
        """pyspark.sql.SparkSession: The Spark session used to run the transform."""
        return self._foundry.spark_session

    @property
    def parameters(self) -> Dict[str, Any]:
        """dict of (str, any): Transform parameters."""
        return self._parameters

    @property
    def environment(self) -> List[str]:
        """List[str]: The list of solved conda packages."""
        return self._environment.get("packages", [])

    def resolve_role(self, role: str) -> List[str]:
        return self._foundry._resolve_role(role)

    def abort_job(self):
        """Aborts the job and ends execution. This will abort all output transactions."""
        raise AbortJobException()


def _format_reference(compute_function: Callable[..., Any]) -> str:
    return f"{compute_function.__module__}:{compute_function.__name__}"


class AbortJobException(Exception):
    """Raised when a user ends a job with a call to abort"""
