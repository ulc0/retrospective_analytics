# Copyright 2020 Palantir Technologies, Inc.
import inspect
from enum import Enum
from functools import wraps
from typing import TYPE_CHECKING, Any, Callable, List, Optional, Union

from .. import _utils
from ._dataset import Input, Output
from ._lightweight import ContainerTransformsConfiguration, lightweight
from ._transform import DataframeTransform, PandasTransform, Transform

if TYPE_CHECKING:
    from datetime import timedelta


def transform(**kwargs: Union[Input, Output, Any]):
    """Wrap up a compute function as a Transform object.

    The ``transform`` decorator is used to construct a :class:`Transform` object from a
    compute function. The names used for inputs, outputs or other parameters should be the function arguments
    of the wrapped compute function. At compute-time, parameters are instantiated to specific objects defined by each
    parameter type.

        >>> @transform(
        ...     first_input=Input('/path/to/first/input/dataset'),
        ...     second_input=Input('/path/to/second/input/dataset'),
        ...     first_output=Output('/path/to/first/output/dataset'),
        ...     second_output=Output('/path/to/second/output/dataset'),
        ... )
        ... def my_compute_function(first_input, second_input, first_output, second_output):
        ...     # type: (TransformInput, TransformInput, TransformOutput, TransformOutput) -> None
        ...     first_output.write_dataframe(first_input.dataframe())
        ...     second_output.write_dataframe(second_input.dataframe())

    Args:

        **kwargs (Param): kwargs comprised of named :class:`Param` or subclasses.

    Note:

        The compute function is responsible for writing data to its outputs.
    """

    def _transform(compute_func: Callable[..., Any]):
        inputs = _utils.filter_instances(kwargs, Input)
        outputs = _utils.filter_instances(kwargs, Output)
        return Transform(
            compute_func, inputs=inputs, outputs=outputs, parameters=kwargs
        )

    return _transform


def transform_df(output: Output, **inputs: Input):
    """Register the wrapped compute function as a dataframe transform.

    The ``transform_df`` decorator is used to construct a :class:`Transform` object from
    a compute function that accepts and returns :class:`pyspark.sql.DataFrame` objects. Similar
    to the :func:`transform` decorator, the input names become the compute function's parameter
    names. However, a ``transform_df`` accepts only a single :class:`Output` spec as a
    positional argument. The return value of the compute function is also a
    :class:`~pyspark.sql.DataFrame` that is automatically written out to the single output
    dataset.

        >>> @transform_df(
        ...     Output('/path/to/output/dataset'),  # An unnamed Output spec
        ...     first_input=Input('/path/to/first/input/dataset'),
        ...     second_input=Input('/path/to/second/input/dataset'),
        ... )
        ... def my_compute_function(first_input, second_input):
        ...     # type: (pyspark.sql.DataFrame, pyspark.sql.DataFrame) -> pyspark.sql.DataFrame
        ...     return first_input.union(second_input)

    Args:

        output (Output): The single :class:`Output` spec for the transform.
        **inputs (Input): kwargs comprised of named :class:`Input` specs.
    """

    if not isinstance(output, Output):
        raise ValueError(
            "@transform_df decorator only supports dataset outputs. To output any other data type, use the @transform decorator."
        )
    for name, input in inputs.items():
        if not isinstance(input, Input):
            raise ValueError(
                f"@transform_df decorator only supports dataset inputs. Input {name} is not a dataset input. To use inputs of other types use the @transform decorator."
            )

    def _transform_df(compute_func: Callable[..., Any]):
        return DataframeTransform(compute_func, output, inputs=inputs)

    return _transform_df


def transform_pandas(output: Output, **inputs: Input):
    """Register the wrapped compute function as a Pandas transform.

    Note:
        To use the Pandas library, you must add ``pandas`` as a **run** dependency in your ``meta.yml`` file .
        For more information, refer to the
        :ref:`section describing the meta.yml file <transforms-python-proj-structure-meta>`.

    The ``transform_pandas`` decorator is used to construct a :class:`Transform` object from
    a compute function that accepts and returns :class:`pandas.DataFrame` objects. This
    decorator is similar to the :func:`transform_df` decorator, however the :class:`pyspark.sql.DataFrame`
    objects are converted to :class:`pandas.DataFrame` object before the computation, and converted back
    afterwards.

        >>> @transform_pandas(
        ...     Output('/path/to/output/dataset'),  # An unnamed Output spec
        ...     first_input=Input('/path/to/first/input/dataset'),
        ...     second_input=Input('/path/to/second/input/dataset'),
        ... )
        ... def my_compute_function(first_input, second_input):
        ...     # type: (pandas.DataFrame, pandas.DataFrame) -> pandas.DataFrame
        ...     return first_input.concat(second_input)

    Note that ``transform_pandas`` should only be used on datasets that can fit into memory. If you have larger
    datasets that you wish to filter down first before converting to Pandas you should write your transformation
    using the :func:`transform_df` decorator and the :meth:`pyspark.sql.SparkSession.createDataFrame` method.

        >>> @transform_df(
        ...     Output('/path/to/output/dataset'),  # An unnamed Output spec
        ...     first_input=Input('/path/to/first/input/dataset'),
        ...     second_input=Input('/path/to/second/input/dataset'),
        ... )
        ... def my_compute_function(ctx, first_input, second_input):
        ...     # type: (pyspark.sql.DataFrame, pyspark.sql.DataFrame) -> pyspark.sql.DataFrame
        ...     pd = first_input.filter(first_input.county == 'UK').toPandas()
        ...     # Perform pandas operations on a subset of the data before converting back to a PySpark DataFrame
        ...     return ctx.spark_session.createDataFrame(pd)

    Args:

        output (Output): The single :class:`Output` spec for the transform.
        **inputs (Input): kwargs comprised of named :class:`Input` specs.
    """

    def _transform_pandas(compute_func: Callable[..., Any]):
        return PandasTransform(compute_func, output, inputs=inputs)

    return _transform_pandas


def transform_polars(
    output: Output,
    **inputs: Input,
):
    """Register the wrapped compute function as a Polars transform.

    Note:
        To use the Polars library, you must add ``polars`` as a **run** dependency in your ``meta.yml`` file .
        For more information, refer to the
        :ref:`section describing the meta.yml file <transforms-python-proj-structure-meta>`.

        The ``transform_polars`` decorator is just a thin wrapper around the ``lightweight`` decorator. Using it
        results in the creation of a Lightweight transform which lacks some features of a regular transform.

        This works similarly to the :func:`transform_pandas` decorator, however, instead of Pandas DataFrames, the
        user code is given and is expected to return Polars DataFrames.

        Note that spark profiles can't be used with Lightweight transforms, hence, neither with @transforms_polars.

        >>> @transform_polars(
        ...     Output('ri.main.foundry.dataset.out'),  # An unnamed Output spec
        ...     first_input=Input('ri.main.foundry.dataset.in1'),
        ...     second_input=Input('ri.main.foundry.dataset.in2'),
        ... )
        ... def my_compute_function(ctx, first_input, second_input):
        ...     # type: (polars.DataFrame, polars.DataFrame) -> polars.DataFrame
        ...     return first_input.join(second_input, on='id', how="inner")

      Args:
        output (Output): The single :class:`Output` spec for the transform.
        **inputs (Input): kwargs comprised of named :class:`Input` specs.
    """

    def _transform_polars(
        compute_func: Callable[..., Any],
    ) -> ContainerTransformsConfiguration:
        @wraps(compute_func)
        def _wrapper(**kwargs):
            bound_output = kwargs.pop("bound_output")
            polars_dfs = {
                name: value.polars() if hasattr(value, "polars") else value
                for name, value in kwargs.items()
            }

            result = compute_func(**polars_dfs)

            bound_output.write_table(result)

        # set the file path to the actual user function's not _wrapper's
        _wrapper._original_file_path = inspect.getfile(compute_func)
        return lightweight(transform(bound_output=output, **inputs)(_wrapper))

    return _transform_polars


EXECUTOR_MEMORY_PROFILES = [
    "EXECUTOR_MEMORY_EXTRA_EXTRA_SMALL",
    "EXECUTOR_MEMORY_EXTRA_SMALL",
    "EXECUTOR_MEMORY_SMALL",
    "EXECUTOR_MEMORY_MEDIUM",
    "EXECUTOR_MEMORY_LARGE",
    "EXECUTOR_MEMORY_EXTRA_LARGE",
]


class ComputeBackend(Enum):
    """
    Enum class for representing the different compute backends and their corresponding memory configurations.

    The available backends are:
    - SPARK
    - VELOX
    """

    SPARK = "SPARK"
    VELOX = "VELOX"


def configure(
    profile: Union[str, List[str], None] = None,
    allowed_run_duration: Optional["timedelta"] = None,
    run_as_user: bool = False,
    backend: Union[str, ComputeBackend] = ComputeBackend.SPARK,
) -> Callable[[Transform], Transform]:
    """Decorator to modify the configuration of a transform.

    The ``configure`` decorator must be used to wrap a :class:`Transform`:

        >>> @configure(profile=['high-memory'])
        ... @transform(...)
        ... def my_compute_function(...):
        ...     pass

    Args:

        profile (str or List[str], optional): The transforms profile(s) to use.
        allowed_run_duration (timedelta, optional): Allowed duration for job to complete after which infrastructure \
            will fail and maybe retry a job. Use carefully. When configuring allowed \
            duration, consider variables such as changes in data scale or shape. \
            Duration is minute precision only. IMPORTANT Do not use for incremental \
            transforms as duration can change significantly when running snapshot.
        run_as_user (boolean, optional): Determines whether a transforms runs with user permissions. When \
            enabled, a job can behave differently depending on the permissions of the \
            user running the job.
        backend (str or ComputeBackend, optional): Which compute backend to use for this transform. Defaults to Spark. \
            Velox can be selected to natively accelerate the transform.

    Raises:

        :exc:`~exceptions.TypeError`: If the object wrapped is not a :class:`Transform` object.

    Note:

       For more information about definining custom Transforms profiles, refer to the :ref:`section on defining
       Transforms profiles in the Authoring and Transforms documentation <transforms:transforms-profiles>`.
    """

    def _configure(transform_obj: Transform) -> Transform:
        if not isinstance(transform_obj, Transform):
            raise TypeError(
                "configure decorator must be used on a Transform object. Did you apply decorators "
                "in the wrong order?"
            )

        if allowed_run_duration:
            # pylint: disable=import-outside-toplevel
            from datetime import timedelta

            if not isinstance(allowed_run_duration, timedelta):
                raise TypeError(
                    "allowed_run_duration must be instance of timedelta. "
                    "Usage: allowed_run_duration=datetime.timedelta(minutes=15)"
                )

        compute_backend = _get_compute_backend()
        profiles_with_backend = _profile_with_compute_backend(compute_backend)
        _verify_fractional_memory_profiles_valid()

        transform_obj.profile = profiles_with_backend
        transform_obj._allowed_run_duration = allowed_run_duration
        transform_obj._run_as_user = run_as_user

        return transform_obj

    def _profile_with_compute_backend(
        backend: ComputeBackend,
    ) -> Union[str, List[str], None]:
        if backend == ComputeBackend.SPARK:
            return profile
        if profile is None:
            return [backend.value]
        _check_some_offheap_memory_set()
        if isinstance(profile, str):
            return [profile, backend.value]
        if isinstance(profile, List):
            return profile + [backend.value]
        raise TypeError(
            "Profile was no one of expected types: Union[str, List[str], None]"
        )

    def _get_compute_backend() -> ComputeBackend:
        if isinstance(backend, str):
            return getattr(ComputeBackend, backend)
        return backend

    def _check_some_offheap_memory_set() -> None:
        if isinstance(profile, str):
            if "OFFHEAP" in profile:
                return
        elif isinstance(profile, list):
            if any("OFFHEAP" in entry for entry in profile):
                return
        raise ValueError(
            "To use VELOX backend you must set an off-heap memory profile."
        )

    def _verify_fractional_memory_profiles_valid() -> None:
        if profile is None:
            return
        if isinstance(profile, str):
            if "OFFHEAP_FRACTION" in profile:
                raise ValueError(
                    "Fractional off-heap executor memory profiles require you specify Executor memory one of: "
                    + str(EXECUTOR_MEMORY_PROFILES)
                )
            return
        if isinstance(profile, list):
            if any("OFFHEAP_FRACTION" in entry for entry in profile):
                if not any(entry in EXECUTOR_MEMORY_PROFILES for entry in profile):
                    raise ValueError(
                        "Fractional off-heap executor memory profiles require you specify Executor memory one of: "
                        + str(EXECUTOR_MEMORY_PROFILES)
                    )
            return
        raise TypeError(
            "Profile was no one of expected types: Union[str, List[str], None]"
        )

    return _configure
