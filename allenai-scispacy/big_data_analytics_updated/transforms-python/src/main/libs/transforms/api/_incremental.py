# Copyright 2017 Palantir Technologies, Inc.
# pylint: disable=raising-format-tuple, too-many-lines
"""Classes and decorators for writing incremental transforms."""
import logging
from typing import Dict, Optional, Tuple

from pyspark.sql import DataFrame

from transforms import _errors
from transforms.api._incremental_constants import (
    CONSENSUS_ON_INCOMPATIBLE_BUILD,
    CONSENSUS_ON_INCREMENTAL,
    CONSENSUS_ON_NEW_BUILD,
    INCREMENTAL_INPUT_RESOLUTIONS,
    INCREMENTAL_INPUTS,
    INCREMENTAL_OPTIONS,
    INCREMENTAL_OUTPUTS,
    INPUT,
    NO_CONSENSUS_ON_PRIOR_BUILD,
    OUTCOME,
    OUTPUT,
    OUTPUT_HAS_BEEN_ALTERED,
    SEMANTIC_VERSION_NAME,
    SEMANTIC_VERSION_RID,
)
from transforms.foundry import _incremental

from . import _param, _transform
from ._abstract_incremental_params import (
    AbstractNonCatalogIncrementalCompatibleInput,
    AbstractNonCatalogIncrementalCompatibleOutput,
    AbstractNonCatalogIncrementalCompatibleTransformInput,
    AbstractNonCatalogIncrementalCompatibleTransformOutput,
)
from ._incremental_semantic_version import (
    IncrementalSemanticVersion,
    IncrementalSemanticVersionParam,
)
from ._parameter_spec import ParameterSpecInput

log = logging.getLogger(__name__)


def incremental(
    require_incremental=False,
    semantic_version=1,
    snapshot_inputs=None,
    allow_retention=False,
    strict_append=False,
    v2_semantics=False,
):
    """Decorator to convert inputs and outputs into their :mod:`transforms.api.incremental` counterparts.

    The ``incremental`` decorator must be used to wrap a :class:`Transform`:

        >>> @incremental()
        ... @transform(...)
        ... def my_compute_function(...):
        ...     pass

    The decorator reads build history from the output datasets to determine the state of the inputs at the time
    of the last build. This information is used to convert the :class:`~transforms.api.TransformInput`,
    :class:`~transforms.api.TransformOutput` and :class:`~transforms.api.TransformContext` objects into their
    incremental counterparts: :class:`~transforms.api.incremental.IncrementalTransformInput`,
    :class:`~transforms.api.incremental.IncrementalTransformOutput` and
    :class:`~transforms.api.incremental.IncrementalTransformContext`.

    This decorator can also be used to wrap the :func:`transform_df` and :func:`transform_pandas` decorators.
    These decorators call :meth:`~transforms.api.incremental.IncrementalTransformInput.dataframe` and
    :meth:`~transforms.api.incremental.IncrementalTransformInput.pandas` on the inputs, without any
    arguments, to extract the PySpark and Pandas DataFrame objects. This means the read mode used will always
    be ``added`` and the write mode will be determined by the ``incremental`` decorator. For reading or writing
    any of the non-default modes, you must use the :func:`transform` decorator.

    Note:

        If your PySpark or Pandas transform is one where the added output rows are a function only of the added
        input rows as per the :ref:`append <transforms-python-incremental-examples-append>` example in the
        documentation, the default modes will produce a correct incremental transform.

    Note:

        If your transform takes a dataset as input which has ``SNAPSHOT`` transactions but that does not alter
        the ability to run the transform incrementally (a common use-case is reference tables) then review the
        ``snapshot_inputs`` argument which can help prevent the need to run a transform as a full ``SNAPSHOT``.

    Warning:

        If your transform performs complex logic (involving joins, aggregations, distinct, etc...), then it's
        recommended that you fully read the :ref:`incremental documentation <transforms-python-incremental>`
        before using this decorator.

    Args:

        require_incremental (bool, optional): If true, the transform will refuse to run non-incrementally unless the\
            transform has never been run before. This is determined based on all\
            output datasets having no committed transactions.
        semantic_version (int, optional): Defaults to 1. This number represents the semantic nature of a transform. \
            It should be changed whenever the logic of a transform changes in a way \
            that would invalidate the existing output. Changing this number causes a \
            subsequent run of the transform to be run non-incrementally.
        snapshot_inputs (list of str, optional): The inputs for which a ``SNAPSHOT`` transaction does not invalidate \
            the current output of a transform. For example an update to a lookup table \
            does not mean previously computed outputs are incorrect. A transform is \
            run incrementally when all inputs except for these have only added or no \
            new data. When reading snapshot inputs, the \
            `~transforms.api.IncrementalTransformInput` will only expose the current \
            view of the input dataset.
        allow_retention (bool, optional): If True, deletes made by foundry-retention will not break incrementality.
        strict_append (bool, optional): If True and the transform runs incrementally, \
            the underlying Foundry transaction type will be an APPEND. 
            If True and the transform is not running incrementally, to force an incremental APPEND transcation \
            it requires require_incremental to be True.
            Note that the write operation may not overwrite any files, even auxiliary ones such as Parquet summary metadata or \
            Hadoop SUCCESS files. Incremental writes for all Foundry formats should support this mode.
        v2_semantics (bool): Defaults to False. If True, will use v2 incremental semantics. There should be no \
            difference in behavior between v2 and v1 incremental semantics, and we recommend all users set this \
            to True. Non-Catalog incremental inputs and outputs are only supported if using v2 semantics.

    Raises:

        :exc:`~exceptions.TransformTypeError`: If the object wrapped is not a :class:`Transform` object.
        :exc:`~exceptions.TransformKeyError`: If snapshot input does not exist on :class:`Transform` object.

    .. versionadded:: 1.7.0

    .. versionchanged:: 1.35.0
        Added ``snapshot_inputs``

    .. versionchanged:: 1.312.0
        Added ``strict_append``
    """

    def _incremental(transform_obj):
        if not isinstance(transform_obj, _transform.Transform):
            raise _errors.TransformTypeError(
                "incremental decorator must be used on a Transform object. "
                "Did you apply decorators in the wrong order?"
            )

        if not isinstance(semantic_version, int):
            raise _errors.TransformTypeError(
                "Incremental semantic_version must be an integer."
            )
        if not isinstance(allow_retention, bool):
            raise _errors.TransformTypeError(
                "Incremental allow_retention must be a bool."
            )
        if not isinstance(strict_append, bool):
            raise _errors.TransformTypeError(
                "Incremental strict_append must be a bool."
            )

        sinputs = snapshot_inputs or []
        if not isinstance(sinputs, list):
            raise _errors.TransformTypeError("snapshot_inputs must be a list.")

        for sinput in sinputs:
            if not isinstance(sinput, str):
                raise _errors.TransformTypeError(
                    "Snapshot input must be a string, got %s.", type(sinput)
                )
            if sinput not in transform_obj.parameters:
                raise _errors.TransformKeyError(
                    "Snapshot input %s does not exist on given transform.", sinput
                )
            sinput_param = transform_obj.parameters[sinput]
            if not isinstance(sinput_param, _param.FoundryInputParam):
                raise _errors.TransformTypeError(
                    "Snapshot input must be an input, got %s.", type(sinput_param)
                )

        valid_types = (
            (
                _param.FoundryInputParam,
                _transform.Output,
                AbstractNonCatalogIncrementalCompatibleOutput,
                AbstractNonCatalogIncrementalCompatibleInput,
                ParameterSpecInput,
            )
            if v2_semantics
            else (_param.FoundryInputParam, _transform.Output, ParameterSpecInput)
        )

        # TODO(mbakovic): Support InputSet/OutputSet in incremental #1357
        _check_param_types(parameters=transform_obj.parameters, valid_types=valid_types)

        if v2_semantics:
            incremental_semantic_version = IncrementalSemanticVersion(semantic_version)
            transform_obj.parameters[
                SEMANTIC_VERSION_NAME
            ] = incremental_semantic_version

            # buildgraph will create one incremental input/output for each alias, and then shrinkwrap will
            # collapse them when aliases are replaced with rids
            transform_obj.incremental_resolution_request = _create_incremental_request(
                allow_retention, transform_obj.parameters, sinputs
            )

        transform_obj.compute = _get_incremental_compute(
            v2_semantics,
            transform_obj.compute,
            require_incremental,
            semantic_version,
            sinputs,
            allow_retention,
            strict_append,
        )
        return transform_obj

    _incremental._is_incremental = True

    return _incremental


def _create_incremental_request(allow_retention, parameters, sinputs):
    incremental_inputs = [
        _get_incremental_input_request(name, tio, allow_retention)
        for name, tio in parameters.items()
        if (
            isinstance(
                tio, (_transform.Input, AbstractNonCatalogIncrementalCompatibleInput)
            )
            and name not in sinputs
        )
    ]

    incremental_outputs = [
        _get_incremental_output_request(name, tio)
        for name, tio in parameters.items()
        if isinstance(
            tio, (_transform.Output, AbstractNonCatalogIncrementalCompatibleOutput)
        )
    ]

    return {
        INCREMENTAL_INPUTS: incremental_inputs,
        INCREMENTAL_OUTPUTS: incremental_outputs,
    }


def _get_incremental_input_request(name, tinput, allow_retention):
    incremental_input_options = None
    if isinstance(tinput, _transform.Input):
        # TODO(tpowell): Need to handle Table inputs
        # catalog input
        incremental_input_options = {
            "ignoreRetentionDeletes": allow_retention,
            "useSchemaModificationType": False,
        }
    elif isinstance(tinput, AbstractNonCatalogIncrementalCompatibleInput):
        # abstract incremental input
        incremental_input_options = tinput.get_incremental_options(allow_retention)
    else:
        raise _errors.TransformTypeError(
            f"Input {name} is of type {type(tinput)}, which is incomaptible with incremental"
        )
    return {INPUT: tinput, INCREMENTAL_OPTIONS: incremental_input_options}


def _get_incremental_output_request(name, toutput):
    incremental_output_options = None
    if isinstance(toutput, _transform.Output):
        # catalog output
        incremental_output_options = {}
    elif isinstance(toutput, AbstractNonCatalogIncrementalCompatibleOutput):
        # abstract incremental output
        incremental_output_options = toutput.get_incremental_options()
    else:
        raise _errors.TransformTypeError(
            f"Input {name} is of type {type(toutput)}, which is incomaptible with incremental"
        )
    return {OUTPUT: toutput, INCREMENTAL_OPTIONS: incremental_output_options}


def _check_param_types(parameters, valid_types):
    for name, param in parameters.items():
        if not isinstance(param, valid_types) and not _is_export_control_instance(
            param
        ):
            types_accepted = ", ".join(map(lambda type: type.__name__, valid_types))
            raise _errors.TransformValueError(
                "Only certain parameter types are currently supported for incremental transforms",
                f"{name} of type {type(param)} was invalid",
                f"accepted types are {types_accepted}",
            )


def _get_incremental_compute(
    v2_semantics,
    current_compute,
    require_incremental,
    semantic_version,
    sinputs,
    allow_retention,
    strict_append,
):
    if v2_semantics:
        return _IncrementalComputeV2(
            current_compute,
            require_incremental,
            sinputs,
            strict_append,
            allow_retention,
        )
    return _IncrementalCompute(
        current_compute,
        require_incremental,
        semantic_version,
        sinputs,
        allow_retention,
        strict_append,
    )


def _is_export_control_instance(instance):
    """Return True if the argument is likely an instance of ExportControl

    ExportControl is defined in:
    https://github.palantir.build/foundry/transforms-addons/blob/d10daa599423a2262edccf7a7daeb773ad863851/transforms-external-systems/python/transforms/external/systems/_export.py#L30
    """
    return (
        hasattr(instance, "_type")
        and instance._type == "magritte-export-output"
        and type(instance).__name__ == "ExportControl"
    )


class _IncrementalCompute(object):
    """A callable that wraps a :meth:`~transforms.api.Transform.compute` method adding incremental functionality."""

    def __init__(
        self,
        compute,
        require_incremental,
        semantic_version,
        snapshot_inputs,
        allow_retention,
        strict_append,
    ):
        self._compute_func = compute
        self._require_incremental = require_incremental
        self._semantic_version = semantic_version
        self._snapshot_inputs = snapshot_inputs
        self._allow_retention = allow_retention
        self._strict_append = strict_append

    def __call__(self, ctx=None, **transform_ios):  # pylint: disable=arguments-differ
        """Run the computation by dynamically constructing IncrementalX objects from the general X objects.

        TransformInput -> IncrementalTransformInput
        TransformOutput -> IncrementalTransformOutput
        TransformContext -> IncrementalTransformContext
        """
        tinputs = {
            name: tio
            for name, tio in transform_ios.items()
            if isinstance(tio, _transform.TransformInput)
            and not isinstance(tio, _transform.TransformOutput)
        }
        toutputs = {
            name: tio
            for name, tio in transform_ios.items()
            if isinstance(tio, _transform.TransformOutput)
        }

        if not toutputs:
            raise _errors.TransformValueError(
                "An incremental transform requires at least one output"
            )

        parameters = {
            name: param
            for name, param in transform_ios.items()
            if not isinstance(param, _transform.TransformInput)
            and not isinstance(param, _transform.TransformOutput)
        }

        foundry = list(toutputs.values())[
            0
        ]._dfreader._foundry  # Kinda horrible, but we grab a foundry instance

        # It is technically possible to pass the same input from two different branches which would result in a same rid
        # as a key for input_txns below. This would lead to unpredictable behaviours, so we prefer to fail fast now.
        encountered_inputs = set([])
        for tinput in tinputs.values():
            if isinstance(tinput, _transform.TableTransformInput):
                raise _errors.NotSupportedForTables(
                    "'v2_semantics' must be used for incremental transforms with virtual tables as inputs"
                )

            input_rid = tinput.rid
            if input_rid in encountered_inputs:
                raise _errors.TransformValueError(
                    "An input cannot appear more than once in an incremental transform, but %s appeared more than once",
                    input_rid,
                )
            encountered_inputs.add(input_rid)

        input_txns = {
            tinput.rid: (tinput._start_txrid, tinput._end_txrid)
            for tinput in tinputs.values()
        }
        output_txns = {
            toutput.rid: toutput.transaction_rid for toutput in toutputs.values()
        }

        snapshot_inputs = [
            tinput.rid
            for name, tinput in tinputs.items()
            if name in self._snapshot_inputs
        ]

        input_tx_specs, output_tx_specs, is_incremental = _incremental.tx_specs(
            foundry,
            self._semantic_version,
            snapshot_inputs,
            input_txns,
            output_txns,
            self._allow_retention,
            self._require_incremental,
        )
        incr_inputs = {
            name: IncrementalTransformInput(
                tinput, prev_txrid=input_tx_specs[tinput.rid].prev
            )
            for name, tinput in tinputs.items()
        }

        # The mode for an incremental write is either 'append' or 'modify', according to the `strict_append` parameter
        mode = "replace"
        if is_incremental:
            mode = "append" if self._strict_append else "modify"

        incr_outputs = {
            name: IncrementalTransformOutput(
                toutput, prev_txrid=output_tx_specs[toutput.rid].prev, mode=mode
            )
            for name, toutput in toutputs.items()
        }

        # Set the semantic version on the output transactions
        semantic_version_service = foundry._service_provider.semantic_version()
        for toutput in toutputs.values():
            log.info(
                "Setting semantic version %s for dataset %s transaction %s",
                self._semantic_version,
                toutput.rid,
                toutput.transaction_rid,
            )
            semantic_version_service.set_semantic_version(
                foundry.auth_header,
                toutput.rid,
                toutput.transaction_rid,
                self._semantic_version,
            )

        for name, incr_input in incr_inputs.items():
            log.info(
                "Created incremental input %s with transaction range (start, prev, end) = %s",
                name,
                incr_input._txspec,
            )
        for name, incr_output in incr_outputs.items():
            log.info(
                "Created incremental output %s with transaction range (start, prev, end) = %s",
                name,
                incr_output._txspec,
            )
        for name, param in parameters.items():
            log.info("Passing parameter %s: %s", name, param)

        # Construct the incremental objects to pass into the compute function
        kwargs = dict(incr_inputs, **incr_outputs, **parameters)
        if ctx:
            kwargs["ctx"] = IncrementalTransformContext._from_transform_context(
                is_incremental, self._snapshot_inputs, ctx
            )

        return self._compute_func(**kwargs)


class _IncrementalComputeV2(object):
    """A callable that wraps a :meth:`~transforms.api.Transform.compute` method adding incremental functionality."""

    def __init__(
        self,
        compute,
        require_incremental,
        snapshot_inputs,
        strict_append,
        allow_retention,
    ):
        self._compute_func = compute
        self._require_incremental = require_incremental
        self._snapshot_inputs = snapshot_inputs
        self._strict_append = strict_append
        self._allow_retention = allow_retention

    def __call__(
        self,
        incremental_resolution_result,
        catalog_prior_output_txns,
        ctx=None,
        **transform_ios,
    ):
        """Run the computation by dynamically constructing IncrementalX objects from the general X objects.

        TransformInput -> IncrementalTransformInput
        TableTransformInput -> IncrementalTableTransformInput
        TransformOutput -> IncrementalTransformOutput
        AbstractNonCatalogIncrementalCompatibleTransformInput -> get_incremental() or get_non_incremental()
        AbstractNonCatalogIncrementalCompatibleTransformOutput -> get_incremental() or get_non_incremental()
        TransformContext -> IncrementalTransformContext
        """
        wrapped_ios = _wrap_transforms_params(
            transform_ios.items(), catalog_prior_output_txns
        )

        tinputs = {
            name: tio
            for name, tio in wrapped_ios.items()
            if isinstance(tio, AbstractNonCatalogIncrementalCompatibleTransformInput)
        }
        toutputs = {
            name: tio
            for name, tio in wrapped_ios.items()
            if isinstance(tio, AbstractNonCatalogIncrementalCompatibleTransformOutput)
        }

        if not toutputs:
            raise _errors.TransformValueError(
                "An incremental transform requires at least one output"
            )

        parameters = {
            name: param
            for name, param in wrapped_ios.items()
            if not isinstance(
                param, AbstractNonCatalogIncrementalCompatibleTransformInput
            )
            and not isinstance(
                param, AbstractNonCatalogIncrementalCompatibleTransformOutput
            )
        }

        # build2 will fail if there are duplicate input or outputs rids, but it's possible a user has a duplicate
        # if the snapshot inputs are included. We check and fail fast if so
        _throw_if_duplicate_rids(tinputs, toutputs)

        (
            semantic_version_changed,
            sem_ver_change_reason,
        ) = _did_semantic_version_change(incremental_resolution_result, tinputs)
        if semantic_version_changed:
            is_incremental = False
            non_incremental_reason = sem_ver_change_reason
        else:
            (is_incremental, non_incremental_reason) = _check_incremental(
                incremental_resolution_result, tinputs, self._snapshot_inputs
            )

            # check that things are OK re require incremental
            # we don't check this if the semantic version changed, as that indicates a snapshot was requested
            if (
                self._require_incremental
                and not is_incremental
                and not incremental_resolution_result.get(OUTCOME)
                == CONSENSUS_ON_NEW_BUILD
            ):
                raise _errors.RequiredIncrementalTransform(
                    "Require incremental set to true, but cannot run incrementally. Reason: %s",
                    non_incremental_reason,
                )

        if not is_incremental:
            log.info(
                "Transform not running incrementally. Reason: %s",
                non_incremental_reason,
            )

        received_incremental_input_resolutions = incremental_resolution_result.get(
            INCREMENTAL_INPUT_RESOLUTIONS, {}
        )

        incr_inputs = {
            name: _to_incremental_input(
                tinput=tinput,
                is_incremental=is_incremental,
                received_incremental_input_resolutions=received_incremental_input_resolutions,
                is_snapshot_input=name in self._snapshot_inputs,
            )
            for name, tinput in tinputs.items()
            # do not pass semantic version param to compute method
            if not isinstance(tinput, IncrementalSemanticVersionParam)
        }

        incr_outputs = {
            name: _to_incremental_output(
                toutput=toutput,
                is_incremental=is_incremental,
                strict_append=self._strict_append,
            )
            for name, toutput in toutputs.items()
        }

        # Construct the incremental objects to pass into the compute function
        kwargs = dict(incr_inputs, **incr_outputs, **parameters)
        if ctx:
            kwargs["ctx"] = IncrementalTransformContext._from_transform_context(
                is_incremental, self._snapshot_inputs, ctx
            )

        return self._compute_func(**kwargs)


# returns a pair: a boolean to indicate if the semantic version changed, and the change reason
def _did_semantic_version_change(
    incremental_resolution_result, tinputs
) -> Tuple[bool, str]:
    semantic_version_param = next(
        filter(lambda tinput: tinput.rid == SEMANTIC_VERSION_RID, tinputs.values()),
        None,
    )
    if not semantic_version_param:
        # if the semantic version param doesn't exist, then we assume that it is unchanged
        return False, ""

    incremental_resolution_outcome = incremental_resolution_result.get(OUTCOME, None)
    if not incremental_resolution_outcome:
        raise _errors.TransformValueError(
            "Did not get an incremental resolution result"
        )
    if incremental_resolution_outcome != CONSENSUS_ON_INCREMENTAL:
        return (False, "")
    received_incremental_input_resolutions = incremental_resolution_result.get(
        INCREMENTAL_INPUT_RESOLUTIONS
    )
    semantic_version_incremental_resolution = (
        received_incremental_input_resolutions.get(SEMANTIC_VERSION_RID, None)
    )
    if not semantic_version_incremental_resolution:
        raise _errors.TransformValueError(
            "Did not get an incremental semantic version result"
        )

    (
        semantic_version_did_not_change,
        reason,
    ) = semantic_version_param.was_change_incremental(
        semantic_version_incremental_resolution
    )
    return not semantic_version_did_not_change, reason


# returns a pair: can this be considered incremental, the reason it cannot
def _check_incremental(
    incremental_resolution_result, tinputs, sinputs
) -> Tuple[bool, str]:
    incremental_resolution_outcome = incremental_resolution_result.get(OUTCOME, None)
    if not incremental_resolution_outcome:
        raise _errors.TransformValueError(
            "Did not get an incremental resolution result"
        )

    if incremental_resolution_outcome != CONSENSUS_ON_INCREMENTAL:
        return (False, _get_non_incremental_reason(incremental_resolution_outcome))

    incremental_compatible_inputs = {
        name: tio for name, tio in tinputs.items() if name not in sinputs
    }

    # sort so that catalog resolutions happen last. they are the most expensive and we want to avoid them
    # if another input is going to prevent incrementallity
    incremental_compatible_inputs = dict(
        sorted(incremental_compatible_inputs.items(), key=_sort_key)
    )

    received_incremental_input_resolutions = incremental_resolution_result.get(
        INCREMENTAL_INPUT_RESOLUTIONS
    )

    # the incremental compatible inputs should exactly match the incremental inputs we requested in the resolution
    incremental_compatible_input_rids = {
        tio.rid for tio in incremental_compatible_inputs.values()
    }
    received_incremental_result_rids = set(
        received_incremental_input_resolutions.keys()
    )
    if incremental_compatible_input_rids != received_incremental_result_rids:
        raise _errors.TransformValueError(
            "did not get back all incremental input resolutions requested"
        )

    # check that each one changed incrementally
    for name, incremental_input in incremental_compatible_inputs.items():
        (was_incremental, reason) = incremental_input.was_change_incremental(
            received_incremental_input_resolutions.get(incremental_input.rid)
        )
        if not was_incremental:
            return (
                False,
                _get_non_incremental_reason(
                    incremental_resolution_outcome, name, reason
                ),
            )

    # it's incremental!
    return (True, None)


# sort so that we check semantic version first and catalog inputs last
def _sort_key(tinput):
    if isinstance(tinput[1], CatalogIncrementalCompatibleTransformInput):
        return 2
    if isinstance(tinput[1], IncrementalSemanticVersionParam):
        return 0
    return 1


def _get_non_incremental_reason(
    resolution_outcome,
    changed_non_incrementally=None,
    non_incremental_input_reason=None,
):
    if resolution_outcome == CONSENSUS_ON_NEW_BUILD:
        return (
            "there was no consensus on what the prior build was as this is a new build"
        )
    if resolution_outcome == NO_CONSENSUS_ON_PRIOR_BUILD:
        return "there was no consensus on what the prior build was"
    if resolution_outcome == CONSENSUS_ON_INCOMPATIBLE_BUILD:
        return (
            "a prior build was found, but it is incompatible with running incrementally"
        )
    if resolution_outcome == OUTPUT_HAS_BEEN_ALTERED:
        return "an output to this build has been altered since the last time the build was run"
    if resolution_outcome == CONSENSUS_ON_INCREMENTAL:
        return f"""input {changed_non_incrementally} changed in a way that is not incremental compatible.
        {non_incremental_input_reason}"""
    return "cannot run incrementally for unknown reason"


def _wrap_transforms_params(params, catalog_prior_output_txns):
    def _wrap_if_transform_param(param):
        # check if output first, because all outputs are also inputs
        if isinstance(param, _transform.TransformOutput):
            return CatalogIncrementalCompatibleTransformOutput(
                param, catalog_prior_output_txns.get(param.rid, None)
            )
        # check if table first, as TableTransformInput is also a TransformInput
        if isinstance(param, _transform.TableTransformInput):
            return TableIncrementalCompatibleTransformInput(param)
        if isinstance(param, _transform.TransformInput):
            return CatalogIncrementalCompatibleTransformInput(param)
        return param

    return {name: _wrap_if_transform_param(param) for name, param in params}


def _throw_if_duplicate_rids(tinputs, toutputs):
    encountered_inputs = set([])
    for tio in set(tinputs.values()).union(set(toutputs.values())):
        rid = tio.rid
        if rid in encountered_inputs:
            raise _errors.TransformValueError(
                "An input or output cannot appear more than once in an incremental transform, but %s did",
                rid,
            )
        encountered_inputs.add(rid)


def _to_incremental_input(
    tinput, is_incremental, received_incremental_input_resolutions, is_snapshot_input
):
    if is_incremental and not is_snapshot_input:
        return tinput.get_incremental(
            received_incremental_input_resolutions.get(tinput.rid)
        )
    return tinput.get_non_incremental()


def _to_incremental_output(toutput, is_incremental, strict_append):
    if is_incremental:
        return toutput.get_incremental(strict_append)
    return toutput.get_non_incremental()


class IncrementalTransformInput(_transform.TransformInput):
    """:class:`~transforms.api.TransformInput` with added functionality for incremental computation.

    .. versionadded:: 1.7.0
    """

    def __init__(self, tinput, prev_txrid=None):
        self._txspec = (tinput._start_txrid, prev_txrid, tinput._end_txrid)
        super().__init__(
            tinput.rid,
            tinput.branch,
            (tinput._start_txrid, tinput._end_txrid),
            tinput._dfreader,
            tinput._fsbuilder,
        )

    def dataframe(
        self, mode: str = "added"
    ) -> DataFrame:  # pylint: disable=arguments-differ
        """Return a :class:`pyspark.sql.DataFrame` for the given read mode.

        Only `current`, `previous` and `added` modes are supported.

        Args:
            mode (str, optional): The read mode, one of `current`, `previous`,
                `added`, `modified`, `removed`. Defaults to `added`.

        Returns:
            :class:`~pyspark.sql.DataFrame`: The dataframe for the dataset.
        """
        if mode in ["current", "previous", "added", "modified", "removed"]:
            return getattr(self._dfreader, mode)(self._txspec)
        raise ValueError(f"Unknown read mode {mode}")

    def filesystem(
        self, mode: str = "added"
    ) -> _transform.FileSystem:  # pylint: disable=arguments-differ
        """Construct a `FileSystem` object for reading from `FoundryFS` for the given read mode.

        Only `current`, `previous` and `added` modes are supported.

        Args:
            mode (str, optional): The read mode, one of `current`, `previous`,
                `added`, `modified`, `removed`. Defaults to `added`.

        Returns:
            :class:`~transforms.api.FileSystem`: A filesystem object for the given view.
        """
        if mode in ["current", "previous", "added", "modified", "removed"]:
            return _transform.FileSystem(
                getattr(self._fsbuilder, mode)(self._txspec), read_only=True
            )
        raise ValueError(f"Unknown filesystem mode {mode}")


# We should not inherit also from TableTransformInput, although natural, due to the issues/ambiguities caused by the
# resultant diamond inheritance. Thus, we must explicitly delegate to the methods overridden by TableTransformInput.
class IncrementalTableTransformInput(IncrementalTransformInput):
    """:class:`~transforms.api.TableTransformInput` with added functionality for incremental computation."""

    def __init__(self, table_tinput: _transform.TableTransformInput, from_version):
        table_tinput._table_dfreader._from_version = from_version
        super().__init__(table_tinput)
        self._table_tinput = table_tinput

    @property
    def start_transaction_rid(self) -> Optional[str]:
        return self._table_tinput.start_transaction_rid

    @property
    def end_transaction_rid(self) -> Optional[str]:
        return self._table_tinput.end_transaction_rid

    @property
    def path(self) -> str:
        return self._table_tinput.path

    @property
    def column_typeclasses(self) -> Dict[str, str]:
        return self._table_tinput.column_typeclasses

    @property
    def column_descriptions(self) -> Dict[str, str]:
        return self._table_tinput.column_descriptions

    def dataframe(
        self, mode: str = "added", options=None
    ) -> DataFrame:  # pylint: disable=arguments-differ
        """Return a :class:`pyspark.sql.DataFrame` for the given read mode.

        Args:
            mode (str, optional): The read mode, one of `current`, `previous`,
                `added`, `modified`, `removed`. Defaults to `added`.
            options (dict, option): Additional Spark read options to pass when reading the table.
        Returns:
            :class:`~pyspark.sql.DataFrame`: The dataframe for the table.
        """
        if mode in ["current", "previous", "added", "modified", "removed"]:
            return getattr(self._table_tinput._table_dfreader, mode)(options)
        raise ValueError(f"Unknown read mode {mode}")

    def filesystem(
        self, mode: str = "added"
    ) -> _transform.FileSystem:  # pylint: disable=arguments-differ
        return self._table_tinput.filesystem()


class IncrementalTransformOutput(_transform.TransformOutput):
    """:class:`~transforms.api.TransformOutput` with added functionality for incremental computation.

    .. versionadded:: 1.7.0
    """

    def __init__(self, toutput, prev_txrid=None, mode="replace"):
        self._txspec = (None, prev_txrid, toutput.transaction_rid)
        self._toutput = toutput
        super().__init__(
            toutput.rid,
            toutput.branch,
            toutput.transaction_rid,
            toutput._dfreader,
            toutput._dfwriter,
            toutput._fsbuilder,
            mode,
        )

    def dataframe(
        self, mode="current", schema=None
    ) -> DataFrame:  # pylint: disable=arguments-differ
        """Return a :class:`pyspark.sql.DataFrame` for the given read mode.

        Args:
            mode (str, optional): The read mode, one of `added`, `current` or `previous`.
                Defaults to `current`.
            schema (pyspark.types.StructType, optional): A PySpark schema to use when constructing
                an empty dataframe. Required when using read mode 'previous' if there is no previous transaction.

        Returns:
            :class:`~pyspark.sql.DataFrame`: The dataframe for the dataset.

        Raises:
            :exc:`~exceptions.ValueError`: If no schema is passed when using mode 'previous' and there is no previous transaction.
        """
        if mode == "added":
            return self._dfreader.added(self._txspec)
        if mode == "current":
            return self._dfreader.current(self._txspec)
        if mode == "previous":
            _, prev, _ = self._txspec
            if not schema and not prev:
                raise ValueError(
                    "A schema must be passed when using mode 'previous' if there is no previous transaction (this occurs when the output dataset is new as well as when the transform is run non-incrementally due to a semantic version change, snapshot transaction on an input, etc.)"
                )
            return self._dfreader.previous(self._txspec, schema=schema)
        raise ValueError(f"Unknown read mode {mode}")

    def pandas(
        self, mode: str = "current", schema=None
    ):  # pylint: disable=arguments-differ
        """:class:`pandas.DataFrame`: A Pandas dataframe for the given read mode."""
        return self.dataframe(mode=mode, schema=schema).toPandas()

    def filesystem(
        self, mode="current"
    ) -> _transform.FileSystem:  # pylint: disable=arguments-differ
        """Construct a `FileSystem` object for writing to `FoundryFS`.

        Args:
            mode (str, optional): The read mode, one of `added`, `current` or `previous`.
                Defaults to `current`. Only the `current` filesystem is writable.
        """
        if mode in ["current", "previous", "added"]:
            read_only = mode != "current"
            return _transform.FileSystem(
                getattr(self._fsbuilder, mode)(self._txspec), read_only=read_only
            )
        raise ValueError(f"Unknown filesystem mode {mode}")

    # TODO(mbakovic): Remove this after #2391 is fixed
    def abort(self):
        log.info("Aborting transaction %s on output", self._txrid)
        self._aborted = True
        self._toutput._aborted = True


class IncrementalTransformContext(_transform.TransformContext):
    """:class:`~transforms.api.TransformContext` with added functionality for incremental computation.

    .. versionadded:: 1.7.0
    """

    def __init__(self, is_incremental, snapshot_inputs, *args):
        super().__init__(*args)
        self._is_incremental = is_incremental
        self._snapshot_inputs = snapshot_inputs

    @staticmethod
    def _from_transform_context(is_incremental, snapshot_inputs, ctx):
        return IncrementalTransformContext(
            is_incremental, snapshot_inputs, *ctx._args()
        )

    @property
    def is_incremental(self) -> bool:
        """bool: If the transform is running incrementally."""
        return self._is_incremental


class CatalogIncrementalCompatibleTransformInput(
    AbstractNonCatalogIncrementalCompatibleTransformInput
):
    """
    We subclass the abstract non-catalog version to make the flow more sensical up-top
    """

    def __init__(self, tinput: _transform.TransformInput):
        self._tinput = tinput
        self._prior_end_txn_rid = None

    @property
    def rid(self):
        return self._tinput.rid

    def was_change_incremental(self, incremental_input_resolution):
        foundry = (
            self._tinput._dfreader._foundry
        )  # kinda gross, but grab a foundry instance
        resolution = _incremental.catalog_input_resolution(
            foundry, self.rid, incremental_input_resolution
        )
        self._prior_end_txn_rid = resolution["priorEndTxnRid"]
        return (resolution["isIncremental"], resolution["notIncrementalReason"])

    def get_incremental(self, _incremental_input_resolution):
        return self._get_internal()

    def get_non_incremental(self):
        # should ignore this if it was set as we are not running incrementally
        self._prior_end_txn_rid = None
        return self._get_internal()

    def _get_internal(self):
        return IncrementalTransformInput(
            self._tinput, prev_txrid=self._prior_end_txn_rid
        )


class CatalogIncrementalCompatibleTransformOutput(
    AbstractNonCatalogIncrementalCompatibleTransformOutput
):
    """
    We subclass the abstract non-catalog version to make the flow more sensical up-top
    """

    def __init__(self, toutput: _transform.TransformOutput, prior_txn_rid):
        self._toutput = toutput
        self._prior_txn_rid = prior_txn_rid

    @property
    def rid(self):
        return self._toutput.rid

    def get_incremental(self, strict_append):
        mode = "append" if strict_append else "modify"
        return self._get_internal(mode)

    def get_non_incremental(self):
        mode = "replace"
        self._prior_txn_rid = None
        return self._get_internal(mode)

    def _get_internal(self, mode):
        return IncrementalTransformOutput(
            self._toutput, prev_txrid=self._prior_txn_rid, mode=mode
        )


class TableIncrementalCompatibleTransformInput(
    AbstractNonCatalogIncrementalCompatibleTransformInput
):
    """
    We subclass the abstract non-catalog version to make the flow more sensical up-top
    """

    def __init__(self, tinput: _transform.TableTransformInput):
        self._tinput = tinput

    @property
    def rid(self):
        return self._tinput.rid

    def was_change_incremental(self, incremental_input_resolution):
        resolution_type = incremental_input_resolution["type"]
        if resolution_type == "newInput":
            # a new input is an "incremental" change
            return True, ""
        if resolution_type == "incrementalInput":
            details = incremental_input_resolution["incrementalInput"]["details"]
            return "fromVersion" in details, details.get("reason", "")
        raise ValueError("Incremental input resolution is malformed for %s", self.rid)

    def get_incremental(self, incremental_input_resolution):
        resolution_type = incremental_input_resolution["type"]
        if resolution_type == "newInput":
            # an incremental new input is just the whole thing
            return self._get_internal(None)
        if resolution_type == "incrementalInput":
            from_version = (
                incremental_input_resolution.get("incrementalInput")
                .get("details")
                .get("fromVersion")
            )
            if from_version is None:
                raise ValueError(
                    "Incremental input resolution does not included from version %s",
                    self.rid,
                )
            return self._get_internal(from_version)
        raise ValueError("Incremental input resolution is malformed for %s", self.rid)

    def get_non_incremental(self):
        # should ignore this if it was set as we are not running incrementally
        return self._get_internal(None)

    def _get_internal(self, from_version):
        return IncrementalTableTransformInput(self._tinput, from_version)
