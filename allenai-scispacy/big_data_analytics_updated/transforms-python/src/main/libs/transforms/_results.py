# Copyright 2024 Palantir Technologies, Inc.
# pylint: disable=raising-format-tuple

import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

import transforms._errors as errors
from transforms.api._param import Param

log = logging.getLogger(__name__)


class JobRunResult(ABC):
    """
    An abstract class which should be subclassed by any valid successful result of a job.
    """

    @abstractmethod
    def to_json(
        self,
        parameters: Optional[Dict[str, Param]],
        provenance_records: List[Dict[str, Any]],
    ):
        """
        Serialize the result as json to be received by the wrapping Java class.
        """


class SuccessResult(JobRunResult):
    """
    The job finished successfully.
    """

    def __init__(self, outputs: Dict[str, Any]):
        self._outputs = outputs

    def to_json(self, parameters, provenance_records):
        output_results = {}
        for name, output in self._outputs.items():
            output_result = (
                parameters[name].output_results(
                    output, provenance_records=provenance_records
                )
                or {}
            )
            if not isinstance(output_result, dict):
                raise errors.TransformTypeError(
                    "Expect dictionary as output result for parameter '%s' but found type '%s'",
                    name,
                    type(output_result),
                )
            for rid, result in output_result.items():
                if rid in output_results:
                    raise errors.TransformValueError(
                        "Output rid '%s' is an output of multiple parameters", rid
                    )
                output_results[rid] = result
        log.info("Generated output_results: %s", output_results)
        return {"jobResult": {"outputResults": output_results}}


class AbortResult(JobRunResult):
    """
    The job finished without error, but the user specified they wanted the job to be aborted.
    That is, all output transactions will be aborted.
    """

    def to_json(self, parameters, provenance_records):
        return {"jobResult": {"type": "finishJobWithoutRunning"}}
