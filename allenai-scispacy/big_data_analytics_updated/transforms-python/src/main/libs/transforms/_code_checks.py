#  (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
import traceback
from typing import TYPE_CHECKING, Any, Dict, List, Optional

from transforms._errors import (
    AbortedDueToFailedExpectation,
    AbortedDueToFailedExpectationWithException,
)

if TYPE_CHECKING:
    from transforms.api._checks import Check
    from transforms.api._transform import Transform

# These are technically two different Conjure enums.
FAIL_BUILD_ON_ERROR = "FAIL"
EXPECTATION_FAIL = "FAIL"


def _get_transform_code_checks(transform: "Transform") -> List[Dict[str, Any]]:
    """Based on the Data Health Code Check Registration except using aliases."""
    checks = []
    for t_input in transform.inputs.values():
        if hasattr(t_input, "checks"):
            for check in t_input.checks:
                checks.append(
                    _as_code_check(check, t_input.alias, branch=t_input.branch)
                )
    for t_output in transform.outputs.values():
        if hasattr(t_output, "checks"):
            for check in t_output.checks:
                checks.append(_as_code_check(check, t_output.alias, branch=None))
    return checks


def _as_code_check(check: "Check", alias: str, branch: Optional[str]) -> Dict[str, Any]:
    return {
        "name": check.name,
        "alias": alias,
        "branch": branch,
        "onError": check.on_error,
        "expectation": check.expectation.definition(),
        "description": check.description,
    }


def _expectation_failed(expectation_result):
    return expectation_result["result"] == EXPECTATION_FAIL


def _code_check_result(check, expectation_result) -> "CodeCheckResult":
    # This returns a CodeCheckResult type. This is defined in Data Health Conjure API.
    return {"name": check.name, "result": expectation_result}


def _get_check_result(check, results):
    expectation_result = check.expectation.result(results)
    should_fail_build = (
        _expectation_failed(expectation_result)
        and check.on_error == FAIL_BUILD_ON_ERROR
    )
    return _code_check_result(check, expectation_result), should_fail_build


def _version_to_int(version_string):
    try:
        # Splitting to handle -rc and _rc versions
        return int(version_string.split("-")[0].split("_")[0])
    except ValueError:
        return -1


# Simple version tuple only returning x.y.z version from python version string
def _version_tuple(version_string):
    versions = version_string.split(".")
    return tuple(map(_version_to_int, versions[:3]))


def _execute_and_get_results(dataframe, checks, params_to_df):
    # pylint: disable=import-outside-toplevel
    from transforms.expectations import _version
    from transforms.expectations.evaluator import evaluate_all

    expectations = [check.expectation for check in checks]
    # params_to_df was added in 0.9.8
    if _version_tuple(_version.__version__) >= _version_tuple("0.9.8"):
        results = evaluate_all(dataframe, expectations, params_to_df=params_to_df)
    else:
        results = evaluate_all(dataframe, expectations)
    return [_get_check_result(check, results) for check in checks], results.exception


def _execute_and_get_results_lightweight(polars_lazyframe, checks):
    # pylint: disable=import-outside-toplevel
    from transforms.expectations.evaluator import (
        evaluate_all_polars,  # pylint: disable=no-name-in-module; pylint: disable=no-name-in-module
    )

    expectations = [check.expectation for check in checks]
    results = evaluate_all_polars(polars_lazyframe, expectations)
    return [_get_check_result(check, results) for check in checks], results.exception


def _run_checks(dataframe_and_checks_tuples, params_to_df):
    all_check_results = []
    results_that_should_fail_build = []
    exceptions = []
    for dataframe, checks in dataframe_and_checks_tuples:
        results, exception = _execute_and_get_results(dataframe, checks, params_to_df)
        if exception is not None:
            exceptions.append(exception)
        for check_result, should_fail_build in results:
            all_check_results.append(check_result)
            if should_fail_build:
                results_that_should_fail_build.append(check_result["name"])

    return {
        "check_results": all_check_results,
        "results_that_should_fail_build": results_that_should_fail_build,
        "exceptions": exceptions,
    }


def _get_lightweight_unsupported_expectations(
    checks: List["Check"],
) -> List["Expectation"]:
    from transforms.expectations.validation import (  # pylint: disable=import-outside-toplevel
        expectation_is_supported_in_polars_mode,
    )

    expectations = [check.expectation for check in checks]
    unsupported_expectations = [
        expectation
        for expectation in expectations
        if not expectation_is_supported_in_polars_mode(expectation)
    ]

    return unsupported_expectations


def abort_build_if_failed_expectations(results_dict):
    if results_dict["results_that_should_fail_build"]:
        if results_dict["exceptions"]:
            raise AbortedDueToFailedExpectationWithException(
                "This job failed because some expectations were not met. "
                "The reason for this could be that an exception was thrown during evaluation of the exceptions. "
                "This could be caused by an out of memory error because the expectations are too expensive,"
                " to evaluate for this dataset. This can happen for expensive expectations like distinct_count.",
                "\n".join(
                    _exception_to_stacktrace_string(ex)
                    for ex in results_dict["exceptions"]
                ),
            )
        raise AbortedDueToFailedExpectation(
            "Expectations not met caused this job to fail",
            results_dict["results_that_should_fail_build"],
        )


def _exception_to_stacktrace_string(ex: Exception) -> str:
    return "".join(traceback.format_exception(type(ex), value=ex, tb=ex.__traceback__))
