import logging
import random
import re
import time
from typing import Callable, TypeVar

from conjure_python_client import ConjureHTTPError

T = TypeVar("T")
LMS_RATE_LIMIT_ERROR_NAME = "LanguageModelService:RateLimitsExceeded"
GLOBAL_RATE_LIMIT_ERROR_NAME = "LanguageModelService:AzurePortalQosException"
HUB_RATE_LIMIT_ERROR_NAME = "LanguageModelService:HubQosException"
LLM_SOCKET_ERROR_NAME = "LanguageModelService:LlmSocketTimeout"
LMS_RATE_LIMIT_WARNING_LOG = "LMS Rate limit hit in language-model-service, will retry the call."
GLOBAL_RATE_LIMIT_WARNING_LOG = "Azure Portal global limit hit in language-model-service, will retry the call."
HUB_RATE_LIMIT_WARNING_LOG = "Hub Rate limit hit in language-model-service, will retry the call."
LLM_SOCKET_WARNING_LOG = "Encountered a network timeout, will retry the call."
EXPONENTIAL_BACKOFF_MAX_SECS = 600
MILLIS_TO_SECONDS = 1000
log = logging.getLogger(__name__)


def _run_lms_request_with_retries(
    lms_call: Callable[[], T], max_rate_limit_retries: int, max_retry_wait_duration: int = EXPONENTIAL_BACKOFF_MAX_SECS
) -> T:
    num_retries = 0
    while True:
        try:
            return lms_call()
        except ConjureHTTPError as conjure_error:
            _throw_on_non_rate_limit_error(conjure_error)
            _throw_on_max_retry_limit_exceeded(num_retries, max_rate_limit_retries, conjure_error)
            num_retries += 1
            # exponential backoff capped at max_retry_wait_duration in seconds
            exponential_backoff_with_jitter = _add_jitter(2**num_retries)
            num_seconds_to_sleep = float(min(max_retry_wait_duration, exponential_backoff_with_jitter))
            if conjure_error.error_name == LMS_RATE_LIMIT_ERROR_NAME:
                _log_lms_rate_limit_warning_message(num_seconds_to_sleep, conjure_error.parameters)
            elif conjure_error.error_name == GLOBAL_RATE_LIMIT_ERROR_NAME:
                num_seconds_to_sleep = _maybe_extract_retry_after_in_seconds(
                    conjure_error.parameters, num_seconds_to_sleep
                )
                _log_azure_portal_rate_limit_warning_message(num_seconds_to_sleep, conjure_error.parameters)
            elif conjure_error.error_name == HUB_RATE_LIMIT_ERROR_NAME:
                _log_hub_rate_limit_warning_message(num_seconds_to_sleep, conjure_error.parameters)
            elif conjure_error.error_name == LLM_SOCKET_ERROR_NAME:
                _log_llm_socket_warning_message(num_seconds_to_sleep, conjure_error.parameters)
            time.sleep(num_seconds_to_sleep)


def _throw_on_non_rate_limit_error(conjure_error: ConjureHTTPError):
    if conjure_error.error_name not in [
        LMS_RATE_LIMIT_ERROR_NAME,
        GLOBAL_RATE_LIMIT_ERROR_NAME,
        HUB_RATE_LIMIT_ERROR_NAME,
        LLM_SOCKET_ERROR_NAME,
    ]:
        raise conjure_error


def _throw_on_max_retry_limit_exceeded(num_retries: int, max_rate_limit_retries: int, conjure_error: ConjureHTTPError):
    if num_retries >= max_rate_limit_retries:
        log.error("Language model rate limit retries exceeded, raising exception")
        raise conjure_error


def _maybe_extract_retry_after_in_seconds(parameters: dict, num_seconds_to_sleep: float) -> float:
    if "retryAfterMillis" not in parameters:
        return num_seconds_to_sleep
    retry_after_millis = parameters.get("retryAfterMillis")
    if retry_after_millis == "Optional.empty":
        return num_seconds_to_sleep
    try:
        return int(re.sub("[^0-9]", "", str(retry_after_millis))) / MILLIS_TO_SECONDS
    except Exception as error:
        warning_message = f"Failed to parse retryAfterMillis parameter {retry_after_millis}"
        log.warning(error, warning_message, exc_info=True)
        return num_seconds_to_sleep


def _add_jitter(expo_delay: float) -> float:
    randomization_factor = 0.5
    random_value = ((2 * random.random()) - 1) * randomization_factor
    return round((1 + random_value) * expo_delay, 2)


def _log_lms_rate_limit_warning_message(num_seconds_to_sleep: float, parameters: dict):
    language_model_rid = parameters.get("languageModelRid")
    rate_limit_type = parameters.get("rateLimitType")
    rate_limit_scope = parameters.get("rateLimitScope")
    limit_per_minute = parameters.get("limitPerMinute")
    limit_for_priority_per_minute = parameters.get("maxPermitCountForRequestPriority")
    requested_in_last_minute = parameters.get("requestedPermitsInLastMinute")
    requested_for_priority_in_last_minute = parameters.get("requestedPermitsForRequestPriority")
    requested_permits = parameters.get("requestedPermits")

    if int(requested_for_priority_in_last_minute or 0) > int(limit_for_priority_per_minute or 0):
        limit_per_minute_to_report = limit_for_priority_per_minute
        requested_in_last_minute_to_report = requested_for_priority_in_last_minute
    else:
        limit_per_minute_to_report = limit_per_minute
        requested_in_last_minute_to_report = requested_in_last_minute
    warning_params = {
        "retryInSeconds": num_seconds_to_sleep,
        "language_model_rid": language_model_rid,
        "rate_limit_scope": rate_limit_scope,
        "rate_limit_type": rate_limit_type,
        "limit_per_minute": limit_per_minute,
        "requested_in_last_minute": requested_in_last_minute,
        "requested": requested_permits,
        "limit_per_minute_to_report": limit_per_minute_to_report,
        "requested_in_last_minute_to_report": requested_in_last_minute_to_report,
    }
    log.warning(LMS_RATE_LIMIT_WARNING_LOG, extra=warning_params)


def _log_azure_portal_rate_limit_warning_message(num_seconds_to_sleep: float, parameters: dict):
    model = parameters.get("model")
    service = parameters.get("service")
    rate_limit_reason = parameters.get("reason")
    retry_after_millis = parameters.get("retryAfterMillis")
    warning_params = {
        "retryInSeconds": num_seconds_to_sleep,
        "model": model,
        "rate_limit_scope": "GLOBAL",
        "service": service,
        "retry_after_millis": retry_after_millis,
        "reason": rate_limit_reason,
    }
    log.warning(GLOBAL_RATE_LIMIT_WARNING_LOG, extra=warning_params)


def _log_hub_rate_limit_warning_message(num_seconds_to_sleep: float, parameters: dict):
    model_api_name = parameters.get("modelApiName")
    reason = parameters.get("reason")
    total_requested_permits_in_last_minute = parameters.get("totalRequestedPermitsInLastMinute")
    requested_permits = parameters.get("requestedPermits")
    warning_params = {
        "retryInSeconds": num_seconds_to_sleep,
        "model_api_name": model_api_name,
        "rate_limit_scope": "HUB",
        "total_requested_permits_in_last_minute": total_requested_permits_in_last_minute,
        "requested_permits": requested_permits,
        "reason": reason,
    }
    log.warning(HUB_RATE_LIMIT_WARNING_LOG, extra=warning_params)


def _log_llm_socket_warning_message(num_seconds_to_sleep: float, parameters: dict):
    model_name = parameters.get("modelName")
    enrollments = parameters.get("enrollments")
    warning_params = {
        "retryInSeconds": num_seconds_to_sleep,
        "model_name": model_name,
        "enrollments": enrollments,
    }
    log.warning(LLM_SOCKET_WARNING_LOG, extra=warning_params)
