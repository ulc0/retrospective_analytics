import re
from datetime import date, datetime, timezone
from typing import Union

from models_api.models_api_experiments import Parameter

PARAMETER_VALUE = Union[bool, date, datetime, float, int, str]
_ISO8601_PATTERN = re.compile(r"^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?(Z|[+-]\d{2}:?\d{2})?$")


def convert_parameter(value: PARAMETER_VALUE) -> Parameter:
    """
    Converts a provided parameter value to a type-annotated conjure Parameter.
    For most values, we just use the python type.
    For strings, we will convert it to a date time if it matches ISO 8601.
    """
    if isinstance(value, bool):
        return Parameter(boolean=value)
    if isinstance(value, float):
        return Parameter(double=value)
    if isinstance(value, int):
        return Parameter(integer=value)
    if isinstance(value, (date, datetime)):
        return Parameter(datetime=_format_iso8601(value))
    return _handle_string_parameter(value)


def _format_iso8601(value: Union[date, datetime]) -> str:
    """
    Formats a date or datetime object to ISO 8601 string with precision yyyy-mm-ddThh:mm:ss.
    If a datetime object is provided, it will be converted to UTC.
    """
    if isinstance(value, datetime):
        # Ensure the datetime is in UTC
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        else:
            value = value.astimezone(timezone.utc)
        return value.strftime("%Y-%m-%dT%H:%M:%S") + "Z"
    elif isinstance(value, date):
        return value.strftime("%Y-%m-%dT00:00:00Z")
    return value.isoformat()


def _handle_string_parameter(value: str) -> Parameter:
    if _ISO8601_PATTERN.match(value):
        return Parameter(datetime=value)
    if value.lower() in ["true", "false"]:
        return Parameter(boolean=value.lower() == "true")

    try:
        int_value = int(value)
        return Parameter(integer=int_value)
    except ValueError:
        pass

    try:
        float_value = float(value)
        return Parameter(double=float_value)
    except ValueError:
        pass

    # Default to string
    return Parameter(string=value)
