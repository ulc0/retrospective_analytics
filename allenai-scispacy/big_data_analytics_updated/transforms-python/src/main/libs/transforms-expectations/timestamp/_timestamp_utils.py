from datetime import datetime, timedelta, timezone
from dateutil.parser import parse
import pytz


def _to_datetime(threshold) -> datetime:
    value = None
    if isinstance(threshold, datetime):
        value = threshold
    if isinstance(threshold, str):
        value = parse(threshold)
    if value is None:
        raise ValueError(f"Unable to parse {threshold} as a datetime object")
    if value.tzinfo is None:
        raise ValueError(
            f"Timezone information is ambiguous in {threshold}."
            f"Please add timezone information (e.g. +0000)"
        )
    return value


def _to_timedelta(offset) -> timedelta:
    if isinstance(offset, (int, float)):
        return timedelta(seconds=offset)
    if isinstance(offset, timedelta):
        return offset
    raise ValueError(
        f"{offset} is neither a number (offset in seconds) or a timedelta object"
    )


def _is_offset_positive(offset) -> bool:
    if isinstance(offset, (int, float)):
        return offset >= 0
    if isinstance(offset, timedelta):
        return offset >= timedelta(seconds=0)


def _reference_timestamp_now() -> datetime:
    return datetime.now(timezone.utc)


def _reference_timestamp():
    return pytz.utc.localize(datetime.utcfromtimestamp(0))
