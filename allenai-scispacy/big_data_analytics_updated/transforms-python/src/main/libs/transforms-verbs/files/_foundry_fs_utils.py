from datetime import datetime, timezone


def extract_timestamp(file_status):
    if file_status.modified:
        return _extract_timestamp_from_file_modified(file_status)
    else:
        return _extract_timestamp_from_path(file_status)


def _extract_timestamp_from_file_modified(file_status):
    # modified comes with milliseconds, while datetime.fromtimestamp only takes in seconds
    millis = file_status.modified
    if isinstance(millis, str):
        millis = int(millis)

    seconds = millis / 1000
    return datetime.fromtimestamp(seconds, tz=timezone.utc)


def _extract_timestamp_from_path(file_status):
    split_filename = file_status.path.split("#")
    if len(split_filename) == 1:
        return None
    timestamp = file_status.path.split("#")[1]
    for ext in ["csv", "json", "xml"]:
        timestamp = timestamp.replace(f".{ext}", "")
    try:
        timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H_%M_%S.%fZ")
        return timestamp
    except ValueError:
        pass

    # Try alternate file timestamp pattern
    timestamp = datetime.strptime(timestamp, "%Y-%m-%dT%H_%M_%SZ")
    return timestamp


class FileStatusProxy:
    def __init__(self, path, modified, fn_generator=None):
        self.path = path
        self.modified = modified
        self.fn_generator = fn_generator

    def yield_row(self):
        try:
            yield from self.fn_generator(self)
        except RuntimeError:
            return


def proxy_udf_generator(fn_to_proxy):
    def _fn(path, modified):
        proxy = FileStatusProxy(path, modified, fn_to_proxy)
        for row in proxy.yield_row():
            yield row.asDict()

    return _fn
