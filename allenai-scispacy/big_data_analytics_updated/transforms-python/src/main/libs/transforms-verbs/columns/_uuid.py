#  (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.
from typing import Union
import uuid
from pyspark.sql import Column, functions as F
from ._utils import column_function


# This is just a randomly generated UUID to use as a namespace to globally scope UUIDs
# produced using this library.
# It should never be changed.
TRANSFORMS_NAMESPACE = uuid.UUID("f5957e7d-03f2-4ec3-8ee6-c443517a4564")


@column_function
def uuid3(name, namespace: Union[Column, uuid.UUID] = TRANSFORMS_NAMESPACE):
    return _name_based_uuid(name, namespace=namespace, hash_fn=F.md5, version=3)


@column_function
def uuid5(name, namespace: Union[Column, uuid.UUID] = TRANSFORMS_NAMESPACE):
    return _name_based_uuid(name, namespace=namespace, hash_fn=F.sha1, version=5)


def _name_based_uuid(
    name,
    *,
    namespace: Union[Column, uuid.UUID] = TRANSFORMS_NAMESPACE,
    hash_fn,
    version,
):
    """Generates RFC-4122 compatible version 5 UUIDs.

    Args:
        name: Column to use as a seed to generate the UUID from.
        namespace: The UUID5 namespace to use.  Can be either a static UUID, or a Column containing binary UUIDs.
    """
    if isinstance(namespace, Column):
        ns_col = namespace
    elif isinstance(namespace, uuid.UUID):
        ns_col = F.lit(bytearray(namespace.bytes))
    else:
        raise TypeError(f"'namespace' must be either a Column or UUID")

    hex_val = hash_fn(F.concat_ws("", ns_col, name)).substr(1, 32)

    time_low = hex_val.substr(1, 8)
    time_mid = hex_val.substr(9, 4)
    time_hi_and_version = hex_val.substr(13, 4)
    clock_seq_hi_and_res = hex_val.substr(17, 4)
    clock_seq_low = hex_val.substr(21, 12)

    # Set Version
    time_hi_and_version = F.lower(
        F.hex(
            F.conv(time_hi_and_version, 16, 10)
            .cast("int")
            .bitwiseAND(F.lit(~0xF000))
            .bitwiseOR(F.lit(version << 12))
        )
    )

    # Set variant
    clock_seq_hi_and_res = F.lower(
        F.hex(
            F.conv(clock_seq_hi_and_res, 16, 10)
            .cast("int")
            .bitwiseAND(F.lit(~0xC000))
            .bitwiseOR(F.lit(0x8000))
        )
    )

    return F.concat_ws(
        "-",
        time_low,
        time_mid,
        time_hi_and_version,
        clock_seq_hi_and_res,
        clock_seq_low,
    )
