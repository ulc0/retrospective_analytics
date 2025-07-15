#  (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.


def _inflate_if_possible(obj, cls):
    if isinstance(obj, cls):
        return obj

    if isinstance(obj, dict):
        return cls.inflate(obj)

    raise ValueError(
        "Could not inflate {0} from object of type {1}".format(cls, obj.__class__)
    )
