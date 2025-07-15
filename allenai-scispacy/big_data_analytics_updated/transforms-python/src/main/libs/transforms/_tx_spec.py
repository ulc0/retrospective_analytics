#  (c) Copyright 2021 Palantir Technologies Inc. All rights reserved.

from collections import namedtuple


class _TxSpec(namedtuple("_TxSpec", ["start", "prev", "end"])):
    """Three-tuple of transaction rids."""
