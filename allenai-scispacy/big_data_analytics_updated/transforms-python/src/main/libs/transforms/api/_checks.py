# Copyright 2020 Palantir Technologies, Inc.
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from transforms.expectations import Expectation


class Check(object):
    """Wraps up an Expectation such that it can be registered with Data Health."""

    def __init__(
        self,
        expectation: "Expectation",
        name: str,
        on_error: str = "FAIL",
        description: Optional[str] = None,
    ) -> None:
        """
        Args:
            expectation (Expectation): The expectation to evaluate.
            name (str): The name of the check, used as a stable identifier over time.
            on_error (str, optional): What action to take if the expectation is not met. Currently 'WARN', 'FAIL'.
            description (str, optional): The description of the check.
        """
        if not name:
            raise ValueError("All expectation checks must have a name.")

        if not hasattr(expectation, "definition"):
            raise ValueError(
                f"Check {name!r} must wrap an expectation. E.g. 'E.col('a').gt(0)'."
            )

        self.expectation = expectation
        self.name = name
        self.description = description

        if on_error not in ("WARN", "FAIL"):
            raise ValueError("'on_error' must be one of 'WARN' or 'FAIL'")

        self.on_error = on_error
