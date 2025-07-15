# Copyright 2017 Palantir Technologies, Inc.
# pylint: disable=raising-format-tuple
"""Module that deals with all things ShrinkWrap."""

from typing import Dict, List, Optional, Union, cast

from transforms import _errors


class Shrinkwrap(object):
    """Class providing access to a shrinkwrap mappings file."""

    READ_MODE = "READ"
    BUILD_MODE = "BUILD"

    def __init__(
        self,
        shrinkwrap: Optional[Dict[str, List[Dict[str, Union[str, List[str]]]]]] = None,
    ) -> None:
        """Instantiate from a shrinkwrap dictionary.

        Args:
            shrinkwrap (dict): Shrinkwrap object
        """

        shrinkwrap = shrinkwrap or {"mappings": []}
        self._mappings = shrinkwrap["mappings"]

    def rid_for_alias(self, alias: str) -> str:
        """Return the RID for the given alias or throws if the shrinkwrap does not contain the mapping.

        Note: this method will not consult Compass to resolve an alias.

        Args:
            alias (str): The alias to lookup

        Returns:
            str: The associated resource identifier.

        Throws:
            KeyError: if the given alias does not exist in the ShrinkWrap
        """
        return self._get_mapping(alias)

    def build_alias_for_rid(self, rid: str) -> str:
        """Retrieve the BUILD mode alias for the given rid.

        Args:
            rid (str): The dataset's resource identifier string.

        Returns:
            str: The BUILD mode alias for the rid.
        """
        for mapping in self._mappings:
            if mapping["rid"] == rid and self.BUILD_MODE in mapping["modes"]:
                return cast(str, mapping["alias"])

        raise _errors.TransformKeyError("Cannot find BUILD alias for rid %s", rid)

    def _get_mapping(self, alias: str) -> str:
        for mapping in self._mappings:
            if mapping["alias"] == alias:
                return cast(str, mapping["rid"])

        raise _errors.TransformKeyError("No shrinkwrap mapping for alias %s", alias)
