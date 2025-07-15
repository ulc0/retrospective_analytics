# Copyright 2017 Palantir Technologies, Inc.
from . import _fsreader


class FileSystemWriter(_fsreader.FileSystemReader):
    """Class to construct read-write FoundryFileSystems."""

    def set_mode(self, mode):
        """Set the mode of the FSBuilder.

        We don't really have a choice here but to hit Catalog and change the transaction type.
        """
        if mode not in ("replace", "modify", "append"):
            raise ValueError(f"Unknown write mode {mode}")
        self._set_transaction_type(mode)

    def _set_transaction_type(self, mode):
        if mode == "replace":
            tx_type = "SNAPSHOT"
        elif mode == "modify":
            tx_type = "UPDATE"
        elif mode == "append":
            tx_type = "APPEND"

        self._foundry._service_provider.catalog().set_transaction_type(
            self._foundry.auth_header, self._rid, self._end_ref, tx_type
        )
