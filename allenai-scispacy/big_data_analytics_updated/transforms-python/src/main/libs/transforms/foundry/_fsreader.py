# Copyright 2017 Palantir Technologies, Inc.
import logging

from transforms._errors import ConjureError, ErrorCode, ErrorType

from . import _transactions
from ._files import EmptyFileSystem, FoundryFileSystem
from ._transactions import has_modified_files, has_removed_files

log = logging.getLogger(__name__)


class NonEmptyUnsupportedFileSystem(ConjureError, ValueError):
    """Thrown when trying to read a filesystem that we don't support, but know is non-empty.

    Useful for modified and removed views that we currently don't know how to construct, but by
    inspecting the transaction history we are able to figure out if any changes exist that would
    fall into these categories.
    """

    error_type = ErrorType(
        ErrorCode.INVALID_ARGUMENT, "TransformsPython:NonEmptyUnsupportedFileSystem"
    )


class FileSystemReader(object):
    """Class to construct read-only FoundryFileSystems."""

    def __init__(self, foundry, rid, end_ref):
        self._foundry = foundry
        self._rid = rid
        self._end_ref = end_ref

    def current(self, txspec):
        start, _prev, end = txspec
        return self._filesystem(start, end)

    def previous(self, txspec):
        start, prev, _end = txspec

        if not prev:
            # There was no previous view, so empty filesystem
            return self._empty_filesystem()

        return self._filesystem(start, prev)

    def added(self, txspec):
        _start, prev, end = txspec

        if not prev:
            # There is no previous view, so 'added' is the full view
            return self.current(txspec)

        if prev and prev == end:
            # Foundry views do not support exclusive ranges, so we just return empty.
            return self._empty_filesystem()

        # Return everything from just after previous, up to the end of the view
        return self._filesystem(
            _transactions.transaction_after(self._foundry, self._rid, prev, end, end),
            end,
        )

    def modified(self, txspec):
        _, previous_end_transaction_rid, end_transaction_rid = txspec
        if has_modified_files(
            self._foundry, self._rid, end_transaction_rid, previous_end_transaction_rid
        ):
            raise NonEmptyUnsupportedFileSystem(
                "Detected modified files since previously processed"
            )
        return self._empty_filesystem()

    def removed(self, txspec):
        _, previous_end_transaction_rid, end_transaction_rid = txspec
        if has_removed_files(
            self._foundry, self._rid, end_transaction_rid, previous_end_transaction_rid
        ):
            raise NonEmptyUnsupportedFileSystem(
                "Detected removed files since previously processed"
            )
        return self._empty_filesystem()

    def _empty_filesystem(self):
        log.info("Creating empty filesystem for rid %s", self._rid)
        return EmptyFileSystem(self._foundry)

    def _filesystem(self, start, end):
        end_ref = end or self._end_ref
        log.info("Creating filesystem for rid %s, %s to %s", self._rid, start, end_ref)
        return FoundryFileSystem(self._foundry, self._rid, end_ref, start_txrid=start)
