# Copyright 2024 Palantir Technologies, Inc.

from abc import ABC, abstractmethod


class AbstractNonCatalogIncrementalCompatibleInput(ABC):
    @abstractmethod
    def get_incremental_options(self, allow_retention):
        """
        Incremental details that are sent to build2 for resolution.
        Returns a map of string to any that is serialized into the put job spec request.
        """


class AbstractNonCatalogIncrementalCompatibleTransformInput(ABC):
    @property
    @abstractmethod
    def rid(self) -> str:
        """str: the resource identifier of the input."""

    @abstractmethod
    def was_change_incremental(self, incremental_input_resolution) -> (bool, str):
        """
        Returns a pair. The first value is a boolean that is true if the changes since the last
        time the build ran were incremental. The second value is a string that contains a user-friendly
        message explaining why the change was not incremental. If the user has specified require-incremental
        then they will get this message in an error.
        This will not be called if the input is new to the build.
        This may not be called if the build is going to run non-incrementally. i.e. if another
        input was checked first and was not incremental.
        """

    @abstractmethod
    def get_incremental(self, incremental_input_resolution):
        """
        Will be called at runtime if the build is to be run incrementally.
        Includes the incremental input resolution provided by the Input Manager.
        Returns a version of this input that can be used in an incremental build, even though it will
        not be read from incrementally.
        """

    @abstractmethod
    def get_non_incremental(self):
        """
        Will be called at runtime if it was determined that the build cannot run incrementally, or if
        this is a new input to the build.
        Returns an incremental version of this input.
        """


class AbstractNonCatalogIncrementalCompatibleOutput(ABC):
    @abstractmethod
    def get_incremental_options(self):
        """
        Incremental details that are set before the job is sent to build2.
        Returns a map of string to any that is serialized into the put job spec request.
        """


class AbstractNonCatalogIncrementalCompatibleTransformOutput(ABC):
    @property
    @abstractmethod
    def rid(self) -> str:
        """str: the resource identifier of the output."""

    @abstractmethod
    def get_incremental(self, strict_append):
        """
        Will be called at runtime if the build is to be run incrementally.
        If strict append is true, then the output should only be appended to.
        If it is false, then it can be modified.
        Returns an incremental version of this output.
        """

    @abstractmethod
    def get_non_incremental(self):
        """
        Will be called at runtime if the build cannot be run incrementally.
        Returns a version of this output that can be used in an incremental build, even though it will
        not be added to incrementally.
        """
