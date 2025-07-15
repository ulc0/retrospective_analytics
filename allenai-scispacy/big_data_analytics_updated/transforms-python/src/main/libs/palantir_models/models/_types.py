#  (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
"""Defines objects for model adapter"""

import abc
import re
from typing import List, NamedTuple, Optional, Union

import models_api.models_api_models as models_api


class CondaVersion(abc.ABC):
    """
    Abstract class of conda version.
    Compatible with the conjure API conda version definition.
    """

    @abc.abstractmethod
    def to_service_api(self) -> models_api.CondaVersion:
        """
        Abstract method for converting conda version.
        Compatible with the conjure API conda version definition.
        :return: conjure-defined CondaVersion object
        """


class CondaVersionAutomatic(CondaVersion):
    """
    Implementation of CondaVersion for conda version automatic.
    Compatible with the conjure API conda version automatic definition.
    """

    def to_service_api(self) -> models_api.CondaVersion:
        return models_api.CondaVersion(automatic=models_api.CondaVersionAutomatic())


class CondaVersionExact(CondaVersion):
    """
    Implementation of CondaVersion for conda version exact.
    Compatible with the conjure API conda version exact definition.
    """

    def __init__(self, *, version, build_string=None):
        self.__version = version
        self.__build_string = build_string

    def to_service_api(self) -> models_api.CondaVersion:
        conda_version_exact = models_api.CondaVersionExact(version=self.__version, build_string=self.__build_string)
        return models_api.CondaVersion(exact=conda_version_exact)


class CondaVersionRange(CondaVersion):
    """
    Implementation of CondaVersion for conda version range.
    Compatible with the conjure API conda version range definition.
    """

    def __init__(self, *, min_version=None, max_version=None):
        self.__min_version = min_version
        self.__max_version = max_version

    def to_service_api(self) -> models_api.CondaVersion:
        conda_version_range = models_api.CondaVersionRange(
            min_version=self.__min_version, max_version=self.__max_version
        )
        return models_api.CondaVersion(range=conda_version_range)


class CondaDependency(NamedTuple):
    """
    NamedTuple for conda dependency.
    Compatible with the conjure API conda dependency definition.
    """

    package_name: str
    version: CondaVersion
    repository_rid: Optional[models_api.ArtifactsRepositoryRid]

    @classmethod
    def from_string(
        cls, dependency: str, repository_rid: Optional[models_api.ArtifactsRepositoryRid] = None
    ) -> "CondaDependency":
        """
        Constructs CondaDependency from string input, which is a space-separated string of 1,2, or 3 parts.
        The first part is the exact name of the package; The second part refers to the version; and the third part is
        the exact build string. When there are 3 parts, the second part must be an exact version.
        For the second part, below operators are supported:
            relational operators of >=, <, ==
            , means AND
        :param dependency: conda package dependency in string representation
        :param repository_rid: expected to be the stemma rid for custom packages
        :return: corresponding structured CondaDependency with repository rid set to None
        """
        parts = dependency.split()
        if not 0 < len(parts) <= 3:
            raise ValueError("A match specification is a space-separated string of 1, 2, or 3 parts, got", dependency)
        package_name = parts[0]

        if len(parts) == 1:
            return CondaDependency(package_name, CondaVersionAutomatic(), repository_rid=repository_rid)

        valid_chars = r"[0-9a-zA-Z_\.]"
        maybe_version = parts[1]
        maybe_exact_version = re.match(rf"^(==?)?(?P<version>{valid_chars}+?)\*?$", maybe_version)
        if maybe_exact_version:
            version_number = maybe_exact_version.group("version")

            build_string = (
                m.group("build_string")
                if len(parts) == 3 and (m := re.match(rf"(?P<build_string>{valid_chars}+)?$", parts[2]))
                else None
            )

            return CondaDependency(
                package_name,
                CondaVersionExact(version=version_number, build_string=build_string),
                repository_rid=repository_rid,
            )

        if not len(parts) == 2:
            raise ValueError("When there are 3 parts, the second part must be an exact version, got", dependency)

        maybe_min_only_version = re.match(rf"^>=(?P<version>{valid_chars}+)$", maybe_version)
        if maybe_min_only_version:
            min_version = maybe_min_only_version.group("version")
            return CondaDependency(
                package_name, CondaVersionRange(min_version=min_version), repository_rid=repository_rid
            )

        maybe_max_only_version = re.match(rf"^<(?P<version>{valid_chars}+)$", maybe_version)
        if maybe_max_only_version:
            max_version = maybe_max_only_version.group("version")
            return CondaDependency(
                package_name, CondaVersionRange(max_version=max_version), repository_rid=repository_rid
            )

        maybe_min_max_version = re.match(
            rf"^>=(?P<min_version>{valid_chars}+),<(?P<max_version>{valid_chars}+)$", maybe_version
        )
        if maybe_min_max_version:
            min_version = maybe_min_max_version.group("min_version")
            max_version = maybe_min_max_version.group("max_version")
            return CondaDependency(
                package_name,
                CondaVersionRange(min_version=min_version, max_version=max_version),
                repository_rid=repository_rid,
            )

        raise ValueError("Input dependency format not supported: ", dependency)

    def to_service_api(self) -> models_api.CondaPackageDependency:
        """
        Converts the object to a conjure-defined CondaPackageDependency object
        :return: conjure-defined CondaDependency object
        """
        return models_api.CondaPackageDependency(
            package_name=self.package_name, version=self.version.to_service_api(), repository_rid=self.repository_rid
        )


class PythonEnvironment(NamedTuple):
    """
    DEPRECATED in palantir_models >= 0.897.0
    A description of the unsolved python environment.
    :param conda_dependencies: List of conda dependencies, in structured or string forms.
    """

    conda_dependencies: List[Union[CondaDependency, str]]

    def to_service_api(self) -> models_api.CondaEnvironment:
        """
        Converts the object to a conjure-defined CondaEnvironment object
        :return: conjure-defined CondaEnvironment object
        """
        consolidated_conda_dependencies = [
            CondaDependency.from_string(dep) if isinstance(dep, str) else dep for dep in self.conda_dependencies
        ]
        return models_api.CondaEnvironment(
            conda_package_dependencies=[dep.to_service_api() for dep in consolidated_conda_dependencies]
        )
