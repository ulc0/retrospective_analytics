#  (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
"""Defines model adapter API Filesystem Input and Output objects"""

from typing import Optional

import models_api.models_api_modelapi as conjure_api
from transforms.api import FileSystem as TransformsFS

from .._types import ApiType, ModelIO, ServiceContext, UserDefinedApiItem


class ModelsFileSystem:
    """
    Wrapper for transforms FileSystem class
    """

    __delegate: TransformsFS

    def __init__(self, io_object: ModelIO):
        self.__delegate = io_object.filesystem()

    def open(self, path, mode="r", **kwargs):
        """Calls open() on the delegate transforms FileSystem object"""
        return self.__delegate.open(path, mode, **kwargs)

    def ls(self, glob=None, regex=".*", show_hidden=False):
        """Calls ls() on the delegate transforms FileSystem object"""
        for result in self.__delegate.ls(glob, regex, show_hidden):
            yield result

    def files(self, glob=None, regex=".*", show_hidden=False, packing_heuristic=None):
        """Calls files() on the delegate transforms FileSystem object"""
        return self.__delegate.files(glob, regex, show_hidden, packing_heuristic)


class FileSystemInput(ApiType):
    """
    An ApiType for foundry filesystem input type
    """

    def bind(self, io_object: ModelIO, _context: Optional[ServiceContext]) -> ModelsFileSystem:
        """
        Binds a FileSystem IO object to an argument of
        the model adapter's run_inference() method
        :param io_object: TransformIO object
        :return: A read-only filesystem object
        associated with io_object.
        """
        return ModelsFileSystem(io_object)

    def _to_service_api(self) -> conjure_api.ModelApiInput:
        api_input_type = conjure_api.ModelApiInputType(filesystem=conjure_api.ModelApiFilesystemType())
        return conjure_api.ModelApiInput(name=self.name, required=self.required, type=api_input_type)

    @classmethod
    def _from_service_api(cls, service_api_type: conjure_api.ModelApiInput):
        assert service_api_type.type.filesystem is not None, "must provide a filesystem input type"
        return cls(name=service_api_type.name, required=service_api_type.required)


class FileSystemOutput(ApiType):
    """
    An ApiType for foundry filesystem output type
    """

    def bind(self, io_object: ModelIO, _context: Optional[ServiceContext]) -> ModelsFileSystem:
        """
        Binds a FileSystem IO object to an argument of
        the model adapter's run_inference() method
        :param io_object: TransformIO object
        :return: A read-and-writable filesystem object
        associated with io_object.
        """
        return io_object.writable_filesystem()

    def _to_service_api(self) -> conjure_api.ModelApiOutput:
        api_output_type = conjure_api.ModelApiOutputType(filesystem=conjure_api.ModelApiFilesystemType())
        return conjure_api.ModelApiOutput(name=self.name, required=self.required, type=api_output_type)

    @classmethod
    def _from_service_api(cls, service_api_type: conjure_api.ModelApiOutput):
        assert service_api_type.type.filesystem is not None, "must provide a filesystem output type"
        return cls(name=service_api_type.name, required=service_api_type.required)


class FileSystem(UserDefinedApiItem):
    """
    UserDefinedApiItem for a filesystem. Is converted to a FileSystemInput or a FileSystemOutput.
    """

    def _to_input(self) -> FileSystemInput:
        return FileSystemInput(name=self.name, required=self.required)

    def _to_output(self) -> FileSystemOutput:
        return FileSystemOutput(name=self.name, required=self.required)
