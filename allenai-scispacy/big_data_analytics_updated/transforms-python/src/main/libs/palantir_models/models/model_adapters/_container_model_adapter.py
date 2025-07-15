#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
import abc
import dataclasses
from typing import Optional, Type, TypeVar

import models_api.models_api_executable as executable_api

from palantir_models.models._model_adapter import ModelAdapter
from palantir_models.models._state_accessors import AbstractModelStateReader


@dataclasses.dataclass
class ContainerContext:
    """Context dataclass for use in containerized model adapters."""

    services: dict
    shared_empty_dir_mount_path: str
    state_reader: AbstractModelStateReader

    @classmethod
    def from_api_context(
        cls, context: executable_api.ContainerizedApplicationContext, state_reader: AbstractModelStateReader
    ):
        """Create a ContainerContext from an executable api ContainerizedApplicationContext."""
        return cls(
            services=context.services,
            shared_empty_dir_mount_path=context.shared_empty_dir_mount_path,
            state_reader=state_reader,
        )

    @classmethod
    def from_sidecar(cls, sidecar_config):
        """Create a ContainerContext from a transforms-sidecar SidecarContainerConfiguration."""
        raise NotImplementedError()


_ContainerModelAdapter = TypeVar("_ContainerModelAdapter", bound="ContainerModelAdapter")


class ContainerModelAdapter(ModelAdapter, abc.ABC):
    """Model adapter for use with container models."""

    @classmethod
    def load(
        cls: Type[_ContainerModelAdapter],
        state_reader: AbstractModelStateReader,
        container_context: Optional[executable_api.ContainerizedApplicationContext] = None,
        external_model_context: Optional[executable_api.ExternalModelExecutionContext] = None,
    ) -> _ContainerModelAdapter:
        if container_context is None:
            raise ValueError("ContainerModelAdapter requires a ContainerizedApplicationContext")
        return cls.init_container(ContainerContext.from_api_context(container_context, state_reader))

    def save(self, state_writer):
        pass

    @classmethod
    @abc.abstractmethod
    def init_container(
        cls: Type[_ContainerModelAdapter], container_context: ContainerContext
    ) -> _ContainerModelAdapter:
        """
        Initialize the container model from the model state and container context.
        Returns an initialized ModelAdapter
        """
