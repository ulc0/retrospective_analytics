#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
import abc
import dataclasses
from typing import Optional, Type, TypeVar

import models_api.models_api_executable as executable_api

from palantir_models.models._model_adapter import ModelAdapter
from palantir_models.models._state_accessors import AbstractModelStateReader


@dataclasses.dataclass
class ExternalContext:
    """Context dataclass for use in external model adapters."""

    model_type: str
    model_id: str
    egress_config: dict
    base_url: str
    connection_config: dict
    resolved_credentials: dict

    @classmethod
    def from_api_context(cls, context: executable_api.ExternalModelExecutionContext):
        """Create an ExternalContext from an executable api ExternalModelExecutionContext."""
        return cls(
            model_type=str(context.external_model.model_type.value),
            model_id=context.external_model.external_model_id,
            egress_config={
                "credentials_rid": context.external_model.credential_egress_config.credentials,
                "egress_policy_rid": context.external_model.credential_egress_config.egress_policy,
            },
            base_url=context.external_model.base_url,
            connection_config=context.external_model.connection_configuration,
            resolved_credentials=context.resolved_credentials,
        )


_ExternalModelAdapter = TypeVar("_ExternalModelAdapter", bound="ExternalModelAdapter")


class ExternalModelAdapter(ModelAdapter, abc.ABC):
    """Model adapter for use with external models."""

    @classmethod
    def load(
        cls: Type[_ExternalModelAdapter],
        state_reader: AbstractModelStateReader,
        container_context: Optional[executable_api.ContainerizedApplicationContext] = None,
        external_model_context: Optional[executable_api.ExternalModelExecutionContext] = None,
    ) -> _ExternalModelAdapter:
        if external_model_context is None:
            raise ValueError("ExternalModelAdapter requires an ExternalModelExecutionContext")
        external_context = ExternalContext.from_api_context(external_model_context)
        return cls.init_external(external_context)

    def save(self, state_writer):
        pass

    @classmethod
    @abc.abstractmethod
    def init_external(cls: Type[_ExternalModelAdapter], external_context: ExternalContext) -> _ExternalModelAdapter:
        """
        Initialize the model adapter from the model state and external model context.
        Returns an initialized ModelAdapter.
        """
