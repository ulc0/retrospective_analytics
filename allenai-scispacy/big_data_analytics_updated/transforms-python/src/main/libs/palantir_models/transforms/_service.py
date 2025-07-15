#  (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
"""Internal helpers to construct services in transforms."""

from typing import Type

from transforms.api import ParamContext

from .._internal._runtime._utils import PartialServiceConfig, _construct_service_for_uris, _Service


def construct_service(
    context: ParamContext, role, service_class: Type[_Service], return_none_for_unknown_union_types: bool = True
) -> _Service:
    """
    Constructs conjure client for a given service.
    :param context: Parameter context provided to a transform parameter's instance method.
    :param role: Discovered role name to resolve.
    :param service_class: Subclass of conjure_python_client.Service to construct.
    :return: Instance of service_class connected to resolved uris.
    """
    # _resolve_role returns a List[str], however it is a py4j java list, which can't be serialized by pickle,
    # so we do this to ensure a list of python strings
    uris = [str(uri) for uri in context.foundry_connector._resolve_role(role)]
    partial_config = PartialServiceConfig(return_none_for_unknown_union_types=return_none_for_unknown_union_types)
    return _construct_service_for_uris(uris, service_class, partial_config)
