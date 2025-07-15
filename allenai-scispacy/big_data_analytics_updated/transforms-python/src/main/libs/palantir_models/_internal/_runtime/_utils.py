#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
import os
import re
from dataclasses import dataclass
from typing import List, Optional, Type, TypeVar

from conjure_python_client import RequestsClient, Service, ServiceConfiguration

from ..._version import __version__

_Service = TypeVar("_Service", bound=Service)


@dataclass(frozen=True)
class PartialServiceConfig:
    # based on defaults from conjure service config
    # service config class that doesnt care about uris
    connect_timeout: float = 10
    read_timeout: float = 300
    max_retries: int = 4
    backoff_slot_size: int = 250
    """
    amount of time taken during backoff is
    random.uniform() * (backoff slot size / 1000) * (2 ** ({number of previous retries}))
    """
    return_none_for_unknown_union_types: bool = True


_BASE_SERVICE_CONFIG = PartialServiceConfig(max_retries=4, read_timeout=720, connect_timeout=60, backoff_slot_size=250)


def _assert_foundry_env(var: str) -> str:
    assert var in os.environ, f"Foundry environment variable {var} is not configured, unable to continue."
    return os.environ[var]


def _construct_service_for_uris(
    uris: List[str], service_class: Type[_Service], partial_config: Optional[PartialServiceConfig] = None
) -> _Service:
    svc_config = partial_config if partial_config is not None else _BASE_SERVICE_CONFIG
    config = ServiceConfiguration(
        connect_timeout=svc_config.connect_timeout,
        read_timeout=svc_config.read_timeout,
        max_num_retries=svc_config.max_retries,
        backoff_slot_size=svc_config.backoff_slot_size,
    )
    config.uris = uris
    return RequestsClient.create(
        service_class,
        f"palantir_models/{__version__}",
        config,
        return_none_for_unknown_union_types=svc_config.return_none_for_unknown_union_types,
    )


def _sidecar_uri() -> str:
    return f"https://{_assert_foundry_env('FOUNDRY_HOSTNAME')}"


def rid_pattern(service: str, type_: str) -> re.Pattern:
    return re.compile(
        "ri"
        f"\\.{service}"
        "\\.(?:[a-z0-9][a-z0-9\\-]*)?"
        f"\\.{type_}"
        "\\.[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
    )
