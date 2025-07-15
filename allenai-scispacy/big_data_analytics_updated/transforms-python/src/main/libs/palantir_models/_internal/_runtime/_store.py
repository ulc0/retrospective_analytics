#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.

import threading
from typing import TYPE_CHECKING, Dict, List, Optional

if TYPE_CHECKING:
    from palantir_models.experiments._experiment import AbstractExperiment


# operates on locators from rids rather than the full rid because mlflow requires uuid only identifiers.
# not really a concern because uuids are still unique, no need to care about service + instance + type
class RuntimeExperimentStore:
    __instance: Optional["RuntimeExperimentStore"] = None
    __lock = threading.Lock()

    def __new__(cls):
        if cls.__instance is None:
            with cls.__lock:
                if cls.__instance is None:
                    cls.__instance = super(RuntimeExperimentStore, cls).__new__(cls)
                    cls.__instance._initialize()
        return cls.__instance

    def _initialize(self):
        self.__registry: Dict[str, "AbstractExperiment"] = {}
        self.__registry_lock = threading.Lock()

    def __contains__(self, rid: str):
        with self.__registry_lock:
            return rid in self.__registry

    def register(self, experiment_rid: str, experiment: "AbstractExperiment"):
        locator = experiment_rid.split(".")[-1]
        with self.__registry_lock:
            self.__registry[locator] = experiment

    def remove(self, experiment_rid: str):
        locator = experiment_rid.split(".")[-1]
        with self.__registry_lock:
            del self.__registry[locator]

    def get(self, experiment_rid: str) -> "AbstractExperiment":
        locator = experiment_rid.split(".")[-1]
        with self.__registry_lock:
            return self.__registry[locator]

    def all(self) -> List["AbstractExperiment"]:
        with self.__registry_lock:
            return list(self.__registry.values())
