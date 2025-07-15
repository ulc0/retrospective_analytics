#  (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
"""Defines model adapter API Tabular Input and Output objects"""

import importlib
from typing import Optional

import models_api.models_api_modelapi as conjure_api

from .._types import ApiType, ModelIO, ObjectPrimaryKey, ObjectSetRid, ServiceContext, UserDefinedApiItem


class ObjectInput(ApiType):
    """
    An ApiType for OSDK object inputs
    """

    def __init__(self, *, name: str, object_type, required: bool = True):
        super().__init__(name=name, required=required)
        self.__object_type = object_type

    @property
    def object_type(self):
        """
        :return: The OSDK-defined object type
        """
        return self.__object_type

    def bind(self, io_object: ModelIO, _context: Optional[ServiceContext]):
        """
        Binds a ObjectIO object to an argument of
        the model adapter's run_inference() method
        :param io_object: ObjectIO object
        :return: The OSDK object passed through to the adapter
        """
        object = io_object.object()
        if isinstance(object, ObjectPrimaryKey):
            return self.__object_type.init_from_primary_key(object.key)
        raise Exception(f"Unexpected ModelInput.Object Type: {type(object)}")

    def _to_service_api(self) -> conjure_api.ModelApiInput:
        module_name = self.__object_type.__module__.split(".")[0]
        osdk_module = importlib.import_module(module_name)
        foundry_client_class = getattr(osdk_module, "FoundryClient")
        client = foundry_client_class()
        api_input_type = conjure_api.ModelApiInputType(
            object=conjure_api.ModelApiObjectType(
                ontology_rid=client._rid,
                object_type_api_name=self.__object_type.__name__,
                object_sdk_python_module_name=module_name,
            )
        )
        return conjure_api.ModelApiInput(name=self.name, required=self.required, type=api_input_type)

    @classmethod
    def _from_service_api(cls, service_api_type: conjure_api.ModelApiInput):
        assert service_api_type.type.object is not None, "must provide an object input type"
        module_name = service_api_type.type.object.object_sdk_python_module_name
        object_api_name = service_api_type.type.object.object_type_api_name
        osdk_module = importlib.import_module(f"{module_name}.ontology.objects")
        osdk_object_type = getattr(osdk_module, object_api_name)
        return cls(
            name=service_api_type.name,
            required=service_api_type.required,
            object_type=osdk_object_type,
        )


class ObjectSetInput(ApiType):
    """
    An ApiType for OSDK object set inputs
    """

    def __init__(self, *, name: str, object_set_type, required: bool = True):
        super().__init__(name=name, required=required)
        self.__object_set_type = object_set_type
        self.__module_name = object_set_type.__module__.split(".")[0]
        self.__object_type_api_name = object_set_type.object_type_api_name()

    @property
    def object_set_type(self):
        """
        :return: The OSDK-defined object set type
        """
        return self.__object_set_type

    def bind(self, io_object: ModelIO, _context: Optional[ServiceContext]):
        """
        Binds a ObjectIO object to an argument of
        the model adapter's run_inference() method
        :param io_object: ObjectIO object
        :return: The OSDK object set passed through to the adapter
        """
        object_set = io_object.object_set()
        if isinstance(object_set, ObjectSetRid):
            return self.__object_set_type.init_from_object_set_rid(object_set.rid)
        raise Exception(f"Unexpected ModelInput.Object Type: {type(object_set)}")

    def _to_service_api(self) -> conjure_api.ModelApiInput:
        osdk_module = importlib.import_module(self.__module_name)
        foundry_client_class = getattr(osdk_module, "FoundryClient")
        client = foundry_client_class()
        api_input_type = conjure_api.ModelApiInputType(
            object_set=conjure_api.ModelApiObjectSetType(
                ontology_rid=client._rid,
                object_type_api_name=self.__object_type_api_name,
                object_sdk_python_module_name=self.__module_name,
            )
        )
        return conjure_api.ModelApiInput(name=self.name, required=self.required, type=api_input_type)

    @classmethod
    def _from_service_api(cls, service_api_type: conjure_api.ModelApiInput):
        assert service_api_type.type.object_set is not None, "must provide an object set input type"
        module_name = service_api_type.type.object_set.object_sdk_python_module_name
        object_api_name = service_api_type.type.object_set.object_type_api_name
        osdk_module = importlib.import_module(f"{module_name}.ontology.object_sets")
        osdk_object_set_type = getattr(osdk_module, f"{object_api_name}ObjectSet")
        return cls(
            name=service_api_type.name,
            required=service_api_type.required,
            object_set_type=osdk_object_set_type,
        )


class Object(UserDefinedApiItem):
    """
    UserDefinedApiItem for an OSDK object
    Is converted to an ObjectInput
    """

    def __init__(self, object_type, name: Optional[str] = None, required: bool = True):
        super().__init__(name, required)
        self.__object_type = object_type

    @property
    def object_type(self):
        """
        :return: The OntologyObject type
        """
        return self.__object_type

    def _to_input(self) -> ObjectInput:
        return ObjectInput(name=self.name, object_type=self.__object_type, required=self.required)


class ObjectSet(UserDefinedApiItem):
    """
    UserDefinedApiItem for an OSDK object set
    Is converted to an ObjectSetInput
    """

    def __init__(self, object_set_type, name: Optional[str] = None, required: bool = True):
        super().__init__(name, required)
        self.__object_set_type = object_set_type

    @property
    def object_set_type(self):
        """
        :return: The OntologyObjectSet type
        """
        return self.__object_set_type

    def _to_input(self) -> ObjectSetInput:
        return ObjectSetInput(name=self.name, object_set_type=self.__object_set_type, required=self.required)
