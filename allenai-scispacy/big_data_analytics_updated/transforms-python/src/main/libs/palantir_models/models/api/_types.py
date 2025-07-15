#  (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
"""Defines model adapter API object and abstract class for API Types"""

import abc
import inspect
import os
import shutil
from enum import Enum
from io import IOBase
from typing import Any, ContextManager, Dict, List, Optional, Type, TypeVar, Union

import mio_api.mio as mio_api
import pandas as pd
import pyspark.sql

# Map of {type to cast to: [types allowed to be cast]}
# to enable type casting for parameter inputs/outputs.
# https://support-jira.palantir.tech/browse/PDS-431074
_ALLOWED_PARAMETER_TYPE_CASTING_TO_FROM: Dict[type, List[type]] = {float: [int], list: [tuple], tuple: [list]}

_VALID_TABULAR_TYPES = (pyspark.sql.DataFrame, pd.DataFrame)
_TabularType = Union[pyspark.sql.DataFrame, pd.DataFrame]

_VALID_PARAMETER_TYPES = (int, float, str, bool, list, set, tuple, dict)
_ParameterType = Union[int, float, str, bool, list, set, tuple, dict]


class DFType(Enum):
    """
    Enum class for dataframe input type
    """

    SPARK = "spark"
    PANDAS = "pandas"


class ClassRegistry:
    """
    Registry of a map of {class: value} used for
    registering ModelIO subclasses to the values they consume.
    """

    def __init__(self):
        """
        A registry from class -> values which supports lookup of parent classes according to the MRO.
        """
        self._registry = {}

    def register(self, cls, value, force=False):
        """
        Register a value for a class in this registry
        :param cls: Class to register
        :param value: Value to register
        :param force: Register this value even if cls already has a registered value
        :return: Nothing
        :raises: RegistryError if cls already has a value and force is False
        """
        if not force and cls in self._registry:
            raise Exception(
                f"Class to register {cls} from {value} already registered in ClassRegistry by {self._registry[cls]}."
            )
        self._registry[cls] = value

    def get(self, base_cls):
        """
        Get the registered value for base_cls or the nearest ancestor class according to MRO
        :param base_cls: Class to get a value for
        :return: Registered value or None if no ancestor class has been registered with a value
        """
        return next(self.get_all(base_cls), None)

    def get_all(self, base_cls):
        """
        Get the registered value for this class and all parent classes, returns a generator
        :param base_cls: Class to get values for
        :yield: Registered values for all parents classes of base_cls in MRO order
        """
        for cls in inspect.getmro(base_cls):
            if cls in self._registry:
                yield self._registry[cls]


_model_io_registry = ClassRegistry()


class ModelIO:
    """
    Generic class for Model IO Types
    """

    _delegate_class_name: Optional[str] = None
    _CLASSES_TO_REGISTER: tuple

    def __init_subclass__(cls):
        if cls._delegate_class_name is None:
            cls._delegate_class_name = cls.__name__
        for io_cls in cls._CLASSES_TO_REGISTER:
            _model_io_registry.register(io_cls, cls)

    @staticmethod
    def create(io_object: Optional[Any]):
        """
        Wraps the input io_object with its respective ModelIO subclass from
        the ModelIO class registry. Throws if the io_object's class is not
        found in the registry
        :param io_object: Input/Output object.
        :return: Instance of a ModelIO subclass wrapping io_object.
        """
        io_class = _model_io_registry.get(io_object.__class__)
        if not io_class:
            raise TypeError(f"Unsupported Model API Input/Output Type: {io_object.__class__}.")
        return io_class(io_object)

    def filesystem(self):
        """
        Method for retrieving filesystem object from foundry input
        :return: FoundryFS filesystem object
        """
        self._raise_type_error_for_invalid_representation("filesystem")

    def pandas_dataframe(self):
        """
        Method for retrieving pandas dataframe from foundry input
        :return: Pandas dataframe
        """
        self._raise_type_error_for_invalid_representation("pandas dataframe")

    def spark_dataframe(self):
        """
        Method for retrieving spark dataframe from foundry input
        :return: Spark dataframe
        """
        self._raise_type_error_for_invalid_representation("spark dataframe")

    def media_reference(
        self,
        binary_media_set_service: mio_api.BinaryMediaSetService,
        media_set_service: mio_api.MediaSetService,
        auth_header: str,
    ):
        """
        Method for retrieving media reference from foundry input
        :return: ModelsMediaReference object
        """
        self._raise_type_error_for_invalid_representation("media reference")

    def raw_json(self):
        """
        Method for retrieving foundry input as a raw json
        :return: JSON object
        """
        self._raise_type_error_for_invalid_representation("raw json")

    def parameter(self):
        """
        Method for retrieving input as a parameter
        :return: the input object unmodified
        """
        self._raise_type_error_for_invalid_representation("parameter")

    def object(self):
        """
        Method for retrieving input as an OSDK object
        :return: the input object unmodified
        """
        self._raise_type_error_for_invalid_representation("object")

    def object_set(self):
        """
        Method for retrieving input as an OSDK object set
        :return: the input object set unmodified
        """
        self._raise_type_error_for_invalid_representation("object_set")

    def writable_parameter(self, param_type: type, required: bool):
        """
        Method for retrieving a writable object
        :return: WritableApiObject
        """
        self._raise_type_error_for_invalid_representation("parameter output")

    def writable_tabular(self, df_type: Optional[DFType] = None):
        """
        Method for retrieving a writable object
        :return: WritableApiObject
        """
        self._raise_type_error_for_invalid_representation("tabular output")

    def writable_filesystem(self):
        """
        Method for retrieving a writable filesystem object
        :return: WritableApiOutput mixed w/ a temp dir backed writer
        """
        self._raise_type_error_for_invalid_representation("filesystem output")

    def _raise_type_error_for_invalid_representation(self, representation):
        raise TypeError(
            f"Unable to represent an input/output object of type {self._delegate_class_name} as a "
            f"{representation}. Please verify that your inputs/outputs conform to this model's API definition."
        )


class UnassignedOutput:
    """
    Type used to create WritableApiOutput
    objects not assigned to foundry output objects.
    """


class ObjectPrimaryKey:
    """
    Type to wrap a primary key for an object to be retrieved via OSDK
    """

    key: Any

    def __init__(self, key):
        self.key = key


class ObjectSetRid:
    """
    Type to wrap an object set rid to be retrieved via OSDK
    """

    rid: str

    def __init__(self, rid):
        self.rid = rid


class WritableApiOutput(abc.ABC):
    """
    A generic type for API outputs which write to foundry objects
    """

    def _perform_write(self):
        """
        Internal method to perform the writing of
        the inference result to the output object
        """
        raise Exception("transform_write() is not supported in this context. Please use transform() instead.")

    def _return_result(self):
        """
        Internal method to return the inference result
        in memory
        """
        raise Exception("transform() is not supported in this context. Please use transform_write() instead.")


class WritableTabularOutput(WritableApiOutput):
    """
    Class for writing tabular outputs.
    Accepts spark or pandas dataframe as input.
    """

    _inference_result: _TabularType
    __df_type: Optional[DFType]

    def __init__(self, df_type: Optional[DFType] = None):
        self.__df_type = df_type

    def write(self, inference_result: _TabularType):
        """
        Write a dataframe object to this output.
        :param inference_result: A pandas or spark dataframe
        """
        self._validate_inference_result(inference_result)
        self._inference_result = inference_result

    def _validate_inference_result(self, inference_result: _TabularType):
        if not isinstance(inference_result, _VALID_TABULAR_TYPES):
            raise Exception(
                f"Invalid tabular type attempted to be written to tabular output: {inference_result.__class__}"
            )
        if self.__df_type == DFType.PANDAS and not isinstance(inference_result, pd.DataFrame):
            raise ValueError(
                f"Expected inference result to be a Pandas dataframe "
                f"but instead received {inference_result.__class__}"
            )
        if self.__df_type == DFType.SPARK and not isinstance(inference_result, pyspark.sql.DataFrame):
            raise ValueError(
                f"Expected inference result to be a Spark dataframe "
                f"but instead received {inference_result.__class__}"
            )

    def _return_result(self):
        return self._inference_result


class WritableParameterOutput(WritableApiOutput):
    """
    Class for writing a parameter to an output of the model.
    Accepts any parameter type. Will perform no operations upon transform_write().
    """

    _parameter: Optional[_ParameterType]
    _required: bool
    __type: type

    def __init__(self, param_type: type, required: bool):
        self.__type = param_type
        self._required = required
        if not required:
            self._parameter = None

    def write(self, parameter):
        """
        Write a parameter to this WritableParameterOutput.
        Throws if the type of the parameter is incompatible with this WritableParameterOutput's param_type
        """
        self._parameter = get_value_as_type(parameter, self.__type, self._required)

    def _return_result(self):
        return self._parameter


class WritableFileSystemOutput(WritableApiOutput, abc.ABC):
    """
    Abstract class for writing files to a filesystem output.
    Subclasses must implement a backing store to be written to.
    """

    @abc.abstractmethod
    def open(self, asset_file_path: str, mode: str = "wb") -> ContextManager[IOBase]:
        """
        Open a file-like object in the backing store for writing.
        """

    def put_file(self, local_file_path: str, asset_file_path: Optional[str] = None):
        """
        Copy and put a single file from local disk to a
        specified path in the backing store
        :param local_file_path: Local file to be copied
        :param asset_file_path: Filepath in the backing
            store to copy the file to. By default, the
            file will be copied to the root path of
            the backing store.
        """
        if not asset_file_path:
            asset_file_path = os.path.basename(local_file_path)
        with self.open(asset_file_path) as fp_out:
            with open(local_file_path, "rb") as local_file:
                shutil.copyfileobj(local_file, fp_out)

    def put_directory(self, local_root_path: str, asset_root_path: str = "/"):
        """
        Copy and put a directory from local disk to a
        specified path in the backing store
        """
        for dir_name, _, files in os.walk(local_root_path):
            rel_dir = os.path.relpath(dir_name, local_root_path)
            for file in files:
                self.put_file(os.path.join(dir_name, file), os.path.join(*asset_root_path.split("/"), rel_dir, file))


class ServiceContext(abc.ABC):
    """
    A runtime context object. Implementations of this class
    are used by Input types to access internal Foundry Service API's.
    """

    @abc.abstractmethod
    def binary_media_set_service(self) -> "mio_api.BinaryMediaSetService":
        """Return an instance of MIO BinaryMediaSetService"""

    @abc.abstractmethod
    def media_set_service(self) -> "mio_api.MediaSetService":
        """Return an instance of MIO MediaSetService"""

    @property
    @abc.abstractmethod
    def auth_header(self) -> str:
        """Return the auth header for the current request"""


_ApiType = TypeVar("_ApiType", bound="ApiType")


class ApiType(abc.ABC):
    """
    A generic input/output type for model adapter APIs
    """

    _api_io_name: str
    _required: bool

    def __init__(self, *, name: str, required: bool = True):
        """
        Constructor for an ApiType.
        Name and Required params are necessary for all ApiTypes.
        """
        self._api_io_name = name
        self._required = required

    @property
    def name(self) -> str:
        """The input/output argument name"""
        return self._api_io_name

    @property
    def required(self) -> bool:
        """Boolean for requiring this input/output"""
        return self._required

    @abc.abstractmethod
    def bind(self, io_object: ModelIO, context: Optional[ServiceContext]):
        """
        Binds the input/output from foundry to an argument of
        the model adapter's run_inference() method
        :param io_object: ModelIO object wrapping a foundry object
            (TransformInput, TransformOutput, Filesystem, Parameter)
        :return: Input to the model's transform() method
            (Pandas DF, Spark DF, Filesystem, etc.)
            Or a writable output object
            (WritableTabularOutput)
        """

    @abc.abstractmethod
    def _to_service_api(self):
        """
        Converts the input/output object to
        its respective conjure-defined object
        :return: conjure-defined api object
        """

    @classmethod
    @abc.abstractmethod
    def _from_service_api(cls: Type[_ApiType], service_api_type) -> _ApiType:
        """
        Converts the object from conjure definition to the python object
        """


class UserDefinedApiItem:
    """
    Parent class for API items specified in the user's api() that are usable as both inputs and outputs.
    Names are optionally provided to allow these types to be used in both lists and dictionaries of inputs/outputs.
    If this object is the value of an inputs/outputs dictionary, its key will be used as its name.
    Subclasses are expected to define conversion logic _to_input and _to_output.
    Conversion to an input/output will throw if no name is set.
    """

    _required: bool
    _name: Optional[str]

    def __init__(self, name: Optional[str] = None, required: bool = True):
        self._name = name
        self._required = required

    @property
    def name(self):
        """Name of this input/output"""
        return self._name

    @name.setter
    def name(self, name):
        if self._name:
            raise ValueError(
                f"Attempted to assign name of {self.__class__.__name__} api item to {name} "
                f"yet a name has already been set: {self._name}"
            )
        self._name = name

    @property
    def required(self):
        """Boolean for requiring this input/output"""
        return self._required

    def as_input(self, name: Optional[str] = None):
        """
        Method to convert this item into an input ApiType.
        Will attempt to set this item's name to the name parameter if passed.
        Throws if a name is already set, or if no name is set.
        """
        if name:
            self.name = name
        self._validate()
        return self._to_input()

    def as_output(self, name: Optional[str] = None):
        """
        Method to convert this item into an output ApiType with a passed-in name parameter.
        Will attempt to set this item's name to the name parameter if passed.
        Throws if a name is already set, or if no name is set.
        """
        if name:
            self.name = name
        self._validate()
        return self._to_output()

    def _to_input(self) -> ApiType:
        """
        Logic to convert this item to an input ApiType. Expected to be implemented by subclasses.
        """
        raise TypeError(f"Type {self.__class__.__name__} is not supported as a model adapter api input.")

    def _to_output(self) -> ApiType:
        """
        Logic to convert this item to an output ApiType. Expected to be implemented by subclasses.
        """
        raise TypeError(f"Type {self.__class__.__name__} is not supported as a model adapter api output.")

    def _validate(self):
        assert self._name, f"No name has been set for api item of type {self.__class__.__name__}"


def get_value_as_type(value, expected_type: Union[type, Any], required: bool = True):
    """
    Attempt to return the value with the provided expected type.
    If the types are different, casting is performed if the value type
    is in the allowed type casting map for the expected type.
    Supports both simple types and type aliases, e.g. List, Dict, etc.
    :param value: value whose type is being validated or casted.
    :param expected_type: type to compare or cast to.
    :return: value as-is, or casted to the expected type if applicable.
    Throws if the expected type is not a valid type, or if the two types are incompatible.
    """
    if expected_type == Any:
        return value

    if value is None and not required:
        return value

    type_to_check = None
    if hasattr(expected_type, "__origin__"):
        type_to_check = expected_type.__origin__
    elif isinstance(expected_type, type):
        type_to_check = expected_type

    if not type_to_check:
        raise Exception(
            f"Attempted to compare type of value {value} with "
            f"type {expected_type} but did not receive a valid type or type alias."
        )

    if isinstance(value, dict) and type_to_check is dict:
        return value
    if value is not None and isinstance(value, list) and type_to_check is dict:
        if all(isinstance(t, tuple) and len(t) == 2 for t in value):
            return dict(value)
        raise TypeError("Unsupported map input format, the expected format is a list of (key,value) tuples.")

    if isinstance(value, type_to_check):
        return value
    if type(value) in _ALLOWED_PARAMETER_TYPE_CASTING_TO_FROM.get(type_to_check, []):
        return type_to_check(value)
    raise ValueError(
        f"Parameter value of type {type(value)} is incompatible with expected parameter type {expected_type}"
    )
