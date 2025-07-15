import abc
import inspect
import re
from functools import wraps
from typing import Dict, Generic, TypeVar

from ._model_adapter import ModelAdapter, ModelAdapterConfigurationException
from ._state_accessors import ModelStateReader, ModelStateWriter

SUPPORTED_PARAM_KINDS = {
    inspect.Parameter.POSITIONAL_ONLY,
    inspect.Parameter.POSITIONAL_OR_KEYWORD,
}
# disallow starting the variable name with _ for future functionality
SERIALIZER_VAR_NAME_REGEX = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]*$")

ARGS_NOT_SUPPORTED_ERR_MSG = """\
Incorrect usage of @auto_serializer. Positional arguments are not supported for __init__ parameters.
Found positional argument(s): '{}'"""
ARGS_AND_KWARGS_NOT_SUPPORTED_ERR_MSG = """\
Incorrect usage of @auto_serializer. Each __init__ parameter must have exactly one serializer.
Found *args or **kwargs parameter: '{}'"""
MISMATCHED_ARGS_AND_SERIALIZERS_ERR_MSG = """\
Incorrect usage of @auto_serializer. Each __init__ parameter must have exactly one serializer.
Known user args: {}
Known serializers: {}"""
DEFAULT_VALUES_NOT_SUPPORTED_ERR_MSG = """\
Incorrect usage of @auto_serializer. Default values are not supported for __init__ parameters.
Found default value for parameter: '{}'"""
INVALID_PARAM_NAME_ERR_MSG = """\
Incorrect usage of @auto_serializer. '{}' is not an allowed name for auto-serialized arguments.
Starting arguments with '_' is reserved for internal use.
Serializer names must match regex '{}'."""
NOT_MODEL_ADAPTER_ERR_MSG = """\
Incorrect usage of @auto_serializer. @auto_serializer can only be used on subclasses of ModelAdapter.
Found class '{}' which is not a subclass of ModelAdapter."""
NOT_MODEL_SERIALIZER_ERR_MSG = """\
Incorrect usage of @auto_serializer. Each serializer must be a subclass of ModelSerializer.
Serializer for param '{}' is an instance of '{}'."""
CUSTOM_SAVE_LOAD_NOT_SUPPORTED_ERR_MSG = """\
Incorrect usage of @auto_serializer. ModelAdapters cannot have both the @auto_serializer decorator
and custom save() and load() methods.
"""

SerializableT = TypeVar("SerializableT")


class ModelSerializer(abc.ABC, Generic[SerializableT]):
    """
    Abstract class for model serialization. Subclasses must implement serialize() and deserialize().
    Each serializer is responsible for serializing and deserializing a single model argument.
    The state reader and state writer are namespaced to the model argument being serialized. This means
    that filenames can be static for a particular serializer, and the serializer can assume that
    the files it reads and writes are only for that model argument.

    Subclasses can use the type parameter SerializableT to specify the type of model arguments that are supported.
    """

    @abc.abstractmethod
    def serialize(self, writer: ModelStateWriter, obj: SerializableT):
        """
        Serialize the model to be restored for later use. The writer must be used otherwise
        files will not be persisted.
        :param writer: ModelStateWriter object to write local files to artifacts
        :param obj: The object to be serialized with this ModelSerializer.
        """

    @abc.abstractmethod
    def deserialize(self, reader: ModelStateReader) -> SerializableT:
        """
        Restore the model weights from the reader. The reader provides access to a filesystem-like
        interface to read the files and directories that were persisted in serialize().
        :param reader: ModelStateReader object to read local files from model storage
        :return: The model object that was passed into serialize()
        """


def auto_serialize(*args, **serializer_kwargs: Dict[str, ModelSerializer]):
    """
    Decorator to automatically serialize and deserialize model arguments.

    Each argument to the decorated __init__ method must have a corresponding serializer passed into
    this decorator. The serializer will be used to serialize and deserialize each argument.

    In addition, the arguments must follow these rules:
    - The name cannot start with an _ (reserved for future auto_serializer functionality)
    - Each serializer must be an instance of ModelSerializer
    - There cannot be any *args or **kwargs parameters to the __init__ method
    - None of the arguments can have default values

    If this decorator is used without arguments, each argument to the init function will be
    serialized with the DillSerializer by default.
    """
    serializers: Dict = dict()

    def auto_serialize_decorator(init_method):
        sig = inspect.signature(init_method)
        serializers_validated = _validate_user_init_sig(sig.parameters, serializers)

        @wraps(init_method)
        def wrapper(self, *user_args, **user_kwargs):
            if not isinstance(self, ModelAdapter):
                raise ModelAdapterConfigurationException(NOT_MODEL_ADAPTER_ERR_MSG.format(self.__class__.__name__))

            self.__serializable_args__ = {
                k: v for k, v in sig.bind(self, *user_args, **user_kwargs).arguments.items() if k != "self"
            }

            init_method(self, *user_args, **user_kwargs)

        wrapper.__auto_serializers__ = serializers_validated
        return wrapper

    if args:
        #  When called without arguments, the outermost auto_serialize decorator here will be called on the init method itself
        if init_method_instead_of_serializers := args[0] if len(args) == 1 and callable(args[0]) else None:
            return auto_serialize_decorator(init_method_instead_of_serializers)
        else:
            raise ModelAdapterConfigurationException(ARGS_NOT_SUPPORTED_ERR_MSG.format(args))

    serializers = serializer_kwargs
    _validate_serializers(serializers)
    return auto_serialize_decorator


def _validate_serializers(serializers):
    for name, serializer in serializers.items():
        if not isinstance(serializer, ModelSerializer):
            raise ModelAdapterConfigurationException(
                NOT_MODEL_SERIALIZER_ERR_MSG.format(name, serializer.__class__.__name__)
            )
        if not SERIALIZER_VAR_NAME_REGEX.match(name):
            raise ModelAdapterConfigurationException(
                INVALID_PARAM_NAME_ERR_MSG.format(name, SERIALIZER_VAR_NAME_REGEX.pattern)
            )


def _validate_user_init_sig(user_init_signature, serializers):
    for name, param in user_init_signature.items():
        if param.kind not in SUPPORTED_PARAM_KINDS:
            raise ModelAdapterConfigurationException(ARGS_AND_KWARGS_NOT_SUPPORTED_ERR_MSG.format(name))
        if param.default != inspect.Parameter.empty:
            raise ModelAdapterConfigurationException(DEFAULT_VALUES_NOT_SUPPORTED_ERR_MSG.format(name))

    user_args_without_self = user_init_signature.keys() - {"self"}
    serializer_args = serializers.keys()

    if not serializer_args:  # Apply DillSerializer to all user args
        #  Importing here avoids circular import and defers importing .serializers submodule until needed
        from palantir_models.serializers.serializers.dill import DillSerializer

        dill_applied_serializers = dict()
        for arg in user_args_without_self:
            dill_applied_serializers[arg] = DillSerializer()
        return dill_applied_serializers

    if not user_args_without_self == serializer_args:
        raise ModelAdapterConfigurationException(
            MISMATCHED_ARGS_AND_SERIALIZERS_ERR_MSG.format(
                sorted(list(user_args_without_self)), sorted(list(serializer_args))
            )
        )
    return serializers
