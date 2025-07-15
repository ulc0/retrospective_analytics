#  (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
"""Defines classes for working with Palantir Hosted models through transforms."""

import logging
from typing import Any, List, Optional

from language_model_service_api.languagemodelservice_api_completion_v3 import Attribution, JobAttribution
from language_model_service_api.languagemodelservice_documentextraction_api import DocumentInformationExtractionService
from language_model_service_api.languagemodelservice_llm_api import LlmService
from language_model_service_api.languagemodelservice_llm_api_embeddings_v3 import EmbeddingService
from language_model_service_api.languagemodelservice_transcription_api import TranscriptionService
from language_model_service_api.languagemodelservice_translations_api import TranslationService
from transforms.api import FoundryInputParam, ParamContext

from palantir_models.transforms._service import construct_service

from ..models import (
    DimensionSpecificEmbeddingModel,
    GenericChatCompletionLanguageModel,
    GenericCompletionLanguageModel,
    GenericEmbeddingModel,
    GenericVisionCompletionLanguageModel,
    OpenAiGptChatLanguageModel,
    OpenAiGptChatWithVisionLanguageModel,
    TbdDocumentInformationExtractionModel,
    TranscriptionModel,
    TranslationModel,
)
from ._schemas import DATASET_RID, LANGUAGE_MODEL_SCHEMA

TRANSFORMS_LMS_INPUT_TYPE = "language-model"
LMS_ROLE = "language-model-service-api"
# these keys are set by the language model service input manager
RESOLVED_ATTRIBUTION_RID_KEY = "attributionRids"
RESOLVED_MODEL_API_NAME = "modelApiName"

_BASE_DOCSTRING = """
    A FoundryInputParam which binds to the {model} API from language model service.
    Using this class in a transform decorator will return a palantir_models.models.{model}
    which can be used within the transform.
    """

log = logging.getLogger("palantir_models")


class _BaseLanguageModelInput(FoundryInputParam):
    def __init__(self, model_rid: str):
        """
        Connects a transform to a Palantir hosted language model for inference.
        :param model_rid: (string) model identifier
        """
        super().__init__(aliases=[model_rid], type=TRANSFORMS_LMS_INPUT_TYPE)
        self._model_rid = model_rid

    @property
    def json_value(self):
        return {
            DATASET_RID: self._model_rid,
        }

    @property
    def schema(self):
        return LANGUAGE_MODEL_SCHEMA

    @property
    def model_rid(self):
        return self._model_rid


def _create_model_instance(context: ParamContext, json_value, model_class, service_class):
    auth_header = context.foundry_connector.auth_header
    model_rid = json_value[DATASET_RID]
    service = construct_service(context, LMS_ROLE, service_class)

    input_spec = _get_language_model_input_spec(model_rid, context.input_specs)
    attribution_rids: List[str] = _extract_key_from_dataset_properties_dict(RESOLVED_ATTRIBUTION_RID_KEY, input_spec)
    model_api_name: str = _extract_key_from_dataset_properties_dict(RESOLVED_MODEL_API_NAME, input_spec)

    return model_class(
        service,
        model_api_name,
        _get_attribution_or_throw(attribution_rids, context.job_rid),
        auth_header,
    )


class OpenAiGptChatLanguageModelInput(_BaseLanguageModelInput):
    _BASE_DOCSTRING.format(model="OpenAiGptChatLanguageModel")

    @staticmethod
    def instance(context: ParamContext, json_value) -> OpenAiGptChatLanguageModel:
        return _create_model_instance(context, json_value, OpenAiGptChatLanguageModel, LlmService)


class OpenAiGptChatWithVisionLanguageModelInput(_BaseLanguageModelInput):
    _BASE_DOCSTRING.format(model="OpenAiGptChatWithVisionLanguageModel")

    @staticmethod
    def instance(context: ParamContext, json_value) -> OpenAiGptChatWithVisionLanguageModel:
        return _create_model_instance(context, json_value, OpenAiGptChatWithVisionLanguageModel, LlmService)


class GenericCompletionLanguageModelInput(_BaseLanguageModelInput):
    _BASE_DOCSTRING.format(model="GenericCompletionLanguageModel")

    @staticmethod
    def instance(context: ParamContext, json_value) -> GenericCompletionLanguageModel:
        return _create_model_instance(context, json_value, GenericCompletionLanguageModel, LlmService)


class GenericChatCompletionLanguageModelInput(_BaseLanguageModelInput):
    _BASE_DOCSTRING.format(model="GenericChatCompletionLanguageModel")

    @staticmethod
    def instance(context: ParamContext, json_value) -> GenericChatCompletionLanguageModel:
        return _create_model_instance(context, json_value, GenericChatCompletionLanguageModel, LlmService)


class GenericVisionCompletionLanguageModelInput(_BaseLanguageModelInput):
    _BASE_DOCSTRING.format(model="GenericVisionCompletionLanguageModel")

    @staticmethod
    def instance(context: ParamContext, json_value) -> GenericVisionCompletionLanguageModel:
        return _create_model_instance(context, json_value, GenericVisionCompletionLanguageModel, LlmService)


class GenericEmbeddingModelInput(_BaseLanguageModelInput):
    _BASE_DOCSTRING.format(model="GenericEmbeddingModel")

    @staticmethod
    def instance(context: ParamContext, json_value) -> GenericEmbeddingModel:
        return _create_model_instance(context, json_value, GenericEmbeddingModel, EmbeddingService)


class DimensionSpecificEmbeddingModelInput(_BaseLanguageModelInput):
    _BASE_DOCSTRING.format(model="DimensionSpecificEmbeddingModel")

    @staticmethod
    def instance(context: ParamContext, json_value) -> DimensionSpecificEmbeddingModel:
        return _create_model_instance(context, json_value, DimensionSpecificEmbeddingModel, EmbeddingService)


class TbdDocumentInformationExtractionModelInput(_BaseLanguageModelInput):
    _BASE_DOCSTRING.format(model="TbdDocumentInformationExtractionModel")

    @staticmethod
    def instance(context: ParamContext, json_value) -> TbdDocumentInformationExtractionModel:
        return _create_model_instance(
            context, json_value, TbdDocumentInformationExtractionModel, DocumentInformationExtractionService
        )


class TranslationModelInput(_BaseLanguageModelInput):
    _BASE_DOCSTRING.format(model="TranslationModel")

    @staticmethod
    def instance(context: ParamContext, json_value) -> TranslationModel:
        return _create_model_instance(context, json_value, TranslationModel, TranslationService)


class TranscriptionModelInput(_BaseLanguageModelInput):
    _BASE_DOCSTRING.format(model="TranscriptionModel")

    @staticmethod
    def instance(context: ParamContext, json_value) -> TranscriptionModel:
        return _create_model_instance(context, json_value, TranscriptionModel, TranscriptionService)


def _get_language_model_input_spec(model_rid: str, input_specs: dict) -> dict:
    """
    Extracts the input spec that corresponds to the provided model_rid
    :param model_rid: Rid of the model
    :param input_specs: Map from rid to input spec entry from the jobspec.
    """
    # python-transforms uses a tuple as the key to this dict, for our case the second value of the tuple will always be
    # None
    model_input_spec = input_specs.get((model_rid, None))
    if not model_input_spec:
        raise KeyError(f"Input spec for language model {model_rid}")
    return model_input_spec


def _extract_key_from_dataset_properties_dict(property_name: str, model_input_spec) -> Any:
    """
    Attempts to extract the provided key from the datasetProperties dict has been set by the
    LanguageModelInputManager. This dict is a map<string, any>, so the accessed value can by anything
    :param property_name: the key to access in the dataset property dict
    :param model_input_spec: input spec entry from the jobspec that should correspond to the language model input.
    """
    props = model_input_spec["datasetLocator"]["datasetProperties"]
    try:
        return props[property_name]
    except KeyError as key_error:
        raise Exception(f"{property_name} is not set in the resolved language model input spec") from key_error


def _get_attribution_or_throw(attribution_rids: List[str], job_rid: Optional[str]) -> Attribution:
    """
    :param attribution_rids: list of rids
    :return: Attribution
    """
    if len(attribution_rids) == 0:
        raise Exception("Transform does not have any attribution rids, but there must be at least one")

    if job_rid is not None:
        log.info("Found job id, using job attribution")
        return Attribution(job=JobAttribution(job_id=job_rid, rids=attribution_rids))
    log.info("No job id found, using rid attribution")
    return Attribution(rids=attribution_rids)
