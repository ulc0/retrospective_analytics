#  (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
from typing import Dict, List, Union

from conjure_python_client import RequestsClient, ServiceConfiguration
from language_model_service_api.languagemodelservice_api_completion_v3 import Attribution
from language_model_service_api.languagemodelservice_documentextraction_api import DocumentInformationExtractionService
from language_model_service_api.languagemodelservice_llm_api import LlmService
from language_model_service_api.languagemodelservice_llm_api_embeddings_v3 import EmbeddingService
from language_model_service_api.languagemodelservice_model_api import LanguageModel, ModelManagementService
from language_model_service_api.languagemodelservice_translations_api import TranslationService
from transforms.api import Param

from palantir_models.models import (
    DimensionSpecificEmbeddingModel,
    GenericChatCompletionLanguageModel,
    GenericCompletionLanguageModel,
    GenericEmbeddingModel,
    GenericVisionCompletionLanguageModel,
    OpenAiGptChatLanguageModel,
    OpenAiGptChatWithVisionLanguageModel,
    TbdDocumentInformationExtractionModel,
    TranslationModel,
)
from palantir_models.transforms import (
    DimensionSpecificEmbeddingModelInput,
    GenericChatCompletionLanguageModelInput,
    GenericCompletionLanguageModelInput,
    GenericEmbeddingModelInput,
    GenericVisionCompletionLanguageModelInput,
    OpenAiGptChatLanguageModelInput,
    OpenAiGptChatWithVisionLanguageModelInput,
    TbdDocumentInformationExtractionModelInput,
    TranslationModelInput,
)

_SUPPORTED_LANGUAGE_MODEL_INPUTS = (
    OpenAiGptChatLanguageModelInput,
    OpenAiGptChatWithVisionLanguageModelInput,
    GenericCompletionLanguageModelInput,
    GenericChatCompletionLanguageModelInput,
    GenericVisionCompletionLanguageModelInput,
    GenericEmbeddingModelInput,
    DimensionSpecificEmbeddingModelInput,
    TbdDocumentInformationExtractionModelInput,
    TranslationModelInput,
)
_LanguageModelClient = Union[
    OpenAiGptChatLanguageModel,
    OpenAiGptChatWithVisionLanguageModel,
    GenericCompletionLanguageModel,
    GenericChatCompletionLanguageModel,
    GenericVisionCompletionLanguageModel,
    GenericEmbeddingModel,
    DimensionSpecificEmbeddingModel,
    TbdDocumentInformationExtractionModel,
    TranslationModel,
]


class LanguageModelInputFactory:
    """
    A factory class meant to be used to construct language model classes from Transform input parameters when running
    in code-assist (for Preview).
    """

    def __init__(
        self,
        auth_header: str,
        user_agent: str,
        lms_service_configuration: ServiceConfiguration,
        attribution_rids: List[str],
    ):
        """
        Constructs a language model input factory
        :param auth_header: auth_header which will be used to construct the language model input classes
        :param user_agent: user agent to be used when constructing a conjure client for language model service
        :param lms_service_configuration: ServiceConfiguration for language model service
        :param attribution_rids: rids used to define attribution when making calls to language model service
        """
        self._lms_service_configuration = lms_service_configuration
        self._attribution_rids = attribution_rids
        self._auth_header = auth_header
        self._user_agent = user_agent

    def extract_language_model_inputs(
        self,
        transform_parameters: Dict[str, Param],
    ) -> Dict[str, _LanguageModelClient]:
        """
        Utility method to extract instances of language models from the transforms.Transform.parameters dict of
        input parameters

        :param transform_parameters: A dict of Param objects that may contain the language model params, keyed by
               the parameter name
        :return: Dict[str, Any]: A dictionary with instantiated language models, where
                the keys are the names of the input parameters and the values are
                the instantiated language model classes.
        """

        if not _contains_language_models(list(transform_parameters.values())):
            return {}

        llm_service: LlmService = RequestsClient.create(
            LlmService,
            self._user_agent,
            self._lms_service_configuration,
            return_none_for_unknown_union_types=True,
        )
        model_management_service: ModelManagementService = RequestsClient.create(
            ModelManagementService,
            self._user_agent,
            self._lms_service_configuration,
            return_none_for_unknown_union_types=True,
        )
        embedding_service: EmbeddingService = RequestsClient.create(
            EmbeddingService,
            self._user_agent,
            self._lms_service_configuration,
            return_none_for_unknown_union_types=True,
        )
        document_information_extraction_service: DocumentInformationExtractionService = RequestsClient.create(
            DocumentInformationExtractionService,
            self._user_agent,
            self._lms_service_configuration,
            return_none_for_unknown_union_types=True,
        )
        translation_service: TranslationService = RequestsClient.create(
            TranslationService,
            self._user_agent,
            self._lms_service_configuration,
            return_none_for_unknown_union_types=True,
        )

        compass_attribution = Attribution(rids=self._attribution_rids)

        # the api for lms requires an api name, but we only have the model_rids at this time, so we must
        # fetch the api name for all known models
        usable_models: List[LanguageModel] = model_management_service.get_usable_models(self._auth_header).models
        model_api_names_by_rid: Dict[str, str] = {model.rid: model.api_name for model in usable_models}

        language_model_inputs = {}
        language_model: _LanguageModelClient
        for input_param_name, input_param in transform_parameters.items():
            if isinstance(input_param, OpenAiGptChatLanguageModelInput):
                model_api_name = _get_model_api_name_or_throw(input_param, model_api_names_by_rid)
                language_model = OpenAiGptChatLanguageModel(
                    llm_service=llm_service,
                    auth_header=self._auth_header,
                    attribution=compass_attribution,
                    model_api_name=model_api_name,
                )
            elif isinstance(input_param, OpenAiGptChatWithVisionLanguageModelInput):
                model_api_name = _get_model_api_name_or_throw(input_param, model_api_names_by_rid)
                language_model = OpenAiGptChatWithVisionLanguageModel(
                    llm_service=llm_service,
                    auth_header=self._auth_header,
                    attribution=compass_attribution,
                    model_api_name=model_api_name,
                )
            elif isinstance(input_param, GenericCompletionLanguageModelInput):
                model_api_name = _get_model_api_name_or_throw(input_param, model_api_names_by_rid)
                language_model = GenericCompletionLanguageModel(
                    llm_service=llm_service,
                    auth_header=self._auth_header,
                    attribution=compass_attribution,
                    model_api_name=model_api_name,
                )
            elif isinstance(input_param, GenericChatCompletionLanguageModelInput):
                model_api_name = _get_model_api_name_or_throw(input_param, model_api_names_by_rid)
                language_model = GenericChatCompletionLanguageModel(
                    llm_service=llm_service,
                    auth_header=self._auth_header,
                    attribution=compass_attribution,
                    model_api_name=model_api_name,
                )
            elif isinstance(input_param, GenericVisionCompletionLanguageModelInput):
                model_api_name = _get_model_api_name_or_throw(input_param, model_api_names_by_rid)
                language_model = GenericVisionCompletionLanguageModel(
                    llm_service=llm_service,
                    auth_header=self._auth_header,
                    attribution=compass_attribution,
                    model_api_name=model_api_name,
                )
            elif isinstance(input_param, GenericEmbeddingModelInput):
                model_api_name = _get_model_api_name_or_throw(input_param, model_api_names_by_rid)
                language_model = GenericEmbeddingModel(
                    embedding_service=embedding_service,
                    auth_header=self._auth_header,
                    attribution=compass_attribution,
                    model_api_name=model_api_name,
                )
            elif isinstance(input_param, DimensionSpecificEmbeddingModelInput):
                model_api_name = _get_model_api_name_or_throw(input_param, model_api_names_by_rid)
                language_model = DimensionSpecificEmbeddingModel(
                    embedding_service=embedding_service,
                    auth_header=self._auth_header,
                    attribution=compass_attribution,
                    model_api_name=model_api_name,
                )
            elif isinstance(input_param, TbdDocumentInformationExtractionModelInput):
                model_api_name = _get_model_api_name_or_throw(input_param, model_api_names_by_rid)
                language_model = TbdDocumentInformationExtractionModel(
                    document_information_extraction_service=document_information_extraction_service,
                    auth_header=self._auth_header,
                    attribution=compass_attribution,
                    model_api_name=model_api_name,
                )
            elif isinstance(input_param, TranslationModelInput):
                model_api_name = _get_model_api_name_or_throw(input_param, model_api_names_by_rid)
                language_model = TranslationModel(
                    translation_service=translation_service,
                    auth_header=self._auth_header,
                    attribution=compass_attribution,
                    model_api_name=model_api_name,
                )
            else:
                continue

            language_model_inputs[input_param_name] = language_model

        return language_model_inputs


def _contains_language_models(transform_parameters: List[Param]) -> bool:
    return any(isinstance(input_param, _SUPPORTED_LANGUAGE_MODEL_INPUTS) for input_param in transform_parameters)


def _get_model_api_name_or_throw(
    input_param: Union[
        OpenAiGptChatWithVisionLanguageModelInput,
        OpenAiGptChatLanguageModelInput,
        GenericCompletionLanguageModelInput,
        GenericChatCompletionLanguageModelInput,
        GenericVisionCompletionLanguageModelInput,
        GenericEmbeddingModelInput,
        DimensionSpecificEmbeddingModelInput,
        TbdDocumentInformationExtractionModelInput,
        TranslationModelInput,
    ],
    model_api_names_by_rid: Dict[str, str],
):
    if input_param.model_rid not in model_api_names_by_rid:
        raise KeyError(f"Usable models did not contain a model with rid '{input_param.model_rid}'")
    return model_api_names_by_rid[input_param.model_rid]
