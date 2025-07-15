#  (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.

from language_model_service_api.languagemodelservice_api_completion_v3 import (
    Attribution,
    CompletionRequestV3,
    CreateCompletionRequest,
    CreateCompletionResponse,
    GenericChatCompletionRequest,
    GenericChatCompletionResponse,
    GenericCompletionRequest,
    GenericCompletionResponse,
    GenericVisionCompletionRequest,
    GptChatCompletionRequest,
    GptChatCompletionResponse,
    GptChatWithVisionCompletionRequest,
)
from language_model_service_api.languagemodelservice_llm_api import LlmService

from palantir_models._internal._runtime._utils import PartialServiceConfig
from palantir_models.models._constants import ERROR_MESSAGE
from palantir_models.models._lms import _run_lms_request_with_retries

from .._internal._runtime import _get_runtime_env

DEFAULT_NUM_RETRIES = 4


class OpenAiGptChatLanguageModel:
    """
    A class to interact with Open Ai GPT models
    """

    _model_api_name: str
    _attribution: Attribution
    _llm_service: LlmService
    _auth_header: str

    def __init__(self, llm_service: LlmService, model_api_name: str, attribution: Attribution, auth_header: str):
        self._model_api_name = model_api_name
        self._llm_service = llm_service
        self._attribution = attribution
        self._auth_header = auth_header

    @classmethod
    def get(cls, model_api_name: str):
        """
        Constructs a OpenAiGptChatLanguageModel in non-transforms context
        :param model_api_name: The api name of the embedding model
        :return: palantir_models.models._language_models.OpenAiGptChatLanguageModel
        """
        (llm_service, attribution, auth_header) = _construct_model_parameters(LlmService)
        return cls(llm_service, model_api_name, attribution, auth_header)

    def create_chat_completion(
        self, completion_request: GptChatCompletionRequest, max_rate_limit_retries: int = DEFAULT_NUM_RETRIES
    ) -> GptChatCompletionResponse:
        """
        Executes the provided chat completion request with automatic request retries if rate limit exceptions
        are thrown by the model.

        :param completion_request: chat completion request
        :param max_rate_limit_retries: the number of times a request will be retried if a rate limit exception
            is thrown. These could be requests per minute or token use per minute rate limits
        :return: language_model_service_api.languagemodelservice_api_completion_v3.GptChatCompletionResponse
        """
        _validate_rate_limit_retry(max_rate_limit_retries)

        req = CreateCompletionRequest(self._attribution, CompletionRequestV3(gpt_chat=completion_request))
        resp: CreateCompletionResponse = _run_lms_request_with_retries(
            lambda: self._llm_service.create_completion(self._auth_header, self._model_api_name, req),
            max_rate_limit_retries,
        )

        if resp.gpt_chat is None:
            raise Exception(
                ERROR_MESSAGE % ("gptChat", resp.type),
                resp.type,
            )

        return resp.gpt_chat


class OpenAiGptChatWithVisionLanguageModel:
    """
    A class to interact with Open Ai GPT chat with vision models
    """

    _model_api_name: str
    _attribution: Attribution
    _llm_service: LlmService
    _auth_header: str

    def __init__(self, llm_service: LlmService, model_api_name: str, attribution: Attribution, auth_header: str):
        self._model_api_name = model_api_name
        self._llm_service = llm_service
        self._attribution = attribution
        self._auth_header = auth_header

    @classmethod
    def get(cls, model_api_name: str):
        """
        Constructs a OpenAiGptChatWithVisionLanguageModel in non-transforms context
        :param model_api_name: The api name of the embedding model
        :return: palantir_models.models._language_models.OpenAiGptChatWithVisionLanguageModel
        """
        (llm_service, attribution, auth_header) = _construct_model_parameters(LlmService)
        return cls(llm_service, model_api_name, attribution, auth_header)

    def create_chat_completion(
        self, completion_request: GptChatWithVisionCompletionRequest, max_rate_limit_retries: int = DEFAULT_NUM_RETRIES
    ) -> GptChatCompletionResponse:
        """
        Executes the provided chat completion request with automatic request retries if rate limit exceptions
        are thrown by the model.

        :param completion_request:
            language_model_service_api.languagemodelservice_api_completion_v3.GptChatWithVisionCompletionRequest
        :param max_rate_limit_retries: the number of times a request will be retried if a rate limit exception
            is thrown. These could be requests per minute or token use per minute rate limits
        :return: language_model_service_api.languagemodelservice_api_completion_v3.GptChatCompletionResponse
        """
        _validate_rate_limit_retry(max_rate_limit_retries)

        req = CreateCompletionRequest(self._attribution, CompletionRequestV3(gpt_chat_with_vision=completion_request))
        resp: CreateCompletionResponse = _run_lms_request_with_retries(
            lambda: self._llm_service.create_completion(self._auth_header, self._model_api_name, req),
            max_rate_limit_retries,
        )

        if resp.gpt_chat is None:
            raise Exception(
                ERROR_MESSAGE % ("gptChat", resp.type),
                resp.type,
            )

        return resp.gpt_chat


class GenericCompletionLanguageModel:
    """
    A class to interact with any completion model
    """

    _model_api_name: str
    _attribution: Attribution
    _llm_service: LlmService
    _auth_header: str

    def __init__(self, llm_service: LlmService, model_api_name: str, attribution: Attribution, auth_header: str):
        self._model_api_name = model_api_name
        self._llm_service = llm_service
        self._attribution = attribution
        self._auth_header = auth_header

    @classmethod
    def get(cls, model_api_name: str):
        """
        Constructs a GenericCompletionLanguageModel in non-transforms context
        :param model_api_name: The api name of the embedding model
        :return: palantir_models.models._language_models.GenericCompletionLanguageModel
        """
        (llm_service, attribution, auth_header) = _construct_model_parameters(LlmService)
        return cls(llm_service, model_api_name, attribution, auth_header)

    def create_completion(
        self, completion_request: GenericCompletionRequest, max_rate_limit_retries: int = DEFAULT_NUM_RETRIES
    ) -> GenericCompletionResponse:
        """
        Executes the provided chat completion request with automatic request retries if rate limit exceptions
        are thrown by the model.

        :param completion_request: chat completion request
        :param max_rate_limit_retries: the number of times a request will be retried if a rate limit exception
            is thrown. These could be requests per minute or token use per minute rate limits
        :return: language_model_service_api.languagemodelservice_api_completion_v3.GenericCompletionResponse
        """
        _validate_rate_limit_retry(max_rate_limit_retries)

        req: CreateCompletionRequest = CreateCompletionRequest(
            self._attribution, CompletionRequestV3(generic=completion_request)
        )
        resp: CreateCompletionResponse = _run_lms_request_with_retries(
            lambda: self._llm_service.create_completion(self._auth_header, self._model_api_name, req),
            max_rate_limit_retries,
        )

        if resp.generic is None:
            raise Exception(
                ERROR_MESSAGE % ("generic", resp.type),
            )

        return resp.generic


class GenericChatCompletionLanguageModel:
    """
    A class to interact with any chat completion model
    """

    _model_api_name: str
    _attribution: Attribution
    _llm_service: LlmService
    _auth_header: str

    def __init__(self, llm_service: LlmService, model_api_name: str, attribution: Attribution, auth_header: str):
        self._model_api_name = model_api_name
        self._llm_service = llm_service
        self._attribution = attribution
        self._auth_header = auth_header

    @classmethod
    def get(cls, model_api_name: str):
        """
        Constructs a GenericChatCompletionLanguageModel in non-transforms context
        :param model_api_name: The api name of the chat completion model
        :return: palantir_models.models._language_models.GenericChatCompletionLanguageModel
        """
        (llm_service, attribution, auth_header) = _construct_model_parameters(LlmService)
        return cls(llm_service, model_api_name, attribution, auth_header)

    def create_chat_completion(
        self, completion_request: GenericChatCompletionRequest, max_rate_limit_retries: int = DEFAULT_NUM_RETRIES
    ) -> GenericChatCompletionResponse:
        """
        Executes the provided chat completion request with automatic request retries if rate limit exceptions
        are thrown by the model.

        :param completion_request: chat completion request
        :param max_rate_limit_retries: the number of times a request will be retried if a rate limit exception
            is thrown. These could be requests per minute or token use per minute rate limits
        :return: language_model_service_api.languagemodelservice_api_completion_v3.GenericChatCompletionResponse
        """
        _validate_rate_limit_retry(max_rate_limit_retries)

        req: CreateCompletionRequest = CreateCompletionRequest(
            self._attribution, CompletionRequestV3(generic_chat=completion_request)
        )
        resp: CreateCompletionResponse = _run_lms_request_with_retries(
            lambda: self._llm_service.create_completion(self._auth_header, self._model_api_name, req),
            max_rate_limit_retries,
        )

        if resp.generic_chat is None:
            raise Exception(
                ERROR_MESSAGE % ("genericChat", resp.type),
            )

        return resp.generic_chat


class GenericVisionCompletionLanguageModel:
    """
    A class to interact with any vision completion model
    """

    _model_api_name: str
    _attribution: Attribution
    _llm_service: LlmService
    _auth_header: str

    def __init__(self, llm_service: LlmService, model_api_name: str, attribution: Attribution, auth_header: str):
        self._model_api_name = model_api_name
        self._llm_service = llm_service
        self._attribution = attribution
        self._auth_header = auth_header

    @classmethod
    def get(cls, model_api_name: str):
        """
        Constructs a GenericVisionCompletionLanguageModel in non-transforms context
        :param model_api_name: The api name of the chat completion model
        :return: palantir_models.models._language_models.GenericVisionCompletionLanguageModel
        """
        (llm_service, attribution, auth_header) = _construct_model_parameters(LlmService)
        return cls(llm_service, model_api_name, attribution, auth_header)

    def create_vision_completion(
        self, completion_request: GenericVisionCompletionRequest, max_rate_limit_retries: int = DEFAULT_NUM_RETRIES
    ) -> GenericChatCompletionResponse:
        """
        Executes the provided vision completion request with automatic request retries if rate limit exceptions
        are thrown by the model.

        :param completion_request: vision completion request
        :param max_rate_limit_retries: the number of times a request will be retried if a rate limit exception
            is thrown. These could be requests per minute or token use per minute rate limits
        :return: language_model_service_api.languagemodelservice_api_completion_v3.GenericChatCompletionResponse
        """
        _validate_rate_limit_retry(max_rate_limit_retries)

        req: CreateCompletionRequest = CreateCompletionRequest(
            self._attribution, CompletionRequestV3(generic_vision=completion_request)
        )
        resp: CreateCompletionResponse = _run_lms_request_with_retries(
            lambda: self._llm_service.create_completion(self._auth_header, self._model_api_name, req),
            max_rate_limit_retries,
        )

        if resp.generic_chat is None:
            raise Exception(
                ERROR_MESSAGE % ("genericChat", resp.type),
            )

        return resp.generic_chat


def _validate_rate_limit_retry(max_retries):
    if max_retries < 0:
        raise ValueError("max_rate_limit_retries cannot be less than 0")


def _construct_model_parameters(service_class):
    """
    Constructs required parameters for creating a lms model in code workspaces
    :param service_class: Subclass of conjure_python_client.Service to construct.
    :return: Tuple of (service class, attribution, auth_header)
    """
    runtime_env = _get_runtime_env()
    service = runtime_env.get_discovered_service(
        "language-model-service", service_class, PartialServiceConfig(return_none_for_unknown_union_types=True)
    )
    attribution = Attribution(user=runtime_env.get_discovered_service_auth_header().replace("Bearer ", ""))
    return service, attribution, runtime_env.get_discovered_service_auth_header()
