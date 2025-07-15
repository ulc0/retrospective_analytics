#  (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
from language_model_service_api.languagemodelservice_api_completion_v3 import Attribution
from language_model_service_api.languagemodelservice_api_embeddings_v3 import (
    CreateEmbeddingsRequest,
    CreateEmbeddingsResponse,
    DimensionSpecificEmbeddingsRequest,
    EmbeddingsRequest,
    GenericEmbeddingsRequest,
    GenericEmbeddingsResponse,
)
from language_model_service_api.languagemodelservice_llm_api_embeddings_v3 import EmbeddingService

from palantir_models.models._constants import ERROR_MESSAGE
from palantir_models.models._lms import _run_lms_request_with_retries

from ._language_models import _construct_model_parameters

DEFAULT_NUM_RETRIES = 4


class GenericEmbeddingModel:
    """
    A class to interact with embedding models.
    """

    _model_api_name: str
    _attribution: Attribution
    _embedding_service: EmbeddingService
    _auth_header: str

    def __init__(
        self, embedding_service: EmbeddingService, model_api_name: str, attribution: Attribution, auth_header: str
    ):
        self._embedding_service = embedding_service
        self._model_api_name = model_api_name
        self._attribution = attribution
        self._auth_header = auth_header

    @classmethod
    def get(cls, model_api_name: str):
        """
        Constructs a GenericEmbeddingModel in non-transforms context
        :param model_api_name: The api name of the embedding model
        :return: palantir_models.models._embedding_models.GenericEmbeddingModel
        """
        (embedding_service, attribution, auth_header) = _construct_model_parameters(EmbeddingService)
        return cls(embedding_service, model_api_name, attribution, auth_header)

    def create_embeddings(
        self, embedding_request: GenericEmbeddingsRequest, max_rate_limit_retries: int = DEFAULT_NUM_RETRIES
    ) -> GenericEmbeddingsResponse:
        """
        Computes embeddings for all inputs provided and returns a list of embeddings for each input.
        :param embedding_request: embedding request
        :param max_rate_limit_retries: the number of times a request will be retried if a rate limit exception
            is thrown. These could be requests per minute or token use per minute rate limits
        :return: language_model_service_api.languagemodelservice_api_embeddings_v3.GenericEmbeddingsResponse
        """

        req = CreateEmbeddingsRequest(self._attribution, EmbeddingsRequest(generic=embedding_request))
        resp: CreateEmbeddingsResponse = _run_lms_request_with_retries(
            lambda: self._embedding_service.create_embeddings(self._auth_header, self._model_api_name, req),
            max_rate_limit_retries,
        )

        if resp.generic is None:
            raise Exception(
                ERROR_MESSAGE % ("generic", resp.type),
                resp.type,
            )

        return resp.generic


class DimensionSpecificEmbeddingModel:
    """
    A class to interact with embedding models that allow specified dimension.
    """

    _model_api_name: str
    _attribution: Attribution
    _embedding_service: EmbeddingService
    _auth_header: str

    def __init__(
        self, embedding_service: EmbeddingService, model_api_name: str, attribution: Attribution, auth_header: str
    ):
        self._embedding_service = embedding_service
        self._model_api_name = model_api_name
        self._attribution = attribution
        self._auth_header = auth_header

    @classmethod
    def get(cls, model_api_name: str):
        """
        Constructs a DimensionSpecificEmbeddingModel in non-transforms context
        :param model_api_name: The api name of the embedding model
        :return: palantir_models.models._embedding_models.DimensionSpecificEmbeddingModel
        """
        (embedding_service, attribution, auth_header) = _construct_model_parameters(EmbeddingService)
        return cls(embedding_service, model_api_name, attribution, auth_header)

    def create_embeddings(
        self, embedding_request: DimensionSpecificEmbeddingsRequest, max_rate_limit_retries: int = DEFAULT_NUM_RETRIES
    ) -> GenericEmbeddingsResponse:
        """
        Computes embeddings for all inputs provided and returns a list of embeddings for each input.
        :param embedding_request: embedding request
        :param max_rate_limit_retries: the number of times a request will be retried if a rate limit exception
            is thrown. These could be requests per minute or token use per minute rate limits
        :return: language_model_service_api.languagemodelservice_api_embeddings_v3.GenericEmbeddingsResponse
        """

        req = CreateEmbeddingsRequest(
            self._attribution, EmbeddingsRequest(dimension_specific_embeddings=embedding_request)
        )
        resp: CreateEmbeddingsResponse = _run_lms_request_with_retries(
            lambda: self._embedding_service.create_embeddings(self._auth_header, self._model_api_name, req),
            max_rate_limit_retries,
        )

        if resp.generic is None:
            raise Exception(
                ERROR_MESSAGE % ("generic", resp.type),
                resp.type,
            )

        return resp.generic
