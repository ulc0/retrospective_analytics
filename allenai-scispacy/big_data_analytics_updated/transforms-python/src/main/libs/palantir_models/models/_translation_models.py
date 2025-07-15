from language_model_service_api.languagemodelservice_api_completion_v3 import Attribution
from language_model_service_api.languagemodelservice_translations_api import (
    CreateTranslationRequest,
    CreateTranslationResponse,
    GenericTranslationResponse,
    GetAllLanguagePairsRequest,
    LanguagesResponse,
    TranslationRequest,
    TranslationService,
)

from palantir_models.models._constants import ERROR_MESSAGE
from palantir_models.models._lms import _run_lms_request_with_retries

from ._language_models import _construct_model_parameters

DEFAULT_NUM_RETRIES = 4


class TranslationModel:
    """
    A class to interact with Translation models.
    """

    _model_api_name: str
    _attribution: Attribution
    _translation_service: TranslationService
    _auth_header: str

    def __init__(
        self,
        translation_service: TranslationService,
        model_api_name: str,
        attribution: Attribution,
        auth_header: str,
    ):
        self._translation_service = translation_service
        self._model_api_name = model_api_name
        self._attribution = attribution
        self._auth_header = auth_header

    @classmethod
    def get(cls, model_api_name: str):
        """
        Constructs a TranslationModel in non-transforms context
        :param model_api_name: The api name of the Translation model
        :return: palantir_models.models._translation_models.TranslationModel
        """
        (translation_service, attribution, auth_header) = _construct_model_parameters(TranslationService)
        return cls(translation_service, model_api_name, attribution, auth_header)

    def translate(
        self,
        translation_request: TranslationRequest,
        max_rate_limit_retries: int = DEFAULT_NUM_RETRIES,
    ) -> GenericTranslationResponse:
        """
        Runs a translation request against the specified model. The request priority is always set to retryable.
        :param translation_request: translation request
        :param max_rate_limit_retries: the number of times a request will be retried if a rate limit exception
            is thrown. These could be requests per minute or token use per minute rate limits
        :return: language_model_service_api.languagemodelservice_translations_api.CreateTranslationResponse
        """

        req = CreateTranslationRequest(self._attribution, translation_request)
        resp: CreateTranslationResponse = _run_lms_request_with_retries(
            lambda: self._translation_service.translate(self._auth_header, self._model_api_name, req),
            max_rate_limit_retries,
        )

        if resp.generic is None:
            raise Exception(
                ERROR_MESSAGE % ("translate", resp.type),
                resp.type,
            )
        return resp.generic

    def get_all_language_pairs(
        self,
        max_rate_limit_retries: int = DEFAULT_NUM_RETRIES,
    ) -> LanguagesResponse:
        """
        Returns a map between translation model and its supported language pair for each translation model
        which the auth header with the given attribution can execute
        :param get_all_language_pairs_request: language pairs request
        :param max_rate_limit_retries: the number of times a request will be retried if a rate limit exception
            is thrown. These could be requests per minute or token use per minute rate limits
        :return: language_model_service_api.languagemodelservice_translations_api.LanguagesResponse
        """

        resp: LanguagesResponse = _run_lms_request_with_retries(
            lambda: self._translation_service.get_all_language_pairs_for_attribution(
                self._auth_header, GetAllLanguagePairsRequest(self._attribution)
            ),
            max_rate_limit_retries,
        )

        if resp is None:
            raise Exception(
                ERROR_MESSAGE % ("language models", resp.type),
                resp.type,
            )
        return resp
