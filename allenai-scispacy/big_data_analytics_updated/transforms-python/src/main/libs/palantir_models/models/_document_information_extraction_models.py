from language_model_service_api.languagemodelservice_api_completion_v3 import Attribution
from language_model_service_api.languagemodelservice_documentextraction_api import (
    CreateDocumentInformationExtractionRequest,
    CreateDocumentInformationExtractionResponse,
    DocumentInformationExtractionRequest,
    DocumentInformationExtractionService,
    TbdDocumentInformationExtractionRequest,
    TbdDocumentInformationExtractionResponse,
)

from palantir_models.models._constants import ERROR_MESSAGE
from palantir_models.models._lms import _run_lms_request_with_retries

from ._language_models import _construct_model_parameters

DEFAULT_NUM_RETRIES = 4


class TbdDocumentInformationExtractionModel:
    """
    A class to interact with document information extraction models.
    """

    _model_api_name: str
    _attribution: Attribution
    _document_information_extraction_service: DocumentInformationExtractionService
    _auth_header: str

    def __init__(
        self,
        document_information_extraction_service: DocumentInformationExtractionService,
        model_api_name: str,
        attribution: Attribution,
        auth_header: str,
    ):
        self._document_information_extraction_service = document_information_extraction_service
        self._model_api_name = model_api_name
        self._attribution = attribution
        self._auth_header = auth_header

    @classmethod
    def get(cls, model_api_name: str):
        """
        Constructs a TbdDocumentInformationExtractionModel in non-transforms context
        :param model_api_name: The api name of the document information extraction model
        :return: palantir_models.models._document_information_extraction_models.TbdDocumentInformationExtractionModel
        """
        (document_information_extraction_service, attribution, auth_header) = _construct_model_parameters(
            DocumentInformationExtractionService
        )
        return cls(document_information_extraction_service, model_api_name, attribution, auth_header)

    def chunk(
        self,
        document_information_extraction_request: TbdDocumentInformationExtractionRequest,
        max_rate_limit_retries: int = DEFAULT_NUM_RETRIES,
    ) -> TbdDocumentInformationExtractionResponse:
        """
        Extracts layout-aware information from the provided document.
        :param document_information_extraction_request: document information extraction request
        :param max_rate_limit_retries: the number of times a request will be retried if a rate limit exception
            is thrown. These could be requests per minute or token use per minute rate limits
        :return: language_model_service_api.languagemodelservice_documentextraction_api.TbdDocumentInformationExtractionResponse
        """

        req = CreateDocumentInformationExtractionRequest(
            self._attribution, DocumentInformationExtractionRequest(tbd=document_information_extraction_request)
        )
        resp: CreateDocumentInformationExtractionResponse = _run_lms_request_with_retries(
            lambda: self._document_information_extraction_service.chunk(self._auth_header, self._model_api_name, req),
            max_rate_limit_retries,
        )

        if resp.tbd is None:
            raise Exception(
                ERROR_MESSAGE % ("tbd", resp.type),
                resp.type,
            )

        return resp.tbd
