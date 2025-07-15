from language_model_service_api.languagemodelservice_api_completion_v3 import Attribution, RequestPriority
from language_model_service_api.languagemodelservice_transcription_api import (
    CreateTranscriptionRequest,
    CreateTranscriptionResponse,
    TranscriptionRequest,
    TranscriptionService,
)

from palantir_models.models._constants import ERROR_MESSAGE
from palantir_models.models._lms import _run_lms_request_with_retries

from ._language_models import _construct_model_parameters

DEFAULT_NUM_RETRIES = 4


class TranscriptionModel:
    """
    A class to interact with Transcription models.
    """

    _model_api_name: str
    _attribution: Attribution
    _transcription_service: TranscriptionService
    _auth_header: str

    def __init__(
        self,
        transcription_service: TranscriptionService,
        model_api_name: str,
        attribution: Attribution,
        auth_header: str,
    ):
        self._transcription_service = transcription_service
        self._model_api_name = model_api_name
        self._attribution = attribution
        self._auth_header = auth_header

    @classmethod
    def get(cls, model_api_name: str):
        """
        Constructs a TranscriptionModel in non-transforms context
        :param model_api_name: The api name of the Transcription model
        :return: palantir_models.models._transcription_models.TranscriptionModel
        """
        (transcription_service, attribution, auth_header) = _construct_model_parameters(TranscriptionService)
        return cls(transcription_service, model_api_name, attribution, auth_header)

    def transcribe(
        self,
        transcription_request: TranscriptionRequest,
        request_priority: RequestPriority,
        max_rate_limit_retries: int = DEFAULT_NUM_RETRIES,
    ) -> CreateTranscriptionResponse:
        """
        Runs a transcription request against the specified model.
        :param transcription_request: transcription request
        :param max_rate_limit_retries: the number of times a request will be retried if a rate limit exception
            is thrown. These could be requests per minute or token use per minute rate limits
        :return: language_model_service_api.languagemodelservice_transcription_api.CreateTranscriptionResponse
        """

        req = CreateTranscriptionRequest(self._attribution, transcription_request, request_priority)
        resp: CreateTranscriptionResponse = _run_lms_request_with_retries(
            lambda: self._transcription_service.transcribe(self._auth_header, self._model_api_name, req),
            max_rate_limit_retries,
        )

        if resp is None:
            raise Exception(
                ERROR_MESSAGE % ("transcription", resp.type),
                resp.type,
            )
        return resp
