#  (c) Copyright 2023 Palantir Technologies Inc. All rights reserved.
"""Defines model adapter API Media Input and Output objects"""

import json
from typing import Optional

import media_reference_api.media as media_ref_api
import mio_api.mio as mio_api
import models_api.models_api_modelapi as conjure_api
from conjure_python_client import ConjureDecoder

from .._types import ApiType, ModelIO, ServiceContext, Union, UserDefinedApiItem


class MediaReferenceInput(ApiType):
    """
    An ApiType for media references
    """

    def bind(self, io_object: ModelIO, context: Optional[ServiceContext]):
        """
        Binds a ParameterIO object to an argument of
        the model adapter's run_inference() method
        :param io_object: The ParameterIO object
        :returns: The json representation of io_object
        """
        if context is None:
            raise ValueError("ServiceContext must be provided to bind media reference input")
        return io_object.media_reference(
            binary_media_set_service=context.binary_media_set_service(),
            media_set_service=context.media_set_service(),
            auth_header=context.auth_header,
        )

    def _to_service_api(self) -> conjure_api.ModelApiInput:
        api_input_type = conjure_api.ModelApiInputType(media_reference=conjure_api.ModelApiMediaReferenceType())
        return conjure_api.ModelApiInput(name=self.name, required=self.required, type=api_input_type)

    @classmethod
    def _from_service_api(cls, service_api_type: conjure_api.ModelApiInput):
        assert service_api_type.type.media_reference is not None, "must provide a media reference input type"
        return cls(name=service_api_type.name, required=service_api_type.required)


class MediaReference(UserDefinedApiItem):
    """
    UserDefinedApiItem for a media reference input. Is converted to a MediaReferenceInput only.
    """

    def _to_input(self) -> ApiType:
        return MediaReferenceInput(name=self.name, required=self.required)


class ModelsMediaReference:
    """
    Type for interacting with media references in model adapters.
    This type wraps a media reference string and is capable of
    interacting with media set service.
    """

    def __init__(
        self,
        reference: Union[str, dict],
        binary_media_set_service: mio_api.BinaryMediaSetService,
        media_set_service: mio_api.MediaSetService,
        auth_header: Optional[str] = None,
    ):
        try:
            media_reference_dict = json.loads(reference) if isinstance(reference, str) else reference
        except json.decoder.JSONDecodeError as exc:
            raise ValueError("Unable to parse JSON string for media reference input.") from exc

        if "token" in media_reference_dict:
            media_reference_obj = ConjureDecoder().decode(media_reference_dict, media_ref_api.MediaReferenceWithToken)
            self._auth_header = media_reference_obj.token
            self._media_reference = media_reference_obj.media_reference
        else:
            if auth_header is None:
                raise ValueError("auth_header must be provided when media reference does not contain a token")
            self._auth_header = auth_header
            self._media_reference = ConjureDecoder().decode(media_reference_dict, mio_api.MediaReference)

        media_set_item_ref = self._media_reference.reference.media_set_item
        media_view_item_ref = self._media_reference.reference.media_set_view_item
        self._media_item_rid = (
            media_set_item_ref.media_item_rid if media_set_item_ref else media_view_item_ref.media_item_rid
        )
        self._media_set_rid = (
            media_set_item_ref.media_set_rid if media_set_item_ref else media_view_item_ref.media_set_rid
        )

        self._binary_media_set_service = binary_media_set_service
        self._media_set_service = media_set_service

    @property
    def media_item_rid(self):
        """
        Returns the media item RID.
        """
        return self._media_item_rid

    @property
    def media_set_rid(self):
        """
        Returns the media set RID.
        """
        return self._media_set_rid

    def get_media_item(self):
        """
        Returns the media item as a file-like object.
        """
        return self._binary_media_set_service.get_media_item(
            auth_header=self._auth_header, media_item_rid=self.media_item_rid, media_set_rid=self.media_set_rid
        )

    def get_media_item_via_access_pattern(self, access_pattern_name, access_pattern_path):
        """
        Returns the access pattern of the media item as a file-like object.
        Depending on the media set's persistence policy, this may cache the access pattern once calculated.
        """
        return self._binary_media_set_service.get_media_item_via_access_pattern(
            access_pattern_name, access_pattern_path, self._auth_header, self.media_item_rid, self.media_set_rid
        )

    def transform_media_item(self, output_path, transformation):
        """
        Applies the transform to the media item and returns it as a file-like object.
        The output_path will be provided to the transformation.
        The transformation computation will be done by Mio, not by this Spark module.
        """
        request = mio_api.TransformMediaItemRequest(output_path, transformation)
        return self._binary_media_set_service.transform_media_item(
            self._auth_header, self.media_item_rid, self.media_set_rid, request
        )

    def get_media_item_metadata(self):
        """
        Returns the media item metadata (width, height, etc.)
        """
        return self._media_set_service.get_media_item_metadata(
            self._auth_header, self.media_item_rid, self.media_set_rid
        )
