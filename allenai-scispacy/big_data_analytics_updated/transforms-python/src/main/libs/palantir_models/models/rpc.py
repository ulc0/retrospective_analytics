#  (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
"""
RPC Client for artifacts service
"""
import uuid
from dataclasses import dataclass
from typing import IO, TYPE_CHECKING, Any, ContextManager, Dict, List, Optional

from conjure_python_client import Service

if TYPE_CHECKING:
    from requests.models import Response
    from urllib3.response import HTTPResponse

_ARTIFACTS_CONNECT_TIMEOUT_SECONDS = 300
_ARTIFACTS_READ_TIMEOUT_SECONDS = 3600


class _Headers:
    ARTIFACTS_TTL = "X-Artifacts-Time-To-Live"
    DOCKER_API_VERSION = "Docker-Distribution-API-Version"
    DOCKER_CONTENT_DIGEST = "Docker-Content-Digest"
    DOCKER_UPLOAD_UUID = "Docker-Upload-UUID"
    FOUNDRY_ARTIFACTS_REPOSITORY = "X-Foundry-Artifacts-Repository"
    FOUNDRY_CHECKSUM_SHA256 = "X-Checksum-Sha256"
    ARTIFACT_DOWNLOAD_SOURCE = "X-Artifact-Download-Source"
    USERNAME = "X-Artifacts-Basic-Username"
    AUTHORIZATION = "Authorization"
    CONTENT_LENGTH = "Content-Length"
    CONTENT_TYPE = "Content-Type"
    ACCEPT = "Accept"


class _ContentTypes:
    OCTET_STREAM = "application/octet-stream"
    DOCKER_CONTAINER_IMAGE_V1_JSON = "application/vnd.docker.container.image.v1+json"
    DOCKER_IMAGE_TAR_GZIP = "application/vnd.docker.image.rootfs.diff.tar.gzip"
    DOCKER_MANIFEST_LIST_V2_JSON = "application/vnd.docker.distribution.manifest.list.v2+json"
    DOCKER_MANIFEST_V2_JSON = "application/vnd.docker.distribution.manifest.v2+json"
    OCI_CONTAINER_IMAGE_V1_JSON = "application/vnd.oci.image.config.v1+json"
    OCI_IMAGE_TAR_GZIP = "application/vnd.oci.image.layer.v1.tar+gzip"
    OCI_MANIFEST_INDEX_V1_JSON = "application/vnd.oci.image.index.v1+json"
    OCI_MANIFEST_V1_JSON = "application/vnd.oci.image.manifest.v1+json"


# TODO(gsilvasimoes): replace with AbstractContextManager after 3.8 deprecation
@dataclass
class RichBody(ContextManager["RichBody"]):
    """
    In addition to a file-like body, this includes optional headers from the response.
    """

    body: "HTTPResponse"
    repository_rid: Optional[str]
    content_type: Optional[str]
    content_length: Optional[int]
    checksum: Optional[str]

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.body.close()


class ArtifactsService(Service):
    """
    Conjure-compatible service for interacting with Foundry Artifacts Service.

    See https://github.palantir.build/foundry/artifacts/blob/0.919.0/artifacts-clients/src/main/java/com/palantir/artifacts/clients/dialogue/DockerArtifactsServiceBlocking.java
    for a reference implementation of the docker registry methods in Java.
    """

    def __init__(
        self,
        requests_session,
        uris: List[str],
        _connect_timeout: float,
        _ignored_read_timeout_value: float,
        _verify: str,
        _return_none_for_unknown_union_types: bool = False,
    ) -> None:
        super().__init__(
            requests_session,
            uris,
            _ARTIFACTS_CONNECT_TIMEOUT_SECONDS,
            _ARTIFACTS_READ_TIMEOUT_SECONDS,
            _verify,
            _return_none_for_unknown_union_types,
        )

    def get_maven_artifact(self, *, auth_header: str, repository_rid: str, maven_coordinate: str) -> RichBody:
        """
        Loads a maven artifact from foundry and returns it as a file like object.
        :param auth_header: User auth header with permission to read the repository.
        :param repository_rid: Rid of repository containing the artifact.
        :param maven_coordinate: Maven coordinate containing group:name:version, assumes a .jar extension
        :return: Decoded IO containing the artifact bytes.
        """
        _headers: Dict[str, str] = {
            _Headers.ACCEPT: _ContentTypes.OCTET_STREAM,
            _Headers.AUTHORIZATION: auth_header,
        }

        _path = _get_maven_coordinate_path(repository_rid, maven_coordinate)

        _response = self._request(  # type: ignore
            "GET",
            self._uri + _path,
            headers=_headers,
            stream=True,
        )

        return _to_rich_body(_response)

    def put_maven_artifact(
        self, *, auth_header: str, repository_rid: str, maven_coordinate: str, content: IO[bytes]
    ) -> None:
        """
        Saves a binary file-like object to a maven artifact.
        :param auth_header: User auth header with permission to read the repository.
        :param repository_rid: Rid of repository containing the artifact.
        :param maven_coordinate: Maven coordinate containing group:name:version, assumes a .jar extension
        :param content: file-like object to be uploaded.
        :return: Response from the PUT request.
        """
        _headers: Dict[str, str] = {
            _Headers.ACCEPT: "application/json",
            _Headers.CONTENT_TYPE: _ContentTypes.OCTET_STREAM,
            _Headers.AUTHORIZATION: auth_header,
        }

        _params: Dict[str, str] = {}

        _path = _get_maven_coordinate_path(repository_rid, maven_coordinate)

        self._request(  # type: ignore
            "PUT",
            self._uri + _path,
            params=_params,
            headers=_headers,
            data=content,
        )

    def start_blob_upload(
        self, auth_header: str, repository_rid: str, docker_name: str, blob_digest: Optional[str]
    ) -> uuid.UUID:
        """
        Prepare for a blob upload to the docker registry.
        Reference: https://distribution.github.io/distribution/spec/api/#starting-an-upload
        :param auth_header: User auth header.
        :param repository_rid: Rid of repository where the blob should be uploaded to.
        :param docker_name: Path-like namespace of the blob.
        :param blob_digest: Optional digest of the blob.
        :return: The upload identifier UUID
        """
        _headers: Dict[str, str] = {
            _Headers.AUTHORIZATION: auth_header,
            _Headers.USERNAME: repository_rid,
        }

        _params: Dict[str, str] = {}
        if blob_digest:
            _params["digest"] = blob_digest

        _path = f"/docker/release/v2/{docker_name}/blobs/uploads/"

        _response = self._request(
            "POST",
            self._uri + _path,
            params=_params,
            headers=_headers,
        )

        upload_uuid = _response.headers.get(_Headers.DOCKER_UPLOAD_UUID)
        return uuid.UUID(upload_uuid)

    def upload_blob_complete(
        self,
        auth_header: str,
        repository_rid: str,
        docker_name: str,
        upload_identifier: uuid.UUID,
        blob_digest: Optional[str],
        size: Optional[int],
        data: IO[bytes],
    ) -> str:
        """
        Finish a blob upload to the docker registry.
        Reference: https://distribution.github.io/distribution/spec/api/#monolithic-upload
        Consumes:
        - _ContentTypes.DOCKER_MANIFEST_V2_JSON
        - _ContentTypes.DOCKER_IMAGE_TAR_GZIP
        - _ContentTypes.DOCKER_CONTAINER_IMAGE_V1_JSON
        - _ContentTypes.OCTET_STREAM

        :param auth_header: User auth header.
        :param repository_rid: Rid of repository where the blob should be uploaded to.
        :param docker_name: Path-like namespace of the blob.
        :param upload_identifier: UUID of the upload.
        :param blob_digest: Optional digest of the blob (example: "sha256:...").
        :param size: Optional content size in bytes to be sent as content length.
        :param data: file-like object to be uploaded.
        :return: The digest for the blob upload.
        """
        _headers: Dict[str, str] = {
            _Headers.CONTENT_TYPE: _ContentTypes.OCTET_STREAM,
            _Headers.AUTHORIZATION: auth_header,
            _Headers.USERNAME: repository_rid,
        }

        if size:
            _headers[_Headers.CONTENT_LENGTH] = str(size)

        _params = {}
        if blob_digest:
            _params["digest"] = blob_digest

        _path = f"/docker/release/v2/{docker_name}/blobs/uploads/{upload_identifier}"

        _response = self._request(
            "PUT",
            self._uri + _path,
            params=_params,
            headers=_headers,
            data=data,
        )

        return _response.headers.get(_Headers.DOCKER_CONTENT_DIGEST)  # type: ignore

    def get_blob(self, auth_header: str, repository_rid: str, docker_name: str, docker_digest: str) -> Any:
        """
        Get a blob, such as a layer, from the docker registry.
        Reference: https://distribution.github.io/distribution/spec/api/#pulling-a-layer
        :param auth_header: User auth header.
        :param repository_rid: Rid of repository where the blob should be downloaded from.
        :param docker_name: Path-like namespace of the blob.
        :param docker_digest: Digest of the blob.
        :return: The blob data.
        """
        _headers: Dict[str, str] = {
            _Headers.AUTHORIZATION: auth_header,
            _Headers.USERNAME: repository_rid,
        }

        _path = f"/docker/release/v2/{docker_name}/blobs/{docker_digest}"

        _response = self._request(
            "GET",
            self._uri + _path,
            headers=_headers,
            stream=True,
        )

        return _to_rich_body(_response)


def _get_maven_coordinate_path(repository_rid: str, maven_coordinate: str) -> str:
    extension_split = maven_coordinate.split("@")
    parsed_maven_coordinate = extension_split[0].split(":")
    file_extension = extension_split[1] if len(extension_split) > 1 else "jar"
    _path_params: Dict[str, str] = {
        "repo_rid": repository_rid,
        "group": parsed_maven_coordinate[0].replace(".", "/"),
        "name": parsed_maven_coordinate[1],
        "version": parsed_maven_coordinate[2],
        "extension": file_extension,
    }
    _path = "/repositories/{repo_rid}/contents/release/maven/{group}/{name}/{version}/{name}-{version}.{extension}"
    return _path.format(**_path_params)


def _to_rich_body(response: "Response") -> RichBody:
    body = response.raw

    repository_rid = response.headers[_Headers.FOUNDRY_ARTIFACTS_REPOSITORY]
    content_type = response.headers[_Headers.CONTENT_TYPE]
    try:
        content_length = int(response.headers[_Headers.CONTENT_LENGTH])
    except (ValueError, TypeError):
        content_length = None
    checksum = response.headers[_Headers.FOUNDRY_CHECKSUM_SHA256]

    return RichBody(
        body=body,
        repository_rid=repository_rid,
        content_type=content_type,
        content_length=content_length,
        checksum=checksum,
    )
