# Copyright 2017 Palantir Technologies, Inc.
# pylint: disable=too-many-lines
import json
import logging
import sys
from typing import Any, Dict, List, Optional, Tuple, Union

from py4j.protocol import Py4JJavaError

from transforms._tx_spec import _TxSpec

from .. import _java_utils as j
from .. import _java_version, _utils

log = logging.getLogger(__name__)


def _get_services_config_factory(service_config):
    jvm = j.get_or_create_gateway().jvm
    jcls = jvm.Class.forName(
        "com.palantir.conjure.java.api.config.service.ServicesConfigBlock"
    )
    jservices_block = j.object_mapper().readValue(json.dumps(service_config), jcls)
    return (
        jvm.com.palantir.conjure.java.api.config.service.ServiceConfigurationFactory.of(
            jservices_block
        )
    )


class ServiceProvider(object):
    def __init__(self, service_config: Dict[str, Any]):
        """Create a ServiceProvider.

        Arguments:
            service_config (dict): the service discovery config
        """
        self._discovery_config = service_config
        jservice_factory = _get_services_config_factory(self._discovery_config)
        self._services_config = json.loads(
            j.object_mapper().writeValueAsString(jservice_factory.getAll())
        )

    @property
    def services_config(self):
        return self._services_config

    @property
    def service_discovery_config(self):
        return self._discovery_config

    @property
    def truststore_path(self):
        # These are both currently optionals
        return self._discovery_config.get("security", {}).get("trustStorePath")

    @property
    def truststore_type(self):
        # These are both currently optionals
        return self._discovery_config.get("security", {}).get("trustStoreType")

    def code_checks_result(self):
        return CodeChecksResultService(self._services_config["data-health"])

    def catalog(self):
        return CatalogService(self._services_config["catalog"])

    def cbac(self):
        return CbacMutationService(self._services_config["multipass"])

    def cbac_banner(self):
        return CbacBannerService(self._services_config["multipass"])

    def compass(self):
        return CompassService(self._services_config["compass"])

    def metadata(self):
        return MetadataService(self._services_config["metadata"])

    def path(self) -> "PathService":
        # Path service uses the Compass configs
        return PathService(self._services_config["compass"])

    def organization_and_project(self):
        # OrganizationAndProject service uses the Compass configs
        return OrganizationAndProjectService(self._services_config["compass"])

    def schema(self):
        # Schema service uses the Metadata configs
        return SchemaService(self._services_config["metadata"])

    def semantic_version(self):
        jservice_factory = _get_services_config_factory(self._discovery_config)
        return SemanticVersion(jservice_factory)

    def input_changes(self):
        jservice_factory = _get_services_config_factory(self._discovery_config)
        return InputChanges(jservice_factory)

    def incremental_resolver(self):
        jservice_factory = _get_services_config_factory(self._discovery_config)
        return IncrementalResolver(jservice_factory)


class EventLogger(j.JavaProxy):
    PRECONDITIONS_START = "preconditions-start"
    PRECONDITIONS_END = "preconditions-end"
    POSTCONDITIONS_START = "postconditions-start"
    POSTCONDITIONS_END = "postconditions-end"
    DISCOVER_TRANSFORMS_START = "discover-transforms-start"
    DISCOVER_TRANSFORMS_END = "discover-transforms-end"

    def __init__(self) -> None:
        super().__init__(
            self._jvm.com.palantir.transforms.lang.python.transforms.PythonTransformsSparkModule
        )

    def emit_progress_event(
        self, safe_progress_name: str, job_rid, registration_rid, version_id
    ):
        j_registration_rid = j.to_optional(registration_rid, j.to_rid)
        j_version_id = j.to_optional(version_id)
        self._proxy.emitProgressEvent(
            safe_progress_name, j.to_rid(job_rid), j_registration_rid, j_version_id
        )


class Service(j.JavaProxy):
    """Wrapper around JAXRSClient services."""

    def __init__(self, config):
        """Construct the Java Service from the given config dictionary.

        Args:
            config (dict): The com.palantir.conjure.java.api.config.service.ServiceConfiguration dictionary.
        """
        jconfig_cls = self._jvm.Class.forName(
            "com.palantir.conjure.java.api.config.service.ServiceConfiguration"
        )
        jservice_conf = {
            "service": j.object_mapper().readValue(json.dumps(config), jconfig_cls)
        }

        juser_agent = self._jvm.com.palantir.conjure.java.api.config.service.UserAgent.of(
            self._jvm.com.palantir.conjure.java.api.config.service.UserAgent.Agent.of(
                "transforms-python-"
                + ("driver" if _utils.is_spark_driver() else "worker"),
                _java_version.__version__,
            )
        )

        jclient_factory = self._jvm.com.palantir.transforms.lang.python.module.ClientFactories.fromServiceConfigBlock(
            jservice_conf, juser_agent
        )

        jservice = jclient_factory.create(
            self._jvm.Class.forName(self.SERVICE_CLASS), "service"
        )
        super().__init__(jservice)

    @property
    def SERVICE_CLASS(self):
        """Return the Java class name for the service to instantiate."""
        raise NotImplementedError("To be implemented by subclass")


class CodeChecksResultService(Service):
    """Wrapper around a `com.palantir.foundry.datahealth.api.codechecks.CodeChecksResultServiceBlocking`."""

    SERVICE_CLASS = (
        "com.palantir.foundry.datahealth.api.codechecks.CodeChecksResultServiceBlocking"
    )

    def publish_results(
        self, auth_header: str, registration_rid: str, job_rid: str, results: Any
    ):
        jrequest_class = self._jvm.Class.forName(
            "com.palantir.foundry.datahealth.api.codechecks.PutCodeCheckResultsRequest"
        )
        put_request_dict = {"results": results}
        jrequest = j.object_mapper().readValue(
            json.dumps(put_request_dict), jrequest_class
        )
        jregistration_rid = self._jvm.com.palantir.foundry.datahealth.api.codechecks.CodeChecksRegistrationRid.valueOf(
            registration_rid
        )
        jjob_rid = (
            self._jvm.com.palantir.foundry.datahealth.api.codechecks.JobRid.valueOf(
                job_rid
            )
        )

        self._proxy.publishResults(
            j.to_auth_header(auth_header), jregistration_rid, jjob_rid, jrequest
        )


class CatalogService(Service):
    """Wrapper around a `com.palantir.foundry.catalog.api.CatalogServiceBlocking`."""

    SERVICE_CLASS = "com.palantir.foundry.catalog.api.CatalogServiceBlocking"

    class TransactionStatus(j.JavaEnum):
        def __init__(self):
            super(CatalogService.TransactionStatus, self).__init__(
                j.get_or_create_gateway().jvm.com.palantir.foundry.catalog.api.transactions.TransactionStatus
            )

    def create_branch(self, auth_header, dataset_rid, branch_id, parent_branch=None):
        """Python wrapper around `CatalogService#createBranch`.

        Args:
            auth_header (str): The auth header to use talking to the service.
            dataset_rid (str): The dataset rid on which to create the branch.
            branch_id (str): The id of the branch to create.
            parent_branch (str, optional): The id of the parent branch. If not specified then a root branch is created.

        Returns:
            str: The rid of the newly created branch.
        """
        if parent_branch is None:
            jreq = (
                self._jvm.com.palantir.foundry.catalog.api.branches.CreateBranchRequest.builder().build()
            )
        else:
            jreq = (
                self._jvm.com.palantir.foundry.catalog.api.branches.CreateBranchRequest.builder()
                .parentRef(j.to_transaction_ref(parent_branch))
                .parentBranchId(parent_branch)
                .build()
            )

        return (
            self._proxy.createBranch2(
                j.to_auth_header(auth_header), j.to_rid(dataset_rid), branch_id, jreq
            )
            .getRid()
            .toString()
        )

    def get_branches(self, auth_header, dataset_rid):
        """Python wrapper around `CatalogService#getBranches`.

        Args:
            auth_header (str): The auth header to use talking to the service.
            dataset_rid (str): The dataset rid on which to get the branch.

        Returns:
            Set[str]: A list of branches found in the `Catalog`.
        """
        return self._proxy.getBranches(
            j.to_auth_header(auth_header), j.to_rid(dataset_rid)
        )

    def create_dataset(self, auth_header, path, filesystem_id):
        """Python wrapper around `CatalogService#createDataset`.

        Args:
            auth_header (str): The auth header to use talking to the service.
            path (str): The Compass path at which to create the dataset.
            filesystem_id (str): The filesystem id to use.

        Returns:
            str: The created dataset resource identifier.
        """
        jreq = (
            self._jvm.com.palantir.foundry.catalog.api.datasets.CreateDatasetRequest.builder()
            .fileSystemId(filesystem_id)
            .path(path)
            .build()
        )
        jdataset = self._proxy.createDataset(j.to_auth_header(auth_header), jreq)
        return jdataset.getRid().toString()

    # pylint: disable=unused-argument
    def get_dataset_view_files(
        self,
        auth_header: str,
        dataset_rid: str,
        end_ref: str,
        start_txrid: Optional[str] = None,
        include_open: bool = False,
        allow_deleted: bool = False,
        page_size: int = 10000,
    ):
        """Python wrapper around `CatalogService#getDatasetViewFiles`.

        Returns the files in the dataset view ending at the given endRef, or, if a startTransactionRid is given,
        the view starting at the startTransactionRid and ending at the endRef. See `Catalog#getDatasetViewRange`
        for how the effective bounds of the view are computed.

        If `logicalPath` is absent, returns all files in the view. If `logicalPath` matches a file exactly,
        returns just that file. Otherwise, returns all files in the "directory" of "logicalPath":
        (a slash is added to the end of logicalPath if necessary and a prefix-match is performed). Transactions
        must be `TransactionStatus#COMMITTED` for inclusion in the view unless `includeOpenTransaction` is
        set to `true`, in which case the end transaction may be either `TransactionStatus#COMMITTED` or
        `TransactionStatus#OPEN`.

        If `pageSize` is specified, returns that many entries in the resulting `FileResourcesPage`; otherwise
        returns all entries.

        If `pageStartLogicalPath` is specified, returns a `FileResourcesPage` containing entries starting at
        that given `pageStartLogicalPath`; otherwise starts at the beginning of the file listing.
        Paging occurs after filtering on `pageStartLogicalPath`.

        Args:
            auth_header (str): The auth header to use talking to the service.
            dataset_rid (str): The dataset resource identifier.
            end_ref (str): The view's end branch ID or transaction rid.
            start_txrid (str, optional): The view's start transaction rid.
            include_open (bool, optional): Include any open transactions in the view.
            allow_deleted (bool, optional): Allow transactions whose files are deleted in the results.
            page_size (int): The number of files to return per page.

        Yields:
            json representation of j.com.palantir.foundry.catalog.api.files.FileResource

        Throws:
            If the dataset does not exist or the ref cannot be resolved to a branch or transaction.
        """
        paged_start_path: Optional[str] = None
        while True:
            filespage = self._proxy.getDatasetViewFiles2(
                j.to_auth_header(auth_header),
                j.to_rid(dataset_rid),
                j.to_transaction_ref(end_ref),
                page_size,
                j.to_optional(start_txrid, j.to_rid),
                j.to_optional(None),
                j.to_optional(paged_start_path),
                j.to_optional(include_open),
                j.to_optional(None),
            )

            for file_resource in json.loads(
                j.object_mapper().writeValueAsString(filespage.getValues())
            ):
                yield file_resource

            if not filespage.getNextPageToken().isPresent():
                break

            paged_start_path = filespage.getNextPageToken().get()

    def get_dataset_view_range(
        self,
        auth_header: str,
        dataset_rid: str,
        end_ref: str,
        start_txrid: Optional[str] = None,
        include_open: bool = False,
    ) -> Union[Tuple[str, str], None]:
        """Python wrapper around `CatalogService#getDatasetViewRange`.

        Returns the effective start and end transaction rids of the view ending at the given endRef, or, if a
        startTransactionRid is given, the view starting at the startTransactionRid and ending at the endRef.

        The returned `TransactionRange#getStartTransactionRid` and `TransactionRange#getEndTransactionRid`
        are the effective start and end transactions used to compute the view; for example, when a SNAPSHOT transaction
        exists in the given transaction range then the returned effective start transaction may be later than the
        requested start transaction. The start and end transactions will always be `TransactionStatus#COMMITTED`,
        unless `include_open` is set to `True`, in which case the end transaction may be either
        `TransactionStatus#COMMITTED` or `TransactionStatus#OPEN`.

        Note: The behavior of this method is undefined if `start_txrid` and `end_ref` are not on the same root-to-leaf
        path.

        Args:
            auth_header (str): The auth header to use talking to the service.
            dataset_rid (str): The dataset resource identifier.
            end_ref (str): The view's end branch ID or transaction rid.
            start_txrid (str, optional): The view's start transaction rid.
            include_open (bool, optional): Include any open transactions in the view.

        Returns:
            (str, str) or None: The range of transactions comprising the view; or None when no COMMITTED transactions
                exist in the dataset, or when no OPEN or COMMITTED transactions exist and `include_open` is `True`.

        Throws:
            If the dataset does not exist or the ref cannot be resolved to a branch or transaction.
        """
        jtransaction_range = self._proxy.getDatasetViewRange2(
            j.to_auth_header(auth_header),
            j.to_rid(dataset_rid),
            j.to_transaction_ref(end_ref),
            j.to_optional(start_txrid, j.to_rid),
            j.to_optional(include_open),
        )
        if not jtransaction_range.isPresent():
            return None
        return (
            jtransaction_range.get().getStartTransactionRid().toString(),
            jtransaction_range.get().getEndTransactionRid().toString(),
        )

    def start_transaction(
        self, auth_header: str, dataset_rid: str, branch_id: str
    ) -> str:
        """Python wrapper around `CatalogService#startTransaction`.

        Args:
            auth_header (str): The auth header to use talking to the service.
            dataset_rid (str): The dataset resource identifier.
            branch_id (str): The id of the branch on which to open the transaction.

        Returns:
            str: The resource identifier of the created transaction.
        """
        jreq = (
            self._jvm.com.palantir.foundry.catalog.api.transactions.StartTransactionRequest.builder()
            .branchId(branch_id)
            .build()
        )
        jtransaction = self._proxy.startTransaction(
            j.to_auth_header(auth_header), j.to_rid(dataset_rid), jreq
        )
        return jtransaction.getRid().toString()

    def abort_transaction(
        self,
        auth_header: str,
        dataset_rid: str,
        txrid: str,
        record: Dict[Any, Any] = None,
    ) -> None:
        """Python wrapper around `CatalogService#abortTransaction`.

        Args:
            auth_header (str): The auth header to use talking to the service.
            dataset_rid (str): The dataset resource identifier.
            txrid (str): The transaction rid to abort.
            record (dict, optional): The record to set on the transaction.
        """
        jreq = (
            self._jvm.com.palantir.foundry.catalog.api.transactions.CloseTransactionRequest.builder()
            .record(record or {})
            .build()
        )
        self._proxy.abortTransaction(
            j.to_auth_header(auth_header), j.to_rid(dataset_rid), j.to_rid(txrid), jreq
        )

    def _convert_modification_type(self, modification_type: str):
        if modification_type == "UPDATE":
            return (
                self._jvm.com.palantir.foundry.catalog.api.files.ModificationType.UPDATE
            )
        if modification_type == "DELETE":
            return (
                self._jvm.com.palantir.foundry.catalog.api.files.ModificationType.DELETE
            )
        raise ValueError(
            f'Modification type "{modification_type}" is unsupported.'
            ' Supported types are "UPDATE" and "DELETE".'
        )

    def _convert_transaction_provenance(self, provenances, non_catalog_provenances):
        """Python function to create TransactionProvenance objects.

        Args:
            provenances (list of dicts): list of records matching the ProvenanceRecord schema.
            non_catalog_provenances (list of dicts): list of records matching the NonCatalogProvenanceRecord schema.
        """
        provenance_records = []
        for prov in provenances:
            jpr = (
                self._jvm.com.palantir.foundry.catalog.api.transactions.ProvenanceRecord.builder()
            )
            jpr.datasetRid(j.to_rid(prov["datasetRid"]))
            jpr.schemaBranchId(j.to_optional(prov.get("schemaBranchId")))
            jpr.schemaVersionId(j.to_optional(prov.get("schemaVersionId")))

            if "transactionRange" in prov:
                jtrange = (
                    self._jvm.com.palantir.foundry.catalog.api.transactions.TransactionRange.builder()
                )
                jtrange.startTransactionRid(
                    j.to_rid(prov["transactionRange"]["startTransactionRid"])
                )
                jtrange.endTransactionRid(
                    j.to_rid(prov["transactionRange"]["endTransactionRid"])
                )
                jpr.transactionRange(jtrange.build())

            provenance_records.append(jpr.build())

        non_catalog_provenance_records = []
        for prov in non_catalog_provenances:
            jpr = (
                self._jvm.com.palantir.foundry.catalog.api.transactions.NonCatalogProvenanceRecord.builder()
            )
            jpr.resources(j.to_set(prov["resources"], j.to_rid))
            jpr.resources(j.to_set(prov["assumedMarkings"], j.to_marking_id))
            jpr.dependencyType(
                j.to_optional(prov["dependencyType"], j.to_dependency_type)
            )

            non_catalog_provenance_records.append(jpr.build())

        return (
            self._jvm.com.palantir.foundry.catalog.api.transactions.TransactionProvenance.builder()
            .provenanceRecords(j.to_set(provenance_records))
            .nonCatalogProvenanceRecords(j.to_set(non_catalog_provenance_records))
            .build()
        )

    def commit_transaction(
        self,
        auth_header,
        dataset_rid,
        txrid,
        record=None,
        provenances=None,
        non_catalog_provenances=None,
    ):
        """Python wrapper around `CatalogService#commitTransaction`.

        Args:
            auth_header (str): The auth header to use talking to the service.
            dataset_rid (str): The dataset resource identifier.
            txrid (str): The transaction rid to commit
            record (dict, optional): The record to set on the transaction.
            provenances (list of dicts, optional): Provenance records to set on the transaction.
            non_catalog_provenances (list of dicts, optional): Non-catalog provenance records to set on the transaction.
        """
        jreq = self._jvm.com.palantir.foundry.catalog.api.transactions.CloseTransactionRequest.builder().record(
            record or {}
        )

        provenances = provenances or []
        non_catalog_provenances = non_catalog_provenances or []
        transaction_provenance = self._convert_transaction_provenance(
            provenances, non_catalog_provenances
        )
        jreq = jreq.provenance(transaction_provenance)

        jreq = jreq.build()

        self._proxy.commitTransaction(
            j.to_auth_header(auth_header), j.to_rid(dataset_rid), j.to_rid(txrid), jreq
        )

    def set_transaction_type(self, auth_header, dataset_rid, txrid, tx_type):
        """Python wrapper around `CatalogService#setTransactionType`.

        Args:
            auth_header (str): The auth header to use talking to the service.
            dataset_rid (str): The dataset resource identifier.
            txrid (str): The transaction rid to commit
            tx_type (str): The transaction type.
        """
        # XXX: Should create constants for these values
        tx_types = {
            "UPDATE": self._jvm.com.palantir.foundry.catalog.api.transactions.TransactionType.UPDATE,
            "APPEND": self._jvm.com.palantir.foundry.catalog.api.transactions.TransactionType.APPEND,
            "DELETE": self._jvm.com.palantir.foundry.catalog.api.transactions.TransactionType.DELETE,
            "SNAPSHOT": self._jvm.com.palantir.foundry.catalog.api.transactions.TransactionType.SNAPSHOT,
        }
        if tx_type not in tx_types:
            raise ValueError(
                f"Invalid transaction type {tx_type}, must be one of {tx_types.keys()}"
            )

        self._proxy.setTransactionType(
            j.to_auth_header(auth_header),
            j.to_rid(dataset_rid),
            j.to_rid(txrid),
            tx_types[tx_type],
        )

    def get_reverse_transactions(
        self,
        auth_header: str,
        dataset_rid: str,
        start_ref: str,
        include_open_txns: bool,
        page_size: int = 50,
        end_transaction_rid: Optional[str] = None,
    ):
        """Returns the sequence of ancestor transactions of the specified start_ref in a dataset.

        Args:
            auth_header (str): The auth header to use talking to the service.
            dataset_rid (str): The dataset resource identifier.
            start_ref (str): The start ref, rid or branch.
            include_open_txns (bool): If including also non-committed transaction
            page_size (int, optional): size of page for paging transactions
            end_transaction_rid (str, optional): The inclusive end transaction of the request.

        Yields:
            A com.palantir.foundry.catalog.api.transactions.SecuredTransaction json object dictionary.
        """
        paged_start_ref = start_ref
        while True:
            txnpage = self._proxy.getReverseTransactions2(
                j.to_auth_header(auth_header),
                j.to_rid(dataset_rid),
                j.to_transaction_ref(paged_start_ref),
                page_size,
                j.to_optional(end_transaction_rid),
                j.to_optional(include_open_txns),
                j.to_optional(None),
            )
            for secured_txn in txnpage.getValues():
                yield json.loads(j.object_mapper().writeValueAsString(secured_txn))

            if not txnpage.getNextPageToken().isPresent():
                break

            paged_start_ref = txnpage.getNextPageToken().get().toString()

    def get_reverse_transactions_in_view(
        self,
        auth_header: str,
        dataset_rid: str,
        view_transaction_rid: str,
        newest_transaction_rid: str,
        page_size: int = 50,
        oldest_transaction_rid: Optional[str] = None,
    ):
        """Returns the sequence of ancestor transactions of the specified newest_transaction_rid in a dataset
            that belongs to same view as view_transaction_rid.

        Args:
            auth_header (str): The auth header to use talking to the service.
            dataset_rid (str): The dataset resource identifier.
            view_transaction_rid (str): The view end transaction that transactions belong to.
            newest_transaction_rid (str): The transaction to start iterating from.
            page_size (int): number of records to return per page
            oldest_transaction_rid (str): The inclusive end transaction of the request

        Yields:
            A com.palantir.foundry.catalog.api.transactions.SecuredTransaction json object dictionary.
        """
        page_start_transaction = newest_transaction_rid
        while True:
            txnpage = self._proxy.getReverseTransactionsInView(
                j.to_auth_header(auth_header),
                j.to_rid(dataset_rid),
                j.to_rid(view_transaction_rid),
                j.to_rid(page_start_transaction),
                page_size,
                j.to_optional(oldest_transaction_rid, j.to_rid),
            )
            for secured_txn in txnpage.getValues():
                yield json.loads(j.object_mapper().writeValueAsString(secured_txn))

            if not txnpage.getNextPageToken().isPresent():
                break

            page_start_transaction = txnpage.getNextPageToken().get().toString()

    def add_files_to_delete_transaction(
        self, auth_header: str, dataset_rid: str, txrid: str, files_paths: List[str]
    ) -> None:
        """Python wrapper around `CatalogService#addFilesToDeleteTransaction`.

        Args:
            auth_header (str): The auth header to use talking to the service.
            dataset_rid (str): The dataset resource identifier.
            txrid (str): The rid of the delete transaction to add files to delete to.
            files_paths (list of str): List of paths of files to delete.
        """
        jreq = self._jvm.com.palantir.foundry.catalog.api.files.LogicalFilesRequest.of(
            files_paths
        )
        self._proxy.addFilesToDeleteTransaction(
            j.to_auth_header(auth_header), j.to_rid(dataset_rid), j.to_rid(txrid), jreq
        )


class CbacMutationService(Service):
    """Wrapper around a `com.palantir.multipass.api.cbac.CbacMutationServiceBlocking.`"""

    SERVICE_CLASS = "com.palantir.multipass.api.cbac.CbacMutationServiceBlocking"

    def parse_classification_strings(self, auth_header, classification_strings):
        """Python wrapper around `CbacMutationService#parseClassificationStrings`.

        Args:
            auth_header (str): The auth header to use talking to the service.
            classification_strings (list of str): The CBAC classification strings to parse, e.g. "MU".

        Returns:
            dict: A json representation of a com.palantir.multipass.api.cbac.ParseClassificationStringsResponse.
                Contains two fields, "result", a map from an input classification string to the parsed set of marking
                strings, and "errors", a map from each invalid input classification string to a human-readable error
                message.
        """
        jreq = self._jvm.com.palantir.multipass.api.cbac.ParseClassificationStringsRequest.of(
            j.to_set(classification_strings)
        )
        response = self._proxy.parseClassificationStrings(
            j.to_auth_header(auth_header), jreq
        )
        return json.loads(j.object_mapper().writeValueAsString(response))


class CbacBannerService(Service):
    """Wrapper around a `com.palantir.multipass.api.cbac.CbacBannerServiceBlocking.`"""

    SERVICE_CLASS = "com.palantir.multipass.api.cbac.CbacBannerServiceBlocking"

    class ClassificationBannerDisplayType(j.JavaEnum):
        def __init__(self):
            super(CbacBannerService.ClassificationBannerDisplayType, self).__init__(
                j.get_or_create_gateway().jvm.com.palantir.multipass.api.cbac.ClassificationBannerDisplayType
            )

    def get_banner_for_markings(
        self, auth_header: str, display_type: Optional[str], marking_ids: List[str]
    ):
        """Python wrapper around `CbacBannerService#getBannerForMarkings`.

        Args:
            auth_header (str): The auth header to use talking to the service.
            display_type (str, optional): Specifies which value to use for markings in a banner.
            marking_ids (list of str): The marking IDs to convert to a banner

        Returns:
            dict: A json representation of a com.palantir.multipass.api.cbac.ClassificationBanner.
                The field "classificationString" contains the classification banner text.
        """
        response = self._proxy.getBannerForMarkings(
            j.to_auth_header(auth_header),
            j.to_optional(display_type, self.ClassificationBannerDisplayType().get),
            j.to_set(marking_ids, j.to_multipass_marking_id),
        )
        return json.loads(j.object_mapper().writeValueAsString(response))


class CompassService(Service):
    """Wrapper around a `com.palantir.compass.api.service.CompassServiceBlocking.`"""

    SERVICE_CLASS = "com.palantir.compass.api.service.CompassServiceBlocking"

    class Decoration(j.JavaEnum):
        def __init__(self):
            super(CompassService.Decoration, self).__init__(
                j.get_or_create_gateway().jvm.com.palantir.compass.api.Decoration
            )

    def create_folder(self, auth_header: str, name: str, parent_rid: str) -> str:
        """Create a folder in Compass.

        Args:
            auth_header (str): The auth header to use talking to the service.
            name (str): The name of the Compass folder.
            parent_rid (str): The resource identifier of the parent folder.

        Returns:
            str: The resource identifier of the created folder.
        """
        jreq = self._jvm.com.palantir.compass.api.request.CreateFolderRequest.of(
            name, j.to_rid(parent_rid)
        )
        return (
            self._proxy.createFolder(
                j.to_auth_header(auth_header), j.to_optional(None), jreq
            )
            .getRid()
            .toString()
        )

    def get_resource(
        self, auth_header: str, rid: str, decorations: Optional[List[str]] = None
    ):
        """Get a Compass resource by resource identifier.

        Args:
            auth_header (str): The auth header to use talking to the service.
            rid (str): The resource identifier to fetch.
            decorations (list of str): The decorations to include in the response.

        Returns:
            dict: A json representation of a com.palantir.compass.api.DecoratedResource
        """
        joptional_resource = self._proxy.getResource(
            j.to_auth_header(auth_header),
            j.to_rid(rid),
            j.to_set(set(self.Decoration().get(d) for d in decorations or [])),
            j.to_optional(False),
            j.to_set([]),
        )
        if not joptional_resource.isPresent():
            return None
        return json.loads(
            j.object_mapper().writeValueAsString(joptional_resource.get())
        )


class MetadataService(Service):
    """Wrapper around a `com.palantir.foundry.metadata.api.MetadataServiceBlocking`."""

    SERVICE_CLASS = "com.palantir.foundry.metadata.api.MetadataServiceBlocking"

    def get_transaction_metadata(
        self,
        auth_header: str,
        rid: str,
        txrid: str,
        namespace: str,
        version_id: Optional[str] = None,
    ):
        """Returns the metadata for the requested namespace on the given transaction.
        Args:
            rid (str): The resource identifier of the dataset.
            txrid (str): The resource identifier of the transaction.
            namespace (str): The metadata namespace.
            version_id (str, optional): Optionally the version ID of the metadata.
        Returns:
            dict: The metadata if it exists, or None
        """
        joptional_meta = self._proxy.getTransactionMetadata(
            j.to_auth_header(auth_header),
            j.to_rid(rid),
            j.to_rid(txrid),
            namespace,
            j.to_optional(version_id),
        )
        if not joptional_meta.isPresent():
            return None
        return json.loads(j.object_mapper().writeValueAsString(joptional_meta.get()))

    def get_dataset_view_metadata(
        self,
        auth_header: str,
        rid: str,
        branch_id: str,
        namespace: str,
        version_id: Optional[str] = None,
    ):
        """Returns the metadata for the requested namespace on the given branch.

        Args:
            rid (str): The resource identifier of the dataset.
            branch_id (str): The identifier of the branch.
            namespace (str): The metadata namespace.
            version_id (str, optional): Optionally the version ID of the metadata.

        Returns:
            dict: The metadata if it exists, or None
        """
        joptional_meta = self._proxy.getDatasetViewMetadata(
            j.to_auth_header(auth_header),
            j.to_rid(rid),
            branch_id,
            {namespace},
            j.to_optional(None),
            j.to_optional(version_id),
        )
        if not joptional_meta.isPresent():
            return None
        return json.loads(j.object_mapper().writeValueAsString(joptional_meta.get()))

    def put_transaction_metadata(
        self,
        auth_header: str,
        rid: str,
        txrid: str,
        namespace: str,
        metadata: Dict[Any, Any],
        replace: bool = False,
    ):
        """Puts the given metadata to the transaction.

        Args:
            rid (str): The resource identifier of the dataset.
            txrid (str): The resource identifier of the transaction.
            namespace (str): The metadata namespace.
            metadata (dict): The metadata to put.
            replace (bool, optional): If true, replace all metadata. Otherwise, merge.

        Returns:
            str: The version id of the written metadata.
        """
        return self._proxy.putTransactionMetadata(
            j.to_auth_header(auth_header),
            j.to_rid(rid),
            j.to_rid(txrid),
            namespace,
            replace,
            metadata,
        ).getVersionId()

    def put_dataset_view_metadata(
        self,
        auth_header: str,
        rid: str,
        branch_id: str,
        namespace: str,
        metadata: Dict[Any, Any],
        replace: bool = False,
    ):
        """Puts the given metadata to the branch.

        Args:
            rid (str): The resource identifier of the dataset.
            branch_id (str): The identifier of the branch.
            namespace (str): The metadata namespace.
            replace (bool, optional): If true, replace all metadata. Otherwise, merge.
            metadata (dict): The metadata to put

        Returns:
            str: The version id of the written metadata.
        """
        return self._proxy.putDatasetViewMetadata(
            j.to_auth_header(auth_header),
            j.to_rid(rid),
            branch_id,
            namespace,
            replace,
            j.to_optional(None),
            metadata,
        ).getVersionId()


class PathService(Service):
    """Wrapper around a `com.palantir.compass.api.service.PathServiceBlocking`."""

    SERVICE_CLASS = "com.palantir.compass.api.service.PathServiceBlocking"

    def get_resource_by_path(self, auth_header: str, path: str) -> Optional[str]:
        """Return the resource by its Compass path.

        Args:
            auth_header (str): The auth header to use talking to the service.
            path (str): A Compass path.

        Returns:
            str or None: The rid of the resource or `None` if it does not exist.
        """
        jdecorated_resource = self._proxy.getResourceByPath(
            j.to_auth_header(auth_header),
            j.to_optional(path),
            j.to_set([]),
            j.to_optional(False),
            j.to_set([]),
        )
        if not jdecorated_resource.isPresent():
            return None
        return jdecorated_resource.get().getRid().toString()


class OrganizationAndProjectService(Service):
    """Wrapper around a `com.palantir.compass.api.service.OrganizationAndProjectServiceBlocking`."""

    SERVICE_CLASS = (
        "com.palantir.compass.api.service.OrganizationAndProjectServiceBlocking"
    )

    def create_organization(self, auth_header: str, org_name: str) -> str:
        """Creates a compass organization and returns the rid of the created resource.
        Args:
            auth_header: The auth header for the request.
            org_name: The name of the organization to create.
        Returns:
            rid: The rid of the created resource
        """
        return (
            self._proxy.createOrganization(
                j.to_auth_header(auth_header),
                j.to_create_organization_request(org_name),
            )
            .getResource()
            .getRid()
            .toString()
        )

    def create_project(self, auth_header: str, org_rid: str, project_name: str) -> None:
        """Creates a compass project.
        Args:
            auth_header (str): The auth header for the request.
            org_rid (str): The rid of the organization to create the project under.
            project_name (str): The name of the project to create.
        """
        self._proxy.createProject(
            j.to_auth_header(auth_header),
            j.to_create_project_request(org_rid, project_name),
        )


class SchemaService(Service):
    """Wrapper around a `com.palantir.foundry.schemas.api.SchemaService`."""

    SERVICE_CLASS = "com.palantir.foundry.schemas.api.SchemaService"

    def get_schema(
        self,
        auth_header: str,
        dataset_rid: str,
        branch: str,
        end_txrid: Optional[str] = None,
        version: Optional[str] = None,
    ):
        """Return the schema for the given datasets.

        Args:
            auth_header (str): The auth header to use talking to the service.
            dataset_rid (str): The dataset resource identifier.
            branch (str): The branch to read from.
            end_txrid (str, optional): The end transaction to use.
            version (str, optional): The schema version to read.

        Returns:
            dict of com.palantir.foundry.schemas.api.VersionedFoundrySchema
        """
        joptional_schema = self._proxy.getSchema(
            j.to_auth_header(auth_header),
            j.to_rid(dataset_rid),
            branch,
            j.to_optional(end_txrid, j.to_rid),
            j.to_optional(version),
        )
        if not joptional_schema.isPresent():
            return None
        return json.loads(j.object_mapper().writeValueAsString(joptional_schema.get()))


class SemanticVersion(j.JavaProxy):
    def __init__(self, jservice_factory):
        juser_agent = self._jvm.com.palantir.conjure.java.api.config.service.UserAgent.of(
            self._jvm.com.palantir.conjure.java.api.config.service.UserAgent.Agent.of(
                "transforms-python-"
                + ("driver" if _utils.is_spark_driver() else "worker"),
                _java_version.__version__,
            )
        )
        jservice = self._jvm.com.palantir.transforms.lang.python.transforms.incremental.SemanticVersionService(
            jservice_factory, juser_agent
        )
        super().__init__(jservice)

    def get_semantic_version_change(
        self,
        auth_header: str,
        dataset_rid: str,
        transaction_rid: str,
        current_semantic_version: str,
    ) -> Dict[str, Union[str, None]]:
        semantic_version_change = self._proxy.getSemanticVersionChange(
            j.to_auth_header(auth_header),
            j.to_rid(dataset_rid),
            j.to_rid(transaction_rid),
            current_semantic_version,
        )

        return {
            "current_semantic_version": semantic_version_change.currentSemanticVersion(),
            "previous_semantic_version": semantic_version_change.previousSemanticVersion().orElse(
                None
            ),
            "has_semantic_version_changed": semantic_version_change.hasSemanticVersionChanged(),
        }

    def set_semantic_version(
        self, auth_header, dataset_rid, transaction_rid, semantic_version
    ):
        self._proxy.setSemanticVersion(
            j.to_auth_header(auth_header),
            j.to_rid(dataset_rid),
            j.to_rid(transaction_rid),
            semantic_version,
        )


class InputChanges(j.JavaProxy):
    def __init__(self, jservice_factory):
        juser_agent = self._jvm.com.palantir.conjure.java.api.config.service.UserAgent.of(
            self._jvm.com.palantir.conjure.java.api.config.service.UserAgent.Agent.of(
                "transforms-python-"
                + ("driver" if _utils.is_spark_driver() else "worker"),
                _java_version.__version__,
            )
        )
        jservice = self._jvm.com.palantir.transforms.lang.python.transforms.incremental.InputChangesService(
            jservice_factory, juser_agent
        )
        super().__init__(jservice)

    def is_transaction_range_deleting_files(
        self,
        auth_header,
        dataset_rid,
        end_transaction_rid,
        previous_end_transaction_rid,
    ):
        return self._proxy.isTransactionRangeDeletingFiles(
            j.to_auth_header(auth_header),
            j.to_rid(dataset_rid),
            j.to_rid(end_transaction_rid),
            j.to_optional(previous_end_transaction_rid, j.to_rid),
        )

    def is_transaction_range_updating_files(
        self,
        auth_header,
        dataset_rid,
        end_transaction_rid,
        previous_end_transaction_rid,
    ):
        return self._proxy.isTransactionRangeUpdatingFiles(
            j.to_auth_header(auth_header),
            j.to_rid(dataset_rid),
            j.to_rid(end_transaction_rid),
            j.to_optional(previous_end_transaction_rid, j.to_rid),
        )


class IncrementalResolver(j.JavaProxy):
    def __init__(self, jservice_factory):
        juser_agent = self._jvm.com.palantir.conjure.java.api.config.service.UserAgent.of(
            self._jvm.com.palantir.conjure.java.api.config.service.UserAgent.Agent.of(
                "transforms-python-"
                + ("driver" if _utils.is_spark_driver() else "worker"),
                _java_version.__version__,
            )
        )
        jservice = self._jvm.com.palantir.transforms.lang.python.transforms.incremental.IncrementalSpecsResolver.newReportingIncrementalSpecsResolver(  # pylint: disable=line-too-long
            jservice_factory, juser_agent
        )
        super().__init__(jservice)

    def _to_rid_safe(self, rid):
        """Some consumers of transforms-python (such as bellaso-python-lib) sometimes pass inputs with transaction rids
        which are not valid rids (e.g. None, or dummy_end). This is because these inputs are not Catalog inputs, so they
        do not have a transaction range, but the fields are expected.
        Calling com.palantir.ri.ResourceIdentifier.of(String rid) with these invalid rids throws an
        IllegalArgumentException. When this happens, we therefore catch the exception, and instead return a
        java.util.Optional.empty() to represent the fact that there is no transaction.
        """
        try:
            return j.to_optional(rid, j.to_rid)
        except Py4JJavaError as e:
            exc_type, exc_value, _tb = sys.exc_info()
            if (
                exc_type is not None
                and issubclass(exc_type, Py4JJavaError)
                and exc_value.java_exception is not None
            ):
                java_exc = exc_value.java_exception
                if java_exc.getClass().getSimpleName() == "IllegalArgumentException":
                    return self._jvm.java.util.Optional.empty()
            raise e

    def resolve_incremental_transaction_specs(
        self,
        auth_header,
        snapshot_inputs,
        input_dataset_to_transaction_range,
        output_dataset_to_transaction,
        allow_retention,
        semantic_version,
    ):
        resolution_result = self._proxy.resolveIncrementalTransactionSpecs(
            j.to_auth_header(auth_header),
            j.to_set(snapshot_inputs, j.to_rid),
            j.to_map(
                input_dataset_to_transaction_range,
                j.to_rid,
                lambda r: self._jvm.org.apache.commons.lang3.tuple.Pair.of(
                    self._to_rid_safe(r[0]),
                    self._to_rid_safe(r[1]),
                ),
            ),
            j.to_map(output_dataset_to_transaction, j.to_rid, j.to_rid),
            allow_retention,
            semantic_version,
        )

        def extract_specs(specs):
            return j.from_map(
                specs,
                key_extractor=lambda rid: rid.toString(),
                value_extractor=lambda x: _TxSpec(
                    j.from_optional(x.start(), lambda rid: rid.toString()),
                    j.from_optional(x.previous(), lambda rid: rid.toString()),
                    j.from_optional(x.end(), lambda rid: rid.toString()),
                ),
            )

        return {
            "inputTransactionSpecs": extract_specs(
                resolution_result.inputTransactionSpecs()
            ),
            "outputTransactionSpecs": extract_specs(
                resolution_result.outputTransactionSpecs()
            ),
            "isIncremental": resolution_result.isIncremental(),
            "ignoreRequireIncremental": j.from_optional(
                resolution_result.ignoreRequireIncremental()
            ),
            "notIncrementalReason": j.from_optional(
                resolution_result.notIncrementalReason()
            ),
        }

    def resolve_incremental_input(
        self, auth_header, input_dataset_rid, incremental_input_resolution
    ):
        result = self._proxy.resolveInput(
            j.to_auth_header(auth_header),
            j.to_rid(input_dataset_rid),
            json.dumps(incremental_input_resolution),
        )
        return {
            "isIncremental": result.isIncremental(),
            "priorEndTxnRid": j.from_optional(
                result.priorEndTxnRid(), lambda rid: rid.toString()
            ),
            "notIncrementalReason": j.from_optional(result.notIncrementalReason()),
        }
