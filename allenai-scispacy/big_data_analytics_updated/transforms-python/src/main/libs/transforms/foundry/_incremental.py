# Copyright 2017 Palantir Technologies, Inc.

from transforms import _errors


def tx_specs(
    foundry,
    semantic_version,
    snapshot_inputs,
    input_txns,
    output_txns,
    allow_retention,
    require_incremental,
):
    resolution_result = foundry._service_provider.incremental_resolver().resolve_incremental_transaction_specs(
        foundry.auth_header,
        snapshot_inputs,
        input_txns,
        output_txns,
        allow_retention,
        semantic_version,
    )
    is_incremental = resolution_result["isIncremental"]

    if (
        (not is_incremental)
        and require_incremental
        and (not resolution_result["ignoreRequireIncremental"])
    ):
        raise _errors.RequiredIncrementalTransform(
            resolution_result["notIncrementalReason"]
        )

    return (
        resolution_result["inputTransactionSpecs"],
        resolution_result["outputTransactionSpecs"],
        is_incremental,
    )


def catalog_input_resolution(foundry, input_dataset_rid, incremental_input_resolution):
    return foundry._service_provider.incremental_resolver().resolve_incremental_input(
        foundry.auth_header, input_dataset_rid, incremental_input_resolution
    )
