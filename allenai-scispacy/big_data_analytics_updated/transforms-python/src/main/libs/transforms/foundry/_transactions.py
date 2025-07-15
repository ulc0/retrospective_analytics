# Copyright 2017 Palantir Technologies, Inc.
from transforms._utils import memoized


@memoized
def has_modified_files(
    foundry, dataset_rid, end_transaction_rid, previous_end_transaction_rid
):
    if end_transaction_rid is None:
        return False
    input_changes_service = foundry._service_provider.input_changes()
    return input_changes_service.is_transaction_range_updating_files(
        foundry.auth_header,
        dataset_rid,
        end_transaction_rid,
        previous_end_transaction_rid,
    )


@memoized
def has_removed_files(
    foundry, dataset_rid, end_transaction_rid, previous_end_transaction_rid
):
    if end_transaction_rid is None:
        return False
    input_changes_service = foundry._service_provider.input_changes()
    return input_changes_service.is_transaction_range_deleting_files(
        foundry.auth_header,
        dataset_rid,
        end_transaction_rid,
        previous_end_transaction_rid,
    )


@memoized
def transaction_after(foundry, rid, oldest_txrid, newest_txrid, view_transaction):
    """Returns next non aborted transaction after oldest_txrid

    Args:
        rid (str): dataset rid
        oldest_txrid (str): oldest transaction that you want to find descendant of
        newest_txrid (str): newest transaction that you want to start paging from
        view_transaction (str): view end transaction that the transactions should belong to
    """

    txn_iter = foundry._service_provider.catalog().get_reverse_transactions_in_view(
        foundry.auth_header,
        rid,
        view_transaction,
        newest_txrid,
        oldest_transaction_rid=oldest_txrid,
    )
    txns = list(txn_iter)
    num_txns = len(txns)
    if num_txns == 0:
        return None
    # If there's only one transaction returned it must be txrid and end_txrid is the result we want
    # Happens when the incremental change in dataset is 1 transaction so prev + 1 is end_txrid
    if num_txns == 1:
        return newest_txrid
    return txns[-2].get("rid")


def transactions_for_type(transactions, txn_type):
    return (txn for txn in transactions if txn["type"] == txn_type)
