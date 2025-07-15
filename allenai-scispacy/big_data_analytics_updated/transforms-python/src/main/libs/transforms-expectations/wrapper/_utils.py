from ._backend import GET_CURRENT_BACKEND, Backend


def value_col_is_valid(result) -> bool:
    """
    Polars and PySpark have different philosophies on how to handle when-then-else constructs.

    In pyspark, the expression "when(predicate).then(none).else(struct)" generates a column that contains
    None when predicate is true.

    In polars, the same expression will generate a struct with all fields equal to the value "NONE".
    As such, this method, detects that occurrence and swaps the value back the None instead of an invalid struct
    """
    key = "value"
    if key not in result or not result[key]:
        return False
    if GET_CURRENT_BACKEND() == Backend.SPARK:
        return True
    else:
        return not are_all_values_in_dict_none(result[key])


def are_all_values_in_dict_none(d):
    """
    Recursively checks if all values in a nested dictionary are None.
    """
    if not isinstance(d, dict):
        return False
    for value in d.values():
        # If the value is a dictionary, recurse into it
        if isinstance(value, dict):
            if not are_all_values_in_dict_none(value):
                return False
        # If the value is not None (and not a dictionary), return False
        elif value is not None:
            return False
    # If we've checked all values and haven't returned False, return True
    return True
