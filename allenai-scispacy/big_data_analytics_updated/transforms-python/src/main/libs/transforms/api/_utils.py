import math
import os
from typing import Iterable, List, TypeVar, Union, cast

_T = TypeVar("_T")


def _as_list(list_or_single_item: Union[_T, List[_T], None]) -> List[_T]:
    """
    Convert an item into a list:

    - If the item is falsy, returns an empty list.
    - If the item is already a list, returns the list unmodified.
    - Else, returns a list of length one which contains the item.

    Returns `list[_T]`.
    """

    if not list_or_single_item:
        return []

    if isinstance(list_or_single_item, list):
        return cast(List[_T], list_or_single_item)

    return [list_or_single_item]


def _get_most_similar_to(word: str, candidates: Iterable[str]) -> Union[str, None]:
    """Get the most similar string to 'word' from a list of candidate words if one exists."""
    if not candidates:
        return None

    best_distance, best_candidate = min(
        (_levenshtein_distance(c, word), c) for c in candidates
    )

    return best_candidate if best_distance < math.ceil(0.8 * len(word)) else None


def _levenshtein_distance(a: str, b: str) -> int:
    """Calculate the Levenshtein (edit) distance between two pieces of text.

    The edit distance is commutative.

    :param a: first string
    :param b: second string
    :return: edit distance between the inputs
    """
    distances = [
        [max(i, j) if i == 0 or j == 0 else 0 for j in range(len(b) + 1)]
        for i in range(len(a) + 1)
    ]

    for i in range(1, len(a) + 1):
        for j in range(1, len(b) + 1):
            if a[i - 1] == b[j - 1]:
                # inputs share a character at this position
                distances[i][j] = distances[i - 1][j - 1]
            else:
                distances[i][j] = (
                    min(
                        distances[i][j - 1],  # delete from a
                        distances[i - 1][j],  # insert to a
                        distances[i - 1][j - 1],  # replace character in a
                    )
                    + 1
                )  # each operation has a cost of 1

    return distances[-1][-1]


def _module_from_path(path: str, module_root: str) -> str:
    path_without_ext = os.path.splitext(path)[0]

    parts = path_without_ext.split(os.sep)

    for i, folder in enumerate(parts):
        if folder == module_root:
            return ".".join(parts[i + 1 :])

    return ".".join(parts)


def _raise_lib_python_import_error(exc):
    raise ModuleNotFoundError(
        "Usage of lightweight without foundry-transforms-lib-python. Make sure the latest version"
        " of 'foundry-transforms-lib-python' is installed in your environment and available"
        " through a backing repository"
    ) from exc
