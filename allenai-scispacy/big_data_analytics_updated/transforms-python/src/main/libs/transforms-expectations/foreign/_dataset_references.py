#  (c) Copyright 2020 Palantir Technologies Inc. All rights reserved.
from abc import ABCMeta
from typing import List

from transforms.expectations.utils._col_utils import missing_columns
from transforms.expectations.evaluator import LazyTransformsDataFrame, ParamsToDF
from transforms.expectations.wrapper import SparkOrPolarsSchema


class DatasetReference(object):
    def __init__(self, dataset_parameter):
        self._dataset_parameter = dataset_parameter

    def count(self):
        """Reference the row count of the dataset."""
        return DatasetCount(DatasetReference(self._dataset_parameter))

    def col(self, col: str):
        return DatasetColumn(col, DatasetReference(self._dataset_parameter))

    def cols(self, *cols: str):
        return DatasetComposite(DatasetReference(self._dataset_parameter), cols)

    def join(self, keys: List[str], foreign_keys: List[str]):
        """
        Create a dataset reference that is joined to the dataset under test to compare columns between the datasets
        keys: Keys from the dataset under test to join on
        foreign_keys: Keys from the foreign dataset to join on
        To join correctly, the key lists must match in order and number.
        """
        return JoinedDatasetReference(self._dataset_parameter, keys, foreign_keys)

    def schema(self):
        """
        Return the schema of the dataset in the Array[StructType] format.
        This allows comparisons with other datasets' schemas.
        """
        return DatasetSchema(DatasetReference(self._dataset_parameter))

    def _get(self, params_to_df: ParamsToDF) -> LazyTransformsDataFrame:
        if self._dataset_parameter not in params_to_df:
            raise _MissingDatasetException(self._dataset_parameter)
        return params_to_df[self._dataset_parameter]

    def definition(self):
        return {
            # This will be converted to dataset rid by Gradle-Transforms
            "datasetRid": self._dataset_parameter,
        }


class JoinedDatasetReference(DatasetReference):
    def __init__(self, dataset_parameter, keys, foreign_keys):
        super(JoinedDatasetReference, self).__init__(dataset_parameter)
        self._keys = keys
        self._foreign_keys = foreign_keys

    def col(self, col: str):
        return JoinedDatasetColumn(col, self, self._keys, self._foreign_keys)


class DatasetProperty(metaclass=ABCMeta):
    def __init__(self, dataset_reference: DatasetReference):
        self._dataset_reference = dataset_reference

    def definition(self):
        raise NotImplementedError()

    def evaluate(self, params_to_df: ParamsToDF):
        raise NotImplementedError()


class DatasetCount(DatasetProperty):
    def __init__(self, dataset_reference: DatasetReference):
        super(DatasetCount, self).__init__(dataset_reference)

    def definition(self):
        return {
            "dataset": {
                # This will be converted to dataset rid by Gradle-Transforms
                "datasetRid": self._dataset_reference._dataset_parameter,
            },
            "value": {
                "type": "rowCount",
                "rowCount": {},
            },
        }

    def evaluate(self, params_to_df: ParamsToDF) -> LazyTransformsDataFrame:
        return self._dataset_reference._get(params_to_df).count()


class DatasetSchema(DatasetProperty):
    def __init__(self, dataset_reference: DatasetReference):
        super(DatasetSchema, self).__init__(dataset_reference)

    def definition(self):
        return {
            "datasetRid": self._dataset_reference._dataset_parameter,
        }

    def evaluate(self, params_to_df: ParamsToDF) -> SparkOrPolarsSchema:
        return self._dataset_reference._get(params_to_df).schema()


class DatasetColumn(DatasetProperty):
    def definition(self):
        raise NotImplementedError()

    def __init__(self, col: str, dataset_reference: DatasetReference):
        super(DatasetColumn, self).__init__(dataset_reference)
        self._col = col

    def evaluate(self, params_to_df: ParamsToDF):
        dataframe = self._dataset_reference._get(params_to_df).get()
        maybe_missing_foreign_column = missing_columns([self._col], dataframe)
        if maybe_missing_foreign_column:
            raise _MissingForeignColumnsException(
                self._dataset_reference._dataset_parameter, [self._col]
            )
        return dataframe

    def col_name(self):
        return self._col

    def dataset_reference(self):
        return self._dataset_reference


class JoinedDatasetColumn(DatasetColumn):
    def definition(self):
        raise NotImplementedError()

    def __init__(
        self,
        col: str,
        dataset_reference: DatasetReference,
        keys: List[str],
        foreign_keys: List[str],
    ):
        super(JoinedDatasetColumn, self).__init__(col, dataset_reference)
        self._keys = keys
        self._foreign_keys = foreign_keys

    def keys(self):
        return self._keys

    def foreign_keys(self):
        return self._foreign_keys


class DatasetComposite(DatasetProperty):
    def definition(self):
        raise NotImplementedError()

    def __init__(self, dataset_reference: DatasetReference, cols: List[str]):
        super(DatasetComposite, self).__init__(dataset_reference)
        self._cols = cols

    def evaluate(self, params_to_df: ParamsToDF):
        dataframe = self._dataset_reference._get(params_to_df).get()
        maybe_missing_foreign_columns = missing_columns(self._cols, dataframe)
        if maybe_missing_foreign_columns:
            raise _MissingForeignColumnsException(
                self._dataset_reference._dataset_parameter, self._cols
            )
        return dataframe

    def col_names(self) -> List[str]:
        return self._cols

    def dataset_reference(self):
        return self._dataset_reference


class _MissingDatasetException(Exception):
    def __init__(self, dataset_parameter):
        self._dataset_parameter = dataset_parameter

    def as_conjure_definition(self):
        return {
            "type": "missingDataset",
            "missingDataset": {
                "parameterName": self._dataset_parameter,
            },
        }


class _MissingForeignColumnsException(Exception):
    def __init__(self, dataset_parameter: str, columns: List[str]):
        self._dataset_parameter = dataset_parameter
        self._cols = columns

    def as_conjure_definition(self):
        return {
            "type": "missingForeignColumns",
            "missingForeignColumns": {
                "parameterName": self._dataset_parameter,
                "columns": self._cols,
            },
        }


# Error for the case when composite join keys are of different lengths.
# Todo: Add to data health api
class _MismatchedCompositesException(Exception):
    def __init__(self, left_cols, right_cols):
        self.left_cols = left_cols
        self.right_cols = right_cols

    def as_conjure_definition(self):
        return {
            "type": "mismatchedCompositeKeys",
            "mismatchedCompositeKeys": {
                "left_key": self.left_cols,
                "right_key": self.right_cols,
            },
        }
