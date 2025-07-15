import logging
import tempfile
from pyspark.sql import SparkSession
from .datastores import LocalDiskDatastore, CompoundDatastore
from .dummies import DummyTransformContext, DummyTransformInput, DummyTransformOutput

log = logging.getLogger(__name__)


# pylint: disable=too-many-instance-attributes
class TransformRunner(object):
    """
    TransformRunner is a test utility to assist with running transforms pipelines in tests easier.
    When asked to build a table (by full foundry path), it will inspect your pipeline object and
    recursively build all tables in the pipeline back until raw, which it loads from files on disk.
    Supported file formats include:
      - csv files with columns delimited by ',' (with multiline support)
      - JSON (https://spark.apache.org/docs/latest/sql-data-sources-json.html), specifically Spark's default supported
        JSON Lines format
    This means you need only maintain sample data for your raw tables.

    If raw file format is unspecified (None), file format (CSV or JSON) will be automatically detected.
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        pipeline,
        raw_path_prefix=None,
        data_dir=None,
        infer_schema="true",
        datastore=None,
        raw_file_format=None,
    ):
        self.pipeline = pipeline
        self.raw_path_prefix = raw_path_prefix
        self.raw_file_format = raw_file_format
        self.data_dir = data_dir
        self.infer_schema = infer_schema in [True, "true"]
        self.transforms_by_output = self._index_pipeline(pipeline)
        self.temp_dir = tempfile.mkdtemp()
        self._datastore = datastore
        self._spark_session = None

    @property
    def spark_session(self):
        """Returns the spark session associated with this TransformRunner."""
        # Needs to be created lazily after the test framework has created the
        # spark session.
        if self._spark_session is None:
            self._spark_session = SparkSession.builder.getOrCreate()
        return self._spark_session

    @property
    def datastore(self):
        """Returns the datastore associated with this TransformRunner."""
        # Needs to be created lazily once the spark session has already been set
        # up.
        if self._datastore is None:
            raw_store = LocalDiskDatastore(
                working_dir=self.data_dir,
                path_prefix=self.raw_path_prefix,
                infer_schema=self.infer_schema,
                file_format=self.raw_file_format,
            )
            derived_store = LocalDiskDatastore(working_dir=self.temp_dir)
            self._datastore = CompoundDatastore(derived_store, raw_store)
        return self._datastore

    @staticmethod
    def _index_pipeline(pipeline):
        transforms = pipeline.transforms
        transforms_by_output = {}
        for transform in transforms:
            output_aliases = [o.alias for o in transform.outputs.values()]
            for alias in output_aliases:
                transforms_by_output[alias] = transform

        return transforms_by_output

    def build_dataset(self, spark_session, alias):
        """Recursively builds a dataset all the way from raw."""
        self._build_dataset_recursive(spark_session, alias)
        dataset_df = self._input_for_alias(alias).dataframe()
        dataset_df.show()
        return dataset_df

    def _build_dataset_recursive(self, spark_session, alias):
        if self.datastore.contains(alias):
            return

        if alias in self.transforms_by_output:
            self._build_derived(spark_session, alias)
            return

        raise Exception("Don't know how to build alias {0}".format(alias))

    def _build_derived(self, spark_session, alias):
        log.info("Computing table: %s", alias)
        transform = self.transforms_by_output[alias]

        # Firstly, recursively compute all input datasets that we can.
        for inp in transform.inputs.values():
            if inp.alias in self.transforms_by_output:
                self._build_dataset_recursive(spark_session, inp.alias)

        # Build dummy inputs and outputs
        dummy_inputs = {
            key: self._input_for_alias(inp.alias)
            for key, inp in transform.inputs.items()
        }
        dummy_outputs = {
            key: self._output_for_alias(out.alias)
            for key, out in transform.outputs.items()
        }

        kwargs = {"ctx": DummyTransformContext(spark_session)}
        kwargs.update(dummy_inputs)
        kwargs.update(dummy_outputs)
        transform.compute(**kwargs)

    def _input_for_alias(self, alias):
        return DummyTransformInput(self.datastore, alias)

    def _output_for_alias(self, alias):
        return DummyTransformOutput(self.datastore, alias)

    def load_raw(self, alias):
        """Loads a dataset from a file on disk."""
        return self._input_for_alias(alias).dataframe()
