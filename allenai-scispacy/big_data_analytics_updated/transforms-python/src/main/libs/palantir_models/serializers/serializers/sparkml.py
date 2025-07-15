import importlib
import logging
import os
import shutil
import tarfile
import uuid
from types import ModuleType
from typing import TYPE_CHECKING, List, Optional, Tuple

from palantir_models.models._serialization import ModelSerializer
from palantir_models.models._state_accessors import ModelStateReader, ModelStateWriter

if TYPE_CHECKING:
    from pyspark import RDD, SparkContext, SparkFiles
    from pyspark.ml import PipelineModel

log = logging.getLogger("palantir_models")


class SparkMLAutoSerializer(ModelSerializer):
    """Serializer for SparkMLå models"""

    DIR_NAME = "sparkml"
    spark: ModuleType
    sparkml: ModuleType

    _spark_context: Optional["SparkContext"] = None
    _executor_count: Optional[int] = None

    def __init__(self):
        log.warning("SparkML support in palantir_models is experimental")
        self.spark = importlib.import_module("pyspark")
        self.sparkml = importlib.import_module("pyspark.ml")

    def spark_context(self) -> "SparkContext":
        assert (
            "SPARK_HOME" in os.environ
        ), "No SPARK_HOME environment variable found. Spark is required to use SparkML models. Currently SparkML is only supported for use in Code Repositories and Spark transforms."
        assert (
            "JAVA_HOME" in os.environ
        ), "No JAVA_HOME environment variable found. Java is required to use SparkML models. Currently SparkML is only supported for use in Code Repositories and Spark transforms."
        if self._spark_context is None:
            self._spark_context = self.spark.SparkContext.getOrCreate()
        return self._spark_context

    def num_executors(self):
        if self._executor_count is None:
            # one less than number of hosts to ignore the driver - driver doesnt do any work unless you are on local spark
            self._executor_count = max(
                len([executor.host() for executor in self.spark_context()._jsc.sc().statusTracker().getExecutorInfos()])
                - 1,
                1,
            )
            log.info(f"Detected {self._executor_count} executors")
        return self._executor_count

    def execute_task_on_all_executors(self, task) -> "RDD":
        rdd = self.spark_context().parallelize(range(self.num_executors()), self.num_executors())

        # barrier enforces that each task of the rdd must run at the same time. Running `num_executors` tasks all at once = one task per executor
        return rdd.barrier().mapPartitions(task)

    def serialize(self, writer: ModelStateWriter, obj: "PipelineModel"):
        assert isinstance(obj, self.sparkml.PipelineModel), "Model object must be a pyspark.ml.PipelineModel"

        base_dir = writer.mkdir(self.DIR_NAME)
        path_segment = os.path.join(base_dir, str(uuid.uuid4()))

        obj.write().overwrite().save(path_segment)

        # we don't care about reading the local file data, we can just copy it across
        files_on_driver = [path for (path, _) in _list_files_in_directory(path_segment)]
        files_from_executors: List[Tuple[str, bytes]] = (
            self.execute_task_on_all_executors(_get_collect_files_task(path_segment)).distinct().collect()  # type: ignore
        )

        log.info(f"Executor files before remapping: {files_from_executors}")

        clean_executor_files = _remap_executor_files(files_on_driver, files_from_executors)

        for executor_file, file_bytes in clean_executor_files:
            # skip these files, they are not needed
            if executor_file.endswith(".crc") or "_SUCCESS" in executor_file:
                continue
            log.info(f'Saving model file "{executor_file}" to model version')
            with writer.open(os.path.join(base_dir, executor_file), "wb") as f:
                f.write(file_bytes)

        # delete the leftover local success files
        shutil.rmtree(path_segment)

    def deserialize(self, reader: ModelStateReader) -> "PipelineModel":
        model_path = reader.dir(self.DIR_NAME)

        # Random id to ensure multiple loaded ModelInputs don't have the same filename which breaks SparkContext.addFile()
        unique_id = str(uuid.uuid4())
        tar_name = f"spark_model_{unique_id}"  # no tar.gz suffix because spark will try to automatically unpack and that breaks things

        log.info("Creating model tar to distribute to executors")
        with tarfile.open(tar_name, "w:gz") as tar:
            # set arcname as "" to ensure the result is unpacked relative to whatever path the executor unpacks at
            tar.add(model_path, arcname="")

        log.info("Distributing model tar to each executor")
        self.spark_context().addFile(tar_name, recursive=False)

        log.info("Unpacking model tar on each executor")
        # collecting here is critical, it ensures this is a blocking operation and driver won't continue until all
        # executors have unpacked
        unpack_result = self.execute_task_on_all_executors(
            _get_open_and_extract_files_task(self.spark.SparkFiles, tar_name, unique_id)  # type: ignore
        ).collect()
        log.info("Finished unpacking models on executors")

        for i, result in enumerate(unpack_result):
            log.info(f"Executor {i} model files: {result[1]}")

        log.info("Unpacking model metadata on driver")
        with tarfile.open(tar_name, "r:gz") as tar_file:
            tar_file.extractall(unique_id)

        return self.sparkml.PipelineModel.read().load(unique_id)


def _get_collect_files_task(path):
    def _collect_files(_):
        return _list_files_in_directory(path)

    return _collect_files


def _get_open_and_extract_files_task(sparkfiles: "SparkFiles", tar_name: str, unique_id: str):
    def open_and_extract_files(_):
        path = sparkfiles.get(tar_name)
        with tarfile.open(path, "r:gz") as tar_file:
            # extract to models unique id, which matches where the model lives on the driver
            tar_file.extractall(unique_id)
        return [(unique_id, [f[0] for f in _list_files_in_directory(unique_id)])]

    return open_and_extract_files


def _list_files_in_directory(directory: str) -> List[Tuple[str, bytes]]:
    import os

    file_list = []
    for root, _, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            relative_path = os.path.relpath(file_path, directory)
            try:
                with open(file_path, "rb") as f:
                    file_bytes = f.read()
                file_list.append((relative_path, file_bytes))
            except Exception as e:
                # Handle exceptions such as permission issues
                print(f"Error reading file {file_path}: {e}")
    return file_list


def _remap_executor_files(driver_files: List[str], executor_files: List[Tuple[str, bytes]]) -> List[Tuple[str, bytes]]:
    """
    In spark the driver must say whether or not a task is done, which will rename/move any resulting files.
    For writing model metadata files, this seems to just not happen without hdfs - so we need to do it manually
    and rename the files.

    This function maps between any executor files with `_temporary` in the path, and checks if there is a corresponding
    driver success file. If there is, it will correct the path.

    Driver files will be something like stages/0_LinearRegresion_asdfasdfasdf/metadata/_SUCCESS
    Corresponding executor metadata file will be something like stages/0_LinearRegresion_asdfasdfasdf/metadata/_temporary/0/task_SOME_RANDOM_THING/part-00000
    We want the part-00000 bit, that is json metadata that must be saved.
    """
    driver_success_files = [
        driver_file.replace("_SUCCESS", "") for driver_file in driver_files if "/_SUCCESS" in driver_file
    ]
    temporary_executor_files = [executor_file for executor_file in executor_files if "_temporary" in executor_file[0]]
    non_temporary_executor_files = [
        executor_file for executor_file in executor_files if "_temporary" not in executor_file[0]
    ]

    for executor_file, data in temporary_executor_files:
        for driver_file in driver_success_files:
            if executor_file.startswith(driver_file):
                non_temporary_executor_files.append((os.path.join(driver_file, os.path.basename(executor_file)), data))
                continue

    return non_temporary_executor_files
