from typing import Dict, List, Optional

from palantir_models._internal._runtime._store import RuntimeExperimentStore
from palantir_models._internal._runtime._utils import rid_pattern
from palantir_models.experiments._noop import NOOP_EXPERIMENT_RID

# we can throw here, this file should never be invoked unless mlflow does it specifically
# because it is not exposed in __init__ and mlflow will load the file via an entry point
try:
    import mlflow  # noqa: F401
except ModuleNotFoundError:
    raise Exception("mlflow must be installed to use mlflow features in palantir_models")

from mlflow.entities import LifecycleStage, Metric, Param
from mlflow.entities import Run as MlflowRun
from mlflow.entities import RunData, RunInfo, RunTag
from mlflow.store.artifact.artifact_repo import ArtifactRepository
from mlflow.store.tracking.abstract_store import AbstractStore
from mlflow.utils.file_utils import local_file_uri_to_path

_EXPERIMENT_RID = rid_pattern("models", "experiment")


# This class is registered to mlflow via an entrypoint defined in pyproject.toml
class PalantirModelsTrackingStore(AbstractStore):
    def __init__(self, store_uri: str, artifact_uri: str):
        super().__init__()
        self.store_uri = store_uri
        self.artifact_uri = artifact_uri
        self.runs: Dict[str, MlflowRun] = {}

    def create_run(self, experiment_id, user_id, start_time, tags, run_name):
        if not _EXPERIMENT_RID.match(experiment_id) and experiment_id != NOOP_EXPERIMENT_RID:
            raise ValueError(
                f'Expected experiment rid passed as experiment_id in mlflow create_run, found "{experiment_id}". '
                "This can be caused by creating nested runs (mlflow.create_run(nested=True)), which is not supported."
            )

        experiment = RuntimeExperimentStore().get(experiment_rid=experiment_id)
        locator = experiment.experiment_rid.split(".")[-1]

        run = MlflowRun(
            run_info=RunInfo(
                run_id=locator,  # run_id must be a uuid
                run_uuid=locator,
                start_time=start_time,
                end_time=None,
                experiment_id=experiment_id,
                user_id=user_id,
                lifecycle_stage=LifecycleStage.ACTIVE,
                status="running",
                artifact_uri=f"{self.store_uri}/{experiment.experiment_rid}",
                run_name=run_name,
            ),
            run_data=RunData(),
        )

        self.runs[locator] = run

        return run

    def get_run(self, run_id):
        return self.runs[run_id]

    def log_batch(self, run_id, metrics: List[Metric], params: List[Param], tags: List[RunTag]):
        experiment = RuntimeExperimentStore().get(run_id)
        for metric in metrics:
            current_step = experiment._get_current_step(metric.key)
            step = metric.step
            if current_step is not None and step <= current_step and step == 0:
                # auto increment this
                step = None
            experiment._log_metrics_with_timestamp(
                values={metric.key: metric.value}, step=step, timestamp=metric.timestamp
            )

        param_map = {}

        # TODO(tuckers): support tags in a first class way
        # for tag in tags:
        #     param_map[tag.key] = tag.value

        for param in params:
            param_map[param.key] = param.value

        experiment.log_params(param_map)


# This class is registered to mlflow via an entrypoint defined in pyproject.toml
class PalantirModelsArtifactRepository(ArtifactRepository):
    def __init__(self, artifact_uri: str):
        super(PalantirModelsArtifactRepository, self).__init__(artifact_uri)
        self._artifact_dir = local_file_uri_to_path(self.artifact_uri)
        self._experiment_rid: str = artifact_uri.split("/")[-1]

    def log_artifact(self, local_file: str, artifact_path: Optional[str] = None):
        if local_file.endswith("compressed.webp"):
            # mlflow logs both a compressed thumbnail and image - we don't care about the thumbnail
            return

        if not local_file.endswith(".png"):
            # we dont support non image artifacts
            return

        # see https://sourcegraph.com/github.com/mlflow/mlflow@v2.20.2/-/blob/mlflow/tracking/client.py?L2377
        parts = local_file.split("/")[-1].split("%")

        # invert sanitization - this won't break anything, mlflow doesn't support # in strings passed by the user
        # see https://sourcegraph.com/github.com/mlflow/mlflow@v2.20.2/-/blob/mlflow/tracking/client.py?L2372
        key = parts[0].replace("#", "/")
        step = int(parts[2])
        timestamp = int(parts[4])

        experiment = RuntimeExperimentStore().get(self._experiment_rid)
        experiment._log_image_with_timestamp(
            series_name=key, step=step, image=local_file, caption=None, timestamp=timestamp
        )

    def log_artifacts(self, local_dir, artifact_path=None):
        # this doesnt do step based logging like we want, so ignoring for now
        pass
