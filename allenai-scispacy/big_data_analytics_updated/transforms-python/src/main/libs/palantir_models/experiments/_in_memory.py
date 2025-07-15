import tempfile
import uuid
from dataclasses import dataclass
from threading import RLock
from typing import Dict, List, Optional

import pandas as pd
from models_api.models_api_experiments import Parameter

from palantir_models.experiments._experiment import AbstractExperiment
from palantir_models.experiments._series import SeriesValueRecord


@dataclass(frozen=True)
class LocalImage:
    """On disk image stored in a temporary directory"""

    path: str
    step: int
    timestamp: int
    caption: Optional[str]


class InMemoryExperiment(AbstractExperiment):
    """
    In memory experiment
    """

    def __init__(self, experiment_rid: str):
        super().__init__(experiment_rid)
        self.__rw_lock = RLock()
        self.__tmpdir = tempfile.mkdtemp()
        self.__metrics_df = pd.DataFrame(columns=["key", "timestamp", "value", "step"])
        self.__parameters: Dict[str, Parameter] = {}
        self.__images: Dict[str, List[LocalImage]] = {}

    @property
    def metrics(self) -> pd.DataFrame:
        return self.__metrics_df

    @property
    def parameters(self) -> Dict[str, Parameter]:
        return self.__parameters

    @property
    def images(self) -> Dict[str, List[LocalImage]]:
        return self.__images

    def _close(self):
        return self.experiment_rid

    def _store_metric(self, key: str, record: SeriesValueRecord):
        with self.__rw_lock:
            new_row = {"key": key, "timestamp": record.timestamp, "value": record.value, "step": record.step}
            self.__metrics_df = pd.concat([self.__metrics_df, pd.DataFrame([new_row])], ignore_index=True)

    def _store_parameters(self, parameters: Dict[str, Parameter]):
        with self.__rw_lock:
            for k, v in parameters.items():
                self.__parameters[k] = v

    def _store_image(self, key, image, caption, step, timestamp):
        with self.__rw_lock:
            if key not in self.__images:
                self.__images[key] = []

            img_path = f"{self.__tmpdir}/{uuid.uuid4()}.png"
            with open(img_path, "wb") as f:
                f.write(image)

            self.__images[key].append(LocalImage(path=img_path, step=step, timestamp=timestamp, caption=caption))
