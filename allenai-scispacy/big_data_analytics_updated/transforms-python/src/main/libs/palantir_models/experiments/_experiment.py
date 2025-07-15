#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.

import abc
import io
import queue
import time
from typing import Dict, Optional, Type

from models_api.models_api_experiments import (
    CreateExperimentRequest,
    ExperimentService,
    ExperimentSource,
    InitializeStagedExperimentRequest,
    Parameter,
    PutParametersRequest,
    SeriesType,
)

from palantir_models._internal._runtime._store import RuntimeExperimentStore
from palantir_models.experiments._errors import ErrorHandlerType, ExperimentErrorHandler
from palantir_models.experiments._media import PUT_IMAGE_UNION, read_and_validate_image
from palantir_models.experiments._mlflow._setup import setup_mlflow_run
from palantir_models.experiments._parameter import PARAMETER_VALUE, convert_parameter
from palantir_models.experiments._series import (
    SERIES_VALUE,
    SeriesValueRecord,
    SeriesWriterThread,
    _is_millisecond_timestamp,
    create_series_value_record,
)


class CloseableQueue(queue.Queue):
    def __init__(self, maxsize=1000):
        super(CloseableQueue, self).__init__(maxsize=maxsize)
        self.__drop_all = False

    def put_droppable(self, item, block=True, timeout=None):
        if self.__drop_all:
            return
        self.put(item, block, timeout)

    def set_drop(self, drop: bool):
        self.__drop_all = drop


class AbstractExperiment(abc.ABC):
    """
    Abstract base class for experiment tracking
    """

    def __init__(self, experiment_rid: str):
        self.__experiment_rid = experiment_rid
        self.__last_image_write = 0.0
        self.__step_map: Dict[str, int] = {}
        self.__type_map: Dict[str, SeriesType] = {}
        self.__is_closed: bool = False

        RuntimeExperimentStore().register(self.__experiment_rid, self)

    @property
    def experiment_rid(self):
        return self.__experiment_rid

    @property
    def is_closed(self):
        return self.__is_closed

    def as_mlflow_run(self, autolog=False):
        return setup_mlflow_run(self.experiment_rid, autolog=autolog)

    def log_param(self, key: str, value: PARAMETER_VALUE):
        """
        Log a single parameter value to the experiment.

        :param key: Unique identifier for the parameter.
        :param value: Value of the parameter.
        """

        self.log_params({key: value})

    def log_params(self, parameter_map: Dict[str, PARAMETER_VALUE]):
        """
        Log a set of parameter values to the experiment.

        :param parameter_map: Dictionary mapping of key -> value.
        """
        converted_params = {k: convert_parameter(v) for k, v in parameter_map.items()}
        self._store_parameters(converted_params)

    def log_metric(self, metric_name: str, metric_value: SERIES_VALUE, step: Optional[int] = None):
        """
        Log a single metric value to an experiment series. If the series does not already exist, one will be created.
        Otherwise, the value will be appended to the series.

        :param metric_name: Unique identifier for the series.
        :param metric_value: Metric value to append to the series.
        :param step: Optional step value indicating what step to log this value at. If the step is less than the current
        maximum step value of the series, the log will be rejected. If no value is provided, the step will be automatically
        incremented.
        """

        self.log_metrics({metric_name: metric_value}, step)

    def log_metrics(self, values: Dict[str, SERIES_VALUE], step: Optional[int] = None):
        """
        Log a set of metric values to the experiment. If any of the series do not already exist, they will be created.
        Otherwise, the values will be appended to the series.

        :param values: Dictionary mapping of metric name -> metric value.
        :param step: Optional step value indicating what step to log these values at. If the step is less than the current
        maximum step value of any of the series being written to, the log will be rejected. If no value is provided,
        the step will be automatically incremented.
        """

        self._log_metrics_with_timestamp(values=values, step=step, timestamp=int(time.time_ns() // 1_000_000))

    def _log_metrics_with_timestamp(self, values: Dict[str, SERIES_VALUE], step: Optional[int], timestamp: int):
        for key, value in values.items():
            next_step = self._get_next_step(key, step)
            record = create_series_value_record(key=key, val=value, step=next_step, timestamp=timestamp)
            self._validate_type(key, record.series_type)
            self._store_metric(key, record)
            self.__step_map[key] = next_step

    def log_image(
        self,
        series_name: str,
        image: PUT_IMAGE_UNION,
        caption: Optional[str] = None,
        step: Optional[int] = None,
    ):
        """
        Log an image to the experiment. If the series does not already exist, it will be created. Otherwise, the
        image will be appended to the series.

        :param series_name: Unique identifier for the series.
        :param image: String path to image, bytes representing the image, or a Pillow image. The image must be a png.
        :param caption: Optional caption for the image.
        :param step: Optional step value indicating what step to log this image at. If the step is less than the current
        maximum step value of the series, the log will be rejected. If no value is provided, the step will be automatically
        incremented.
        """
        self._log_image_with_timestamp(
            series_name=series_name, image=image, caption=caption, step=step, timestamp=int(time.time_ns() // 1_000_000)
        )

    def _log_image_with_timestamp(
        self, series_name: str, image: PUT_IMAGE_UNION, caption: Optional[str], step: Optional[int], timestamp: int
    ):
        # limit speed - allow up to 1 per 200ms as defined in attribution limits (technically can go higher because multiple nodes)
        while time.time() - self.__last_image_write < 0.2:
            time.sleep(0.05)
        next_step = self._get_next_step(series_name, step)
        image_bytes = read_and_validate_image(image)
        self._store_image(series_name, image_bytes, caption, next_step, timestamp=timestamp)
        self.__step_map[series_name] = next_step
        self.__last_image_write = time.time()

    def close(self) -> str:
        """
        Closes any background process that manages the experiment. Once close has been called, no more writes will be accepted.

        :returns: The experiment rid of the experiment.
        """
        if self.__is_closed:
            return self.experiment_rid
        RuntimeExperimentStore().remove(self.experiment_rid)
        self._close()
        self.__is_closed = True
        return self.experiment_rid

    @abc.abstractmethod
    def _close(self) -> str:
        """
        Closes any background process that manages the experiment. Once close has been called, no more writes will be accepted.

        :returns: The experiment rid of the experiment.
        """

    @abc.abstractmethod
    def _store_parameters(self, parameters: Dict[str, Parameter]):
        """
        Store the parameters in whatever backing storage is used for the class implementation.

        It is safe to assume the parameter value has already been validated.
        """

    @abc.abstractmethod
    def _store_metric(self, key: str, record: SeriesValueRecord):
        """
        Store the metric value in whatever backing storage is used for the class implementation.

        It is safe to assume the metric value has already been validated.
        """

    @abc.abstractmethod
    def _store_image(self, key: str, image: bytes, caption: Optional[str], step: int, timestamp: int):
        """
        Store the iamge in whatever backing storage is used for the class implementation.

        It is safe to assume the image has already been validated.
        """

    def _get_next_step(self, key: str, step: Optional[int] = None) -> int:
        if key in self.__step_map:
            if step is not None:
                assert (
                    step > self.__step_map[key]
                ), f'Cannot write to a step less than the previous step (metric "{key}" previously logged step was {self.__step_map[key]}). Steps must be increasing, and cannot be overwritten.'
                self.__step_map[key] = step
                return step
            return self.__step_map[key] + 1

        self.__step_map[key] = 0 if step is None else step
        return self.__step_map[key]

    def _validate_type(self, key: str, type: SeriesType):
        if key in self.__type_map:
            assert self.__type_map[key] == type
        self.__type_map[key] = type

    def _get_current_step(self, key: str) -> Optional[int]:
        if key not in self.__step_map:
            return None
        return self.__step_map[key]


class Experiment(AbstractExperiment):
    """
    Class for tracking model training experiments. An experiment can be created from any model output class.

    ```python
    experiment = model_output.create_experiment(name="my-experiment")

    experiment.log_param("learning_rate", 1e-3)
    experiment.log_metric("loss", 0.5)

    # save the experiment
    model_output.publish(model_adapter, experiment=experiment)
    ```
    """

    def __init__(
        self,
        auth_header: str,
        experiment_rid: str,
        experiment_service: ExperimentService,
        initial_parameters: Optional[Dict[str, PARAMETER_VALUE]],
        error_handler_type: ErrorHandlerType,
    ):
        super().__init__(experiment_rid)
        self.__auth_header = auth_header
        self.__experiment_service = experiment_service

        # input_queue is used to send writes to the writer thread and shutdown gracefully on close
        self.__input_queue = CloseableQueue(1000)

        # metrics writes are done asynchronously; errors are handled on the main thread using error_handler
        self.__write_thread = SeriesWriterThread(
            auth_header=auth_header,
            experiment_rid=self.experiment_rid,
            experiment_service=experiment_service,
            input_queue=self.__input_queue,
            batch_write_interval_seconds=1,
        )
        self.__error_handler = ExperimentErrorHandler(error_handler_type, self.__write_thread)
        self.__write_thread.start()

        if initial_parameters is not None and len(initial_parameters) != 0:
            self.log_params(initial_parameters)

    @classmethod
    def create_new(
        cls: Type["Experiment"],
        name: str,
        model_rid: str,
        auth_header: str,
        job_rid: str,
        experiment_service: ExperimentService,
        experiment_source: ExperimentSource,
        initial_parameters: Optional[Dict[str, PARAMETER_VALUE]] = None,
        error_handler_type: ErrorHandlerType = ErrorHandlerType.WARN,
    ) -> "Experiment":
        experiment_rid = experiment_service.create_experiment(
            auth_header=auth_header,
            request=CreateExperimentRequest(
                model_rid=model_rid, job_rid=job_rid, source=experiment_source, name=name, provenance_dependencies=[]
            ),
        )
        return cls(
            auth_header=auth_header,
            experiment_rid=experiment_rid,
            experiment_service=experiment_service,
            error_handler_type=error_handler_type,
            initial_parameters=initial_parameters,
        )

    @classmethod
    def initialize_from_staged(
        cls: Type["Experiment"],
        name: str,
        staged_experiment_rid: str,
        auth_header: str,
        experiment_service: ExperimentService,
        initial_parameters: Optional[Dict[str, PARAMETER_VALUE]] = None,
        error_handler_type: ErrorHandlerType = ErrorHandlerType.WARN,
    ) -> "Experiment":
        experiment_service.initialize_staged_experiment(
            auth_header=auth_header,
            experiment_rid=staged_experiment_rid,
            request=InitializeStagedExperimentRequest(name=name),
        )

        return cls(
            auth_header=auth_header,
            experiment_rid=staged_experiment_rid,
            experiment_service=experiment_service,
            error_handler_type=error_handler_type,
            initial_parameters=initial_parameters,
        )

    # need to add a callback after log_metrics is called
    def log_metrics(self, values: Dict[str, SERIES_VALUE], step: Optional[int] = None):
        super().log_metrics(values=values, step=step)

        exc = self.__write_thread.exception
        if exc is not None:
            self.__error_handler.handle(exc)
            self.__input_queue.set_drop(True)

    def _close(self):
        self.__input_queue.set_drop(True)
        self.__write_thread.close()
        self.__write_thread.join()
        return self.experiment_rid

    def _store_metric(self, key, record):
        self.__input_queue.put_droppable(record)

    def _store_parameters(self, parameters):
        def partition_parameters(parameters, chunk_size):
            items = list(parameters.items())
            return [dict(items[i : i + chunk_size]) for i in range(0, len(items), chunk_size)]

        parameter_chunks = partition_parameters(parameters, 100)

        for chunk in parameter_chunks:
            self.__experiment_service.put_parameters(
                auth_header=self.__auth_header,
                experiment_rid=self.experiment_rid,
                request=PutParametersRequest(parameters=chunk),
            )

    def _store_image(self, key: str, image: bytes, caption: Optional[str], step: int, timestamp: int):
        assert _is_millisecond_timestamp(timestamp), "Timestamps must be milliseconds"
        self.__experiment_service.put_image(
            auth_header=self.__auth_header,
            experiment_rid=self.experiment_rid,
            caption=caption,
            series_name=key,
            step=step,
            timestamp=int(timestamp),
            request=io.BytesIO(image),
        )
