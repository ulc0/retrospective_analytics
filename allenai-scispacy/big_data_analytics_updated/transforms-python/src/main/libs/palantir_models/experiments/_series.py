#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.

import queue
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from models_api.models_api_experiments import (
    DoubleSeriesV1,
    DoubleSeriesValueV1,
    ExperimentService,
    PutSeriesBatch,
    Series,
    SeriesType,
)

SERIES_VALUE = Union[float]


@dataclass(frozen=True)
class SeriesValueRecord:
    key: str
    value: Any
    step: int
    timestamp: int  # ms
    series_type: SeriesType

    @classmethod
    def of(cls, key: str, value: Any, step: int, series_type: SeriesType, timestamp: int) -> "SeriesValueRecord":
        assert _is_millisecond_timestamp(timestamp), "Timestamp must be milliseconds"
        return cls(key=key, value=value, step=step, series_type=series_type, timestamp=int(timestamp))


class SeriesWriterThread(threading.Thread):
    def __init__(
        self,
        auth_header: str,
        experiment_rid: str,
        experiment_service: ExperimentService,
        input_queue: queue.Queue,
        batch_write_interval_seconds=1.0,
    ):
        # daemon thread means it will not block exiting the python process
        # we also register an atexit callback (which will run before the threads are killed) to close the experiments
        # which will send a stop event to the thread.
        super(SeriesWriterThread, self).__init__(daemon=True)
        self.__auth_header = auth_header
        self.__experiment_rid = experiment_rid
        self.__experiment_service = experiment_service
        self.__input_queue = input_queue
        self.__batch_write_interval_seconds = batch_write_interval_seconds

        self.__stop_event = threading.Event()
        self.__send_lock = threading.RLock()
        self.__last_send_time = time.time()

        self.exception: Optional[Exception] = None

    def run(self):
        batch = []
        while not self.__stop_event.is_set():
            try:
                metric = self.__input_queue.get(timeout=0.1)
                batch.append(metric)
                if len(batch) == 1000:
                    self._send_batch(batch)
                    batch = []
                    continue
            except queue.Empty:
                pass

            with self.__send_lock:
                current_time = time.time()
                if (current_time - self.__last_send_time) >= self.__batch_write_interval_seconds:
                    if batch:
                        self._send_batch(batch)
                        batch = []
        with self.__send_lock:
            if len(batch) != 0:
                self._send_batch(batch)

    def _send_batch(self, batch: List[SeriesValueRecord]):
        with self.__send_lock:
            request = None
            try:
                request = _series_value_list_to_put_batch(batch)
            except Exception as e:
                self.exception = e
                return
            current_time = time.time()
            time_since_last_send = current_time - self.__last_send_time
            if time_since_last_send < self.__batch_write_interval_seconds:
                time.sleep(1.0 - time_since_last_send)

            try:
                self.__experiment_service.put_series_batch(
                    auth_header=self.__auth_header, experiment_rid=self.__experiment_rid, request=request
                )
                self.__last_send_time = time.time()
            except Exception as e:
                self.exception = e

    def clear_exception(self):
        self.exception = None

    def close(self):
        """Gracefully close the thread, sending all remaining entries in the queue."""
        if not self.is_alive():
            return

        self.__stop_event.set()
        self.join()
        remaining_entries = []
        while not self.__input_queue.empty():
            try:
                remaining_entries.append(self.__input_queue.get_nowait())
            except queue.Empty:
                break
        if remaining_entries:
            self._send_batch(remaining_entries)

    def hard_close(self):
        """Immediately stop the thread without sending remaining entries."""
        if not self.is_alive():
            return

        self.__stop_event.set()
        self.join()


class SeriesGenerator:
    def __init__(self, initial_record: SeriesValueRecord):
        self.initial_record = initial_record
        self.records = [self.initial_record]

    def append(self, record: SeriesValueRecord):
        assert record.key == self.initial_record.key
        assert record.series_type == self.initial_record.series_type
        assert record.timestamp >= self.records[-1].timestamp
        assert record.step > self.records[-1].step
        self.records.append(record)

    def to_series(self):
        if self.initial_record.series_type == SeriesType.DOUBLE_V1:
            return Series(
                double_v1=DoubleSeriesV1(
                    series=[
                        DoubleSeriesValueV1(step=record.step, timestamp=record.timestamp, value=record.value)
                        for record in self.records
                    ]
                )
            )
        raise ValueError()


def create_series_value_record(key: str, val: SERIES_VALUE, step: int, timestamp: int):
    coerced, ty = _coerce_to_series_type(value=val)
    return SeriesValueRecord.of(
        key=key,
        value=coerced,
        step=step,
        series_type=ty,
        timestamp=timestamp,
    )


def _is_millisecond_timestamp(timestamp):
    min_millis = 1_000_000_000_000  # same number of characters as a modern timestamp (this is some time in sep 2001)
    max_millis = (
        int(datetime.now().timestamp() * 1000) + 60_000
    )  # dont let users log anything more than a minute into the future
    return min_millis <= int(timestamp) <= max_millis


def _series_value_list_to_put_batch(records: List[SeriesValueRecord]):
    assert len(records) <= 1000
    series_batch: Dict[str, SeriesGenerator] = {}
    for record in records:
        if record.key in series_batch:
            series_batch[record.key].append(record)
        else:
            series_batch[record.key] = SeriesGenerator(record)

    return PutSeriesBatch({k: v.to_series() for k, v in series_batch.items()})


def _coerce_to_series_type(value: SERIES_VALUE):
    if isinstance(value, int):
        return float(value), SeriesType.DOUBLE_V1
    if isinstance(value, float):
        return float(value), SeriesType.DOUBLE_V1
    raise ValueError("Unsupported series type")
