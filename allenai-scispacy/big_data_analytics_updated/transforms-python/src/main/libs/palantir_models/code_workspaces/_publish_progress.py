#  (c) Copyright 2024 Palantir Technologies Inc. All rights reserved.
import time
from enum import Enum
from threading import Thread

from tqdm import tqdm


class Steps(Enum):
    EXPORTING_ADAPTER = "Exporting adapter package"
    SAVING_STATE = "Saving model state"
    CREATING_WEIGHTS_ZIP = "Creating weights zip"
    CREATING_WEIGHTS_LAYER = "Creating weights layer"
    CREATING_ENVIRONMENT_LAYER = "Creating environment layer"
    CREATING_ADAPTER_LAYER = "Creating adapter layer"
    PUBLISHING = "Publishing model version"
    PUBLISHED = "Published"


def tqdm_wrapper(update_delay: float = 1, **kwargs):
    pbar = tqdm(**kwargs)

    def refresh():
        while not pbar.disable:
            time.sleep(update_delay)
            pbar.refresh()

    thread = Thread(target=refresh, daemon=True)
    thread.start()

    return pbar


class ModelPublishProgressBar:
    def __init__(self, include_zip=True):
        self.steps = list(Steps)
        if not include_zip:
            self.steps = [s for s in self.steps if s != Steps.CREATING_WEIGHTS_ZIP]
        bar_format = "{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}]"
        self.pbar = tqdm_wrapper(
            total=len(self.steps), bar_format=bar_format, desc="Progress", unit="step", miniters=1, mininterval=0
        )
        self.current_step = 0

    def step(self, next_step: Steps):
        if self.current_step < len(self.steps):
            self.pbar.set_description(next_step.value)
            self.pbar.update(1)
            self.current_step += 1

    def close(self):
        self._stop_thread = True
        self.pbar.close()
