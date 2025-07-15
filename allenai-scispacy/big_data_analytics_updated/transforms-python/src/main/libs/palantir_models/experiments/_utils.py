from typing import Optional

from palantir_models.experiments._experiment import AbstractExperiment, Experiment


def make_none_if_cant_publish(experiment: Optional[AbstractExperiment]) -> Optional[Experiment]:
    if experiment is None:
        return None

    if not isinstance(experiment, Experiment):
        return None

    return experiment
