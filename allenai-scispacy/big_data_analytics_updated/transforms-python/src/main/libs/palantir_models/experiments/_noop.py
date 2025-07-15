import logging

from palantir_models.experiments._experiment import AbstractExperiment

log = logging.getLogger("palantir_models.experiments")

NOOP_EXPERIMENT_RID = "ri.models.main.experiment.noop"


class NoopExperiment(AbstractExperiment):
    """
    Noop experiment. Intended to be used in previews.
    """

    def __init__(self):
        super().__init__(NOOP_EXPERIMENT_RID)
        self.has_logged = False

    def _close(self):
        return self.experiment_rid

    def _store_metric(self, key, record):
        if not self.has_logged:
            log.warning("No-op experiment implementation used. No logged values will be persisted.")
            self.has_logged = True

    def _store_parameters(self, parameters):
        if not self.has_logged:
            log.warning("No-op experiment implementation used. No logged values will be persisted.")
            self.has_logged = True

    def _store_image(self, key, image, caption, step, timestamp):
        if not self.has_logged:
            log.warning("No-op experiment implementation used. No logged values will be persisted.")
            self.has_logged = True
