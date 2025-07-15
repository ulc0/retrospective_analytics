#  (c) Copyright 2022 Palantir Technologies Inc. All rights reserved.
"""
Sets up pipeline for palantir_models
"""
import logging

from transforms.api import Pipeline

from palantir_models_pipelines.batch_execution import batch_execution

pipeline = Pipeline()
logging.info("Reference for batch_execution transform that got passed to pipeline: %s", batch_execution.reference)
pipeline.add_transforms(batch_execution)

__all__ = ["pipeline"]
