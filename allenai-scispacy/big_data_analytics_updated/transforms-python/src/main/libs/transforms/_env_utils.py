import logging
import os

log = logging.getLogger(__name__)

ROOT_PROJECT_DIR = os.environ.get("ROOT_PROJECT_DIR")
PROJECT_SOURCE_DIR = os.environ.get("PROJECT_SOURCE_DIR")

if not ROOT_PROJECT_DIR:
    # Log once here instead of once per transform
    log.warning("ROOT_PROJECT_DIR not in environment, not setting source provenance")

if not PROJECT_SOURCE_DIR:
    # Log once here instead of once per transform
    log.warning(
        "PROJECT_SOURCE_DIR not in environment, not setting python TLLV or advanced source provenance"
    )
