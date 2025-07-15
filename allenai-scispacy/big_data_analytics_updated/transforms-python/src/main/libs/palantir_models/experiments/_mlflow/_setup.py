import inspect
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import mlflow

IS_PATCHED = False


def setup_mlflow_run(experiment_rid: str, autolog: bool) -> "mlflow.ActiveRun":
    import mlflow
    import mlflow.config

    patch_mlflow()
    # tracking url is static because the tracking store is a singleton
    mlflow.set_tracking_uri("palantir-models://PALANTIR_MODELS_STORE")
    # disable all mlflow logs from being async by default, we handle async ourselves where needed
    mlflow.config.enable_async_logging(False)

    if autolog:
        mlflow.autolog(log_model_signatures=False, log_models=False, log_input_examples=False, log_datasets=False)

    # the tracking store knows to use this arg to create a run that wraps an experiment
    return mlflow.start_run(experiment_id=experiment_rid)


def patch_mlflow():
    global IS_PATCHED
    if IS_PATCHED:
        return
    _patch_log_image()
    IS_PATCHED = True


# mlflow image logging is always async, which doesnt work too well with the synchronous image logging we do
# this disables clients ability to set the synchronous arg
def _patch_log_image():
    import mlflow

    original_log_image = mlflow.log_image
    mlflow.log_image = argument_override(original_log_image, "synchronous", True)


# will not do anything if the argument isn't available
def argument_override(function, param_name, override_value):
    params = inspect.signature(function).parameters
    if param_name not in params.keys():
        # dont need to patch
        return function

    synchronous_arg_idx = list(params.keys()).index(param_name)

    def patched(*args, **kwargs):
        args = list(args)
        # if count non-kw args is larger than the index, then the arg has been passed
        if len(args) > synchronous_arg_idx:
            args[synchronous_arg_idx] = override_value
        else:
            # can safely do this because this branch only occurs if the arg is passed as a kwarg or not at all
            # which would fallback to the default which we can override like this
            kwargs[param_name] = override_value
        function(*args, **kwargs)

    return patched
