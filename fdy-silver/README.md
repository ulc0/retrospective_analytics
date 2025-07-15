# Python Compute Module Template

Compute modules enable you to run serverless Docker images in Foundry and horizontally scale them up or down based on load in your frontend applications. 

## Links to Foundry Docs
- [Compute Module Overview](https://www.palantir.com/docs/foundry/compute-modules/overview)
- [Compute Module SDK](https://github.com/palantir/python-compute-module)



## Creating Endpoint Functions in `app.py`
The `app.py` file (located at `src/python/app.py`) is the main entry point into your Compute Module. **You must not change the name or location of the app.py file.**

Each function in this `app.py` file with the `@function` annotation will be mounted as an endpoint in the Compute Module when you deploy it. Any function in this file will be "callable" by its case-sensitive name.

You can write all of your code in this file, or write it across files. If you do create multiple files, _you must not use relative imports_. For example, if you create another file in the same directory as `src/python/app.py` called `src/python/myExampleLib.py` with a class called `ExampleClass`, you can import it into `app.py` as follows:

```py
from .myExampleLib import ExampleClass # This will not work, the '.' makes it relative
from myExampleLib import ExampleClass  # This will work
```

Any function annotated with `@function` in `app.py` must take 2 args:
- The first argument (named `context` in `app.py`) contains metadata about the specific invocation ("job") of the function you're calling. This object contains fields like `jobId` and `tempToken`.
- The second argument (named `event` in `app.py`) is a dictionary containing the actual payload of parameters passed to the function. You have control over the structure & values of the fields in this object.

Any functions in `app.py` must return an object that is JSON serializable (i.e., you should be able to call `json.dumps` on the return value of your endpoint function).



## Publishing Your Code Repository
When you are ready to deploy your code to a Compute Module, locate the top-right section of the page in Code Repository. You should see either a "Commit" button or a "Tag version" button. If you see a "Commit" button, click it then fill out a commit message and click the blue "Commit" button that appears in the left nav section. 

Once you have all your changes committed, click the "Tag version" button in the top-right section of the page. Select a tag name and then click "Tag and release". Once this completes, you will have created an artifact that can be deployed to a Compute Module.




## Running Your Code on a Compute Module

Once you have tagged and published your custom image from Code Repository, you can create a Compute Module using that image. 

- You can do that by navigating to your home directory, then clicking `New > Compute module` in the green dropdown at the top right corner of the page. 
- Select/verify the name & location for your Compute Module
- In the "Contaier type" section, select "Create in Foundry"
- On the next page, you should see the Code Repository you created earlier show up here. Select that option
- Under the "Tag" dropdown, you should see an option corresponding to the tag name you used for the release earlier

After these steps, you will be directed to an overview page for your Compute Module that is not yet running. Click the "Start" button to start up the Compute Module.

When your Compute Module is up and running, you can trigger an endpoint function synchronously by opening the `Query` tab at the bottom of the page. For example, if you were to deploy an app with the `add` function in this template, you can trigger it with the following input:

**Function name:**
```
add
```

**Input:**
```json
{"x": 10, "y": 20}
```

Upon clicking run, you should eventually see a result appear on the righthand side of the `Query` panel.



## Managing External Dependencies
This file at `conda_recipe/meta.yaml` contains the [metadata for the conda-build recipe](https://docs.conda.io/projects/conda-build/en/latest/resources/define-metadata.html) that is used to build your compute module as a conda-forge package. 

You can manage dependencies by adjusting this file, such as by adding a package name and version (pinning the version will improve build performance) under the Run section. By default, these repositories have external-conda-forge as a backing repository. If you need a package from a different artifact repository, feel free to add one.

Alternatively, you can search for and add new libraries to your Compute Module by using the "Libraries" tab on the left navigation bar within Code Repository.

### Configuring the backing artifacts repositories

The libraries you may add to this code repository are limited to those that are avaliable within the artifacts repositories backing this repository template. You can configure what artifacts repositories can be accessed by this code repository by navigating to the "Settings" tab at the top of this page, then going to the "Libraries" section.



## Metrics and Logging

### Basic Logging

You can use logging as you normally would in any other python program:

```py
import logging

log = logging.getLogger("myCustomLogger")
log.info("I'm a little teapot")
```

You can access the logs in the "Logs" section of the "Overview" tab in the Compute Module application. Alternatively, you can also find them within the build details by clicking the "View active job in Builds app" link.

### Structured Logs & Custom Metrics

Palantir provides a [structured logging library](https://github.palantir.build/foundry/python-sls-logging) that make it easy to produce logs that are easier to query. This same library also allows you to generate telemetry/metrics within your custom Compute Module.

To use this library, make sure to install the `slslogging` python package. This can be done by adding `slslogging` to the `run` section of your `conda_recipe/meta.yaml` file, or by navigating to the "Libraries" tab of the editor navbar on the left side and searching for `slslogging`, then clicking "Add library".

Below are some examples on how to use this library for enhanced logging & metrics:

```py
import logging
from slslogging import install_metrics, MetricArg, Gauge, METRICS_REGISTRY

# This will attach proper Formatter to logger (give your desire origin_prefix)
install_metrics(origin_prefix="python:myservice")
metrics_log = logging.getLogger("metrics")


def periodically_emission_of_metrics(context, event):
    """Once this query is called ONCE it will start emitting the metric every 60 seconds"""
    Gauge(name="processorUtilization", update_func=lambda: 50.0)
    METRICS_REGISTRY.emit_thread(delay_s=60.0)
    return {'result': "thread to start emitting metrics started"}


def direct_emission_of_metric(context, event):
    """This is an endpoint that can be triggered to emit a metric on-demand"""
    log.info(MetricArg("myGauge", 99.9))
    log.info(MetricArg("myHistogram", {"count": 100, "max": 10}, type="histogram"))
    return {'result': "metric log successfully emitted"}
```
