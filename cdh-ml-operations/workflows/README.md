### Docs

In Azure Databricks Pipelines are parameterized Databricks Jobs Workflows running code from a CDH Github, using Jobs Compute appropriate for the pipeline. Each pipeline task can have it's own compute, and a task should be reusable code, with a clear delineation of purpose. If Databricks Experiments and MLFlow are used, each workflow pipeline should correspond to one, and only one, Databricks Experiment.

[Workflow API](https://docs.databricks.com/api/azure/workspace/jobs)
