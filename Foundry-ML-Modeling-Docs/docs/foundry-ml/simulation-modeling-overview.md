@title Simulation Modeling in FoundryML

## Overview

Other parts of the documentation are oriented towards traditional machine learning models (i.e. regression & classification models). However, FoundryML also
supports configuration-based/simulation models for forecasting use-cases as well. These models typically do not fit the traditional train/test paradigm; in addition,
validation metrics/methodologies for these models tend to be distinct from traditional ML settings. 

## Python-Based Simulation Models

To support this category of models, FoundryML provides the `foundry_ml_simulation` Python library. The primary entry-point to building simulation models is the 
`SimulationWrapper`. This is a stub class which provides one method to override (`run(self, input_data, input_params)`):

```python
class SimulationWrapper(object):
  def run(self, input_data: Dict[str, pd.DataFrame], input_params: Dict[str, Any]) -> pd.DataFrame:
    pass
```

### Implementing the forecasting logic
The `run` method takes two arguments specifying its initial parameters, and produces a `pd.DataFrame` containing the forecast.
Parameters/initial conditions are provided to the model code via the `input_data` and `input_params` arguments. `input_data` is a dictionary which maps parameter names to 
a `pd.DataFrame`, while `input_params` is a dictionary which maps names to an arbitrary value (i.e. a scalar parameter). For instance, if a model requires `R_0` and
`mean_recovery_time` as input parameters, as well as a table of historical cases, then the parameters would contain:

```python
historical_cases_df: pd.DataFrame = ...
R_0: float = ...
mean_recovery_time: float = ...

input_data = {'historical_cases': historical_cases_df}
input_params = {'R_0': R_0, 'mean_recovery_time': mean_recovery_time}

parameters = {
  'params': input_params,
  'data': input_data,
}
```

### Using a simulation model in Foundry
In order to save a model implementing the `SimulationWrapper` to Foundry, simply wrap the model class in a FoundryML `Model` object. 

```python
from foundry_ml import Model, Stage
from foundry_ml_simulation import SimulationWrapper

def my_model():
  class MyForecastModel(SimulationWrapper):
    def run(self, input_data, input_params):
      df = input_data['df']
      r_0 = input_params['r_0']
      ...
      return pd.DataFrame(...)

  foundry_ml_model = Model(Stage(MyForecastModel()))
  return foundry_ml_model
```

To consume the model in a down-stream transformation/piece of logic, include the dataset containing the model as an input to your transformation. Continuing our example
from Code Workbooks:
```python
def my_model_forecast(my_model, dataset):
  input_data = {
    'df': dataset.toPandas(),
  }
  input_params = {
    'r_0': 0.1,
  }
  parameters = {'params': input_params, 'data': input_data}
  return model.transform(parameters)
```
The `model.transform(...)` method takes as input a dictionary with two keys: `params` and `data`. These line up with the previously-defined input dictionaries that are
passed to the `run` method inside the `SimulationWrapper`. The `my_model_forecast` transformation will return the `pd.DataFrame` produced by `model.transform`, which will
be saved to Foundry and can be analyzed in other tools such as Contour, Reports, etc.

The `foundry_ml_simulation` library is included by default in all Code Workbooks environments. To add it to an environment in a Code Repository, edit the
`run` dependencies section in `meta.yaml` to include the package:
```yaml
  run:
    - python 3.6.*
    - transforms {{environ.get('PYTHON_TRANSFORMS_VERSION', '')}}
    - foundry_ml
    - foundry_ml_simulation
``` 
