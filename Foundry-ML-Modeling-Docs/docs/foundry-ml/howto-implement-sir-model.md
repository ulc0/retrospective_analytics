@title Implementing an SIR Model in Foundry

This tutorial demonstrates how to implement an SIR (Susceptible, Infected, Recovered) model in Foundry. This is a simple version of a larger class
of "compartmentalized" models that are used to forecast the spread of a pandemic. For this example, we will focus on re-implementing the 
[simple SIR model](https://scipython.com/book/chapter-8-scipy/additional-examples/the-sir-epidemic-model/) provided in the SciPy documentation in 
the FoundryML ecosystem.

The following code implements the core functionality of the model:
```python
import numpy as np
from scipy.integrate import odeint

# Total population, N.
N = 1000
# Initial number of infected and recovered individuals, I0 and R0.
I0, R0 = 1, 0
# Everyone else, S0, is susceptible to infection initially.
S0 = N - I0 - R0
# Contact rate, beta, and mean recovery rate, gamma, (in 1/days).
beta, gamma = 0.2, 1./10 
# A grid of time points (in days)
t = np.linspace(0, 160, 160)

# The SIR model differential equations.
def deriv(y, t, N, beta, gamma):
    S, I, R = y
    dSdt = -beta * S * I / N
    dIdt = beta * S * I / N - gamma * I
    dRdt = gamma * I
    return dSdt, dIdt, dRdt

# Initial conditions vector
y0 = S0, I0, R0
# Integrate the SIR equations over the time grid, t.
ret = odeint(deriv, y0, t, args=(N, beta, gamma))
S, I, R = ret.T
```
### Getting Started
Create a new "Code Workbook," and skip the introductory dialogue to import datasets into the environment. Then, create a new Transform, 
and select "Python" as the langauge. 

### Using the `SimulationWrapper`
In order to integrate this model with the FoundryML `SimulationWrapper`, this functionality must be wrapped within the `run` method. 

```python
import pandas as pd
from foundry_ml_simulation import SimulationWrapper
from scipy.integrate import odeint

class SIRModel(SimulationWrapper):
    def run(self, input_data, input_params):
        N = input_params['total_population']
        n_days = input_params['n_days']
        I0 = input_params['I0']
        R0 = input_params['R0']
        S0 = N - I0 - R0

        beta = input_params['beta']
        gamma = input_params['gamma']
        t = np.linspace(0, n_days, n_days)

        y0 = S0, I0, R0
        ret = odeint(self.deriv, y0, t, args=(N, beta, gamma))
        S, I, R = ret.T
        return pd.DataFrame({
            'susceptible': S,
            'infected': I,
            'recovered': R,
            'n_days': range(n_days),
        })

    def deriv(self, y, t, N, beta, gamma):
        S, I, R = y
        dSdt = -beta * S * I / N
        dIdt = beta * S * I / N - gamma * I
        dRdt = gamma * I
        return dSdt, dIdt, dRdt
```

Add the above code to the Python transformation node in the Code Workbook. In order to save this model to Foundry, 
wrap the class in a FoundryML `Model` when returning it from a Code Workbook node:

```python
import numpy as np
import pandas as pd
from scipy.integrate import odeint

from foundry_ml import Model, Stage
from foundry_ml_simulation import SimulationWrapper

def SIR_Model():
    return Model(Stage(
        SIRModel()
    ))

class SIRModel(SimulationWrapper):
    def run(self, input_data, input_params):
        ...
```

### Forecasting with the Saved Model
To use the model to produce a forecast, create a downstream transform by clicking the "New" on the node containing the model and select a new Python transform. 
This will automatically populate the transform with the model as an input. In order to apply the model to a set of parameters/initial conditions, use the 
`model.transform(...)` method:

```python
def SIR_Forecast(SIR_Model):
    input_params = {
        'total_population': 1000.,
        'n_days': 160,
        'I0': 1.,
        'R0': 0.,
        'beta': 0.2,
        'gamma': 1/10.,
    }
    args = {
        'params': input_params,
        'data': {},
    }
    return SIR_Model.transform(args)
```

Running this node will produce a dataset entitled `SIR_Forecast` containing the projected susceptible, infected, and recovered for 160 days.

#### Running multiple scenarios
In many cases, multiple sets of initial conditions corresponding to different scenarios/interventions are of interest. For instance,
we may wish to vary the contact rate (`beta`) to encode various NPIs, or `gamma` to experiment with different virulence scenarios. This can be accomplished by
simply feeding multiple scenarious through the model and re-assembling the output:

```python
import itertools
import pandas as pd

def SIR_Forecast_Multi_Scenarios(SIR_Model):
    common_params = {
        'total_population': 1000.,
        'n_days': 160,
        'I0': 1.,
        'R0': 0.,

    }
    betas = [0.1, 0.2, 0.5, 0.8]
    gammas = [1/5., 1/10., 1/14.]
    # Iterate over all combinations of (beta, gamma)
    forecasts = []
    for beta, gamma in itertools.product(betas, gammas):
        input_params = {
            'beta': beta,
            'gamma': gamma,
            **common_params,
        } 
        args = {
            'params': input_params,
            'data': {},
        }
        scenario_forecast = SIR_Model.transform(args)
        # Add columns specifying the beta/gamma values so per-scenario analysis
        # is possible down-stream.
        scenario_forecast['beta'] = beta
        scenario_forecast['gamma'] = gamma
        forecasts.append(scenario_forecast)
    return pd.concat(forecasts, ignore_index=True)
```

