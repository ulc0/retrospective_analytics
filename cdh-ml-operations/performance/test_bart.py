# Databricks notebook source
import numpy as np
import pymc as pm
import pytest
from numpy.random import RandomState
from numpy.testing import assert_almost_equal, assert_array_equal
from pymc.initial_point import make_initial_point_fn
from pymc.logprob.basic import joint_logp

import pymc_bart as pmb


def assert_moment_is_expected(model, expected, check_finite_logp=True):
    fn = make_initial_point_fn(
        model=model,
        return_transformed=False,
        default_strategy="moment",
    )
    moment = fn(0)["x"]
    expected = np.asarray(expected)
    try:
        random_draw = model["x"].eval()
    except NotImplementedError:
        random_draw = moment

    assert moment.shape == expected.shape
    assert expected.shape == random_draw.shape
    assert np.allclose(moment, expected)

    if check_finite_logp:
        logp_moment = (
            joint_logp(
                (model["x"],),
                rvs_to_values={model["x"]: pm.math.constant(moment)},
                rvs_to_transforms={},
                rvs_to_total_sizes={},
            )[0]
            .sum()
            .eval()
        )
        assert np.isfinite(logp_moment)


def test_bart_vi():
    X = np.random.normal(0, 1, size=(250, 3))
    Y = np.random.normal(0, 1, size=250)
    X[:, 0] = np.random.normal(Y, 0.1)

    with pm.Model() as model:
        mu = pmb.BART("mu", X, Y, m=10)
        sigma = pm.HalfNormal("sigma", 1)
        y = pm.Normal("y", mu, sigma, observed=Y)
        idata = pm.sample(random_seed=3415)
        var_imp = (
            idata.sample_stats["variable_inclusion"]
            .stack(samples=("chain", "draw"))
            .mean("samples")
        )
        var_imp /= var_imp.sum()
        assert var_imp[0] > var_imp[1:].sum()
        assert_almost_equal(var_imp.sum(), 1)


def test_missing_data():
    X = np.random.normal(0, 1, size=(50, 2))
    Y = np.random.normal(0, 1, size=50)
    X[10:20, 0] = np.nan

    with pm.Model() as model:
        mu = pmb.BART("mu", X, Y, m=10)
        sigma = pm.HalfNormal("sigma", 1)
        y = pm.Normal("y", mu, sigma, observed=Y)
        idata = pm.sample(tune=100, draws=100, chains=1, random_seed=3415)


def test_shared_variable():
    X = np.random.normal(0, 1, size=(50, 2))
    Y = np.random.normal(0, 1, size=50)

    with pm.Model() as model:
        data_X = pm.MutableData("data_X", X)
        mu = pmb.BART("mu", data_X, Y, m=2)
        sigma = pm.HalfNormal("sigma", 1)
        y = pm.Normal("y", mu, sigma, observed=Y, shape=mu.shape)
        idata = pm.sample(tune=100, draws=100, chains=2, random_seed=3415)
        ppc = pm.sample_posterior_predictive(idata)
        new_X = pm.set_data({"data_X": X[:3]})
        ppc2 = pm.sample_posterior_predictive(idata)

    assert ppc.posterior_predictive["y"].shape == (2, 100, 50)
    assert ppc2.posterior_predictive["y"].shape == (2, 100, 3)


def test_shape():
    X = np.random.normal(0, 1, size=(250, 3))
    Y = np.random.normal(0, 1, size=250)

    with pm.Model() as model:
        w = pmb.BART("w", X, Y, m=2, shape=(2, 250))
        y = pm.Normal("y", w[0], pm.math.abs(w[1]), observed=Y)
        idata = pm.sample(random_seed=3415)

    assert model.initial_point()["w"].shape == (2, 250)
    assert idata.posterior.coords["w_dim_0"].data.size == 2
    assert idata.posterior.coords["w_dim_1"].data.size == 250


class TestUtils:
    X_norm = np.random.normal(0, 1, size=(50, 2))
    X_binom = np.random.binomial(1, 0.5, size=(50, 1))
    X = np.hstack([X_norm, X_binom])
    Y = np.random.normal(0, 1, size=50)

    with pm.Model() as model:
        mu = pmb.BART("mu", X, Y, m=10)
        sigma = pm.HalfNormal("sigma", 1)
        y = pm.Normal("y", mu, sigma, observed=Y)
        idata = pm.sample(random_seed=3415)

    def test_sample_posterior(self):
        all_trees = self.mu.owner.op.all_trees
        rng = np.random.default_rng(3)
        pred_all = pmb.utils._sample_posterior(all_trees, X=self.X, rng=rng, size=2)
        rng = np.random.default_rng(3)
        pred_first = pmb.utils._sample_posterior(all_trees, X=self.X[:10], rng=rng)

        assert_almost_equal(pred_first[0], pred_all[0, :10], decimal=4)
        assert pred_all.shape == (2, 50, 1)
        assert pred_first.shape == (1, 10, 1)

    @pytest.mark.parametrize(
        "kwargs",
        [
            {},
            {
                "kind": "pdp",
                "samples": 2,
                "xs_interval": "quantiles",
                "xs_values": [0.25, 0.5, 0.75],
                "var_discrete": [3],
            },
            {"kind": "ice", "instances": 2},
            {"var_idx": [0], "rug": False, "smooth": False, "color": "k"},
            {"grid": (1, 2), "sharey": "none", "alpha": 1},
        ],
    )
    def test_pdp(self, kwargs):
        pmb.plot_dependence(self.mu, X=self.X, Y=self.Y, **kwargs)

    @pytest.mark.parametrize(
        "kwargs",
        [
            {},
            {"labels": ["A", "B", "C"], "samples": 2, "figsize": (6, 6)},
        ],
    )
    def test_vi(self, kwargs):
        pmb.plot_variable_importance(self.idata, X=self.X, bartrv=self.mu, **kwargs)

    def test_pdp_pandas_labels(self):
        pd = pytest.importorskip("pandas")

        X_names = ["norm1", "norm2", "binom"]
        X_pd = pd.DataFrame(self.X, columns=X_names)
        Y_pd = pd.Series(self.Y, name="response")
        axes = pmb.plot_dependence(self.mu, X=X_pd, Y=Y_pd)

        figure = axes[0].figure
        assert figure.texts[0].get_text() == "Predicted response"
        assert_array_equal([ax.get_xlabel() for ax in axes], X_names)


@pytest.mark.parametrize(
    "size, expected",
    [
        (None, np.zeros(50)),
    ],
)
def test_bart_moment(size, expected):
    X = np.zeros((50, 2))
    Y = np.zeros(50)
    with pm.Model() as model:
        pmb.BART("x", X=X, Y=Y, size=size)
    assert_moment_is_expected(model, expected)
