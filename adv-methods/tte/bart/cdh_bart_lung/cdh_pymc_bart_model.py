import pymc as pm
import pymc_bart as pmb
#import matplotlib.pyplot as plt

#import sklearn as skl
#import scipy.stats as sp
#import shared.simsurv_func as ssf
import mlflow
# import pyspark
# M = 200 # number of trees
# DRAWS = 200
# TUNE = 100
# CORES = 4
# SPLIT_RULES = "[pmb.ContinuousSplitRule(), pmb.ContinuousSplitRule(), pmb.OneHotSplitRule(), pmb.OneHotSplitRule()]"

experiment_id=dbutils.jobs.taskValues.get("cdh-ml-model",
                                          "experiment_id",
                                          debugValue=2221480985583347)

#run_name = dbutils.jobs.taskValues.get("cdh-ml-init", 
#                                         "run_name", 
#                                         debugValue="test2")

#run_id = dbutils.jobs.taskValues.get("cdh-ml-run",
#                                    "run_id_main",
#                                    debugValue = "5c4b0bab2668466ea9ac022e482adc35")
ptask="cdh-ml-bart-data"
training_data = dbutils.jobs.taskValues.get(ptask, "training")
ptask="cdh-ml-bart-init"
M = dbutils.jobs.taskValues.get(ptask, "M", debugValue=200)
DRAWS = dbutils.jobs.taskValues.get(ptask, "DRAWS", debugValue=200)
TUNE = dbutils.jobs.taskValues.get(ptask, "TUNE", debugValue=200)
CORES = dbutils.jobs.taskValues.get(ptask, "CORES", debugValue=4)
SPLIT_RULES = eval(dbutils.jobs.taskValues.get(ptask, "SPLIT_RULES", debugValue="[pmb.ContinuousSplitRule(), pmb.ContinuousSplitRule(), pmb.OneHotSplitRule(), pmb.OneHotSplitRule()]"))

SEED=2
ALPHA=0.95

mlflow.set_experiment(experiment_id=experiment_id)

training_data=spark.table()
print(training_data.keys())
# BART
# M = 200 # number of trees
# DRAWS = 2000
# TUNE = 1000
# CORES = 4
# SPLIT_RULES = "[pmb.ContinuousSplitRule(), pmb.ContinuousSplitRule(), pmb.OneHotSplitRule(), pmb.OneHotSplitRule()]"

    # run pymc
offset = sp.norm.ppf(np.mean(b_tr_delta))
with pm.Model() as bart:
    x_data = pm.MutableData("x", b_tr_x)
    f = pmb.BART("f", X=x_data, Y=b_tr_delta, m=M, alpha = ALPHA, split_rules=SPLIT_RULES)
    z = pm.Deterministic("z", f + offset)
    mu = pm.Deterministic("mu", pm.math.invprobit(z))
    y_pred = pm.Bernoulli("y_pred", p=mu, observed=b_tr_delta, shape=x_data.shape[0])
    bdata = pm.sample(random_seed=SEED, draws=DRAWS, tune = TUNE, cores=CORES)

#akb add
pm.model_to_graphviz(bart)
with bart:
# pm.set_data({"x":pd.DataFrame(test_x), "off":off_test})
    pm.set_data({"x":spark.DataFrame(b_te_x3)})
    pp = pm.sample_posterior_predictive(bdata, var_names = ["y_pred", "f", "z", "mu"])

with mlflow.start_run(experiment_id=experiment_id, run_id=run_id) as run:

    # get survival
    x_out = np.concatenate([x_sk.to_numpy(), x_sk.to_numpy()], axis=0)
    bart_sv_fx = ssf.get_sv_fx(pp, x_out)

    # get the original and counterfactual
    og_shp = x_sk.shape[0]
    or_bart_sv_fx = bart_sv_fx[0:og_shp,:]
    cf_bart_sv_fx = bart_sv_fx[og_shp:, :]

    # get mean and quantile
    or1 = or_bart_sv_fx.mean(axis=0)
    orp = np.quantile(or_bart_sv_fx, q=[0.05,0.95], axis=0)
    cf1 = cf_bart_sv_fx.mean(axis=0)
    cfp = np.quantile(cf_bart_sv_fx, q=[0.05,0.95], axis=0)


    plt_time = np.unique(b_tr_t)

    # plot
    fig = plt.figure()
    plt.step(plt_time, or1, label = "male", color="darkblue")
    plt.step(plt_time, orp[0], color="darkblue", alpha=.4)
    plt.step(plt_time, orp[1], color="darkblue", alpha=.4)
    plt.step(plt_time, cf1, label = "female", color="darkorange")
    plt.step(plt_time, cfp[0], color="darkorange", alpha=.4)
    plt.step(plt_time, cfp[1], color="darkorange", alpha=.4)
    plt.legend()
    mlflow.log_figure(fig, "male_female.png")