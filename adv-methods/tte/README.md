### BART Options

Insert Pipeline Diagram here

#### TTE
1. [BART (R)](https://www.rdocumentation.org/packages/BART/versions/2.9.4)
2. [PyMC-BART](https://github.com/pymc-devs/pymc-bart)
3. [Nonparametric survival analysis using Bayesian Additive Regression Trees (BART)](https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4899272/)

|Title|N |X_VARS|CENS_SCALE|CENS_IND|ALPHA|ALPHA_F|LAMBDA|
|-------|----|-----|-----|----|-----|------|-------|
||N = 100|2|5|FALSE|2||"np.exp(2 + 0.4*(x_mat[:,0] + x_mat[:,1]))"|
|# Proportional|N = 100|2|5|FALSE|2||"np.exp(2 + 0.4*(x_mat[:,0] + x_mat[:,1]))"|
||N = 100|4|7|TRUE|2||"np.exp(2 + 0.3*(x_mat[:,0] + x_mat[:,1] + x_mat[:,2] + 2* x_mat[:,3]))"|
||N = 100|2|5|TRUE|2|"1 + 2 * x_mat[:,0]"|"np.exp(2 + 0.4*(3 * x_mat[:,0] +  x_mat[:,1]))"|
||N = 100||||2|"1 + (1.5 * x_mat[:,0]) + x_mat[:,1]"|"np.exp(2 + 0.2*(3 * x_mat[:,0] - 1.2*x_mat[:,1] + 2*x_mat[:,2]))"|
|# Non Linear|N = 100|5|5|FALSE|2|"1 + (1.5 * x_mat[:,0]) + x_mat[:,1]"|"np.exp(1 + 0.2*x_mat[:,0] + 0.3*x_mat[:,1] + 0.8*np.sin(x_mat[:,0] * x_mat[:,1]) + np.power((x_mat[:,2] - 0.5),2))"|