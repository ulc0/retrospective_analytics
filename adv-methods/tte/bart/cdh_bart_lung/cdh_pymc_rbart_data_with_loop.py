# Databricks notebook source
import mlflow
import numpy as np
import pandas as pd

# COMMAND ----------


dbutils.widgets.text('experiment_id','1348027641275128')
EXPERIMENT_ID=dbutils.widgets.get('experiment_id')
# DEV EXPERIMENT_ID=2256023545557195
RUN_NAME="Lung Cancer Data"

# COMMAND ----------

# MAGIC %md
# MAGIC ####NCCTG Lung Cancer Data
# MAGIC Survival in patients with advanced lung cancer from the North Central Cancer Treatment Group.
# MAGIC Performance scores rate how well the patient can perform usual aily activities  
# MAGIC
# MAGIC **Source:** Loprinzi CL. Laurie JA. Wieand HS. Krook JE. Novotny PJ. Kugler JW. Bartel J. Law M. Bateman
# MAGIC M. Klatt NE. et al. Prospective evaluation of prognostic variables from patient-completed questionnaires. North Central Cancer Treatment Group. Journal of Clinical Oncology. 12(3):601-7,
# MAGIC 1994

# COMMAND ----------

# MAGIC %md
# MAGIC **inst**: Institution code  
# MAGIC **time**: Survival time in days  
# MAGIC **status**: censoring status 1=censored, 2=dead  
# MAGIC **age**: Age in years  
# MAGIC **sex**: Male=1 Female=2  
# MAGIC **ph.ecog**: ECOG performance score (0=good 5=dead)  
# MAGIC **ph.karno**: Karnofsky performance score (bad=0-good=100) rated by physician  
# MAGIC **pat.karno**: Karnofsky performance score as rated by patient  
# MAGIC **meal.cal**: Calories consumed at meals  
# MAGIC **wt.loss**: Weight loss in last six months  

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Grab data sets from BART
# MAGIC
# MAGIC library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
# MAGIC #sparkR.session()
# MAGIC sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "2g"))
# MAGIC if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
# MAGIC   Sys.setenv(SPARK_HOME = "/home/spark")
# MAGIC }
# MAGIC install.packages('BART')
# MAGIC library(BART)
# MAGIC data(lung)
# MAGIC data(transplant)
# MAGIC data(bladder)
# MAGIC #data(phoneme)
# MAGIC data(ACTG175)
# MAGIC data(alligator)
# MAGIC data(xdm20.train)
# MAGIC data(xdm20.test)
# MAGIC data(ydm20.train)
# MAGIC data(ydm20.test)
# MAGIC #lung = pd.read_csv("lung.csv")
# MAGIC write.csv(lung,"lung.csv")
# MAGIC write.csv(transplant,"transplant.csv")
# MAGIC write.csv(bladder,"bladder.csv")
# MAGIC write.csv(ACTG175,"actg175.csv")
# MAGIC

# COMMAND ----------

import pandas as pd
import numpy as np
lung=pd.read_csv("lung.csv")
# configure analytic dataset
lung["karno2"] = lung["ph.karno"].fillna(lung["pat.karno"])
# adjust time to months
lung["months"] = np.ceil(lung["time"]/30)
lung["weeks"] = np.ceil(lung["time"]/7)
lung["female"] = (lung["sex"]==2)
lung["event"] = (lung.status == 1)


# COMMAND ----------

lung.drop('Unnamed: 0', axis=1, inplace=True)
print(lung)

# COMMAND ----------

lungs=
#x = pd.concat([time, lung[["age","sex2","karno"]]], axis=1)
data_x=lung[["age","female","karno2"]] #remove time???? 
#data_y = np.array(list(zip(np.array(lung["event"], dtype="bool"), lung["time"])), dtype=[("Status","?"),("Survival_in_days", "<f8")])
data_y=lung[["event","months"]]
print(data_y)
print(data_x)


# COMMAND ----------

time="months"
# set up times
    # t_sort = np.append([0], np.unique(data_y["Survival_in_days"]))
t_sort = np.unique(data_y[time])
t_ind = np.arange(0,t_sort.shape[0])
t_dict = dict(zip(t_sort, t_ind))
#print(t_dict)
    # set up delta
delta = np.array(data_y["Status"], dtype = "int")
X_TIME=True    
t_out = []
pat_x_out = []
delta_out = []
for idx, t in enumerate(data_y["Survival_in_days"]):
        # get the pat_time and use to get the array of times for the patient
        p_t_ind = t_dict[t]
        p_t_set = t_sort[0:p_t_ind+1]
        t_out.append(p_t_set)
        
        size = p_t_set.shape[0]
        # get patient array
        pat_x = np.tile(data_x.iloc[idx].to_numpy(), (size, 1))
        print(pat_x)

        pat_x_out.append(pat_x)

        # get delta
        pat_delta = delta[idx]
        delta_set = np.zeros(shape=size, dtype=int)
        delta_set[-1] = pat_delta
        delta_out.append(delta_set)
    
    
t_out, delta_out, pat_x_out = np.concatenate(t_out), np.concatenate(delta_out), np.concatenate(pat_x_out)
if X_TIME:
    pat_x_out = np.array([np.concatenate([np.array([t_out[idx]]), i]) for idx, i in enumerate(pat_x_out)])
print(pat_x_out)

# COMMAND ----------


# karno try categorical and continuous
#lung_adjusted = pd.concat([time, lung[["age","sex2","karno2"]], axis=1)
lung.to_csv("data/lung_tte_bart.csv")
lung_json=lung.to_json(orient='records')


# COMMAND ----------


lung_json=lung.to_dict('records')
with mlflow.start_run(experiment_id=EXPERIMENT_ID, run_name=RUN_NAME) as run_id:
    print(run_id)
    mlflow.log_dict(lung_json,"data/lung_tte_bart.json")
    mlflow.log_artifacts("data")
#print(x.to_json(orient='records'))
