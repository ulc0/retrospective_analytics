# Databricks notebook source
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib as mpl
mpl.rcParams['figure.dpi'] = 600
sns.set_theme(style="white", rc={"axes.facecolor": (0, 0, 0, 0), 'axes.linewidth':2})

# COMMAND ----------

cci_demos = spark.table("cdh_hv_race_ethnicity_covid_exploratory.rvn4_cci_demos")

pdf = cci_demos.toPandas()

# COMMAND ----------

def plot_ridgeline(data_frame=pdf, score_col="cci_score", demo_col="sex"):
    unique_demos = data_frame[demo_col].unique().tolist()
    N_unique_demos = len(unique_demos)
    
    palette = sns.color_palette("bright", N_unique_demos)
    
    g = sns.FacetGrid(data_frame, palette=palette, row=demo_col, hue=demo_col, aspect=8, height=2)

    g.map_dataframe(sns.kdeplot, x=score_col, fill=True, alpha=1, linewidth=1.5)
    g.map_dataframe(sns.kdeplot, x=score_col, color='black', lw=2)

    def label(x, color, label):
        ax = plt.gca()
        ax.text(0.3, .2, label, color=color, fontsize=13,
                ha="left", va="center", transform=ax.transAxes)

    g.map(label, demo_col)
    g.fig.subplots_adjust(hspace=-.25)
    g.set_titles("")
    g.set(
        yticks=[],
        xlabel=score_col,
#         yscale="log"
    )
    g.despine(left=True, bottom=True)
#     plt.yscale("log")
    plt.suptitle(f'CCI Score Distribution (over demo={demo_col.upper()})', y=0.98)
    
    plt.show()

# COMMAND ----------

plot_ridgeline(pdf, score_col="cci_score", demo_col="race")

# COMMAND ----------

for demo in ["sex", "age_group", "region", "race", "ethnicity"]:
    print(f"Ridgeline for {demo}...")
    plot_ridgeline(pdf, score_col="cci_score", demo_col=demo)

# COMMAND ----------


