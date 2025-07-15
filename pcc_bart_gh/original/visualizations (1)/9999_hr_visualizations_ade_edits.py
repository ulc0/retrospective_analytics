# Databricks notebook source
import pyspark.sql.functions as F

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# COMMAND ----------

def generate_pdf(is_adult=True, is_inpatient=True):
    table_name = f"cdh_hv_race_ethnicity_covid_exploratory.sve5_coxph_{'adult' if is_adult else 'pediatric'}_{'inpatient' if is_inpatient else 'outpatient'}_221003"
    pdf = (
        spark.table(table_name)
            .filter(
                (F.col("covariate") == "covid") | F.col("covariate").isNull()
            )
            .withColumn(
                'coef_gt_1',
                F.when(
                    F.col('coef') > 0,
                    F.col('coef')
                )
            )
            .withColumn(
                'exp_coef_gt_1',
                F.when(
                    F.col('exp_coef') > 1,
                    F.col('exp_coef')
                )
            )
            .na.fill(
                {
                    'exp_coef':0,
                    'coef':0,
                    'exp_coef_gt_1':0,
                    'coef_gt_1':0,
                    'p':1
                }
            )
            .withColumn(
                "ccsr_group",
                F.substring("category", 0, 3)
            )
            .filter(
                ~F.col("ccsr_group").isin(["PNL", "PRG", "MAL"])
            )
            .toPandas()
    )
    
    return pdf

# COMMAND ----------

def get_label_rotation(angle, offset):
    # Rotation must be specified in degrees :(
    rotation = np.rad2deg(angle + offset)
    if angle <= np.pi:
        alignment = "right"
        rotation = rotation + 180
    else: 
        alignment = "left"
    return rotation, alignment

def add_labels(angles, values, labels, offset, ax, value_thresh=2):
    
    # This is the space between the end of the bar and the label
    padding = 0.5  # 4
    
    # Iterate over angles, values, and labels, to add all of them.
    for angle, value, label, in zip(angles, values, labels):
        
        if value <= value_thresh:
            continue
            
        angle = angle

        # Obtain text rotation and alignment
        rotation, alignment = get_label_rotation(angle, offset)

        # And finally add the text
        ax.text(
            x=angle, 
            y=value_thresh,
            s="*", 
            ha=alignment, 
            va="center", 
            rotation=rotation, 
            rotation_mode="anchor",
            fontsize=12
        ) 

# COMMAND ----------

def generate_plot(dataframe, is_adult=True, is_inpatient=True, gt_1_only = False, metric = 'coef', show_labels=False, p = 0.05):

    METRIC = metric
    _VALUE = f"{METRIC}_gt_1" if gt_1_only else f"{METRIC}"
    VALUES = pdf[_VALUE].values
    LABELS = [i[-2:] for i in pdf["category"].values]
    GROUP = pdf["ccsr_group"].values
    GROUP_NAMES = pdf["ccsr_group"].unique().tolist()
    HIGHLIGHT = (pdf["p"] < p).values

    PAD = 3
    ANGLES_N = len(VALUES) + PAD * len(np.unique(GROUP))
    ANGLES = np.linspace(0, 2 * np.pi, num=ANGLES_N, endpoint=False)
    WIDTH = (2 * np.pi) / len(ANGLES)

    offset = 0
    IDXS = []
    GROUPS_SIZE = pdf.groupby("ccsr_group").size().to_list()

    for size in GROUPS_SIZE:
        IDXS += list(range(offset + PAD, offset + size + PAD))
        offset += size + PAD

    #with plt.style.context("seaborn-bright"):
    fig, ax = plt.subplots(figsize=(20, 20), subplot_kw={"projection": "polar"}, facecolor="w")
    ax.set_theta_offset(0.5*np.pi)
    ax.set_theta_direction(-1)  # Flip so categories are clockwise (not counter clockwise)
    ax.set_ylim(-2, 2) if METRIC == "coef" else ax.set_ylim(-6,6)
    ax.set_frame_on(False)
    ax.xaxis.grid(False)
    ax.yaxis.grid(False)
    ax.set_xticks([])
    ax.set_yticks([])

    COLORS = ["gold" if i else "lightslategray" for i in HIGHLIGHT]

    ax.bar(
        ANGLES[IDXS], VALUES, width=WIDTH, color=COLORS, 
        edgecolor="white", linewidth=0.25
    )
    
    if show_labels:
        vt = 2 if metric == 'coef' else 6
        add_labels(ANGLES[IDXS], VALUES, LABELS, offset, ax, value_thresh=vt)

    # Extra customization below here --------------------

    # This iterates over the sizes of the groups adding reference
    # lines and annotations.

    offset = 0 
    for group, size in zip(GROUP_NAMES, GROUPS_SIZE):
        # Add line below bars
        x1 = np.linspace(ANGLES[offset + PAD], ANGLES[offset + size + PAD - 1], num=50)
        ax.plot(x1, [0] * 50, color="#333333")

#         ax.plot(x1, [-0.5] * 50, color="#333333")

        # Add text to indicate group
        ax.text(
            np.mean(x1), -0.25, group, color="#333333", fontsize=18, 
            fontweight="bold", ha="center", va="center"
        )

        offset += size + PAD

    ax.text(0, -vt, f"{'Adult' if is_adult else 'Pediatric'}+{'Inpatient' if is_inpatient else 'Outpatient'}", fontsize=20, fontweight="bold", ha="center", va="center")

    # plt.savefig(f"ccsr_hr_{'adult' if is_adult else 'pediatric'}_{'inpatient' if is_inpatient else 'outpatient'}.png", dpi=300)
    plt.show()

# COMMAND ----------

# test
pdf = generate_pdf(is_adult=False, is_inpatient=True)
generate_plot(dataframe=pdf, is_adult=False, is_inpatient=True, gt_1_only=True, show_labels=True)

# COMMAND ----------

# coef 

for adult_status in [True, False]:
    for inpatient_status in [True, False]:
        pdf = generate_pdf(is_adult=adult_status, is_inpatient=inpatient_status)
        generate_plot(dataframe=pdf, is_adult=adult_status, is_inpatient=inpatient_status, gt_1_only=True, show_labels=True)

# COMMAND ----------

# exp_coef 

for adult_status in [True, False]:
    for inpatient_status in [True, False]:
        pdf = generate_pdf(is_adult=adult_status, is_inpatient=inpatient_status)
        generate_plot(dataframe=pdf, is_adult=adult_status, is_inpatient=inpatient_status, gt_1_only=False, show_labels=True, metric='exp_coef')

# COMMAND ----------

# exp_coef with correcrted p

for adult_status in [True, False]:
    for inpatient_status in [True, False]:
        pdf = generate_pdf(is_adult=adult_status, is_inpatient=inpatient_status)
        generate_plot(dataframe=pdf, is_adult=adult_status, is_inpatient=inpatient_status, gt_1_only=False, show_labels=True, metric='exp_coef', p = 6.57e-5)

# COMMAND ----------


