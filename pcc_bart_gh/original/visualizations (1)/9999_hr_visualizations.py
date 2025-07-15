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
            .na.fill(
                {
                    'exp_coef':0,
                    'coef':0,
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

def add_labels(angles, values, labels, offset, ax):
    
    # This is the space between the end of the bar and the label
    padding = 0.5  # 4
    
    # Iterate over angles, values, and labels, to add all of them.
    for i, (angle, value, label) in enumerate(zip(angles, values, labels)):
        
        if int(label) % 3 == 0:
            
            angle = angle

            # Obtain text rotation and alignment
            rotation, alignment = get_label_rotation(angle, offset)

            # And finally add the text
            ax.text(
                x=angle, 
                y=-0.4,  # 0.9,  # value + padding, 
                s=label, 
                ha=alignment, 
                va="center", 
                rotation=rotation, 
                rotation_mode="anchor",
                fontsize=8
            ) 

# COMMAND ----------

def generate_plot(dataframe, is_adult=True, is_inpatient=True):
    VALUES = pdf["coef"].values
    LABELS = [i[-2:] for i in pdf["category"].values]
    GROUP = pdf["ccsr_group"].values
    GROUP_NAMES = pdf["ccsr_group"].unique().tolist()
    HIGHLIGHT = (pdf["p"] < 0.05).values

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
    ax.set_theta_offset(offset)
    ax.set_theta_direction(-1)  # Flip so categories are clockwise (not counter clockwise)
    ax.set_ylim(-2, 1)
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

    add_labels(ANGLES[IDXS], VALUES, LABELS, offset, ax)

    # Extra customization below here --------------------

    # This iterates over the sizes of the groups adding reference
    # lines and annotations.

    offset = 0 
    for group, size in zip(GROUP_NAMES, GROUPS_SIZE):
        # Add line below bars
        x1 = np.linspace(ANGLES[offset + PAD], ANGLES[offset + size + PAD - 1], num=50)
        ax.plot(x1, [0] * 50, color="#333333")

        ax.plot(x1, [-0.5] * 50, color="#333333")

        # Add text to indicate group
        ax.text(
            np.mean(x1), -0.75, group, color="#333333", fontsize=14, 
            fontweight="bold", ha="center", va="center"
        )

        offset += size + PAD

    ax.text(0, -2, f"{'Adult' if is_adult else 'Pediatric'}+{'Inpatient' if is_inpatient else 'Outpatient'}", fontsize=20, fontweight="bold", ha="center", va="center")

    # plt.savefig(f"ccsr_hr_{'adult' if is_adult else 'pediatric'}_{'inpatient' if is_inpatient else 'outpatient'}.png", dpi=300)
    plt.show()

# COMMAND ----------

# Test
pdf = generate_pdf(is_adult=True, is_inpatient=False)
generate_plot(dataframe=pdf, is_adult=True, is_inpatient=False)

# COMMAND ----------

for adult_status in [True, False]:
    for inpatient_status in [True, False]:
        pdf = generate_pdf(is_adult=adult_status, is_inpatient=inpatient_status)
        generate_plot(dataframe=pdf, is_adult=adult_status, is_inpatient=inpatient_status)

# COMMAND ----------

VALUES = pdf["coef"].values
LABELS = pdf["category"].values
GROUP = pdf["ccsr_group"].values
GROUP_NAMES = pdf["ccsr_group"].unique().tolist()

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

with plt.style.context("seaborn-bright"):
    fig, ax = plt.subplots(figsize=(20, 20), subplot_kw={"projection": "polar"}, facecolor="w")
    ax.set_theta_offset(offset)
    ax.set_ylim(-4, 2)
    ax.set_frame_on(False)
    ax.xaxis.grid(False)
    ax.yaxis.grid(False)
    ax.set_xticks([])
    ax.set_yticks([])

    COLORS = [f"C{i}" for i, size in enumerate(GROUPS_SIZE) for _ in range(size)]

    ax.bar(
        ANGLES[IDXS], VALUES, width=WIDTH, color=COLORS, 
        edgecolor="white", linewidth=0
    )

    # plt.show()

    # add_labels(ANGLES[IDXS], VALUES, LABELS, offset, ax)

    # Extra customization below here --------------------

    # This iterates over the sizes of the groups adding reference
    # lines and annotations.

    offset = 0 
    for group, size in zip(GROUP_NAMES, GROUPS_SIZE):
        # Add line below bars
        x1 = np.linspace(ANGLES[offset + PAD], ANGLES[offset + size + PAD - 1], num=50)
        ax.plot(x1, [0] * 50, color="#333333")

        ax.plot(x1, [-0.5] * 50, color="#333333")

        # Add text to indicate group
        ax.text(
            np.mean(x1), -0.75, group, color="#333333", fontsize=14, 
            fontweight="bold", ha="center", va="center"
        )

        # Add reference lines at 20, 40, 60, and 80
        x2 = np.linspace(ANGLES[offset], ANGLES[offset + PAD - 1], num=50)
        ax.plot(x2, [2] * 50, color="#bebebe", lw=0.8)
        ax.plot(x2, [4] * 50, color="#bebebe", lw=0.8)
        ax.plot(x2, [6] * 50, color="#bebebe", lw=0.8)
        ax.plot(x2, [8] * 50, color="#bebebe", lw=0.8)

        offset += size + PAD

    ax.text(0, -4, "Adult+Outpatient", fontsize=20, fontweight="bold", ha="center", va="center")

    plt.show()

# COMMAND ----------


