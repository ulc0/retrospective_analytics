# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

DB_WRITE = "cdh_hv_race_ethnicity_covid_exploratory"

# COMMAND ----------

# MAGIC %run ../../includes/0000-utils

# COMMAND ----------

df_cci = spark.table(f"{DB_WRITE}.sve5_hvid_cci")
df_ccsr_wide = spark.table(f"{DB_WRITE}.sve5_hvid_ccsr_wide_cohort")
df_demo = spark.table(f"{DB_WRITE}.covid_full_hvids_demographics_0812")

# COMMAND ----------

df_ccsr_wide = assign_regions("patient_state")(df_ccsr_wide)

# COMMAND ----------

cci_demos = (
    df_cci
        # Join with ccsr_wide mainly to extract age
        .join(
            df_ccsr_wide.select(
                "hvid",
                "region",
                F.col("patient_state").alias("state"),
                # Rename patient_gender to sex
                F.col("patient_gender").alias("sex"),
                # Compute age group
                F.when(
                    F.col("age") < 12,
                    "00-11"
                ).when(
                    F.col("age") < 18,
                    "12-17"
                ).when(
                    F.col("age") < 50,
                    "18-49"
                ).when(
                    F.col("age") < 65,
                    "50-64"
                ).when(
                    F.col("age") < 75,
                    "65-74"
                ).when(
                    F.col("age") >= 75,
                    "75+"
                ).alias("age_group"),
            ),
            "hvid",
            "left"
        )
        # Join with demo to extract race/ethnicity (not in ccsr_wide)
        .join(
            df_demo.select("hvid", "race", "ethnicity"),
            "hvid",
            "left"
        )
)

# COMMAND ----------

cci_demos.write.mode("overwrite").saveAsTable(f"{DB_WRITE}.rvn4_cci_demos")

# COMMAND ----------

cci_demos = spark.table(f"{DB_WRITE}.rvn4_cci_demos")

cci_demos.limit(10).display()

# COMMAND ----------

# DBTITLE 1,Number of Unique Patients
display(
    cci_demos.select(F.countDistinct("hvid"))
)

# COMMAND ----------

display(
    cci_demos
        .groupBy("cci_score")
        .agg(
            F.countDistinct("hvid")
        )
        .orderBy("cci_score")
)

# COMMAND ----------

# DBTITLE 1,CCI Score by Sex
display(
    cci_demos
        .groupBy("cci_score")
        .pivot("sex")
        .agg(
            F.countDistinct("hvid")
        )
        .orderBy("cci_score")
)

# px.histogram(temp_df, x="cci_score", y=["F", "M"], barmode="overlay")

# COMMAND ----------

# import plotly.figure_factory as ff
# from plotly.colors import n_colors

# temp_df = (
#     cci_demos
#         .groupBy("cci_score")
#         .pivot("sex")
#         .agg(
#             F.countDistinct("hvid")
#         )
#         .orderBy("cci_score")
# ).toPandas()

# COMMAND ----------

# colors = n_colors('rgb(5, 200, 200)', 'rgb(200, 10, 10)', 2, colortype='rgb')

# fig = go.Figure()
# for data_line, color in zip(["F", "M"], colors):
#     fig.add_trace(go.Violin(x=temp_df[data_line], line_color=color, name=data_line))

# fig.update_traces(orientation='h', side='positive', width=3, points=False)
# fig.update_layout(xaxis_showgrid=False, xaxis_zeroline=False)
# fig.show()

# COMMAND ----------

# DBTITLE 1,CCI Score by Age Group
display(
    cci_demos
        .groupBy("cci_score")
        .pivot("age_group")
        .agg(
            F.countDistinct("hvid")
        )
        .orderBy("cci_score")
)

# COMMAND ----------

# DBTITLE 1,CCI Score by Region
display(
    cci_demos
        .groupBy("cci_score")
        .pivot("region")
        .agg(
            F.countDistinct("hvid")
        )
        .orderBy("cci_score")
)

# COMMAND ----------

# DBTITLE 1,CCI Score by State
def melt(
        df, 
        id_vars, value_vars, 
        var_name="variable", value_name="value"):
    """Convert :class:`DataFrame` from wide to long format."""

    # Create array<struct<variable: str, value: ...>>
    _vars_and_vals = F.array(*(
        F.struct(F.lit(c).alias(var_name), F.col(c).alias(value_name)) 
        for c in value_vars))

    # Add to the DataFrame and explode
    _tmp = df.withColumn("_vars_and_vals", F.explode(_vars_and_vals))

    cols = id_vars + [
            F.col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]
    return _tmp.select(*cols)

temp_df = (
    cci_demos
        .groupBy("cci_score")
        .pivot("state")
        .agg(
            F.countDistinct("hvid")
        )
        .orderBy("cci_score")
)

temp_pdf = (melt(temp_df, id_vars=["cci_score"], value_vars=temp_df.columns[1:])).toPandas()

# COMMAND ----------

temp_pdf.head()

# COMMAND ----------

temp_pdf['log_value'] = np.log10(temp_pdf['value'])

# COMMAND ----------

import plotly.express as px

px.choropleth(
    temp_pdf,
    locations="variable",
    color="log_value",
    animation_frame="cci_score",
    locationmode="USA-states",
    scope="usa",
#     title="Counts per State over CCI Score"
    range_color=[0, temp_pdf["log_value"].max()]
)

# COMMAND ----------

# DBTITLE 1,CCI Score by Race
display(
    cci_demos
        .groupBy("cci_score")
        .pivot("race")
        .agg(
            F.countDistinct("hvid")
        )
        .orderBy("cci_score")
)

# COMMAND ----------

# DBTITLE 1,CCI Score by Ethnicity
display(
    cci_demos
        .groupBy("cci_score")
        .pivot("ethnicity")
        .agg(
            F.countDistinct("hvid")
        )
        .orderBy("cci_score")
)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ## Experiments

# COMMAND ----------

# %pip install seaborn --upgrade

# COMMAND ----------

# cci_demos = spark.table("cdh_hv_race_ethnicity_covid_exploratory.rvn4_cci_demos")

# pdf = cci_demos.toPandas()

# COMMAND ----------

# import numpy as np
# import pandas as pd
# import seaborn as sns
# import matplotlib.pyplot as plt
# sns.set_theme(style="white", rc={"axes.facecolor": (0, 0, 0, 0)})

# COMMAND ----------

# # Initialize the FacetGrid object
# pal = sns.cubehelix_palette(2, rot=-.25, light=.7)
# g = sns.FacetGrid(pdf, row="sex", hue="sex", aspect=15, height=1, palette=pal)

# # Draw the densities in a few steps
# g.map(sns.kdeplot, "cci_score",
#       bw_adjust=.5, clip_on=False,
#       fill=True, alpha=1, linewidth=1.5)
# g.map(sns.kdeplot, "cci_score", clip_on=False, color="w", lw=2, bw_adjust=.5)

# # passing color=None to refline() uses the hue mapping
# g.refline(y=0, linewidth=2, linestyle="-", color=None, clip_on=False)

# # Define and use a simple function to label the plot in axes coordinates
# def label(x, color, label):
#     ax = plt.gca()
#     ax.text(0, .2, label, fontweight="bold", color=color,
#             ha="left", va="center", transform=ax.transAxes)


# g.map(label, "cci_score")

# # Set the subplots to overlap
# g.figure.subplots_adjust(hspace=-.25)

# # Remove axes details that don't play well with overlap
# g.set_titles("")
# g.set(yticks=[], ylabel="")
# g.tight_layout()
# g.despine(bottom=True, left=True)

# COMMAND ----------

# import plotly.graph_objects as go
# from plotly.colors import n_colors

# colors = n_colors('rgb(5, 200, 200)', 'rgb(200, 10, 10)', 2, colortype='rgb')

# fig = go.Figure()

# for data_line, color in zip(["M", "F"], colors):
#     fig.add_trace(go.Violin(x=pdf[pdf["sex"]==data_line]["cci_score"], line_color=color))
    
# fig.update_traces(orientation='h', side='positive', width=3, points=False)
# fig.update_layout(xaxis_showgrid=False, xaxis_zeroline=False)
# fig.show()
