# Databricks notebook source
import pyspark.sql.functions as F

# COMMAND ----------

DB_WRITE = "cdh_hv_race_ethnicity_covid_exploratory"

# COMMAND ----------

# spark configuration
spark.conf.set('spark.sql.shuffle.partitions', 6400)
spark.conf.set('spark.databricks.adaptive.autoOptimizeShuffle.enabled', False)
spark.conf.set('spark.databricks.queryWatchdog.enabled', False)

# COMMAND ----------

# MAGIC %run ../../includes/0000-utils

# COMMAND ----------

df_pmca = spark.table(f"{DB_WRITE}.sve5_hvid_pmca")
df_ccsr_wide = spark.table(f"{DB_WRITE}.sve5_hvid_ccsr_wide_cohort")
df_demo = spark.table(f"{DB_WRITE}.covid_full_hvids_demographics_0812")

# COMMAND ----------

df_ccsr_wide = assign_regions("patient_state")(df_ccsr_wide)

# COMMAND ----------

pmca_demos = (
    df_pmca
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

pmca_demos.write.mode("overwrite").saveAsTable(f"{DB_WRITE}.rvn4_pmca_demos")

# COMMAND ----------

pmca_demos = spark.table(f"{DB_WRITE}.rvn4_pmca_demos")

pmca_demos.limit(10).display()

# COMMAND ----------

# DBTITLE 1,Number of Unique Patients
display(
    pmca_demos.select(F.countDistinct("hvid"))
)

# COMMAND ----------

display(
    pmca_demos
        .groupBy("pmca_score")
        .agg(
            F.countDistinct("hvid")
        )
        .orderBy("pmca_score")
)

# COMMAND ----------

# DBTITLE 1,PMCA Score by Sex
display(
    pmca_demos
        .groupBy("pmca_score")
        .pivot("sex")
        .agg(
            F.countDistinct("hvid")
        )
        .orderBy("pmca_score")
)

# px.histogram(temp_df, x="pmca_score", y=["F", "M"], barmode="overlay")

# COMMAND ----------

# import plotly.figure_factory as ff
# from plotly.colors import n_colors

# temp_df = (
#     pmca_demos
#         .groupBy("pmca_score")
#         .pivot("sex")
#         .agg(
#             F.countDistinct("hvid")
#         )
#         .orderBy("pmca_score")
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

# DBTITLE 1,PMCA Score by Age Group
display(
    pmca_demos
        .groupBy("pmca_score")
        .pivot("age_group")
        .agg(
            F.countDistinct("hvid")
        )
        .orderBy("pmca_score")
)

# COMMAND ----------

# DBTITLE 1,PMCA Score by Region
display(
    pmca_demos
        .groupBy("pmca_score")
        .pivot("region")
        .agg(
            F.countDistinct("hvid")
        )
        .orderBy("pmca_score")
)

# COMMAND ----------

# DBTITLE 1,PMCA Score by State
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
    pmca_demos
        .groupBy("pmca_score")
        .pivot("state")
        .agg(
            F.countDistinct("hvid")
        )
        .orderBy("pmca_score")
)

temp_pdf = (melt(temp_df, id_vars=["pmca_score"], value_vars=temp_df.columns[1:])).toPandas()

# COMMAND ----------

temp_pdf.head()

# COMMAND ----------

import plotly.express as px
import numpy as np

temp_pdf['log_value'] = np.log10(temp_pdf['value'])
px.choropleth(
    temp_pdf,
    locations="variable",
    color="log_value",
    animation_frame="pmca_score",
    locationmode="USA-states",
    scope="usa",
#     title="Counts per State over pmca Score"
    range_color=[0, temp_pdf["log_value"].max()]
)

# COMMAND ----------

# DBTITLE 1,PMCA Score by Race
display(
    pmca_demos
        .groupBy("pmca_score")
        .pivot("race")
        .agg(
            F.countDistinct("hvid")
        )
        .orderBy("pmca_score")
)

# COMMAND ----------

# DBTITLE 1,PMCA Score by Ethnicity
display(
    pmca_demos
        .groupBy("pmca_score")
        .pivot("ethnicity")
        .agg(
            F.countDistinct("hvid")
        )
        .orderBy("pmca_score")
)
