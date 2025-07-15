# Databricks notebook source
# MAGIC %pip install feedparser

# COMMAND ----------

import feedparser as fp


# COMMAND ----------

pkglist=['spacy','scispacy','pymc','pymc-bart','huggingface',]
limit=3

# COMMAND ----------

for pkg in pkglist:
    feed=fp.parse(f"https://pypi.org/rss/project/{pkg}/releases.xml")
    print("Feed Title:", feed.feed.title)
    print("Feed Description:", feed.feed.description)
    print("Feed Link:", feed.feed.link)
    n=0
    for entry in feed.entries[:limit]:
        print("Entry Title:", entry.title)
        print("Entry Link:", entry.link)
        print("Entry Published Date:", entry.published)
        print("Entry Summary:", entry.summary)
        print("\n")

# COMMAND ----------

dbutils.fs.ls("dbfs:/cluster-logs/mlgpu/0630-181430-yfox4zns/init_scripts/0630-181430-yfox4zns_172_18_16_168/20240130_144505_02_init_scispacy.sh.stderr.log")
