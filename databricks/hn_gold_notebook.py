# Databricks notebook source
# MAGIC %md
# MAGIC # Hacker News: silver to gold (PySpark + Delta)
# MAGIC Reads the silver Parquet files that Airflow uploads to the volume, then builds Delta tables.
# MAGIC **First time only:** run the setup cell below, then run the Airflow DAG once before running the rest.

# COMMAND ----------

# Setup (safe to re-run)
spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.hn")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.hn.landing")
dbutils.fs.mkdirs("/Volumes/workspace/hn/landing/silver")

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

SILVER_PATH = "/Volumes/workspace/hn/landing/silver/"

silver = spark.read.parquet(SILVER_PATH).dropDuplicates(["item_id", "snapshot_ts"])

# Data quality checks
total = silver.count()
bad = silver.filter(F.col("item_id").isNull() | F.col("snapshot_ts").isNull()).count()
assert total > 0, "No silver data found. Run the Airflow DAG first."
assert bad == 0, f"{bad} rows have a null item_id or snapshot_ts"

silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.hn.silver_stories")
print(f"silver_stories: {total} rows")

# COMMAND ----------

# Gold 1: latest version of each story, plus how it performed across snapshots
latest_w = Window.partitionBy("item_id").orderBy(F.col("snapshot_ts").desc())
latest = silver.withColumn("rn", F.row_number().over(latest_w)).filter("rn = 1").drop("rn")

history = silver.groupBy("item_id").agg(
    F.min("rank").alias("best_rank"),
    F.count("*").alias("snapshots_seen"),
    F.max("score").alias("peak_score"),
)

gold_story = latest.join(history, "item_id")
gold_story.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.hn.gold_story_latest")

# COMMAND ----------

# Gold 2: which websites reach the front page most, and how well they do
gold_domain = (
    gold_story.groupBy("domain")
    .agg(
        F.count("*").alias("stories"),
        F.round(F.avg("peak_score"), 1).alias("avg_peak_score"),
        F.sum("comment_count").alias("total_comments"),
    )
    .orderBy(F.desc("stories"), F.desc("avg_peak_score"))
)
gold_domain.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.hn.gold_domain_stats")

# Gold 3: what hour (UTC) front-page stories were posted, and their average score
gold_hour = (
    gold_story.withColumn("posted_hour_utc", F.hour("posted_at"))
    .groupBy("posted_hour_utc")
    .agg(F.count("*").alias("stories"), F.round(F.avg("peak_score"), 1).alias("avg_peak_score"))
    .orderBy("posted_hour_utc")
)
gold_hour.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("workspace.hn.gold_posting_hour")

# COMMAND ----------

display(spark.table("workspace.hn.gold_domain_stats").limit(20))

# COMMAND ----------

display(spark.table("workspace.hn.gold_posting_hour"))
