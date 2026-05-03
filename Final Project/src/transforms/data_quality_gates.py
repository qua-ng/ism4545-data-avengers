from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder.appName("MediaWave Stage 4 Data Quality Gates").getOrCreate()

base_path = "hdfs://hadoop-namenode:9000/mediawave-lake/landing"

viewing_df = spark.read.csv(f"{base_path}/viewing-history/viewing-history.csv.gz", header=True, inferSchema=True)
users_df = spark.read.csv(f"{base_path}/user-profiles/user-profiles.csv.gz", header=True, inferSchema=True)
catalog_df = spark.read.json(f"{base_path}/content-catalog/content-catalog.json.gz")
quality_df = spark.read.csv(f"{base_path}/streaming-quality/streaming-quality.csv.gz", header=True, inferSchema=True)
interactions_df = spark.read.json(f"{base_path}/user-interactions/user-interactions.json.gz")

print("Running MediaWave Stage 4 data quality gates...")

invalid_viewing_duration_count = viewing_df.filter(
    (col("duration_watched_min") > col("total_duration_min")) |
    (col("completion_pct") < 0) |
    (col("completion_pct") > 100)
).count()

missing_user_count = viewing_df.join(users_df.select("user_id"), on="user_id", how="left_anti").count()
missing_content_count = viewing_df.join(catalog_df.select("content_id"), on="content_id", how="left_anti").count()

invalid_quality_count = quality_df.filter(
    (col("buffering_events") < 0) |
    (col("avg_bitrate_mbps") <= 0) |
    (col("resolution_changes") < 0) |
    (col("latency_ms") < 0)
).count()

invalid_interaction_user_count = interactions_df.join(
    users_df.select("user_id"),
    on="user_id",
    how="left_anti"
).count()

print("Quality gate results:")
print(f"Invalid viewing duration rows: {invalid_viewing_duration_count}")
print(f"Viewing rows with missing users: {missing_user_count}")
print(f"Viewing rows with missing content: {missing_content_count}")
print(f"Invalid streaming quality rows: {invalid_quality_count}")
print(f"Interaction rows with missing users: {invalid_interaction_user_count}")

if missing_user_count > 0:
    raise ValueError("Quality gate failed: some viewing records reference unknown user_id values.")
if missing_content_count > 0:
    raise ValueError("Quality gate failed: some viewing records reference unknown content_id values.")
if invalid_quality_count > 0:
    raise ValueError("Quality gate failed: streaming quality data contains impossible technical values.")
if invalid_interaction_user_count > 0:
    raise ValueError("Quality gate failed: interaction records reference unknown user_id values.")

print("All required Stage 4 quality gates passed.")
spark.stop()
