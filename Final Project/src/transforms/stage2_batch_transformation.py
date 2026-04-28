from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, lower, trim, lit,
    countDistinct, count,
    sum as spark_sum,
    avg, round,
    to_timestamp, year, month
)

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("MediaWave Stage 2 Batch Transformation") \
    .config("spark.ui.enabled", "false") \
    .getOrCreate()

print("Spark is running")
print("Spark version:", spark.version)

base_path = "hdfs://hadoop-namenode:9000/mediawave-lake/landing"
curated_path = "hdfs://hadoop-namenode:9000/mediawave-lake/curated"
analytics_path = "hdfs://hadoop-namenode:9000/mediawave-lake/analytics"

viewing_df = spark.read.csv(
    f"{base_path}/viewing-history/viewing-history.csv.gz",
    header=True,
    inferSchema=True
)

catalog_df = spark.read.json(
    f"{base_path}/content-catalog/content-catalog.json.gz"
)

users_df = spark.read.csv(
    f"{base_path}/user-profiles/user-profiles.csv.gz",
    header=True,
    inferSchema=True
)

interactions_df = spark.read.json(
    f"{base_path}/user-interactions/user-interactions.json.gz"
)

quality_df = spark.read.csv(
    f"{base_path}/streaming-quality/streaming-quality.csv.gz",
    header=True,
    inferSchema=True
)

print("All files loaded successfully")

# -----------------------------
# Cleaning
# -----------------------------

viewing_clean = viewing_df \
    .filter(col("user_id").isNotNull()) \
    .filter(col("content_id").isNotNull()) \
    .withColumn(
        "duration_watched_min",
        when(col("duration_watched_min") > col("total_duration_min"), col("total_duration_min"))
        .otherwise(col("duration_watched_min"))
    ) \
    .withColumn(
        "completion_pct",
        when(col("completion_pct") < 0, 0)
        .when(col("completion_pct") > 100, 100)
        .otherwise(col("completion_pct"))
    ) \
    .withColumn(
        "device_standardized",
        when(lower(col("device")).contains("tv"), "TV")
        .when(lower(col("device")).contains("phone"), "Mobile")
        .when(lower(col("device")).contains("mobile"), "Mobile")
        .when(lower(col("device")).contains("tablet"), "Tablet")
        .when(lower(col("device")).contains("web"), "Web")
        .when(lower(col("device")).contains("desktop"), "Web")
        .otherwise("Other")
    )

interactions_clean = interactions_df \
    .withColumn(
        "search_query",
        when(col("search_query").isNull(), lit("no_search_query"))
        .otherwise(trim(col("search_query")))
    )

print("Cleaning complete")

viewing_clean.write.mode("overwrite").parquet(
    f"{curated_path}/viewing-history/"
)

interactions_clean.write.mode("overwrite").parquet(
    f"{curated_path}/user-interactions/"
)

print("Curated datasets written")

# -----------------------------
# Join datasets
# -----------------------------

viewing_content_df = viewing_clean.join(
    catalog_df,
    on="content_id",
    how="left"
)

full_sessions_df = viewing_content_df.join(
    users_df,
    on="user_id",
    how="left"
)

# Quality data does not share session_id with viewing history.
# We aggregate quality by user_id and join at the user level.
quality_user_metrics = quality_df \
    .groupBy("user_id") \
    .agg(
        round(avg("buffering_events"), 2).alias("avg_buffering_events"),
        round(avg("avg_bitrate_mbps"), 2).alias("avg_bitrate_mbps"),
        round(avg("resolution_changes"), 2).alias("avg_resolution_changes"),
        round(avg("latency_ms"), 2).alias("avg_latency_ms")
    )

full_sessions_df = full_sessions_df.join(
    quality_user_metrics,
    on="user_id",
    how="left"
)

full_sessions_df.write.mode("overwrite").parquet(
    f"{curated_path}/enriched-viewing-sessions/"
)

print("Enriched viewing sessions written")

# -----------------------------
# User monthly engagement metrics
# -----------------------------

user_monthly_metrics = full_sessions_df \
    .withColumn("event_ts", to_timestamp(col("start_time"))) \
    .withColumn("year", year(col("event_ts"))) \
    .withColumn("month", month(col("event_ts"))) \
    .groupBy("user_id", "year", "month", "plan_type", "is_churned") \
    .agg(
        round(spark_sum(col("duration_watched_min")) / 60, 2).alias("total_watch_hours"),
        countDistinct("content_id").alias("unique_titles_watched"),
        round(avg("completion_pct"), 2).alias("avg_completion_pct"),
        count("*").alias("total_viewing_records"),
        round(avg("avg_buffering_events"), 2).alias("avg_buffering_events"),
        round(avg("avg_resolution_changes"), 2).alias("avg_resolution_changes"),
        round(avg("avg_latency_ms"), 2).alias("avg_latency_ms")
    )

interaction_counts = interactions_clean \
    .withColumn("event_ts", to_timestamp(col("timestamp"))) \
    .withColumn("year", year(col("event_ts"))) \
    .withColumn("month", month(col("event_ts"))) \
    .groupBy("user_id", "year", "month") \
    .agg(
        count("*").alias("interaction_count"),
        countDistinct("content_id").alias("interacted_titles")
    )

user_monthly_final = user_monthly_metrics.join(
    interaction_counts,
    on=["user_id", "year", "month"],
    how="left"
).fillna({
    "interaction_count": 0,
    "interacted_titles": 0
})

# -----------------------------
# Content performance metrics
# -----------------------------

content_performance = full_sessions_df \
    .groupBy(
        "content_id",
        "title",
        "release_year",
        "imdb_score",
        "license_cost_monthly"
    ) \
    .agg(
        countDistinct("user_id").alias("unique_viewers"),
        count("*").alias("total_viewing_records"),
        round(spark_sum(col("duration_watched_min")) / 60, 2).alias("total_viewing_hours"),
        round(avg("completion_pct"), 2).alias("avg_completion_pct")
    ) \
    .withColumn(
        "cost_per_viewing_hour",
        when(
            col("total_viewing_hours") > 0,
            round(col("license_cost_monthly") / col("total_viewing_hours"), 2)
        ).otherwise(None)
    )

# -----------------------------
# Streaming quality by CDN and ISP
# -----------------------------

quality_by_region_isp = quality_df \
    .groupBy("cdn_region", "isp") \
    .agg(
        countDistinct("session_id").alias("total_sessions"),
        round(avg("buffering_events"), 2).alias("avg_buffering_events"),
        round(avg("avg_bitrate_mbps"), 2).alias("avg_bitrate_mbps"),
        round(avg("resolution_changes"), 2).alias("avg_resolution_changes"),
        round(avg("latency_ms"), 2).alias("avg_latency_ms")
    )

# -----------------------------
# Write analytics outputs
# -----------------------------

user_monthly_final.write.mode("overwrite").parquet(
    f"{analytics_path}/user-monthly-engagement/"
)

content_performance.write.mode("overwrite").parquet(
    f"{analytics_path}/content-performance/"
)

quality_by_region_isp.write.mode("overwrite").parquet(
    f"{analytics_path}/streaming-quality-region-isp/"
)

print("Analytics datasets written")

print("User monthly engagement preview:")
user_monthly_final.show(10, truncate=False)

print("Content performance preview:")
content_performance.show(10, truncate=False)

print("Streaming quality preview:")
quality_by_region_isp.show(10, truncate=False)

spark.stop()

print("Stage 2 batch transformation complete")
