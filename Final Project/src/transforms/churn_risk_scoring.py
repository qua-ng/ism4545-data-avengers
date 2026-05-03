from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, round


spark = SparkSession.builder.appName("MediaWave Stage 4 Churn Risk Scoring").getOrCreate()

analytics_path = "hdfs://hadoop-namenode:9000/mediawave-lake/analytics"

user_engagement = spark.read.parquet(f"{analytics_path}/user-monthly-engagement/")

churn_risk = user_engagement \
    .withColumn(
        "watch_hours_risk",
        when(col("total_watch_hours") < 2, 1.0)
        .when(col("total_watch_hours") < 5, 0.7)
        .when(col("total_watch_hours") < 10, 0.4)
        .otherwise(0.0)
    ) \
    .withColumn(
        "completion_risk",
        when(col("avg_completion_pct") < 25, 1.0)
        .when(col("avg_completion_pct") < 50, 0.6)
        .when(col("avg_completion_pct") < 75, 0.3)
        .otherwise(0.0)
    ) \
    .withColumn(
        "buffering_risk",
        when(col("avg_buffering_events") >= 3, 1.0)
        .when(col("avg_buffering_events") >= 1, 0.5)
        .otherwise(0.0)
    ) \
    .withColumn(
        "interaction_risk",
        when(col("interaction_count") <= 1, 1.0)
        .when(col("interaction_count") <= 5, 0.5)
        .otherwise(0.0)
    ) \
    .withColumn(
        "churn_risk_score",
        round(
            0.40 * col("watch_hours_risk") +
            0.25 * col("completion_risk") +
            0.20 * col("buffering_risk") +
            0.15 * col("interaction_risk"),
            2
        )
    ) \
    .withColumn(
        "churn_risk_segment",
        when(col("churn_risk_score") >= 0.70, "High Risk")
        .when(col("churn_risk_score") >= 0.40, "Medium Risk")
        .otherwise("Low Risk")
    )

churn_risk.write.mode("overwrite").parquet(f"{analytics_path}/churn-risk/")

print("Churn risk scores written to /mediawave-lake/analytics/churn-risk/")
churn_risk.show(20, truncate=False)

spark.stop()
