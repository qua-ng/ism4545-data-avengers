from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder.appName("MediaWave Stage 4 Export Daily Reports").getOrCreate()

analytics_path = "hdfs://hadoop-namenode:9000/mediawave-lake/analytics"
reports_path = f"{analytics_path}/reports"

churn_risk = spark.read.parquet(f"{analytics_path}/churn-risk/")
content_performance = spark.read.parquet(f"{analytics_path}/content-performance/")
user_engagement = spark.read.parquet(f"{analytics_path}/user-monthly-engagement/")
streaming_quality = spark.read.parquet(f"{analytics_path}/streaming-quality-region-isp/")

high_risk_users = churn_risk.filter(col("churn_risk_segment") == "High Risk")
top_content = content_performance.orderBy(col("total_viewing_hours").desc()).limit(100)
worst_quality_regions = streaming_quality.orderBy(
    col("avg_buffering_events").desc(),
    col("avg_latency_ms").desc()
).limit(100)

high_risk_users.write.mode("overwrite").csv(f"{reports_path}/high-risk-users/", header=True)
top_content.write.mode("overwrite").csv(f"{reports_path}/top-content/", header=True)
worst_quality_regions.write.mode("overwrite").csv(f"{reports_path}/worst-quality-region-isp/", header=True)
user_engagement.write.mode("overwrite").parquet(f"{reports_path}/daily-user-engagement/")

print("Daily reports exported successfully.")
print("Reports written to /mediawave-lake/analytics/reports/")

spark.stop()
