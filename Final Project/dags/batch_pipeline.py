from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


default_args = {
    "owner": "mediawave-stage4",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="mediawave_batch_pipeline",
    description="Stage 4 batch orchestration DAG for MediaWave Streaming analytics",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["mediawave", "stage4", "batch", "spark", "hdfs"],
) as dag:

    start_pipeline = EmptyOperator(task_id="start_pipeline")

    check_landing_files_exist = BashOperator(
        task_id="check_landing_files_exist",
        bash_command="""
        curl -sf "http://hadoop-namenode:9870/webhdfs/v1/mediawave-lake/landing/viewing-history/viewing-history.csv.gz?op=GETFILESTATUS" &&
        curl -sf "http://hadoop-namenode:9870/webhdfs/v1/mediawave-lake/landing/user-profiles/user-profiles.csv.gz?op=GETFILESTATUS" &&
        curl -sf "http://hadoop-namenode:9870/webhdfs/v1/mediawave-lake/landing/content-catalog/content-catalog.json.gz?op=GETFILESTATUS" &&
        curl -sf "http://hadoop-namenode:9870/webhdfs/v1/mediawave-lake/landing/user-interactions/user-interactions.json.gz?op=GETFILESTATUS" &&
        curl -sf "http://hadoop-namenode:9870/webhdfs/v1/mediawave-lake/landing/streaming-quality/streaming-quality.csv.gz?op=GETFILESTATUS"
        """
    )

    run_data_quality_gates = SparkSubmitOperator(
        task_id="run_data_quality_gates",
        application="/opt/airflow/dags/spark_jobs/data_quality_gates.py",
        conn_id="spark_default"
    )

    run_stage2_batch_transformation = SparkSubmitOperator(
        task_id="run_stage2_batch_transformation",
        application="/opt/airflow/dags/spark_jobs/stage2_batch_transformation.py",
        conn_id="spark_default"
    )

    validate_curated_outputs = BashOperator(
        task_id="validate_curated_outputs",
        bash_command="""
        curl -sf "http://hadoop-namenode:9870/webhdfs/v1/mediawave-lake/curated/viewing-history?op=GETFILESTATUS" &&
        curl -sf "http://hadoop-namenode:9870/webhdfs/v1/mediawave-lake/curated/user-interactions?op=GETFILESTATUS" &&
        curl -sf "http://hadoop-namenode:9870/webhdfs/v1/mediawave-lake/curated/enriched-viewing-sessions?op=GETFILESTATUS"
        """
    )

    validate_analytics_outputs = BashOperator(
        task_id="validate_analytics_outputs",
        bash_command="""
        curl -sf "http://hadoop-namenode:9870/webhdfs/v1/mediawave-lake/analytics/user-monthly-engagement?op=GETFILESTATUS" &&
        curl -sf "http://hadoop-namenode:9870/webhdfs/v1/mediawave-lake/analytics/content-performance?op=GETFILESTATUS" &&
        curl -sf "http://hadoop-namenode:9870/webhdfs/v1/mediawave-lake/analytics/streaming-quality-region-isp?op=GETFILESTATUS"
        """
    )

    calculate_churn_risk_scores = SparkSubmitOperator(
        task_id="calculate_churn_risk_scores",
        application="/opt/airflow/dags/spark_jobs/churn_risk_scoring.py",
        conn_id="spark_default"
    )

    export_daily_reports = SparkSubmitOperator(
        task_id="export_daily_reports",
        application="/opt/airflow/dags/spark_jobs/export_reports.py",
        conn_id="spark_default"
    )

    end_pipeline = EmptyOperator(task_id="end_pipeline")

    start_pipeline >> check_landing_files_exist
    check_landing_files_exist >> run_data_quality_gates
    run_data_quality_gates >> run_stage2_batch_transformation
    run_stage2_batch_transformation >> validate_curated_outputs
    validate_curated_outputs >> validate_analytics_outputs
    validate_analytics_outputs >> calculate_churn_risk_scores
    calculate_churn_risk_scores >> export_daily_reports
    export_daily_reports >> end_pipeline
