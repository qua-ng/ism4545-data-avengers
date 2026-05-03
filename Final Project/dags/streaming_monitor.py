from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "mediawave-stage4",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="mediawave_streaming_monitor",
    description="Stage 4 monitoring DAG for the MediaWave Stage 3 streaming pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="*/15 * * * *",
    catchup=False,
    tags=["mediawave", "stage4", "streaming", "monitoring"],
) as dag:

    start_monitoring = EmptyOperator(task_id="start_monitoring")

    check_streaming_pipeline_health = BashOperator(
        task_id="check_streaming_pipeline_health",
        bash_command="""
        echo "Checking MediaWave Stage 3 streaming pipeline health..."
        echo "The Kafka and Spark Streaming pipeline is expected to run continuously."
        echo "In production, this task would check Kafka topic activity, Spark Streaming status, checkpoint freshness, and recent event arrival times."
        """
    )

    end_monitoring = EmptyOperator(task_id="end_monitoring")

    start_monitoring >> check_streaming_pipeline_health >> end_monitoring
