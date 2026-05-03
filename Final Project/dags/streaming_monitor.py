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

    check_kafka_broker = BashOperator(
        task_id="check_kafka_broker",
        bash_command="kafka-broker-api-versions --bootstrap-server kafka:9092 > /dev/null 2>&1 && echo 'Kafka broker is healthy' || (echo 'Kafka broker is down' && exit 1)"
    )

    check_kafka_topic = BashOperator(
        task_id="check_kafka_topic",
        bash_command="kafka-topics --bootstrap-server kafka:9092 --describe --topic mediawave-user-activity 2>/dev/null && echo 'Topic exists and is accessible' || (echo 'Topic not found' && exit 1)"
    )

    check_spark_master = BashOperator(
        task_id="check_spark_master",
        bash_command="curl -sf http://spark-master:8080 > /dev/null && echo 'Spark master is healthy' || (echo 'Spark master is down' && exit 1)"
    )

    check_checkpoint_freshness = BashOperator(
        task_id="check_checkpoint_freshness",
        bash_command="ls -lt /tmp/mediawave_stage3_checkpoints/ 2>/dev/null && echo 'Checkpoints found' || echo 'No checkpoints yet — streaming may not have started'"
    )

    end_monitoring = EmptyOperator(task_id="end_monitoring")

    start_monitoring >> check_kafka_broker >> check_kafka_topic >> check_spark_master >> check_checkpoint_freshness >> end_monitoring
