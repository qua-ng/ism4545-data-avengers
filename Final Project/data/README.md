# Data

The MediaWave Streaming dataset files are already in this folder. No need to download anything.

## Getting started

From the Final Project folder, start the containers you need for your stage:

Stage 2 (Spark transforms):
docker compose up -d namenode datanode1 datanode2 spark-master spark-worker jupyter

Stage 3 (Kafka streaming):
docker compose up -d namenode datanode1 datanode2 spark-master spark-worker zookeeper kafka kafka-ui jupyter

Stage 4 (Airflow orchestration):
docker compose up -d namenode datanode1 datanode2 spark-master spark-worker postgres airflow-init airflow-webserver airflow-scheduler

Full stack (16 GB RAM):
docker compose up -d

Then open Jupyter at http://localhost:8888?token=spark

Data is available inside the containers at /home/jovyan/data/ (Jupyter) and /data/ (HDFS namenode).

To stop everything: docker compose down
