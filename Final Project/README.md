# MediaWave Streaming — Big Data Pipeline

**ISM 6562 · Final Project · Team: Data Avengers**

A complete big data pipeline (HDFS · Spark · Kafka · Airflow) for MediaWave Streaming, a fictional video streaming company facing content recommendation and viewer engagement challenges.

Repository: https://github.com/qua-ng/ism4545-data-avengers

---

## Team Members

| Name | Role | GitHub |
|------|------|--------|
| Juan | Integration Lead + HDFS (Stage 1) | @juanesriverae-bit |
| Yusuke | Spark Batch (Stage 2) | @yoshizawa-yu |
| Quang | Kafka + Streaming (Stage 3) | @qua-ng |
| Alex | Airflow + Data Quality (Stage 4) | @aleksandr-nikolaev66 |
| Helen | Project Management + Report + Presentation | @helennganguyen |

---

## Project Scenario

MediaWave Streaming is a fictional video streaming platform managing 200,000 subscribers, a 20,000-title content catalog, and over 700,000 viewing sessions. Despite generating large volumes of data across five separate systems, the platform had no unified way to answer cross-source business questions — such as which titles cost more in licensing than they earn in viewing hours, or which early behavioral signals predict subscriber cancellation.

This project builds a four-layer big data pipeline that ingests all five raw data sources into an HDFS data lake, transforms and joins them using PySpark, processes real-time viewing events through Kafka and Spark Structured Streaming, and orchestrates the entire workflow with Apache Airflow. The pipeline produces three analytics tables that directly support content ROI analysis, churn prediction, and CDN investment decisions.

The five datasets used are: viewing history (700K records), user profiles (200K records), content catalog (20K titles), user interactions (550K records), and streaming quality metrics (200K records).

---

## Architecture Overview

The pipeline implements four layers, processing MediaWave streaming data from raw landing through curated analytics:

1. **Store** — HDFS data lake with landing / curated / analytics zones
2. **Transform** — PySpark batch jobs for cleaning, joins, and aggregations
3. **Stream** — Kafka + Spark Structured Streaming for real-time viewing events
4. **Orchestrate** — Airflow DAGs with quality gates, retries, and monitoring

![Architecture Diagram](report/architecture-diagram.png)

---

## Setup Instructions

A grader should be able to clone this repo, run `docker compose up -d`, and reproduce the full pipeline.

### Prerequisites

- Docker Desktop (>= 20.10) with **at least 8 GB RAM allocated**
- Git
- ~10 GB free disk space
- Apple Silicon users: Rosetta 2 enabled (`platform: linux/amd64` is set on all services)

### Step 1 — Clone the repo

```
git clone https://github.com/qua-ng/ism4545-data-avengers.git
cd ism4545-data-avengers/"Final Project"
```

### Step 2 — Download the Miniconda installer (required before first build)

The Spark Docker image requires Miniconda. Run this once inside the `docker/` folder before building:

```
curl -L -o docker/miniconda.sh https://repo.anaconda.com/miniconda/Miniconda3-py311_25.1.1-2-Linux-x86_64.sh
```

### Step 3 — Boot the full stack

```
docker compose up -d
```

Wait approximately 3 minutes for all services to become healthy. Then verify each service is reachable:

| Service | URL | Login |
|---------|-----|-------|
| HDFS NameNode | http://localhost:9870 | — |
| Spark Master | http://localhost:8083 | — |
| Airflow | http://localhost:8080 | admin / admin |
| Kafka UI | http://localhost:8088 | — |
| Jupyter | http://localhost:8888?token=spark | token: `spark` |

### Step 4 — Run Stage 1: Load data into HDFS

Open Jupyter at http://localhost:8888?token=spark and run all cells in:

```
notebooks/01-data-lake-setup.ipynb
```

This uploads all 5 datasets into the HDFS landing zone and creates the full data lake directory structure.

### Step 5 — Run Stage 2: Spark batch transformation

In Jupyter, run all cells in:

```
notebooks/stage2_batch_transformation.ipynb
```

This cleans the raw data, performs three-way joins, and writes curated and analytics Parquet tables to HDFS.

### Step 6 — Run Stage 3: Kafka streaming pipeline

In Jupyter, run all cells in:

```
notebooks/stage3-streaming.ipynb
```

This creates the Kafka topic `mediawave-user-activity`, produces synthetic viewing events, and runs three streaming queries: concurrent viewers per title (5-min window), poor-experience alerts (buffering > 3 per session), and trending spike detection. Each query runs for 60 seconds then stops automatically.

**Important:** Run this notebook fully before triggering the Airflow DAG in Step 7, so Spark resources are free.

### Step 7 — Run Stage 4: Airflow orchestration

Go to Airflow at http://localhost:8080 (admin / admin), find `mediawave_batch_pipeline`, and click the play button to trigger a manual run.

The DAG runs 9 tasks in sequence:

1. `start_pipeline` — entry marker
2. `check_landing_files_exist` — verify all 5 raw datasets are present in HDFS
3. `run_data_quality_gates` — null checks and value-range validation
4. `run_stage2_batch_transformation` — clean, join, and aggregate
5. `validate_curated_outputs` — confirm curated zone tables exist
6. `validate_analytics_outputs` — confirm analytics zone tables exist
7. `calculate_churn_risk_scores` — score every user-month for churn risk
8. `export_daily_reports` — write daily CSV/Parquet reports
9. `end_pipeline` — exit marker

A second DAG, `mediawave_streaming_monitor`, runs every 15 minutes and verifies Kafka broker health, topic accessibility, Spark master availability, and streaming checkpoint freshness.

All tasks should complete green in approximately 8–10 minutes.

---

## Data Sources

All datasets are included in the repository under `data/`:

| File | Records | Description | Format |
|------|---------|-------------|--------|
| user-profiles.csv.gz | 200,000 | Subscriber demographics, signup, subscription tier | CSV |
| viewing-history.csv.gz | 700,000 | Per-session viewing events with completion % | CSV |
| content-catalog.json.gz | 20,000 | Title metadata: genre, IMDB score, license cost | JSON |
| user-interactions.json.gz | 550,000 | Ratings, likes, searches, clicks | JSON |
| streaming-quality.csv.gz | 200,000 | Buffering, bitrate, resolution, latency | CSV |

Original source: https://github.com/prof-tcsmith/ism6562s26-class/tree/main/final-projects/data/10-mediawave-streaming/

---

## Repository Structure

```
Final Project/
├── README.md                   # This file
├── docker-compose.yml          # Full infrastructure (HDFS, Spark, Kafka, Airflow, Jupyter)
├── docker/                     # Custom Dockerfiles
│   ├── Dockerfile.airflow      # Airflow + Spark provider
│   └── Dockerfile.spark        # Spark + PySpark
├── data/                       # All 5 datasets + data README
├── docs/                       # Supplementary documentation
│   ├── data-lake-architecture.png
│   └── stage-notes/            # Per-stage design notes
├── notebooks/                  # Jupyter notebooks (one per pipeline stage)
│   ├── 01-data-lake-setup.ipynb           # Stage 1: HDFS data lake
│   ├── stage2_batch_transformation.ipynb  # Stage 2: Spark batch
│   └── stage3-streaming.ipynb             # Stage 3: Kafka streaming
├── dags/                       # Airflow DAGs (Stage 4)
│   ├── batch_pipeline.py       # Main 9-task batch orchestration DAG
│   ├── streaming_monitor.py    # 15-minute streaming health monitor
│   └── spark_jobs/             # Spark scripts called by Airflow
│       ├── data_quality_gates.py
│       ├── stage2_batch_transformation.py
│       ├── churn_risk_scoring.py
│       └── export_reports.py
├── producers/                  # Kafka event producer (Stage 3)
│   └── stage3_producer.py
├── presentation/               # Slide deck, script, screenshots
│   ├── Slides.pptx
│   ├── Script.docx
│   ├── Appendix for Slides.docx
│   └── screenshots/
└── report/                     # Final written report and figures
    ├── Executive Summary - Final Version (05.05.26).docx
    └── architecture-diagram.png
```

---

## Memory Configuration

The full stack targets ~8 GB RAM for all services combined.

| Layer | Services | RAM |
|-------|----------|-----|
| HDFS | namenode + 2 datanodes | ~1.5 GB |
| Spark | master + worker | ~3 GB |
| Kafka | zookeeper + broker + UI | ~1.5 GB |
| Airflow | postgres + webserver + scheduler | ~1.5 GB |
| Jupyter | notebook server | ~2 GB |

### Lightweight Mode (8 GB machines)

Run only the services needed for each stage:

**Stage 1 + 2 only:**
```
docker compose up -d namenode datanode1 datanode2 spark-master spark-worker jupyter
```

**Stage 3 only:**
```
docker compose up -d namenode datanode1 datanode2 spark-master spark-worker zookeeper kafka kafka-ui jupyter
```

**Stage 4 only:**
```
docker compose up -d namenode datanode1 datanode2 spark-master spark-worker postgres airflow-init airflow-webserver airflow-scheduler
```

---

## Key Findings

1. **Content ROI varies by 860×.** The bottom decile of titles by cost-per-viewing-hour drives 17% of total catalog spend ($3.13B annually) with disproportionately low engagement. Dropping these titles at the next license cycle is the highest-impact action available — estimated annual savings of $1.5–3.1B.

2. **Monthly watch hours is the dominant churn signal.** Churned users averaged 15.2 hours/month versus 26.4 hours for retained users (a 42% gap). Completion rate, buffering events, and unique titles per month showed no meaningful separation between cohorts. Churn modeling should focus on volume, not quality, signals.

3. **Streaming quality is geographically uniform.** All 7 CDN regions and 10 ISPs cluster within a 3.3% spread of buffering events, with near-zero correlation (r = −0.0009) between buffering and completion. CDN investment should be deprioritized in favor of content and engagement levers.

---

## Acknowledgments

Built for ISM 6562 (Big Data for Business Applications), Spring 2026, Dr. Tim Smith. Pipeline architecture follows the four-layer pattern covered in Weeks 8–11.
