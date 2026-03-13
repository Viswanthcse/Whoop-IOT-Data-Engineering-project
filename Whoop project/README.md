# Whoop IoT Data Engineering Pipeline
![Whoop Data Engineering pipeline architecture (GCP) - Light Theme](whoop_architecture_v2_1773363253607.png)

*(Alternative Dark/Neon version below)*
![Whoop Data Engineering pipeline architecture (GCP) - Dark Theme](whoop_architecture_v3_1773363266964.png)
## Overview
This project simulates an enterprise-grade IoT Data Engineering pipeline, ingesting both massive-scale batch data and high-frequency streaming sensor data from wearable fitness devices (like Whoop rings). 

The goal of this system is to ingest raw physiological telemetry (Heart Rate, HR Variability, Skin Temp, Sleep Stages) and process it into a highly optimized Dimensional Data Warehouse for downstream product analytics and personal health dashboards.

## Technology Stack
*   **Infrastructure as Code (IaC):** Terraform
*   **OLTP Database:** Cloud SQL (PostgreSQL)
*   **NoSQL / Time-Series Database:** Cloud Bigtable
*   **Data Lake:** Google Cloud Storage (GCS)
*   **Compute / Processing:** Apache Spark (PySpark) on Google Cloud Dataproc
*   **Orchestration:** Apache Airflow
*   **Data Warehouse:** Google BigQuery
*   **API & Simulation Layer:** FastAPI (Python)

---

## The Two-Pronged Ingestion Architecture

### 1. The Batch Pipeline (OLTP)
Daily summaries and workout sessions are compiled directly on the user's phone. When they open the app, this data syncs to a backend API which normalizes the records (Users, Devices, Workouts, Health Rollups) into a **Cloud SQL PostgreSQL** database.

### 2. The Streaming Pipeline (IoT Sensors)
During a workout, the wearable device continuously broadcasts raw heart rate and skin temperature metrics every second. This massive influx of time-series data bypasses the relational database entirely and is streamed directly into an ultra-fast **Cloud Bigtable** instance.

---

## The ETL / Processing Layer
The true Data Engineering magic happens orchestrating **Apache PySpark**. An **Apache Airflow DAG** executes two concurrent massive processing jobs on an ephemeral Dataproc cluster:

1.  **`spark_elt_oltp.py`**: Connects via JDBC to the Postgres instance, extracts the heavily normalized tables, executes business logic (e.g., calculating Basal Metabolic Rates, Performance Age, and rolling 7-day Accumulated Fatigue levels via Window functions), and models the output into a Star Schema inside BigQuery (`dim_users`, `fact_workouts`, `fact_daily_health`).
2.  **`spark_elt_streaming.py`**: Connects to the massive Bigtable cluster, rips through millions of second-by-second telemetry pings, downsamples and aggregates them into clean `hourly_vital` row maximums and averages to save BigQuery ingestion costs and boost dashboard load speeds. 

---

## Business Impact & Derived Insights
Because the data is carefully modeled into a Star Schema, Data Scientists and Product Managers can easily query BigQuery to answer critical questions:
*   *Which firmware versions are repeatedly failing to record sleep data?*
*   *Are cardiovascular strain scores actively reducing next-day Heart Rate Variability?*
*   *What is the platform's overall user retention layered against their 30-day "Recovery" metrics?*

## Deployment Instructions
1. Establish GCP Project, Enable Billing, and download a Service Account JSON Key with required privileges.
2. Initialize the Infrastructure:
```bash
cd infrastructure
terraform init
terraform apply
```
3. Set your GCP environment variables within Airflow.
4. Drop your PySpark scripts into the generated `script-zone` GCS bucket.
5. Trigger the `whoop_enterprise_etl` DAG from the Airflow UI!
