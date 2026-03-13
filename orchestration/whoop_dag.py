import os
from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)

# Core Configurations
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "your-gcp-project-id")
REGION = os.environ.get("GCP_REGION", "us-central1")
CLUSTER_NAME = "whoop-dataeng-cluster"
SCRIPT_BUCKET = os.environ.get("GCP_SCRIPT_BUCKET", "whoop-scripts-bucket-12345")
BQ_DATASET = os.environ.get("GCP_BQ_DATASET", "whoop_dw")

# Postgres Info (Cloud SQL)
PG_URL = os.environ.get("PG_JDBC_URL", "jdbc:postgresql://<CLOUDSQL_IP>:5432/fitness_oltp")
PG_USER = os.environ.get("PG_JDBC_USER", "api_user")
PG_PASS = os.environ.get("PG_JDBC_PASS", "supersecurepassword123")

# Bigtable Info
BT_INSTANCE = "whoop-sensor-stream"
BT_TABLE = "heart_rate_timeseries"

# Scripts
SPARK_OLTP_URI = f"gs://{SCRIPT_BUCKET}/scripts/spark_elt_oltp.py"
SPARK_STREAM_URI = f"gs://{SCRIPT_BUCKET}/scripts/spark_elt_streaming.py"

# Dataproc Cluster Config 
CLUSTER_CONFIG = {
    "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-2", "disk_config": {"boot_disk_size_gb": 50}},
    "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-2", "disk_config": {"boot_disk_size_gb": 50}},
    "software_config": {
        "image_version": "2.0-debian10"
    }
}

# Jobs Include Required JARs for PostgreSQL, BigQuery, and BigTable/HBase
JOB_EXTRACT_OLTP = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": SPARK_OLTP_URI,
        "args": [
            "--jdbc_url", PG_URL,
            "--jdbc_user", PG_USER,
            "--jdbc_password", PG_PASS,
            "--bq_dataset", f"{PROJECT_ID}.{BQ_DATASET}",
            "--temp_gcs_bucket", SCRIPT_BUCKET
        ],
        "jar_file_uris": [
            "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
            "gs://your-bucket-for-jars/postgresql-42.2.24.jar"  # Make sure you upload postgres JDBC jar to GCS!
        ]
    }
}

JOB_AGGREGATE_STREAMING = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": SPARK_STREAM_URI,
        "args": [
            "--gcp_project", PROJECT_ID,
            "--bt_instance", BT_INSTANCE,
            "--bt_table", BT_TABLE,
            "--bq_dataset", f"{PROJECT_ID}.{BQ_DATASET}",
            "--temp_gcs_bucket", SCRIPT_BUCKET
        ],
        "jar_file_uris": [
            "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
            "gs://your-bucket-for-jars/shc-core-spark-2.4-1.1.1-SNAPSHOT.jar" # BigTable/Hbase connector
        ]
    }
}


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "retries": 1,
}

with DAG("whoop_enterprise_etl", default_args=default_args, schedule_interval=None, catchup=False) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        cluster_config=CLUSTER_CONFIG,
    )

    # Run these two massive Spark ETL jobs in parallel!
    run_oltp_job = DataprocSubmitJobOperator(
        task_id="extract_oltp_postgres",
        job=JOB_EXTRACT_OLTP,
        region=REGION,
        project_id=PROJECT_ID,
    )

    run_streaming_job = DataprocSubmitJobOperator(
        task_id="aggregate_bigtable_stream",
        job=JOB_AGGREGATE_STREAMING,
        region=REGION,
        project_id=PROJECT_ID,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule="all_done",
    )

    create_cluster >> [run_oltp_job, run_streaming_job] >> delete_cluster
