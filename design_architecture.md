# Architecture V2: Event-Driven & Streaming IoT Ingestion

Based on the updated requirements, the architecture will now mirror a highly scalable, real-world IoT Data landscape before moving into the actual Data Engineering ETL phase.

## 1. The Source Systems (Data Simulation Phase)
We are simulating the "wild" environment where user fitness devices continuously pump data in two different flavors:
- **Batch / Periodic Syncs (OLTP & Unstructured)**: When a user opens their app, the phone syncs the day's completed summaries and workouts.
- **Continuous Sensor Stream (Time-Series)**: The user's device continuously beams raw heart rate and biometric data to the cloud whenever active.

## 2. Ingestion Components (Cloud Run APIs)
Two distinct microservices will be deployed onto **Google Cloud Run** to ingest this data.

### API 1: OLTP & Batch Ingestion (FastAPI)
- **Role:** Simulates the backend for the mobile app.
- **Function:** Reads the compiled JSON payload drops from a **GCS Bucket**. Parses the data and upserts relational entities (Users, Workouts, Daily Score rollups) into **Cloud SQL (PostgreSQL)**.
- **State:** Acts as the primary transactional master for user configurations and verified daily metrics.

### API 2: IoT Sensor Streaming (FastAPI)
- **Role:** Simulates the high-throughput ingestion endpoint for real-time sensor data.
- **Function:** Accepts continuous `POST` requests containing `{timestamp, user_id, heart_rate, skin_temp}`.
- **Destination:** Writes directly to **Cloud Bigtable**, a NoSQL wide-column store designed for high-throughput time-series data. 

## 3. The Data Engineering Phase (Phase 2)
Once this simulation infrastructure is humming and data is flowing, we will begin the core Data Engineering project:
- Extract dimensions from **Cloud SQL**.
- Extract massive time-series aggregations from **Bigtable**.
- Extract unstructured artifacts from **GCS**.
- Process all of this concurrently using **Dataproc/PySpark** into analytical models inside **BigQuery**.

## Directory Structure Update
```text
/infrastructure/    # Terraform for Cloud SQL, Bigtable, GCS, Cloud Run
/simulation/
  /batch_generator/ # Drops device JSON payloads into GCS
  /sensor_streamer/ # Blasts time-series data at API 2
  /api_oltp/        # API 1 (Cloud Run -> Cloud SQL)
  /api_streaming/   # API 2 (Cloud Run -> Bigtable)
```
