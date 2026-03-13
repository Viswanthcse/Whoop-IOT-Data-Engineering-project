terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project     = var.project_id
  region      = var.region
  credentials = file(var.credentials_path)
}

# ---------------------------------------------------------
# Google Cloud Storage Buckets (Data Lake)
# ---------------------------------------------------------
resource "google_storage_bucket" "unstructured_landing_zone" {
  name          = "${var.project_id}-unstructured-landing"
  location      = var.region
  force_destroy = true
}

# ---------------------------------------------------------
# Cloud SQL (PostgreSQL for OLTP)
# ---------------------------------------------------------
resource "google_sql_database_instance" "oltp_db_instance" {
  name             = "whoop-oltp-instance"
  database_version = "POSTGRES_14"
  region           = var.region

  settings {
    tier = "db-f1-micro"
  }
  deletion_protection = false
}

resource "google_sql_database" "oltp_db" {
  name     = "fitness_oltp"
  instance = google_sql_database_instance.oltp_db_instance.name
}

resource "google_sql_user" "users" {
  name     = "api_user"
  instance = google_sql_database_instance.oltp_db_instance.name
  password = "supersecurepassword123"
}

# ---------------------------------------------------------
# Cloud Bigtable (NoSQL for Time-Series Streaming)
# ---------------------------------------------------------
resource "google_bigtable_instance" "timeseries_cluster" {
  name = "whoop-sensor-stream"
  cluster {
    cluster_id   = "whoop-sensor-stream-c1"
    num_nodes    = 1
    storage_type = "HDD"
    zone         = "${var.region}-a"
  }
  deletion_protection = false
}

resource "google_bigtable_table" "sensor_data" {
  name          = "heart_rate_timeseries"
  instance_name = google_bigtable_instance.timeseries_cluster.name
  
  column_family {
    family = "metrics"
  }
}

# ---------------------------------------------------------
# Cloud Run Services (Prepared for API deployments)
# ---------------------------------------------------------
# Note: These are placeholders. You will deploy your actual FastAPIs here.
resource "google_cloud_run_v2_service" "api_oltp" {
  name     = "api-oltp-ingestion"
  location = var.region
  template {
    containers {
      image = "us-docker.pkg.dev/cloudrun/container/hello" # Placeholder
    }
  }
}

resource "google_cloud_run_v2_service" "api_streaming" {
  name     = "api-sensor-streaming"
  location = var.region
  template {
    containers {
      image = "us-docker.pkg.dev/cloudrun/container/hello" # Placeholder
    }
  }
}
