variable "project_id" {
  description = "The ID of the GCP Project"
  type        = string
}

variable "region" {
  description = "The GCP region to deploy resources in"
  type        = string
  default     = "us-central1"
}

variable "credentials_path" {
  description = "Absolute path to the GCP Service Account Key JSON file"
  type        = string
}

variable "raw_bucket_name" {
  description = "Name for the raw data GCS bucket (must be globally unique)"
  type        = string
}

variable "script_bucket_name" {
  description = "Name for the scripts and temp data GCS bucket (must be globally unique)"
  type        = string
}

variable "bq_dataset_id" {
  description = "ID of the BigQuery Dataset"
  type        = string
  default     = "whoop_dw"
}
