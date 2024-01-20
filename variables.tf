variable "credentials" {
  description = "My Credentials"
  default     = "./keys/my-creds.json"
}

variable "region" {
  description = "region"
  default     = "us-central1"
}

variable "project" {
  description = "project"
  default     = "storied-sound-142110"
}

variable "bq_dataset_name" {
  description = "My bigquery dataset name"
  default     = "demo_dataset"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "gcs_bucket_name" {
  description = "GCS Bucket Name"
  default     = "storied-sound-142110-terra-bucket"
}


variable "gcs_storage_class" {
  description = "Storage Class"
  default     = "STANDARD"
}