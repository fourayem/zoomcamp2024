variable "credentials" {
  description = "My Credentials"
  default = "./keys/my-creds.json"
}

variable "project" {
  description = "Project"
  default = "dezoomcamp2024-412620"
}

variable "region" {
  description = "Region"
  default = "us-central1"
}


variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default = "demo_dataset"
}

variable "location" {
  description = "Project Location"
  default = "US"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default = "dezoomcamp2024-412620-terraform"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default = "STANDARD"
}