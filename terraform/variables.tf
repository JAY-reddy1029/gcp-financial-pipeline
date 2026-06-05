variable "project_id" {
  description = "GCP Project ID"
  type        = string
  default     = "gcp-financial-pipeline"
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "asia-south1"
}

variable "zone" {
  description = "GCP Zone"
  type        = string
  default     = "asia-south1-a"
}

variable "service_account_name" {
  description = "Service Account Name"
  type        = string
  default     = "financial-pipeline-sa"
}

variable "bucket_name" {
  description = "GCS Bucket Name"
  type        = string
  default     = "financial-pipeline-staging"
}

variable "pubsub_topic_name" {
  description = "Pub/Sub Topic Name"
  type        = string
  default     = "stock-prices-topic"
}

variable "pubsub_subscription_name" {
  description = "Pub/Sub Subscription Name"
  type        = string
  default     = "stock-prices-sub"
}