output "project_id" {
  description = "GCP Project ID"
  value       = var.project_id
}

output "region" {
  description = "GCP Region"
  value       = var.region
}

output "service_account_email" {
  description = "Service Account Email"
  value       = google_service_account.pipeline.email
}

output "gcs_bucket_name" {
  description = "GCS Staging Bucket Name"
  value       = google_storage_bucket.staging.name
}

output "gcs_bucket_url" {
  description = "GCS Staging Bucket URL"
  value       = "gs://${google_storage_bucket.staging.name}"
}

output "pubsub_topic_name" {
  description = "Pub/Sub Topic Name"
  value       = google_pubsub_topic.stock_prices.name
}

output "pubsub_topic_path" {
  description = "Pub/Sub Topic Full Path"
  value       = google_pubsub_topic.stock_prices.id
}

output "pubsub_subscription_name" {
  description = "Pub/Sub Subscription Name"
  value       = google_pubsub_subscription.stock_prices.name
}

output "pubsub_subscription_path" {
  description = "Pub/Sub Subscription Full Path"
  value       = google_pubsub_subscription.stock_prices.id
}

output "raw_layer_dataset_id" {
  description = "Raw Layer BigQuery Dataset ID"
  value       = google_bigquery_dataset.raw_layer.dataset_id
}

output "processed_layer_dataset_id" {
  description = "Processed Layer BigQuery Dataset ID"
  value       = google_bigquery_dataset.processed_layer.dataset_id
}

output "analytics_layer_dataset_id" {
  description = "Analytics Layer BigQuery Dataset ID"
  value       = google_bigquery_dataset.analytics_layer.dataset_id
}

output "stock_prices_raw_table_id" {
  description = "Raw Stock Prices Table ID"
  value       = google_bigquery_table.stock_prices_raw.table_id
}

output "stock_prices_cleaned_table_id" {
  description = "Cleaned Stock Prices Table ID"
  value       = google_bigquery_table.stock_prices_cleaned.table_id
}

output "daily_price_summary_table_id" {
  description = "Daily Price Summary Table ID"
  value       = google_bigquery_table.daily_price_summary.table_id
}

output "all_resources_summary" {
  description = "Summary of all created resources"
  value = {
    project_id                = var.project_id
    service_account_email     = google_service_account.pipeline.email
    gcs_bucket                = google_storage_bucket.staging.name
    pubsub_topic              = google_pubsub_topic.stock_prices.name
    pubsub_subscription       = google_pubsub_subscription.stock_prices.name
    raw_layer_dataset         = google_bigquery_dataset.raw_layer.dataset_id
    processed_layer_dataset   = google_bigquery_dataset.processed_layer.dataset_id
    analytics_layer_dataset   = google_bigquery_dataset.analytics_layer.dataset_id
  }
}