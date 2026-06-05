# Configure Terraform
terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

# Configure GCP Provider
provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

# ============================================================
# ENABLE REQUIRED APIs
# ============================================================

resource "google_project_service" "bigquery" {
  service            = "bigquery.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "pubsub" {
  service            = "pubsub.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "dataflow" {
  service            = "dataflow.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "storage" {
  service            = "storage-api.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "compute" {
  service            = "compute.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "logging" {
  service            = "logging.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "monitoring" {
  service            = "monitoring.googleapis.com"
  disable_on_destroy = false
}

# ============================================================
# SERVICE ACCOUNT
# ============================================================

resource "google_service_account" "pipeline" {
  account_id   = var.service_account_name
  display_name = "Financial Pipeline Service Account"
}

# ============================================================
# IAM ROLES
# ============================================================

resource "google_project_iam_member" "bigquery_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_project_iam_member" "dataflow_admin" {
  project = var.project_id
  role    = "roles/dataflow.admin"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_project_iam_member" "pubsub_admin" {
  project = var.project_id
  role    = "roles/pubsub.admin"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_project_iam_member" "storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

# ============================================================
# CLOUD STORAGE BUCKET
# ============================================================

resource "google_storage_bucket" "staging" {
  name          = "${var.bucket_name}-${var.project_id}"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  versioning {
    enabled = false
  }

  lifecycle {
    prevent_destroy = false
  }
}

# ============================================================
# PUB/SUB TOPIC
# ============================================================

resource "google_pubsub_topic" "stock_prices" {
  name = var.pubsub_topic_name

  message_retention_duration = "604800s"  # 7 days

  depends_on = [google_project_service.pubsub]
}

# ============================================================
# PUB/SUB SUBSCRIPTION
# ============================================================

resource "google_pubsub_subscription" "stock_prices" {
  name  = var.pubsub_subscription_name
  topic = google_pubsub_topic.stock_prices.name

  ack_deadline_seconds = 10

  message_retention_duration = "604800s"  # 7 days

  depends_on = [google_pubsub_topic.stock_prices]
}

# ============================================================
# BIGQUERY DATASETS
# ============================================================

resource "google_bigquery_dataset" "raw_layer" {
  dataset_id    = "raw_layer"
  friendly_name = "Raw Layer (Bronze)"
  description   = "Raw incoming data"
  location      = var.region

  depends_on = [google_project_service.bigquery]
}

resource "google_bigquery_dataset" "processed_layer" {
  dataset_id    = "processed_layer"
  friendly_name = "Processed Layer (Silver)"
  description   = "Cleaned and validated data"
  location      = var.region

  depends_on = [google_project_service.bigquery]
}

resource "google_bigquery_dataset" "analytics_layer" {
  dataset_id    = "analytics_layer"
  friendly_name = "Analytics Layer (Gold)"
  description   = "Business-ready analytics data"
  location      = var.region

  depends_on = [google_project_service.bigquery]
}

# ============================================================
# BIGQUERY TABLES - RAW LAYER
# ============================================================

resource "google_bigquery_table" "stock_prices_raw" {
  dataset_id = google_bigquery_dataset.raw_layer.dataset_id
  table_id   = "stock_prices_raw"

  schema = jsonencode([
    {
      name        = "timestamp"
      type        = "TIMESTAMP"
      mode        = "NULLABLE"
      description = "Event timestamp"
    },
    {
      name        = "symbol"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Stock symbol (RELIANCE, INFY, TCS, WIPRO, HDFC)"
    },
    {
      name        = "price"
      type        = "FLOAT64"
      mode        = "NULLABLE"
      description = "Stock price in INR"
    },
    {
      name        = "volume"
      type        = "INTEGER"
      mode        = "NULLABLE"
      description = "Trading volume"
    },
    {
      name        = "exchange"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Stock exchange (NSE)"
    },
    {
      name        = "data_source"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Data source (api, batch_api)"
    }
  ])

  depends_on = [google_bigquery_dataset.raw_layer]
}

# ============================================================
# BIGQUERY TABLES - PROCESSED LAYER
# ============================================================

resource "google_bigquery_table" "stock_prices_cleaned" {
  dataset_id = google_bigquery_dataset.processed_layer.dataset_id
  table_id   = "stock_prices_cleaned"

  schema = jsonencode([
    {
      name        = "timestamp"
      type        = "TIMESTAMP"
      mode        = "NULLABLE"
      description = "Event timestamp"
    },
    {
      name        = "symbol"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Stock symbol"
    },
    {
      name        = "price"
      type        = "FLOAT64"
      mode        = "NULLABLE"
      description = "Stock price"
    },
    {
      name        = "volume"
      type        = "INTEGER"
      mode        = "NULLABLE"
      description = "Trading volume"
    },
    {
      name        = "exchange"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Stock exchange"
    },
    {
      name        = "data_source"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Data source"
    },
    {
      name        = "processed_at"
      type        = "TIMESTAMP"
      mode        = "NULLABLE"
      description = "Processing timestamp"
    }
  ])

  depends_on = [google_bigquery_dataset.processed_layer]
}

# ============================================================
# BIGQUERY TABLES - ANALYTICS LAYER
# ============================================================

resource "google_bigquery_table" "daily_price_summary" {
  dataset_id = google_bigquery_dataset.analytics_layer.dataset_id
  table_id   = "daily_price_summary"

  schema = jsonencode([
    {
      name        = "date"
      type        = "DATE"
      mode        = "NULLABLE"
      description = "Trading date"
    },
    {
      name        = "symbol"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Stock symbol"
    },
    {
      name        = "open_price"
      type        = "FLOAT64"
      mode        = "NULLABLE"
      description = "Opening price"
    },
    {
      name        = "high_price"
      type        = "FLOAT64"
      mode        = "NULLABLE"
      description = "Highest price of the day"
    },
    {
      name        = "low_price"
      type        = "FLOAT64"
      mode        = "NULLABLE"
      description = "Lowest price of the day"
    },
    {
      name        = "close_price"
      type        = "FLOAT64"
      mode        = "NULLABLE"
      description = "Closing price"
    },
    {
      name        = "total_volume"
      type        = "INTEGER"
      mode        = "NULLABLE"
      description = "Total trading volume"
    }
  ])

  depends_on = [google_bigquery_dataset.analytics_layer]
}