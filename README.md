# GCP Financial Data Pipeline

**A production-grade financial analytics platform** on Google Cloud Platform.

Real-time stock price ingestion + Daily transaction processing + Automated reporting.

---

## Overview

This project demonstrates enterprise-grade data engineering patterns using:
- **Pub/Sub** — Real-time market data streaming
- **Dataflow** — Stream & batch processing
- **Cloud Composer** — Workflow orchestration
- **BigQuery** — Data warehouse
- **Cloud Firestore** — Reference data cache
- **Cloud Scheduler** — Automated job triggering
- **Cloud Monitoring** — Alerts & dashboards

---

## Architecture
Pub/Sub (Stock Prices) → Dataflow → BigQuery
GCS (Transactions) → Dataflow → BigQuery
↓
Cloud Composer DAG → Reports + Alerts

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for detailed design.

---

## Quick Start

### Prerequisites
- GCP Project with billing enabled
- gcloud CLI installed
- Terraform installed
- Python 3.9+
- Git installed

### Setup

```bash
# 1. Clone repository
git clone https://github.com/YOUR-USERNAME/gcp-financial-pipeline.git
cd gcp-financial-pipeline

# 2. Configure GCP
gcloud config set project YOUR-PROJECT-ID
gcloud auth application-default login

# 3. Deploy infrastructure
cd terraform
terraform init
terraform plan
terraform apply
```

See [docs/SETUP_GUIDE.md](docs/SETUP_GUIDE.md) for detailed steps.

---

## Project Structure
terraform/        — Infrastructure as Code (Terraform)
dataflow/         — Dataflow jobs (Python)
composer/         — Cloud Composer DAGs (Python/Airflow)
bigquery/         — SQL schemas & queries
scripts/          — Setup & deployment scripts
docs/             — Documentation
config/           — Configuration templates

---

## Learning Progress

- [x] Phase 1: Foundation Setup
- [ ] Phase 2: Real-time Streaming (Pub/Sub + Dataflow)
- [ ] Phase 3: Batch Processing (GCS + Dataflow)
- [ ] Phase 4: Orchestration (Cloud Composer)
- [ ] Phase 5: Scheduling & Monitoring
- [ ] Phase 6: Analytics & Reporting

---

## Technologies Used

| Service | Purpose |
|---------|---------|
| **Pub/Sub** | Real-time message streaming |
| **Dataflow** | Stream & batch processing |
| **BigQuery** | Data warehouse |
| **Cloud Composer** | Workflow orchestration |
| **Cloud Scheduler** | Job scheduling |
| **Cloud Firestore** | Reference data |
| **Cloud Monitoring** | Alerting & dashboards |
| **Terraform** | Infrastructure as Code |

---

## Author

**Mohan** — Data Engineer (Fresher)  
Hyderabad, India  
Date: June 2026

---

## License

MIT License — See [LICENSE](LICENSE) file for details.