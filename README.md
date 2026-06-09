# GCP Financial Pipeline 📊

Production-grade financial data pipeline on Google Cloud Platform with real-time streaming, batch processing, and automated analytics aggregation.

## 🏗️ Architecture
┌─────────────────────────────────────────────────────────────────┐
│                    REAL-TIME STREAMING                           │
├─────────────────────────────────────────────────────────────────┤
│ Publisher → Pub/Sub Topic → Dataflow (Streaming) → BigQuery     │
│                                         ↓                        │
│                                   raw_layer (Bronze)            │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                    BATCH PROCESSING                              │
├─────────────────────────────────────────────────────────────────┤
│ GCS CSV → Dataflow (Batch) → BigQuery processed_layer (Silver)  │
│              ↓ Data Validation                                  │
│         Cloud Scheduler (9 AM IST Daily)                        │
│              ↓                                                  │
│         Cloud Function (trigger_batch_job)                      │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                    ANALYTICS AGGREGATION                         │
├─────────────────────────────────────────────────────────────────┤
│ processed_layer → Dataflow (Analytics) → BigQuery               │
│                     ↓ Daily OHLC Summaries                      │
│              analytics_layer (Gold)                             │
└─────────────────────────────────────────────────────────────────┘


**Medallion Architecture:** Bronze (raw) → Silver (cleaned) → Gold (analytics)

---

## 📦 Project Structure
gcp-financial-pipeline/
├── cloud_functions/
│   ├── main.py                          # Cloud Function for batch job triggering
│   └── requirements.txt                 # Dependencies
├── dataflow/
│   ├── publisher.py                     # Real-time streaming publisher
│   ├── streaming_job.py                 # Pub/Sub → BigQuery streaming
│   ├── batch_job.py                     # GCS CSV → BigQuery batch processing
│   ├── analytics_job.py                 # Analytics aggregation pipeline
│   └── requirements.txt                 # Apache Beam dependencies
├── terraform/
│   ├── main.tf                          # 21 GCP resources (APIs, service account, IAM, Pub/Sub, BigQuery, GCS)
│   ├── variables.tf                     # Input variables
│   ├── outputs.tf                       # Output values
│   └── terraform.tfvars                 # Variable values
├── data/
│   └── historical_prices.csv            # Sample data for batch processing
├── config/
│   └── .env                             # Environment variables (git-ignored)
├── README.md                            # This file
├── .gitignore                           # Git ignore rules
└── LICENSE


---

## 🚀 Quick Start

### Prerequisites
- Google Cloud Account (with ₹82+ credits for testing)
- `gcloud` CLI installed
- `terraform` v1.8.0+
- Python 3.11
- Git

### Setup

1. **Clone repository**
```bash
   git clone https://github.com/JAY-reddy1029/gcp-financial-pipeline.git
   cd gcp-financial-pipeline
```

2. **Authenticate with GCP**
```bash
   gcloud auth application-default login
   gcloud config set project gcp-financial-pipeline
```

3. **Deploy infrastructure with Terraform**
```bash
   cd terraform
   terraform init
   terraform plan
   terraform apply
```

4. **Install dependencies**
```bash
   pip install -r dataflow/requirements.txt
   pip install -r cloud_functions/requirements.txt
```

---

## 📊 Data Pipeline Stages

### Stage 1: Real-Time Streaming
- **Source:** Python Publisher (Pub/Sub)
- **Processing:** Dataflow Streaming Job
- **Destination:** `raw_layer.stock_prices_raw` (BigQuery)
- **Frequency:** Every 5 seconds
- **Status:** ✅ Live

### Stage 2: Batch Processing
- **Source:** GCS CSV (`historical_prices.csv`)
- **Processing:** Dataflow Batch Job + Data Validation
- **Destination:** `processed_layer.stock_prices_cleaned` (BigQuery)
- **Frequency:** Daily 9:00 AM IST (Cloud Scheduler)
- **Triggers:** Cloud Function `trigger_batch_job`
- **Features:** 
  - ✅ Data type validation
  - ✅ Price/volume validation
  - ✅ Symbol whitelist checking
  - ✅ Timestamp format validation
  - ✅ Retry logic (3 attempts with exponential backoff)

### Stage 3: Analytics Aggregation
- **Source:** `processed_layer.stock_prices_cleaned` (BigQuery)
- **Processing:** Dataflow Analytics Job
- **Destination:** `analytics_layer.daily_price_summary` (BigQuery)
- **Metrics:** Open, High, Low, Close (OHLC) + Total Volume per stock per day
- **Features:**
  - ✅ Daily price aggregation
  - ✅ Error handling with retry logic (3 attempts)
  - ✅ Data validation in aggregation

---

## 🛡️ Production Features

### Error Handling
- **Cloud Function:** Retry logic with exponential backoff (max 3 attempts)
- **Batch Job:** Field validation, data type checking, business rule validation
- **Analytics Job:** Retry mechanism, aggregation validation

### Data Validation
- Required field checks
- Data type validation (float, int, string)
- Business logic validation (positive prices, non-negative volumes)
- Symbol whitelist (RELIANCE, INFY, TCS, WIPRO, HDFC)
- Exchange validation (NSE, BSE)
- Timestamp format validation

### Logging & Monitoring
- Cloud Logging integrated in all jobs
- Error metrics tracked: `batch_job_failures`
- Email notification channel configured
- Structured logging with timestamps and severity levels

### Automation
- **Cloud Scheduler:** Triggers batch job daily at 9:00 AM IST
- **Cloud Function:** HTTP endpoint for manual/scheduled triggering
- **No manual intervention** required for daily operations

---

## 🔧 Configuration

### Environment Variables (`.env`)
PROJECT_ID=gcp-financial-pipeline SERVICE_ACCOUNT_EMAIL=financial-pipeline-sa@gcp-financial-pipeline.iam.gserviceaccount.com REGION=asia-south1 ZONE=asia-south1-a


### BigQuery Datasets
- **raw_layer:** Bronze layer - raw incoming data
- **processed_layer:** Silver layer - cleaned, validated data
- **analytics_layer:** Gold layer - business-ready summaries

### Cloud Resources
- **Pub/Sub Topic:** `stock-prices-topic`
- **Pub/Sub Subscription:** `stock-prices-sub`
- **GCS Bucket:** `financial-pipeline-staging-gcp-financial-pipeline`
- **Service Account:** `financial-pipeline-sa@gcp-financial-pipeline.iam.gserviceaccount.com`
- **Cloud Function:** `trigger_batch_job` (HTTP-triggered)
- **Cloud Scheduler Job:** `batch-job-scheduler` (Daily 9 AM IST)

---

## 📈 Sample Data

**Stocks Tracked:** RELIANCE, INFY, TCS, WIPRO, HDFC

**Sample CSV Format:**
timestamp,symbol,price,volume,exchange,data_source
2026-06-01 09:30:00,RELIANCE,2450.5,1000,NSE,api
2026-06-01 09:31:00,INFY,1820.75,500,NSE,api
2026-06-01 09:32:00,TCS,3480.25,750,NSE,api


---

## 🧪 Testing

### Test Real-Time Streaming
```bash
cd dataflow
python publisher.py &
python streaming_job.py
```

### Test Batch Processing
```bash
cd dataflow
python batch_job.py
```

### Test Analytics Aggregation
```bash
cd dataflow
python analytics_job.py
```

### Verify Data in BigQuery
```bash
bq query --nouse_legacy_sql 'SELECT * FROM gcp-financial-pipeline.raw_layer.stock_prices_raw LIMIT 10'
bq query --nouse_legacy_sql 'SELECT * FROM gcp-financial-pipeline.processed_layer.stock_prices_cleaned LIMIT 10'
bq query --nouse_legacy_sql 'SELECT * FROM gcp-financial-pipeline.analytics_layer.daily_price_summary LIMIT 10'
```

---

## 📋 Git Commits

| Phase | Description | Commit |
|-------|-------------|--------|
| Phase 1 | Initial setup, folder structure | 181239e |
| Phase 2 | Real-time streaming (Pub/Sub → Dataflow) | fe61d95 |
| Phase 3A | Batch processing + Analytics layer | 94f221b |
| Phase 3B | Terraform IaC (21 resources) | 467c291 |
| Phase 3C | Cloud Functions + Cloud Scheduler | 9335a60 |
| Phase 4A | Production Hardening (error handling, validation) | 7be66f8 |
| Phase 4B | Complete Documentation | *pending* |

---

## 🤝 Contributing

1. Create a feature branch: `git checkout -b feature/your-feature`
2. Commit changes: `git commit -m "Add feature"`
3. Push to GitHub: `git push origin feature/your-feature`
4. Create Pull Request

---

## 📚 Resources

- [Apache Beam Documentation](https://beam.apache.org/)
- [Google Cloud Dataflow](https://cloud.google.com/dataflow/docs)
- [Google Cloud Pub/Sub](https://cloud.google.com/pubsub/docs)
- [BigQuery Documentation](https://cloud.google.com/bigquery/docs)
- [Terraform Google Provider](https://registry.terraform.io/providers/hashicorp/google/latest/docs)
- [Cloud Functions Python](https://cloud.google.com/functions/docs/writing/write-http-functions)
- [Cloud Scheduler](https://cloud.google.com/scheduler/docs)

---

## 📄 License

MIT License - See LICENSE file

---

## 👤 Author

**Jayachandra Reddy (Jay)**
- Location: Hyderabad, India
- Email: p.v.jay2003@gmail.com
- GitHub: [@JAY-reddy1029](https://github.com/JAY-reddy1029)

---

## 🎯 Next Steps (Future Enhancements)

- [ ] Add dbt for SQL transformations
- [ ] Implement data quality checks (Great Expectations)
- [ ] Build Looker/Data Studio dashboards
- [ ] Add unit tests for all jobs
- [ ] Set up CI/CD pipeline
- [ ] Implement cost optimization alerts
- [ ] Add API layer for data access
- [ ] Implement encryption for sensitive data

---

**Status:** ✅ Production Ready (v1.0)

Last Updated: 09/06/2026