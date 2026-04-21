# Data Quality & Observability Platform

Automated data quality framework with ELK Stack logging, Prometheus/Grafana monitoring, AWS S3/CloudWatch alerting, and PostgreSQL lineage tracking.

## Architecture
```
Data Sources → Quality Checks → PostgreSQL (lineage) → ELK (logs) → Grafana (dashboards)
                              → AWS CloudWatch (alerts)
                              → Prometheus (metrics)
```

## Tech Stack
`Python` `PostgreSQL` `Elasticsearch` `Kibana` `Prometheus` `Grafana` `AWS S3` `AWS CloudWatch` `SNS` `Terraform` `Bash`

## Quick Start
```bash
pip install -r requirements.txt

# 1. Deploy AWS infrastructure (CloudWatch, SNS, S3)
cd terraform && terraform init && terraform apply

# 2. Set up PostgreSQL
psql -U postgres -f database/schema.sql

# 3. Run quality checks
python quality/run_checks.py

# 4. View metrics
python monitoring/metrics_server.py   # http://localhost:9090/metrics

# 5. Run full pipeline
bash scripts/run_pipeline.sh
```

## Project Structure
```
data-quality-platform/
├── terraform/               # CloudWatch alarms, SNS, S3, IAM
├── quality/
│   ├── checks.py            # data quality rules engine
│   ├── run_checks.py        # orchestrates all checks
│   └── report.py            # generates quality report
├── pipeline/
│   └── etl_with_quality.py  # ETL with inline quality gates
├── database/
│   └── schema.sql           # PostgreSQL lineage + quality tables
├── monitoring/
│   ├── elk_logger.py        # ships logs to Elasticsearch
│   ├── metrics_server.py    # Prometheus metrics server
│   └── grafana_dashboard.json
└── scripts/
    ├── run_pipeline.sh      # full pipeline bash script
    └── rbac_setup.sh        # PostgreSQL RBAC setup
```
