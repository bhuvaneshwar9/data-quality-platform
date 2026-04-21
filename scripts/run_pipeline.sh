#!/usr/bin/env bash
# Full pipeline: deploy infra → run quality checks → export metrics

set -euo pipefail

echo "============================================"
echo " Data Quality Platform Pipeline"
echo "============================================"

# 1. Deploy AWS infrastructure
echo "[1/4] Deploying AWS infrastructure..."
cd terraform
terraform init -input=false -upgrade
terraform apply -auto-approve
export REPORTS_BUCKET=$(terraform output -raw reports_bucket)
export SNS_TOPIC_ARN=$(terraform output -raw sns_topic_arn)
cd ..

# 2. Set up PostgreSQL schema
echo "[2/4] Setting up PostgreSQL schema..."
if command -v psql &>/dev/null; then
    psql "${DATABASE_URL:-postgres://postgres:postgres@localhost/dq_platform}" \
         -f database/schema.sql -q
    echo "  Schema applied"
else
    echo "  psql not found — skipping PostgreSQL setup"
fi

# 3. Run quality checks
echo "[3/4] Running data quality checks..."
python quality/run_checks.py

# 4. Start metrics server in background
echo "[4/4] Starting Prometheus metrics server..."
python monitoring/metrics_server.py &
METRICS_PID=$!
echo "  Metrics server PID: ${METRICS_PID} | http://localhost:9090/metrics"

echo ""
echo "Pipeline complete!"
echo "  Reports : ./reports/"
echo "  Metrics : http://localhost:9090/metrics"
echo "  S3      : s3://${REPORTS_BUCKET}/reports/"
