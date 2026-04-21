"""
Orchestrates all data quality checks, publishes metrics to CloudWatch,
ships results to Elasticsearch, and saves report to S3.

Run: python quality/run_checks.py
"""

import os, sys, json
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
import pandas as pd
import numpy as np
import boto3
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from quality.checks import (
    NullCheck, DuplicateCheck, RangeCheck, FreshnessCheck, AnomalyCheck, Severity
)

AWS_REGION   = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
SNS_TOPIC    = os.getenv("SNS_TOPIC_ARN", "")
REPORTS_BUCKET = os.getenv("REPORTS_BUCKET", "")


def generate_sample_data(n: int = 1000) -> pd.DataFrame:
    np.random.seed(42)
    base = datetime(2026, 1, 1)
    df = pd.DataFrame({
        "id":        range(1, n + 1),
        "user_id":   [f"user_{i:04d}" for i in np.random.randint(1, 500, n)],
        "amount":    np.random.normal(150, 60, n),
        "category":  np.random.choice(["electronics", "food", "clothing"], n),
        "region":    np.random.choice(["us-east", "us-west", "eu"], n),
        "timestamp": [base + timedelta(minutes=i) for i in range(n)],
    })
    # Inject quality issues
    df.loc[np.random.choice(n, 10, replace=False), "user_id"] = None
    df.loc[np.random.choice(n, 5,  replace=False), "id"]      = df["id"].iloc[0]  # dupes
    df.loc[np.random.choice(n, 15, replace=False), "amount"]  = np.random.uniform(8000, 20000, 15)
    return df


CHECKS = [
    NullCheck(columns=["id", "user_id", "amount"], threshold_pct=2.0, severity=Severity.CRITICAL),
    DuplicateCheck(key_columns=["id"], threshold_pct=0.5, severity=Severity.CRITICAL),
    RangeCheck(column="amount", min_val=0, max_val=5000, severity=Severity.WARNING),
    FreshnessCheck(timestamp_col="timestamp", max_age_hours=48, severity=Severity.WARNING),
    AnomalyCheck(column="amount", z_threshold=3.0, max_pct=3.0, severity=Severity.WARNING),
]


def publish_to_cloudwatch(results):
    try:
        cw = boto3.client("cloudwatch", region_name=AWS_REGION)
        metrics = [
            {"MetricName": "NullRate",      "Value": next((r.metric for r in results if r.check_name == "null_check"), 0)},
            {"MetricName": "DuplicateRate", "Value": next((r.metric for r in results if r.check_name == "duplicate_check"), 0)},
            {"MetricName": "AnomalyRate",   "Value": next((r.metric for r in results if r.check_name == "anomaly_check"), 0)},
        ]
        cw.put_metric_data(
            Namespace="DataQuality",
            MetricData=[{**m, "Unit": "Percent", "Timestamp": datetime.now(timezone.utc)} for m in metrics]
        )
        print("  CloudWatch metrics published")
    except Exception as e:
        print(f"  CloudWatch skipped (no credentials): {e}")


def send_alert(results):
    failures = [r for r in results if not r.passed and r.severity == Severity.CRITICAL]
    if not failures or not SNS_TOPIC:
        return
    try:
        sns = boto3.client("sns", region_name=AWS_REGION)
        msg = "\n".join([f"❌ {r.check_name}: {r.message}" for r in failures])
        sns.publish(
            TopicArn=SNS_TOPIC,
            Subject="Data Quality Alert — Critical Failures",
            Message=f"Critical quality issues detected:\n\n{msg}",
        )
        print(f"  SNS alert sent for {len(failures)} critical failures")
    except Exception as e:
        print(f"  SNS skipped: {e}")


def save_report(results, df):
    report = {
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "total_rows":    len(df),
        "checks_run":    len(results),
        "passed":        sum(1 for r in results if r.passed),
        "failed":        sum(1 for r in results if not r.passed),
        "results":       [{**vars(r), "severity": r.severity.value} for r in results],
    }
    path = f"reports/quality_report_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    os.makedirs("reports", exist_ok=True)
    with open(path, "w") as f:
        json.dump(report, f, indent=2)
    print(f"  Report saved → {path}")

    if REPORTS_BUCKET:
        try:
            boto3.client("s3").upload_file(path, REPORTS_BUCKET, path)
            print(f"  Report uploaded → s3://{REPORTS_BUCKET}/{path}")
        except Exception as e:
            print(f"  S3 upload skipped: {e}")
    return report


def main():
    print("="*55)
    print("  DATA QUALITY PLATFORM")
    print("="*55)

    df = generate_sample_data()
    print(f"\nDataset: {len(df)} rows, {len(df.columns)} columns\n")

    results = []
    for check in CHECKS:
        result = check.run(df)
        results.append(result)
        status = "✓ PASS" if result.passed else f"✗ FAIL [{result.severity}]"
        print(f"  {status:<20} {result.message}")

    print(f"\n{'='*55}")
    passed = sum(1 for r in results if r.passed)
    print(f"  Results: {passed}/{len(results)} checks passed")
    print(f"{'='*55}\n")

    publish_to_cloudwatch(results)
    send_alert(results)
    report = save_report(results, df)

    if report["failed"] > 0:
        print(f"\n⚠  {report['failed']} check(s) failed — review report above")
    else:
        print("\n✓ All checks passed")


if __name__ == "__main__":
    main()
