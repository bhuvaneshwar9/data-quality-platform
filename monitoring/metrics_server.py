"""
Prometheus metrics server for data quality.
Exposes check pass/fail rates, anomaly rates, row counts.

Run: python monitoring/metrics_server.py
Scrape: http://localhost:9090/metrics
"""

import time, os, sys, json, glob
from prometheus_client import start_http_server, Gauge, Counter, Histogram

PORT = 9090

checks_passed  = Counter("dq_checks_passed_total",   "Total quality checks passed", ["check_name"])
checks_failed  = Counter("dq_checks_failed_total",   "Total quality checks failed", ["check_name", "severity"])
null_rate      = Gauge("dq_null_rate_pct",            "Current null rate %")
duplicate_rate = Gauge("dq_duplicate_rate_pct",       "Current duplicate rate %")
anomaly_rate   = Gauge("dq_anomaly_rate_pct",         "Current anomaly rate %")
rows_processed = Counter("dq_rows_processed_total",   "Total rows processed")
check_duration = Histogram("dq_check_duration_seconds","Quality check duration")


def load_latest_report():
    reports = sorted(glob.glob("reports/quality_report_*.json"))
    if not reports:
        return None
    with open(reports[-1]) as f:
        return json.load(f)


def update_metrics():
    report = load_latest_report()
    if not report:
        return

    rows_processed.inc(report.get("total_rows", 0))

    for result in report.get("results", []):
        name   = result["check_name"]
        passed = result["passed"]

        if passed:
            checks_passed.labels(check_name=name).inc()
        else:
            checks_failed.labels(check_name=name, severity=result["severity"]).inc()

        if name == "null_check":
            null_rate.set(result["metric"])
        elif name == "duplicate_check":
            duplicate_rate.set(result["metric"])
        elif name == "anomaly_check":
            anomaly_rate.set(result["metric"])


if __name__ == "__main__":
    start_http_server(PORT)
    print(f"Prometheus metrics at http://localhost:{PORT}/metrics")
    while True:
        update_metrics()
        time.sleep(30)
