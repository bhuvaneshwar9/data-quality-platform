"""
Data Quality Platform — Live Demo API
Runs all 5 quality checks on synthetic e-commerce transaction data.
No AWS credentials required — demonstrates the full check engine.
"""
import sys, os
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.path.insert(0, os.path.dirname(__file__))

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from quality.checks import (
    NullCheck, DuplicateCheck, RangeCheck, FreshnessCheck, AnomalyCheck,
    CheckResult, Severity
)

app = FastAPI(
    title="Data Quality Platform — Demo",
    description="Automated data quality rules engine — 5 check types on live synthetic data",
    version="1.0.0",
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── Synthetic data generator ───────────────────────────────────────────────
def generate_transactions(n: int = 600, seed: int = 42) -> pd.DataFrame:
    rng = np.random.default_rng(seed)
    now = datetime.now(timezone.utc)

    data = {
        "transaction_id": [f"TXN-{i:05d}" for i in range(n)],
        "user_id": rng.integers(1000, 9999, n).astype(str),
        "amount": rng.uniform(1, 5000, n),
        "category": rng.choice(["electronics", "clothing", "food", "travel", None], n,
                                p=[0.25, 0.25, 0.25, 0.23, 0.02]),
        "status": rng.choice(["completed", "pending", "refunded"], n, p=[0.8, 0.15, 0.05]),
        "created_at": [
            (now - timedelta(hours=rng.integers(0, 72).item())).isoformat()
            for _ in range(n)
        ],
        "country": rng.choice(["US", "UK", "DE", None], n, p=[0.6, 0.2, 0.18, 0.02]),
    }
    df = pd.DataFrame(data)
    # inject anomalies: 12 very large amounts
    anomaly_idx = rng.choice(n, 12, replace=False)
    df.loc[anomaly_idx, "amount"] = rng.uniform(15000, 50000, 12)
    # inject 8 duplicates
    dup_idx = rng.choice(n, 8, replace=False)
    df.loc[dup_idx, "transaction_id"] = "TXN-DUP-001"
    return df


# ── Run all checks ─────────────────────────────────────────────────────────
def run_all_checks(df: pd.DataFrame) -> list[dict]:
    checks = [
        NullCheck(columns=["user_id", "amount", "category", "country"], threshold=0.05),
        DuplicateCheck(subset=["transaction_id"], threshold=0.01),
        RangeCheck(column="amount", min_val=0.01, max_val=10000, threshold=0.03),
        FreshnessCheck(timestamp_column="created_at", max_age_hours=48),
        AnomalyCheck(column="amount", z_threshold=3.0, threshold=0.03),
    ]
    results = []
    for check in checks:
        try:
            r = check.run(df)
            results.append({
                "check": r.check_name,
                "passed": bool(r.passed),
                "severity": r.severity.value if hasattr(r.severity, 'value') else str(r.severity),
                "metric": round(float(r.metric), 4),
                "threshold": float(r.threshold),
                "message": r.message,
                "rows_affected": int(r.rows_affected),
                "timestamp": r.timestamp,
                "status": "PASS" if r.passed else ("WARN" if str(r.severity) in ("WARNING", "Severity.WARNING") else "FAIL"),
            })
        except Exception as e:
            results.append({
                "check": getattr(check, 'name', str(check)),
                "passed": False,
                "severity": "WARNING",
                "metric": 0.0,
                "threshold": 0.0,
                "message": f"Check error: {str(e)}",
                "rows_affected": 0,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "status": "ERROR",
            })
    return results


# ── Cache last run ──────────────────────────────────────────────────────────
_last_run: dict = {}


@app.get("/health")
def health():
    return {"status": "ok", "service": "data-quality-platform", "version": "1.0.0"}


@app.get("/run", summary="Run all 5 quality checks on synthetic transaction data")
def run_checks(n_rows: int = 600):
    df = generate_transactions(n=min(n_rows, 2000))
    results = run_all_checks(df)
    passed = sum(1 for r in results if r["passed"])
    summary = {
        "run_at": datetime.now(timezone.utc).isoformat(),
        "dataset": {"rows": len(df), "columns": list(df.columns)},
        "summary": {
            "total_checks": len(results),
            "passed": passed,
            "failed": len(results) - passed,
        },
        "checks": results,
    }
    global _last_run
    _last_run = summary
    return summary


@app.get("/results", summary="Return results from the last run")
def get_results():
    if not _last_run:
        return run_checks()
    return _last_run


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def dashboard():
    results = run_checks()
    rows_html = ""
    for r in results["checks"]:
        color = "#34d399" if r["status"] == "PASS" else ("#fbbf24" if r["status"] == "WARN" else "#ef4444")
        icon  = "✅" if r["status"] == "PASS" else ("⚠️" if r["status"] == "WARN" else "❌")
        rows_html += f"""
        <tr>
          <td><code>{r['check']}</code></td>
          <td style="color:{color};font-weight:700">{icon} {r['status']}</td>
          <td>{r['severity']}</td>
          <td>{r['metric']:.2%}</td>
          <td>{r['threshold']:.0%}</td>
          <td>{r['rows_affected']:,}</td>
          <td style="color:#94a3b8;font-size:.8rem">{r['message']}</td>
        </tr>"""

    summary = results["summary"]
    health_color = "#34d399" if summary["failed"] == 0 else ("#fbbf24" if summary["failed"] <= 1 else "#ef4444")
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Data Quality Platform</title>
  <style>
    body{{background:#0d0a06;color:#e2e8f0;font-family:'Segoe UI',sans-serif;margin:0;padding:2rem}}
    h1{{color:#f97316;margin-bottom:.3rem}}
    .sub{{color:#94a3b8;margin-bottom:2rem;font-size:.95rem}}
    .stat-row{{display:flex;gap:1rem;margin-bottom:2rem;flex-wrap:wrap}}
    .stat{{background:rgba(249,115,22,.1);border:1px solid rgba(249,115,22,.2);border-radius:10px;padding:1rem 1.5rem;min-width:120px}}
    .stat-num{{font-size:2rem;font-weight:800;color:#f97316}}
    .stat-label{{color:#94a3b8;font-size:.8rem;margin-top:.2rem}}
    table{{width:100%;border-collapse:collapse;background:rgba(20,12,4,.8);border-radius:10px;overflow:hidden}}
    th{{background:rgba(249,115,22,.15);color:#f97316;padding:.75rem 1rem;text-align:left;font-size:.82rem;letter-spacing:1px;text-transform:uppercase}}
    td{{padding:.75rem 1rem;border-bottom:1px solid rgba(249,115,22,.08);font-size:.88rem}}
    tr:last-child td{{border-bottom:none}}
    code{{background:rgba(249,115,22,.12);padding:.1rem .4rem;border-radius:4px;color:#fbbf24;font-size:.82rem}}
    .btn{{display:inline-block;margin-top:1.5rem;background:#f97316;color:#000;padding:.6rem 1.4rem;border-radius:8px;text-decoration:none;font-weight:700;font-size:.9rem}}
    .btn:hover{{background:#ea580c}}
  </style>
</head>
<body>
  <h1>🔍 Data Quality Platform</h1>
  <p class="sub">Live quality checks on {results['dataset']['rows']:,} synthetic e-commerce transactions &nbsp;·&nbsp; {results['run_at'][:19]} UTC</p>
  <div class="stat-row">
    <div class="stat"><div class="stat-num">{summary['total_checks']}</div><div class="stat-label">Checks Run</div></div>
    <div class="stat"><div class="stat-num" style="color:#34d399">{summary['passed']}</div><div class="stat-label">Passed</div></div>
    <div class="stat"><div class="stat-num" style="color:{health_color}">{summary['failed']}</div><div class="stat-label">Failed</div></div>
    <div class="stat"><div class="stat-num">{results['dataset']['rows']:,}</div><div class="stat-label">Rows Checked</div></div>
  </div>
  <table>
    <thead>
      <tr><th>Check</th><th>Status</th><th>Severity</th><th>Metric</th><th>Threshold</th><th>Rows Affected</th><th>Message</th></tr>
    </thead>
    <tbody>{rows_html}</tbody>
  </table>
  <a class="btn" href="/run">↻ Re-run Checks</a>&nbsp;&nbsp;
  <a class="btn" style="background:rgba(249,115,22,.2);color:#f97316;border:1px solid rgba(249,115,22,.4)" href="/docs">📖 API Docs</a>
</body>
</html>"""
