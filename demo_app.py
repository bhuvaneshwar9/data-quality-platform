"""
Data Quality Platform — Live Demo API
Fetches real e-commerce cart & product data from DummyJSON public API,
flattens it into transactions, then runs all 5 quality checks.
Auto-refreshes every 60 seconds. Falls back to synthetic data if API is down.
"""
import sys, os
sys.stdout.reconfigure(encoding='utf-8', errors='replace')
sys.path.insert(0, os.path.dirname(__file__))

import requests
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from quality.checks import (
    NullCheck, DuplicateCheck, RangeCheck, FreshnessCheck, AnomalyCheck,
)

app = FastAPI(
    title="Data Quality Platform — Live Demo",
    description="Real-time quality checks on live e-commerce data from DummyJSON API",
    version="2.0.0",
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

CARTS_URL    = "https://dummyjson.com/carts?limit=100"
PRODUCTS_URL = "https://dummyjson.com/products?limit=100"

# ── 5-minute cache so we don't hammer the public API ──────────────────────────
_cache: dict = {"df": None, "fetched_at": None, "source": "none"}
CACHE_TTL_SEC = 300


def fetch_live_data() -> tuple[pd.DataFrame, str]:
    """
    Pull carts + products from DummyJSON, flatten into one row per cart-item.
    Returns (DataFrame, source_label).
    """
    now = datetime.now(timezone.utc)

    # Return cached copy if still fresh
    if _cache["df"] is not None and _cache["fetched_at"]:
        if (now - _cache["fetched_at"]).total_seconds() < CACHE_TTL_SEC:
            return _cache["df"], _cache["source"]

    try:
        carts_resp    = requests.get(CARTS_URL,    timeout=8)
        products_resp = requests.get(PRODUCTS_URL, timeout=8)
        carts_resp.raise_for_status()
        products_resp.raise_for_status()

        carts    = carts_resp.json()["carts"]
        products = {p["id"]: p for p in products_resp.json()["products"]}

        rows = []
        for cart in carts:
            cart_id  = cart["id"]
            user_id  = str(cart["userId"])
            for item in cart.get("products", []):
                pid      = item["id"]
                product  = products.get(pid, {})
                category = product.get("category") or item.get("category")
                price    = item.get("price", product.get("price", 0))
                qty      = item.get("quantity", 1)
                disc     = item.get("discountPercentage", product.get("discountPercentage", 0))
                amount   = round(price * qty * (1 - disc / 100), 2)

                # Simulate a created_at spread over the last 48 h
                offset_h = (cart_id * 7 + pid * 3) % 48
                created  = (now - timedelta(hours=offset_h)).isoformat()

                rows.append({
                    "transaction_id": f"C{cart_id:03d}-P{pid:04d}",
                    "user_id":        user_id,
                    "product":        item.get("title", product.get("title", "Unknown")),
                    "category":       category,
                    "quantity":       qty,
                    "unit_price":     price,
                    "discount_pct":   disc,
                    "amount":         amount,
                    "created_at":     created,
                })

        df = pd.DataFrame(rows)
        source = f"DummyJSON API ({len(carts)} carts, {len(df)} line-items)"
        _cache.update(df=df, fetched_at=now, source=source)
        return df, source

    except Exception as exc:
        # ── fallback: synthetic data so the demo never breaks ─────────────────
        df, source = _synthetic_fallback(), f"Synthetic fallback (API error: {exc})"
        _cache.update(df=df, fetched_at=now, source=source)
        return df, source


def _synthetic_fallback(n: int = 600) -> pd.DataFrame:
    rng  = np.random.default_rng(42)
    now  = datetime.now(timezone.utc)
    cats = ["electronics", "smartphones", "clothing", "groceries", "beauty", "sports", None]
    data = {
        "transaction_id": [f"TXN-{i:05d}" for i in range(n)],
        "user_id":        rng.integers(1, 201, n).astype(str).tolist(),
        "product":        [f"Product {i}" for i in range(n)],
        "category":       rng.choice(cats, n, p=[.18,.17,.18,.17,.15,.13,.02]).tolist(),
        "quantity":       rng.integers(1, 6, n).tolist(),
        "unit_price":     rng.uniform(5, 500, n).round(2).tolist(),
        "discount_pct":   rng.uniform(0, 40, n).round(2).tolist(),
        "amount":         rng.uniform(5, 2000, n).round(2).tolist(),
        "created_at":     [(now - timedelta(hours=int(rng.integers(0, 72)))).isoformat() for _ in range(n)],
    }
    df = pd.DataFrame(data)
    # inject anomalies & duplicates so checks have something to flag
    df.loc[rng.choice(n, 10, replace=False), "amount"] = rng.uniform(8000, 20000, 10)
    df.loc[rng.choice(n, 8,  replace=False), "transaction_id"] = "TXN-DUP-001"
    return df


# ── Run all 5 checks ───────────────────────────────────────────────────────────
def run_all_checks(df: pd.DataFrame) -> list[dict]:
    checks = [
        NullCheck(columns=["user_id", "amount", "category"],  threshold_pct=3.0),
        DuplicateCheck(key_columns=["transaction_id"],         threshold_pct=1.0),
        RangeCheck(column="amount", min_val=0.01, max_val=5000),
        FreshnessCheck(timestamp_col="created_at",             max_age_hours=48),
        AnomalyCheck(column="amount", z_threshold=3.0,         max_pct=5.0),
    ]
    results = []
    for check in checks:
        try:
            r = check.run(df)
            sev_str = r.severity.value if hasattr(r.severity, "value") else str(r.severity)
            results.append({
                "check":         r.check_name,
                "passed":        bool(r.passed),
                "severity":      sev_str,
                "metric":        round(float(r.metric), 4),
                "threshold":     float(r.threshold),
                "message":       r.message,
                "rows_affected": int(r.rows_affected),
                "timestamp":     r.timestamp,
                "status":        "PASS" if r.passed else ("WARN" if "WARNING" in sev_str else "FAIL"),
            })
        except Exception as e:
            results.append({
                "check": getattr(check, "name", str(check)),
                "passed": False, "severity": "ERROR",
                "metric": 0.0, "threshold": 0.0,
                "message": f"Check error: {e}",
                "rows_affected": 0,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "status": "ERROR",
            })
    return results


_last_run: dict = {}


@app.get("/health")
def health():
    return {"status": "ok", "service": "data-quality-platform", "version": "2.0.0"}


@app.get("/run", summary="Fetch live data and run all 5 quality checks")
def run_checks():
    df, source = fetch_live_data()
    results    = run_all_checks(df)
    passed     = sum(1 for r in results if r["passed"])

    summary = {
        "run_at":  datetime.now(timezone.utc).isoformat(),
        "source":  source,
        "dataset": {"rows": len(df), "columns": list(df.columns)},
        "summary": {
            "total_checks": len(results),
            "passed":       passed,
            "failed":       len(results) - passed,
        },
        "checks": results,
    }
    global _last_run
    _last_run = summary
    return summary


@app.get("/results", summary="Return the last run results (cached)")
def get_results():
    return _last_run if _last_run else run_checks()


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def dashboard():
    data    = run_checks()
    summary = data["summary"]
    source  = data.get("source", "DummyJSON API")

    rows_html = ""
    for r in data["checks"]:
        color = "#34d399" if r["status"] == "PASS" else ("#fbbf24" if r["status"] == "WARN" else "#ef4444")
        icon  = "✅" if r["status"] == "PASS" else ("⚠️" if r["status"] == "WARN" else "❌")
        metric_display = f"{r['metric']:.2f}" if r["metric"] > 1 else f"{r['metric']:.2%}"
        thresh_display = f"{r['threshold']:.0f}" if r["threshold"] > 1 else f"{r['threshold']:.0%}"
        rows_html += f"""
        <tr>
          <td><code>{r['check']}</code></td>
          <td style="color:{color};font-weight:700">{icon} {r['status']}</td>
          <td><span class="badge {'crit' if r['severity']=='CRITICAL' else 'warn'}">{r['severity']}</span></td>
          <td>{metric_display}</td>
          <td>{thresh_display}</td>
          <td>{r['rows_affected']:,}</td>
          <td style="color:#94a3b8;font-size:.8rem">{r['message']}</td>
        </tr>"""

    health_color = "#34d399" if summary["failed"] == 0 else ("#fbbf24" if summary["failed"] <= 1 else "#ef4444")
    health_label = "HEALTHY" if summary["failed"] == 0 else ("DEGRADED" if summary["failed"] <= 1 else "FAILING")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Data Quality Platform</title>
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{background:#0d0a06;color:#e2e8f0;font-family:'Segoe UI',sans-serif;padding:2rem}}
    h1{{color:#f97316;margin-bottom:.3rem;font-size:1.6rem}}
    .sub{{color:#94a3b8;margin-bottom:1.5rem;font-size:.88rem;line-height:1.6}}
    .source-badge{{display:inline-block;background:rgba(52,211,153,.12);border:1px solid rgba(52,211,153,.3);
      color:#34d399;padding:.2rem .7rem;border-radius:20px;font-size:.75rem;font-weight:600;margin-bottom:1.2rem}}
    .live-dot{{display:inline-block;width:8px;height:8px;background:#34d399;border-radius:50%;
      margin-right:.4rem;animation:pulse 1.5s infinite}}
    @keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.3}}}}
    .stat-row{{display:flex;gap:1rem;margin-bottom:2rem;flex-wrap:wrap}}
    .stat{{background:rgba(249,115,22,.08);border:1px solid rgba(249,115,22,.2);border-radius:10px;
      padding:1rem 1.5rem;min-width:130px}}
    .stat-num{{font-size:2rem;font-weight:800;color:#f97316}}
    .stat-label{{color:#94a3b8;font-size:.78rem;margin-top:.2rem}}
    .health{{background:rgba(52,211,153,.08);border-color:rgba(52,211,153,.25)}}
    .health .stat-num{{color:{health_color}}}
    table{{width:100%;border-collapse:collapse;background:rgba(20,12,4,.9);border-radius:12px;
      overflow:hidden;margin-bottom:1.5rem}}
    th{{background:rgba(249,115,22,.12);color:#f97316;padding:.7rem 1rem;text-align:left;
      font-size:.78rem;letter-spacing:1px;text-transform:uppercase}}
    td{{padding:.7rem 1rem;border-bottom:1px solid rgba(249,115,22,.07);font-size:.85rem}}
    tr:last-child td{{border-bottom:none}}
    tr:hover td{{background:rgba(249,115,22,.04)}}
    code{{background:rgba(249,115,22,.12);padding:.1rem .4rem;border-radius:4px;
      color:#fbbf24;font-size:.8rem}}
    .badge{{display:inline-block;padding:.15rem .55rem;border-radius:20px;font-size:.7rem;font-weight:700}}
    .badge.crit{{background:rgba(239,68,68,.15);color:#ef4444}}
    .badge.warn{{background:rgba(251,191,36,.15);color:#fbbf24}}
    .btn{{display:inline-block;background:#f97316;color:#000;padding:.55rem 1.3rem;
      border-radius:8px;text-decoration:none;font-weight:700;font-size:.88rem;
      margin-top:1rem;margin-right:.5rem;transition:.2s}}
    .btn:hover{{background:#ea580c}}
    .btn.outline{{background:rgba(249,115,22,.15);color:#f97316;border:1px solid rgba(249,115,22,.4)}}
    .btn.outline:hover{{background:rgba(249,115,22,.25)}}
    .refresh-bar{{background:rgba(249,115,22,.08);border:1px solid rgba(249,115,22,.15);
      border-radius:8px;padding:.6rem 1rem;font-size:.8rem;color:#94a3b8;
      margin-bottom:1.5rem;display:flex;align-items:center;justify-content:space-between}}
    #countdown{{color:#f97316;font-weight:700}}
  </style>
</head>
<body>
  <h1>🔍 Data Quality Platform</h1>
  <p class="sub">
    Automated quality checks &nbsp;·&nbsp; {data['run_at'][:19]} UTC &nbsp;·&nbsp;
    {data['dataset']['rows']:,} transactions &nbsp;·&nbsp; {len(data['dataset']['columns'])} columns
  </p>

  <div class="source-badge">
    <span class="live-dot"></span>Live &nbsp;·&nbsp; {source}
  </div>

  <div class="refresh-bar">
    <span>Auto-refreshing in <span id="countdown">60</span>s</span>
    <span style="color:#f97316;font-weight:600">{health_label}</span>
  </div>

  <div class="stat-row">
    <div class="stat"><div class="stat-num">{summary['total_checks']}</div><div class="stat-label">Checks Run</div></div>
    <div class="stat"><div class="stat-num" style="color:#34d399">{summary['passed']}</div><div class="stat-label">Passed</div></div>
    <div class="stat"><div class="stat-num" style="color:{health_color}">{summary['failed']}</div><div class="stat-label">Failed</div></div>
    <div class="stat"><div class="stat-num">{data['dataset']['rows']:,}</div><div class="stat-label">Rows Checked</div></div>
    <div class="stat health"><div class="stat-num" style="color:{health_color}">{health_label}</div><div class="stat-label">Pipeline Health</div></div>
  </div>

  <table>
    <thead>
      <tr>
        <th>Check</th><th>Status</th><th>Severity</th>
        <th>Metric</th><th>Threshold</th><th>Rows Affected</th><th>Message</th>
      </tr>
    </thead>
    <tbody>{rows_html}</tbody>
  </table>

  <a class="btn" href="/run">↻ Re-run Now</a>
  <a class="btn outline" href="/docs">📖 API Docs</a>
  <a class="btn outline" href="https://dummyjson.com/carts?limit=100" target="_blank">🌐 Raw Data Source</a>

  <script>
    let t = 60;
    const el = document.getElementById('countdown');
    setInterval(() => {{
      t--;
      el.textContent = t;
      if (t <= 0) location.reload();
    }}, 1000);
  </script>
</body>
</html>"""
