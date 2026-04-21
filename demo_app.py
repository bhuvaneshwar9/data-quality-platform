"""
Data Quality Platform — Live Demo API
Scrapes real product data from books.toscrape.com (a live e-commerce site),
builds purchase transactions from the live catalog, then runs 5 automated
quality checks. Auto-refreshes every 60 s. Falls back to synthetic data
if the site is unreachable.
"""
import sys, os
sys.stdout.reconfigure(encoding="utf-8", errors="replace")
sys.path.insert(0, os.path.dirname(__file__))

import requests
from bs4 import BeautifulSoup
from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
import numpy as np
from datetime import datetime, timezone, timedelta
from quality.checks import NullCheck, DuplicateCheck, RangeCheck, FreshnessCheck, AnomalyCheck

app = FastAPI(
    title="Data Quality Platform — Live",
    description="Quality checks on real e-commerce data scraped from books.toscrape.com",
    version="3.0.0",
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# ── Scrape config ──────────────────────────────────────────────────────────────
BASE  = "https://books.toscrape.com"
CATS  = [
    ("Fiction",       "fiction_10"),
    ("Mystery",       "mystery_3"),
    ("Sci-Fi",        "science-fiction_16"),
    ("Romance",       "romance_8"),
    ("Business",      "business_35"),
    ("Self Help",     "self-help_41"),
    ("History",       "history_32"),
    ("Health",        "health_47"),
]
STARS = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}
HDR   = {"User-Agent": "Mozilla/5.0 (DataQualityBot/3.0)"}
CACHE_TTL = 300   # seconds

_catalog_cache: dict = {"df": None, "fetched_at": None, "source": ""}


# ── Step 1: Scrape live catalog ────────────────────────────────────────────────
def scrape_catalog() -> tuple[pd.DataFrame, str]:
    now = datetime.now(timezone.utc)
    if _catalog_cache["df"] is not None:
        age = (now - _catalog_cache["fetched_at"]).total_seconds()
        if age < CACHE_TTL:
            return _catalog_cache["df"], _catalog_cache["source"]

    products, scraped = [], []
    for cat_name, slug in CATS:
        try:
            url  = f"{BASE}/catalogue/category/books/{slug}/index.html"
            resp = requests.get(url, headers=HDR, timeout=8)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            for art in soup.select("article.product_pod"):
                try:
                    title  = art.select_one("h3 a")["title"]
                    raw_px = art.select_one("p.price_color").text
                    price  = float("".join(c for c in raw_px if c.isdigit() or c == "."))
                    stars  = art.select_one("p.star-rating")["class"]
                    rating = STARS.get(stars[1] if len(stars) > 1 else "Three", 3)
                    avail  = "In Stock" if "In stock" in art.select_one("p.availability").text else "Out of Stock"
                    products.append({"product": title, "category": cat_name,
                                     "unit_price": price, "rating": rating, "availability": avail})
                except Exception:
                    continue
            scraped.append(cat_name)
        except Exception:
            continue

    if not products:
        df = _fallback_catalog()
        src = "Synthetic fallback (books.toscrape.com unreachable)"
    else:
        df  = pd.DataFrame(products)
        src = f"books.toscrape.com — {len(df)} products across {len(scraped)} categories"

    _catalog_cache.update(df=df, fetched_at=now, source=src)
    return df, src


def _fallback_catalog(n: int = 120) -> pd.DataFrame:
    rng  = np.random.default_rng(42)
    cats = ["Fiction", "Mystery", "Sci-Fi", "Romance", "Business", "History"]
    return pd.DataFrame({
        "product":      [f"Book Title {i}" for i in range(n)],
        "category":     rng.choice(cats, n).tolist(),
        "unit_price":   rng.uniform(5, 55, n).round(2).tolist(),
        "rating":       rng.integers(1, 6, n).tolist(),
        "availability": rng.choice(["In Stock", "Out of Stock"], n, p=[0.88, 0.12]).tolist(),
    })


# ── Step 2: Build transactions from catalog ────────────────────────────────────
def build_transactions(catalog: pd.DataFrame, n: int = 500) -> pd.DataFrame:
    """
    Simulate realistic purchase events from the scraped catalog.
    Intentionally injects:
      • 8 duplicate transaction IDs  (simulates double-submit bug)
      • 6 bulk / bot orders          (statistical anomalies in amount)
      • ~5 % purchases of OOS items  (business-rule violations for RangeCheck)
    """
    # New seed each cache window so transactions look "live" each refresh
    seed = int(datetime.now(timezone.utc).timestamp()) // CACHE_TTL
    rng  = np.random.default_rng(seed)
    now  = datetime.now(timezone.utc)

    in_stock = catalog[catalog["availability"] == "In Stock"]
    pool_all = catalog
    pool_ok  = in_stock if len(in_stock) > 10 else catalog

    rows = []
    for i in range(n):
        # 5 % of orders are attempted on Out-of-Stock items
        pool   = pool_all if rng.random() < 0.05 else pool_ok
        idx    = int(rng.integers(0, len(pool)))
        prod   = pool.iloc[idx]
        qty    = int(rng.choice([1, 1, 1, 2, 2, 3], p=[0.40, 0.20, 0.15, 0.12, 0.08, 0.05]))
        amount = round(float(prod["unit_price"]) * qty, 2)
        rows.append({
            "transaction_id": f"ORD-{i+1:05d}",
            "user_id":        f"U{int(rng.integers(1, 301)):04d}",
            "product":        prod["product"],
            "category":       prod["category"],
            "unit_price":     float(prod["unit_price"]),
            "quantity":       qty,
            "amount":         amount,
            "rating":         int(prod["rating"]),
            "availability":   prod["availability"],
            "created_at":     (now - timedelta(minutes=int(rng.integers(0, 2880)))).isoformat(),
        })

    df = pd.DataFrame(rows)

    # Inject duplicate transaction IDs (double-submit simulation)
    dup_idx = rng.choice(n, 8, replace=False)
    df.loc[dup_idx, "transaction_id"] = "ORD-DUP-99"

    # Inject bulk bot orders (anomaly injection)
    bulk_idx = rng.choice(n, 6, replace=False)
    df.loc[bulk_idx, "quantity"] = rng.integers(80, 250, 6)
    df.loc[bulk_idx, "amount"]   = (
        df.loc[bulk_idx, "unit_price"] * df.loc[bulk_idx, "quantity"]
    ).round(2)

    return df


# ── Step 3: Quality checks ─────────────────────────────────────────────────────
def run_all_checks(df: pd.DataFrame) -> list[dict]:
    checks = [
        NullCheck(columns=["user_id", "amount", "category"], threshold_pct=3.0),
        DuplicateCheck(key_columns=["transaction_id"],        threshold_pct=1.0),
        RangeCheck(column="amount", min_val=0.01, max_val=500.0),
        FreshnessCheck(timestamp_col="created_at",            max_age_hours=48),
        AnomalyCheck(column="amount", z_threshold=3.0,        max_pct=5.0),
    ]
    results = []
    for chk in checks:
        try:
            r   = chk.run(df)
            sev = r.severity.value if hasattr(r.severity, "value") else str(r.severity)
            results.append({
                "check": r.check_name, "passed": bool(r.passed), "severity": sev,
                "metric": round(float(r.metric), 4), "threshold": float(r.threshold),
                "message": r.message, "rows_affected": int(r.rows_affected),
                "timestamp": r.timestamp,
                "status": "PASS" if r.passed else ("WARN" if "WARNING" in sev else "FAIL"),
            })
        except Exception as e:
            results.append({
                "check": getattr(chk, "name", str(chk)), "passed": False,
                "severity": "ERROR", "metric": 0.0, "threshold": 0.0,
                "message": f"Error: {e}", "rows_affected": 0,
                "timestamp": datetime.now(timezone.utc).isoformat(), "status": "ERROR",
            })
    return results


_last_run: dict = {}


# ── API routes ─────────────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "service": "data-quality-platform", "version": "3.0.0"}


@app.get("/run", summary="Scrape live catalog, build transactions, run quality checks")
def run_checks():
    catalog, source = scrape_catalog()
    df              = build_transactions(catalog)
    checks          = run_all_checks(df)
    passed          = sum(1 for r in checks if r["passed"])

    # Category breakdown for the dashboard
    cat_counts = df.groupby("category").agg(
        transactions=("transaction_id", "count"),
        revenue=("amount", "sum"),
    ).round(2).reset_index().to_dict(orient="records")

    result = {
        "run_at":  datetime.now(timezone.utc).isoformat(),
        "source":  source,
        "dataset": {"rows": len(df), "columns": list(df.columns)},
        "summary": {"total_checks": len(checks), "passed": passed, "failed": len(checks) - passed},
        "categories": cat_counts,
        "checks":  checks,
    }
    global _last_run
    _last_run = result
    return result


@app.get("/results", summary="Cached results from last run")
def get_results():
    return _last_run if _last_run else run_checks()


# ── HTML Dashboard ─────────────────────────────────────────────────────────────
@app.get("/", response_class=HTMLResponse, include_in_schema=False)
def dashboard():
    data    = run_checks()
    s       = data["summary"]
    source  = data.get("source", "")
    hc      = "#34d399" if s["failed"] == 0 else ("#fbbf24" if s["failed"] <= 1 else "#ef4444")
    hlabel  = "HEALTHY" if s["failed"] == 0 else ("DEGRADED" if s["failed"] <= 1 else "FAILING")

    check_rows = ""
    for r in data["checks"]:
        col  = "#34d399" if r["status"] == "PASS" else ("#fbbf24" if r["status"] == "WARN" else "#ef4444")
        icon = "✅" if r["status"] == "PASS" else ("⚠️" if r["status"] == "WARN" else "❌")
        m    = f"{r['metric']:.2f}" if r["metric"] > 1 else f"{r['metric']:.2%}"
        t    = f"{r['threshold']:.0f}" if r["threshold"] > 1 else f"{r['threshold']:.0%}"
        check_rows += f"""
        <tr>
          <td><code>{r['check']}</code></td>
          <td style="color:{col};font-weight:700">{icon} {r['status']}</td>
          <td><span class="badge {'crit' if 'CRITICAL' in r['severity'] else 'warn'}">{r['severity']}</span></td>
          <td>{m}</td><td>{t}</td>
          <td>{r['rows_affected']:,}</td>
          <td class="msg">{r['message']}</td>
        </tr>"""

    cat_rows = ""
    for c in sorted(data.get("categories", []), key=lambda x: -x["revenue"]):
        cat_rows += f"""<tr>
          <td>{c['category']}</td>
          <td>{c['transactions']:,}</td>
          <td>${c['revenue']:,.2f}</td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Data Quality Platform</title>
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    body{{background:#0d0a06;color:#e2e8f0;font-family:'Segoe UI',sans-serif;padding:2rem;line-height:1.5}}
    h1{{color:#f97316;font-size:1.6rem;margin-bottom:.25rem}}
    h2{{color:#f97316;font-size:1rem;font-weight:700;letter-spacing:1px;text-transform:uppercase;
        margin:1.8rem 0 .7rem}}
    .sub{{color:#94a3b8;font-size:.88rem;margin-bottom:1rem}}
    /* source badge */
    .src{{display:flex;align-items:center;gap:.6rem;margin-bottom:1.2rem;
          background:rgba(52,211,153,.08);border:1px solid rgba(52,211,153,.25);
          border-radius:8px;padding:.5rem 1rem;font-size:.82rem;flex-wrap:wrap}}
    .dot{{width:9px;height:9px;background:#34d399;border-radius:50%;
          animation:pulse 1.5s infinite;flex-shrink:0}}
    @keyframes pulse{{0%,100%{{opacity:1}}50%{{opacity:.25}}}}
    .src a{{color:#34d399;text-decoration:none;font-weight:600}}
    .src a:hover{{text-decoration:underline}}
    /* refresh bar */
    .rbar{{background:rgba(249,115,22,.06);border:1px solid rgba(249,115,22,.14);
           border-radius:8px;padding:.55rem 1rem;font-size:.8rem;color:#94a3b8;
           display:flex;justify-content:space-between;align-items:center;margin-bottom:1.4rem}}
    #cd{{color:#f97316;font-weight:700}}
    /* stat cards */
    .stats{{display:flex;gap:.8rem;flex-wrap:wrap;margin-bottom:1.8rem}}
    .stat{{background:rgba(249,115,22,.08);border:1px solid rgba(249,115,22,.18);
           border-radius:10px;padding:.9rem 1.3rem;min-width:120px}}
    .stat-n{{font-size:1.9rem;font-weight:800;color:#f97316}}
    .stat-l{{color:#94a3b8;font-size:.75rem;margin-top:.15rem}}
    /* tables */
    table{{width:100%;border-collapse:collapse;background:rgba(20,12,4,.9);
           border-radius:10px;overflow:hidden;margin-bottom:1.5rem}}
    th{{background:rgba(249,115,22,.12);color:#f97316;padding:.65rem 1rem;
        text-align:left;font-size:.76rem;letter-spacing:1px;text-transform:uppercase}}
    td{{padding:.65rem 1rem;border-bottom:1px solid rgba(249,115,22,.07);font-size:.84rem}}
    tr:last-child td{{border-bottom:none}}
    tr:hover td{{background:rgba(249,115,22,.04)}}
    code{{background:rgba(249,115,22,.12);padding:.1rem .4rem;border-radius:4px;
          color:#fbbf24;font-size:.79rem}}
    .msg{{color:#94a3b8;font-size:.78rem}}
    .badge{{display:inline-block;padding:.15rem .5rem;border-radius:20px;font-size:.69rem;font-weight:700}}
    .badge.crit{{background:rgba(239,68,68,.15);color:#ef4444}}
    .badge.warn{{background:rgba(251,191,36,.15);color:#fbbf24}}
    /* buttons */
    .btns{{margin-top:1.2rem;display:flex;gap:.6rem;flex-wrap:wrap}}
    .btn{{display:inline-block;padding:.5rem 1.2rem;border-radius:8px;text-decoration:none;
          font-weight:700;font-size:.85rem;transition:.2s}}
    .btn.solid{{background:#f97316;color:#000}}.btn.solid:hover{{background:#ea580c}}
    .btn.ghost{{background:rgba(249,115,22,.12);color:#f97316;border:1px solid rgba(249,115,22,.35)}}
    .btn.ghost:hover{{background:rgba(249,115,22,.22)}}
    .btn.green{{background:rgba(52,211,153,.12);color:#34d399;border:1px solid rgba(52,211,153,.3)}}
    .btn.green:hover{{background:rgba(52,211,153,.22)}}
    .two-col{{display:grid;grid-template-columns:1fr 1fr;gap:1.5rem}}
    @media(max-width:700px){{.two-col{{grid-template-columns:1fr}}}}
  </style>
</head>
<body>
  <h1>🔍 Data Quality Platform</h1>
  <p class="sub">Automated quality checks &nbsp;·&nbsp; {data['run_at'][:19]} UTC
     &nbsp;·&nbsp; {data['dataset']['rows']:,} transactions &nbsp;·&nbsp;
     {data['dataset']['columns'].__len__()} columns</p>

  <div class="src">
    <span class="dot"></span>
    <span>Live data scraped from</span>
    <a href="{BASE}" target="_blank">books.toscrape.com</a>
    <span style="color:#64748b">·</span>
    <span style="color:#94a3b8">{source}</span>
  </div>

  <div class="rbar">
    <span>Auto-refreshing in <span id="cd">60</span>s</span>
    <span style="color:{hc};font-weight:700">{hlabel}</span>
  </div>

  <div class="stats">
    <div class="stat"><div class="stat-n">{s['total_checks']}</div><div class="stat-l">Checks Run</div></div>
    <div class="stat"><div class="stat-n" style="color:#34d399">{s['passed']}</div><div class="stat-l">Passed</div></div>
    <div class="stat"><div class="stat-n" style="color:{hc}">{s['failed']}</div><div class="stat-l">Failed</div></div>
    <div class="stat"><div class="stat-n">{data['dataset']['rows']:,}</div><div class="stat-l">Rows Checked</div></div>
    <div class="stat" style="border-color:rgba(52,211,153,.3)">
      <div class="stat-n" style="color:{hc}">{hlabel}</div>
      <div class="stat-l">Pipeline Health</div>
    </div>
  </div>

  <h2>Quality Check Results</h2>
  <table>
    <thead><tr>
      <th>Check</th><th>Status</th><th>Severity</th>
      <th>Metric</th><th>Threshold</th><th>Rows Affected</th><th>Message</th>
    </tr></thead>
    <tbody>{check_rows}</tbody>
  </table>

  <div class="two-col">
    <div>
      <h2>By Category</h2>
      <table>
        <thead><tr><th>Category</th><th>Transactions</th><th>Revenue</th></tr></thead>
        <tbody>{cat_rows}</tbody>
      </table>
    </div>
    <div>
      <h2>Data Source</h2>
      <table>
        <thead><tr><th>Property</th><th>Value</th></tr></thead>
        <tbody>
          <tr><td>Website</td><td><a href="{BASE}" target="_blank" style="color:#34d399">books.toscrape.com</a></td></tr>
          <tr><td>Products scraped</td><td>{len(data.get('categories', []))} categories</td></tr>
          <tr><td>Transactions generated</td><td>{data['dataset']['rows']:,}</td></tr>
          <tr><td>Cache TTL</td><td>{CACHE_TTL // 60} minutes</td></tr>
          <tr><td>Injected duplicates</td><td>8 (double-submit simulation)</td></tr>
          <tr><td>Injected bulk orders</td><td>6 (bot/fraud simulation)</td></tr>
        </tbody>
      </table>
    </div>
  </div>

  <div class="btns">
    <a class="btn solid" href="/run">↻ Re-run Now</a>
    <a class="btn ghost" href="/docs">📖 API Docs</a>
    <a class="btn green" href="{BASE}" target="_blank">🌐 Live Data Source</a>
  </div>

  <script>
    let t = 60;
    const el = document.getElementById("cd");
    setInterval(() => {{ t--; el.textContent = t; if (t <= 0) location.reload(); }}, 1000);
  </script>
</body>
</html>"""
