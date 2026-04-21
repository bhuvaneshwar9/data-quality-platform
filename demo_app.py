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
        if r["status"] == "PASS":
            pill = '<span class="pill pass">PASS</span>'
        elif r["status"] == "WARN":
            pill = '<span class="pill warn">WARN</span>'
        else:
            pill = '<span class="pill fail">FAIL</span>'
        m = f"{r['metric']:.2f}" if r["metric"] > 1 else f"{r['metric']:.2%}"
        t = f"{r['threshold']:.0f}" if r["threshold"] > 1 else f"{r['threshold']:.0%}"
        check_rows += f"""<tr>
          <td><span class="check-name">{r['check']}</span></td>
          <td>{pill}</td>
          <td class="msg-col">{r['message']}</td>
          <td class="num">{m}</td><td class="num">{t}</td>
          <td class="num">{r['rows_affected']:,}</td>
        </tr>"""

    cat_rows = ""
    total_rev = sum(c["revenue"] for c in data.get("categories", []))
    for c in sorted(data.get("categories", []), key=lambda x: -x["revenue"]):
        pct = int(c["revenue"] / total_rev * 100) if total_rev else 0
        cat_rows += f"""<tr>
          <td><span class="cat-dot"></span>{c['category']}</td>
          <td class="num">{c['transactions']:,}</td>
          <td class="num">${c['revenue']:,.2f}</td>
          <td><div class="bar-wrap"><div class="bar-fill" style="width:{pct}%"></div></div></td>
        </tr>"""

    score_pct = int(s['passed'] / s['total_checks'] * 100) if s['total_checks'] else 0

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8"/>
  <meta name="viewport" content="width=device-width,initial-scale=1"/>
  <title>Data Quality Platform</title>
  <link rel="preconnect" href="https://fonts.googleapis.com"/>
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet"/>
  <style>
    *{{box-sizing:border-box;margin:0;padding:0}}
    :root{{--bg:#f1f5f9;--surface:#ffffff;--surface2:#f8fafc;--border:#e2e8f0;--text:#0f172a;--muted:#64748b;--indigo:#6366f1;--indigo-light:#eef2ff;--green:#16a34a;--green-light:#dcfce7;--red:#dc2626;--red-light:#fee2e2;--yellow:#d97706;--yellow-light:#fef3c7}}
    body{{background:var(--bg);color:var(--text);font-family:'Inter',system-ui,sans-serif;min-height:100vh}}
    /* sidebar layout */
    .layout{{display:grid;grid-template-columns:220px 1fr;min-height:100vh}}
    .sidebar{{background:var(--surface);border-right:1px solid var(--border);padding:1.5rem 1.2rem;display:flex;flex-direction:column;gap:1.5rem}}
    .brand{{display:flex;align-items:center;gap:.6rem;padding-bottom:1rem;border-bottom:1px solid var(--border)}}
    .brand-icon{{width:32px;height:32px;background:var(--indigo);border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:1rem}}
    .brand-name{{font-weight:700;font-size:.95rem;color:var(--text)}}
    .brand-sub{{font-size:.7rem;color:var(--muted)}}
    .nav-label{{font-size:.65rem;font-weight:600;text-transform:uppercase;letter-spacing:1px;color:var(--muted);margin-bottom:.4rem}}
    .nav-item{{display:flex;align-items:center;gap:.6rem;padding:.5rem .7rem;border-radius:6px;font-size:.82rem;color:var(--muted);margin-bottom:.15rem;text-decoration:none;cursor:default}}
    .nav-item.active{{background:var(--indigo-light);color:var(--indigo);font-weight:600}}
    .score-ring{{display:flex;flex-direction:column;align-items:center;gap:.5rem;padding:1rem;background:var(--surface2);border:1px solid var(--border);border-radius:12px}}
    .ring-val{{font-size:2rem;font-weight:700;color:{hc}}}
    .ring-label{{font-size:.7rem;color:var(--muted);text-align:center;line-height:1.4}}
    .status-badge{{display:inline-flex;align-items:center;gap:.3rem;padding:.25rem .8rem;border-radius:20px;font-size:.72rem;font-weight:600}}
    .status-ok{{background:var(--green-light);color:var(--green)}}
    .status-warn{{background:var(--yellow-light);color:var(--yellow)}}
    .status-fail{{background:var(--red-light);color:var(--red)}}
    .sidebar-footer{{margin-top:auto;font-size:.7rem;color:var(--muted);line-height:1.6}}
    /* main content */
    .main{{padding:1.8rem 2rem;overflow-x:auto}}
    .topbar{{display:flex;align-items:center;justify-content:space-between;margin-bottom:1.8rem;flex-wrap:wrap;gap:1rem}}
    .page-title{{font-size:1.25rem;font-weight:700;color:var(--text)}}
    .page-sub{{font-size:.8rem;color:var(--muted);margin-top:.15rem}}
    .top-actions{{display:flex;align-items:center;gap:.8rem;flex-wrap:wrap}}
    .src-chip{{display:flex;align-items:center;gap:.4rem;background:var(--surface);border:1px solid var(--border);border-radius:20px;padding:.3rem .8rem;font-size:.75rem;color:var(--muted)}}
    .live-dot{{width:7px;height:7px;background:#16a34a;border-radius:50%;animation:blink 1.4s infinite;flex-shrink:0}}
    @keyframes blink{{0%,100%{{opacity:1}}50%{{opacity:.3}}}}
    .src-chip a{{color:var(--indigo);text-decoration:none;font-weight:500}}
    /* cards */
    .cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:1rem;margin-bottom:2rem}}
    .card{{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:1.2rem;transition:.15s}}
    .card:hover{{box-shadow:0 4px 16px rgba(99,102,241,.08);border-color:#c7d2fe}}
    .card-val{{font-size:1.8rem;font-weight:700;line-height:1}}
    .card-label{{font-size:.72rem;color:var(--muted);margin-top:.4rem;font-weight:500}}
    /* section */
    .section-title{{font-size:.78rem;font-weight:600;color:var(--text);text-transform:uppercase;letter-spacing:.8px;margin-bottom:.8rem;display:flex;align-items:center;gap:.5rem}}
    .section-title::after{{content:'';flex:1;height:1px;background:var(--border)}}
    /* tables */
    .tbl-wrap{{background:var(--surface);border:1px solid var(--border);border-radius:12px;overflow:hidden;margin-bottom:1.5rem}}
    table{{width:100%;border-collapse:collapse}}
    thead tr{{background:var(--surface2)}}
    th{{padding:.65rem 1rem;text-align:left;font-size:.7rem;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;border-bottom:1px solid var(--border)}}
    td{{padding:.65rem 1rem;border-bottom:1px solid var(--border);font-size:.83rem}}
    tr:last-child td{{border-bottom:none}}
    tr:hover td{{background:var(--surface2)}}
    .check-name{{font-family:monospace;background:#f1f5f9;padding:.1rem .45rem;border-radius:4px;font-size:.78rem;color:var(--indigo);font-weight:600}}
    .pill{{display:inline-block;padding:.18rem .6rem;border-radius:20px;font-size:.68rem;font-weight:700;letter-spacing:.3px}}
    .pill.pass{{background:var(--green-light);color:var(--green)}}
    .pill.warn{{background:var(--yellow-light);color:var(--yellow)}}
    .pill.fail{{background:var(--red-light);color:var(--red)}}
    .msg-col{{color:var(--muted);font-size:.78rem;max-width:260px}}
    .num{{font-variant-numeric:tabular-nums;font-size:.82rem}}
    .cat-dot{{display:inline-block;width:8px;height:8px;background:var(--indigo);border-radius:50%;margin-right:.5rem;opacity:.6}}
    .bar-wrap{{background:#e2e8f0;border-radius:4px;height:6px;width:100px;overflow:hidden}}
    .bar-fill{{background:var(--indigo);height:100%;border-radius:4px;transition:.3s}}
    .two{{display:grid;grid-template-columns:1fr 1fr;gap:1.2rem}}
    @media(max-width:900px){{.layout{{grid-template-columns:1fr}}.sidebar{{display:none}}.two{{grid-template-columns:1fr}}}}
    /* refresh bar */
    .rbar{{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:.5rem 1rem;font-size:.75rem;color:var(--muted);display:flex;justify-content:space-between;align-items:center;margin-bottom:1.5rem}}
    #cd{{color:var(--indigo);font-weight:600}}
    /* buttons */
    .btns{{display:flex;gap:.6rem;flex-wrap:wrap;margin-top:1.5rem;padding-top:1.2rem;border-top:1px solid var(--border)}}
    .btn{{display:inline-flex;align-items:center;gap:.4rem;padding:.5rem 1.1rem;border-radius:7px;font-size:.8rem;font-weight:600;text-decoration:none;transition:.15s;cursor:pointer;border:none}}
    .btn-primary{{background:var(--indigo);color:#fff}}.btn-primary:hover{{background:#4f46e5;box-shadow:0 4px 12px rgba(99,102,241,.35)}}
    .btn-outline{{background:var(--surface);color:var(--text);border:1px solid var(--border)}}.btn-outline:hover{{background:var(--surface2)}}
    .btn-link{{background:transparent;color:var(--muted);border:1px solid var(--border)}}.btn-link:hover{{color:var(--indigo)}}
  </style>
</head>
<body>
<div class="layout">
  <!-- Sidebar -->
  <aside class="sidebar">
    <div class="brand">
      <div class="brand-icon">🔍</div>
      <div><div class="brand-name">DataQuality</div><div class="brand-sub">Platform v3.0</div></div>
    </div>
    <div>
      <div class="nav-label">Navigation</div>
      <div class="nav-item active">📊 Dashboard</div>
      <a class="nav-item" href="/docs">📖 API Docs</a>
      <a class="nav-item" href="/health" target="_blank">❤️ Health</a>
      <a class="nav-item" href="{BASE}" target="_blank">↗ Data Source</a>
    </div>
    <div class="score-ring">
      <div class="ring-val">{score_pct}%</div>
      <div class="status-badge {'status-ok' if s['failed']==0 else 'status-warn' if s['failed']<=1 else 'status-fail'}">{hlabel}</div>
      <div class="ring-label">Quality Score<br/>{s['passed']}/{s['total_checks']} checks passed</div>
    </div>
    <div class="sidebar-footer">
      Last run: {data['run_at'][11:19]} UTC<br/>
      Rows: {data['dataset']['rows']:,} · Cols: {len(data['dataset']['columns'])}<br/>
      Cache TTL: {CACHE_TTL//60} min
    </div>
  </aside>

  <!-- Main -->
  <main class="main">
    <div class="topbar">
      <div>
        <div class="page-title">Quality Dashboard</div>
        <div class="page-sub">{data['run_at'][:19]} UTC · {data['dataset']['rows']:,} transactions analysed</div>
      </div>
      <div class="top-actions">
        <div class="src-chip">
          <span class="live-dot"></span>
          <span>Live ·</span>
          <a href="{BASE}" target="_blank">books.toscrape.com</a>
        </div>
      </div>
    </div>

    <div class="rbar">
      <span>Auto-refreshing in <span id="cd">60</span>s</span>
      <span style="color:{hc};font-weight:600">{source}</span>
    </div>

    <!-- KPI cards -->
    <div class="cards">
      <div class="card"><div class="card-val">{s['total_checks']}</div><div class="card-label">Checks Run</div></div>
      <div class="card"><div class="card-val" style="color:var(--green)">{s['passed']}</div><div class="card-label">Passed</div></div>
      <div class="card"><div class="card-val" style="color:{hc}">{s['failed']}</div><div class="card-label">Failed</div></div>
      <div class="card"><div class="card-val">{data['dataset']['rows']:,}</div><div class="card-label">Rows Analysed</div></div>
      <div class="card"><div class="card-val" style="color:var(--indigo)">{score_pct}%</div><div class="card-label">Quality Score</div></div>
    </div>

    <!-- Check results -->
    <div class="section-title">Quality Check Results</div>
    <div class="tbl-wrap">
      <table>
        <thead><tr><th>Check</th><th>Status</th><th>Message</th><th>Metric</th><th>Threshold</th><th>Rows</th></tr></thead>
        <tbody>{check_rows}</tbody>
      </table>
    </div>

    <div class="two">
      <div>
        <div class="section-title">Revenue by Category</div>
        <div class="tbl-wrap">
          <table>
            <thead><tr><th>Category</th><th>Orders</th><th>Revenue</th><th>Share</th></tr></thead>
            <tbody>{cat_rows}</tbody>
          </table>
        </div>
      </div>
      <div>
        <div class="section-title">Injected Anomalies</div>
        <div class="tbl-wrap">
          <table>
            <thead><tr><th>Type</th><th>Count</th><th>Purpose</th></tr></thead>
            <tbody>
              <tr><td>Duplicate IDs</td><td class="num" style="color:var(--red)">8</td><td class="msg-col">Double-submit simulation</td></tr>
              <tr><td>Bulk orders</td><td class="num" style="color:var(--yellow)">6</td><td class="msg-col">Bot / fraud simulation</td></tr>
              <tr><td>OOS purchases</td><td class="num" style="color:var(--yellow)">~5%</td><td class="msg-col">Business-rule violation</td></tr>
              <tr><td>Source website</td><td colspan="2"><a href="{BASE}" target="_blank" style="color:var(--indigo);font-weight:500">books.toscrape.com ↗</a></td></tr>
              <tr><td>Products scraped</td><td class="num">{len(data.get('categories',[]))}</td><td class="msg-col">categories</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </div>

    <div class="btns">
      <button class="btn btn-primary" onclick="location.reload()">↻ Re-run Checks</button>
      <a class="btn btn-outline" href="/docs">📖 API Docs</a>
      <a class="btn btn-link" href="{BASE}" target="_blank">↗ Data Source</a>
    </div>
  </main>
</div>

<script>
  let t=60;const el=document.getElementById("cd");
  setInterval(()=>{{t--;el.textContent=t;if(t<=0)location.reload();}},1000);
</script>
</body>
</html>"""
