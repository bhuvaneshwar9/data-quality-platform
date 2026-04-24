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

    # Category breakdown
    cat_counts = df.groupby("category").agg(
        transactions=("transaction_id", "count"),
        revenue=("amount", "sum"),
        avg_amount=("amount", "mean"),
    ).round(2).reset_index()

    # Per-category quality: flag duplicates/anomalies per category
    dup_ids      = df[df["transaction_id"].duplicated(keep=False)]["transaction_id"].unique()
    mean_amt     = df["amount"].mean()
    std_amt      = df["amount"].std() + 1e-9
    df["_anomaly"] = ((df["amount"] - mean_amt) / std_amt).abs() > 3.0
    df["_dup"]     = df["transaction_id"].isin(dup_ids)
    df["_oos"]     = df["availability"] == "Out of Stock"
    df["_issue"]   = df["_anomaly"] | df["_dup"] | df["_oos"]

    cat_issues = df.groupby("category").agg(
        issues=("_issue", "sum"),
        total=("transaction_id", "count"),
    ).reset_index()
    cat_issues["issues"]     = cat_issues["issues"].astype(int)
    cat_issues["issue_rate"] = (cat_issues["issues"] / cat_issues["total"] * 100).round(1)

    cat_full = cat_counts.merge(
        cat_issues[["category", "issues", "issue_rate"]], on="category", how="left"
    ).fillna(0)
    cat_full["transactions"] = cat_full["transactions"].astype(int)
    cat_full["issues"]       = cat_full["issues"].astype(int)

    # Sample bad records (duplicates + anomalies + OOS)
    bad_records = []
    for rec in df[df["_issue"]].head(5)[
        ["transaction_id","user_id","product","category","amount","availability"]
    ].to_dict(orient="records"):
        bad_records.append({
            "transaction_id": str(rec["transaction_id"]),
            "user_id":        str(rec.get("user_id", "—")),
            "product":        str(rec["product"]),
            "category":       str(rec["category"]),
            "amount":         float(rec["amount"]),
            "availability":   str(rec["availability"]),
        })

    # Hourly distribution
    df["_hour"] = pd.to_datetime(df["created_at"]).dt.hour
    hourly      = df.groupby("_hour").size().reset_index(name="count")
    hourly_data = {int(r["_hour"]): int(r["count"]) for _, r in hourly.iterrows()}

    total_issues = int(df["_issue"].sum())

    result = {
        "run_at":       datetime.now(timezone.utc).isoformat(),
        "source":       source,
        "dataset":      {"rows": len(df), "columns": list(df.columns)},
        "summary":      {"total_checks": len(checks), "passed": passed,
                         "failed": len(checks) - passed,
                         "total_issues": total_issues},
        "categories":   [
            {k: (int(v) if hasattr(v, "item") and isinstance(v.item(), int) else
                 float(round(v.item(), 2)) if hasattr(v, "item") else v)
             for k, v in row.items()}
            for row in cat_full.to_dict(orient="records")
        ],
        "bad_records":  bad_records,
        "hourly":       hourly_data,
        "checks":       checks,
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
    hc      = "#16a34a" if s["failed"] == 0 else ("#d97706" if s["failed"] <= 1 else "#dc2626")
    hlabel  = "HEALTHY" if s["failed"] == 0 else ("DEGRADED" if s["failed"] <= 1 else "FAILING")

    score_pct   = int(s['passed'] / s['total_checks'] * 100) if s['total_checks'] else 0
    total_rev   = sum(c["revenue"] for c in data.get("categories", []))
    total_issues= s.get("total_issues", 0)

    # ── Check cards ───────────────────────────────────────────
    CHECK_META = {
        "NullCheck":      {"icon":"🔲","label":"Null / Missing","fix":"Backfill or drop records missing critical fields."},
        "DuplicateCheck": {"icon":"♻️","label":"Duplicate IDs", "fix":"Deduplicate by transaction_id — likely a double-submit bug."},
        "RangeCheck":     {"icon":"📐","label":"Range Validation","fix":"Reject orders with amount outside $0.01–$500; flag OOS purchases."},
        "FreshnessCheck": {"icon":"🕐","label":"Data Freshness", "fix":"Ensure the pipeline ingests data within 48h; check ETL schedule."},
        "AnomalyCheck":   {"icon":"🚨","label":"Anomaly Detection","fix":"Investigate bulk/bot orders — apply rate-limiting or fraud rules."},
    }
    check_cards = ""
    for r in data["checks"]:
        meta  = CHECK_META.get(r["check"], {"icon":"🔍","label":r["check"],"fix":"Review and address the flagged records."})
        sc    = "pass" if r["status"]=="PASS" else ("warn" if r["status"]=="WARN" else "fail")
        sc_c  = "#16a34a" if sc=="pass" else ("#d97706" if sc=="warn" else "#dc2626")
        sc_bg = "#dcfce7" if sc=="pass" else ("#fef3c7" if sc=="warn" else "#fee2e2")
        m_val = r["metric"]
        t_val = r["threshold"]
        # bar: % of metric vs threshold (capped at 100)
        bar_pct = min(100, int(m_val / t_val * 100)) if t_val > 0 else 0
        bar_c   = sc_c
        m_fmt   = f"{m_val:.1f}" if m_val >= 1 else f"{m_val*100:.2f}%"
        t_fmt   = f"{t_val:.1f}" if t_val >= 1 else f"{t_val*100:.0f}%"
        fix_html= "" if sc=="pass" else f'<div class="fix-tip">💡 {meta["fix"]}</div>'
        check_cards += f"""
        <div class="check-card {sc}">
          <div class="cc-top">
            <div class="cc-left">
              <span class="cc-icon">{meta['icon']}</span>
              <div>
                <div class="cc-name">{meta['label']}</div>
                <div class="cc-sub">{r['check']}</div>
              </div>
            </div>
            <span class="pill {sc}">{r['status']}</span>
          </div>
          <div class="cc-msg">{r['message']}</div>
          <div class="cc-bar-row">
            <div class="cc-bar-wrap"><div class="cc-bar-fill" style="width:{bar_pct}%;background:{bar_c}"></div></div>
            <span class="cc-bar-label">{m_fmt} / {t_fmt}</span>
          </div>
          <div class="cc-stats">
            <span>Rows affected: <b>{r['rows_affected']:,}</b></span>
          </div>
          {fix_html}
        </div>"""

    # ── Category table ────────────────────────────────────────
    cat_rows = ""
    for c in sorted(data.get("categories",[]), key=lambda x: -x["revenue"]):
        rev_pct  = int(c["revenue"] / total_rev * 100) if total_rev else 0
        iss_pct  = float(c.get("issue_rate", 0))
        iss_c    = "#16a34a" if iss_pct < 5 else ("#d97706" if iss_pct < 15 else "#dc2626")
        cat_rows += f"""<tr>
          <td><span class="cat-dot" style="background:var(--indigo)"></span>{c['category']}</td>
          <td class="num">{int(c['transactions']):,}</td>
          <td class="num">${c['revenue']:,.0f}</td>
          <td class="num">${c.get('avg_amount',0):.2f}</td>
          <td><div class="bar-wrap"><div class="bar-fill" style="width:{rev_pct}%"></div></div></td>
          <td><span style="color:{iss_c};font-weight:600;font-size:.8rem">{iss_pct:.1f}%</span></td>
        </tr>"""

    # ── Bad records table ─────────────────────────────────────
    bad_rows = ""
    for r in data.get("bad_records",[]):
        avail_c = "#dc2626" if r["availability"] == "Out of Stock" else "#16a34a"
        bad_rows += f"""<tr>
          <td style="font-family:monospace;font-size:.78rem;color:var(--indigo)">{r['transaction_id']}</td>
          <td>{r.get('user_id','—')}</td>
          <td style="max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">{r['product'][:35]}…</td>
          <td>{r['category']}</td>
          <td class="num"><b>${r['amount']:,.2f}</b></td>
          <td><span style="color:{avail_c};font-weight:600;font-size:.75rem">{r['availability']}</span></td>
        </tr>"""

    # ── Hourly mini-chart bars ─────────────────────────────────
    hourly = data.get("hourly", {})
    max_h  = max(hourly.values()) if hourly else 1
    hour_bars = ""
    for h in range(24):
        v   = hourly.get(h, 0)
        ht  = int(v / max_h * 40) if max_h else 0
        hour_bars += f'<div class="h-col" title="{h:02d}:00 — {v} txns"><div class="h-bar" style="height:{ht}px"></div><div class="h-label">{h if h%6==0 else ""}</div></div>'

    # ── Critical issues banner ────────────────────────────────
    fail_checks = [r for r in data["checks"] if r["status"] in ("FAIL","WARN")]
    issues_banner = ""
    if fail_checks:
        issue_items = "".join(f'<div class="issue-item"><span class="pill {"warn" if r["status"]=="WARN" else "fail"}">{r["status"]}</span><span>{CHECK_META.get(r["check"],{{}}).get("label",r["check"])}</span><span class="issue-rows">{r["rows_affected"]:,} rows</span></div>' for r in fail_checks)
        issues_banner = f"""<div class="issues-banner">
          <div class="issues-header">⚠ {len(fail_checks)} issue{"s" if len(fail_checks)>1 else ""} require{"s" if len(fail_checks)==1 else ""} attention · {total_issues:,} total affected rows</div>
          <div class="issue-list">{issue_items}</div>
        </div>"""

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
    .layout{{display:grid;grid-template-columns:230px 1fr;min-height:100vh}}
    /* ── Sidebar ── */
    .sidebar{{background:var(--surface);border-right:1px solid var(--border);padding:1.5rem 1.2rem;display:flex;flex-direction:column;gap:1.4rem;position:sticky;top:0;height:100vh;overflow-y:auto}}
    .brand{{display:flex;align-items:center;gap:.6rem;padding-bottom:1rem;border-bottom:1px solid var(--border)}}
    .brand-icon{{width:34px;height:34px;background:var(--indigo);border-radius:8px;display:flex;align-items:center;justify-content:center;font-size:1.1rem}}
    .brand-name{{font-weight:700;font-size:.95rem}}.brand-sub{{font-size:.68rem;color:var(--muted)}}
    .nav-label{{font-size:.63rem;font-weight:700;text-transform:uppercase;letter-spacing:1.2px;color:var(--muted);margin-bottom:.35rem}}
    .nav-item{{display:flex;align-items:center;gap:.6rem;padding:.48rem .7rem;border-radius:7px;font-size:.82rem;color:var(--muted);margin-bottom:.1rem;text-decoration:none}}
    .nav-item.active{{background:var(--indigo-light);color:var(--indigo);font-weight:600}}
    .nav-item:hover:not(.active){{background:var(--surface2)}}
    /* score donut */
    .score-box{{background:var(--surface2);border:1px solid var(--border);border-radius:14px;padding:1.2rem;text-align:center}}
    .score-num{{font-size:2.6rem;font-weight:800;color:{hc};line-height:1}}
    .score-label{{font-size:.68rem;color:var(--muted);margin-top:.3rem}}
    .status-pill{{display:inline-flex;align-items:center;gap:.3rem;padding:.22rem .75rem;border-radius:20px;font-size:.7rem;font-weight:700;margin-top:.5rem}}
    .sp-ok{{background:var(--green-light);color:var(--green)}}
    .sp-warn{{background:var(--yellow-light);color:var(--yellow)}}
    .sp-fail{{background:var(--red-light);color:var(--red)}}
    /* checks mini list in sidebar */
    .mini-check{{display:flex;align-items:center;justify-content:space-between;padding:.3rem 0;border-bottom:1px solid var(--border);font-size:.75rem}}
    .mini-check:last-child{{border:none}}
    .sidebar-footer{{margin-top:auto;font-size:.68rem;color:var(--muted);line-height:1.7;border-top:1px solid var(--border);padding-top:.8rem}}
    /* ── Main ── */
    .main{{padding:1.8rem 2rem;overflow-x:auto;max-width:1200px}}
    .topbar{{display:flex;align-items:flex-start;justify-content:space-between;margin-bottom:1.4rem;flex-wrap:wrap;gap:1rem}}
    .page-title{{font-size:1.3rem;font-weight:700}}.page-sub{{font-size:.8rem;color:var(--muted);margin-top:.2rem}}
    .src-chip{{display:inline-flex;align-items:center;gap:.4rem;background:var(--surface);border:1px solid var(--border);border-radius:20px;padding:.3rem .8rem;font-size:.75rem;color:var(--muted)}}
    .live-dot{{width:7px;height:7px;background:#16a34a;border-radius:50%;animation:pulse 1.4s infinite;flex-shrink:0}}
    @keyframes pulse{{0%,100%{{opacity:1;box-shadow:0 0 0 0 rgba(22,163,74,.4)}}50%{{opacity:.5;box-shadow:0 0 0 4px rgba(22,163,74,0)}}}}
    .src-chip a{{color:var(--indigo);text-decoration:none;font-weight:500}}
    /* rbar */
    .rbar{{background:var(--surface);border:1px solid var(--border);border-radius:8px;padding:.5rem 1rem;font-size:.75rem;color:var(--muted);display:flex;justify-content:space-between;align-items:center;margin-bottom:1.2rem;flex-wrap:wrap;gap:.5rem}}
    #cd{{color:var(--indigo);font-weight:700}}
    /* issues banner */
    .issues-banner{{background:#fff7ed;border:1px solid #fed7aa;border-radius:10px;padding:1rem 1.2rem;margin-bottom:1.4rem}}
    .issues-header{{font-weight:700;font-size:.85rem;color:#c2410c;margin-bottom:.6rem}}
    .issue-list{{display:flex;flex-wrap:wrap;gap:.5rem}}
    .issue-item{{display:flex;align-items:center;gap:.4rem;background:#fff;border:1px solid #fed7aa;border-radius:6px;padding:.3rem .6rem;font-size:.78rem}}
    .issue-rows{{color:var(--muted);font-size:.72rem}}
    /* KPI cards */
    .cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(130px,1fr));gap:.8rem;margin-bottom:1.6rem}}
    .card{{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:1.1rem 1.2rem;transition:.15s}}
    .card:hover{{box-shadow:0 4px 16px rgba(99,102,241,.1);border-color:#c7d2fe;transform:translateY(-1px)}}
    .card-val{{font-size:1.9rem;font-weight:800;line-height:1}}
    .card-label{{font-size:.7rem;color:var(--muted);margin-top:.35rem;font-weight:500}}
    /* check cards */
    .checks-grid{{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:1rem;margin-bottom:1.8rem}}
    .check-card{{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:1.1rem 1.2rem;transition:.15s}}
    .check-card:hover{{box-shadow:0 4px 16px rgba(0,0,0,.06);transform:translateY(-2px)}}
    .check-card.fail{{border-left:3px solid var(--red)}}
    .check-card.warn{{border-left:3px solid var(--yellow)}}
    .check-card.pass{{border-left:3px solid var(--green)}}
    .cc-top{{display:flex;align-items:center;justify-content:space-between;margin-bottom:.6rem}}
    .cc-left{{display:flex;align-items:center;gap:.6rem}}
    .cc-icon{{font-size:1.3rem}}
    .cc-name{{font-size:.88rem;font-weight:700}}
    .cc-sub{{font-size:.68rem;color:var(--muted);font-family:monospace}}
    .cc-msg{{font-size:.78rem;color:var(--muted);margin-bottom:.7rem;line-height:1.5}}
    .cc-bar-row{{display:flex;align-items:center;gap:.6rem;margin-bottom:.5rem}}
    .cc-bar-wrap{{flex:1;background:#e2e8f0;border-radius:4px;height:6px;overflow:hidden}}
    .cc-bar-fill{{height:100%;border-radius:4px;transition:.4s}}
    .cc-bar-label{{font-size:.7rem;color:var(--muted);white-space:nowrap;min-width:80px;text-align:right}}
    .cc-stats{{font-size:.73rem;color:var(--muted)}}
    .fix-tip{{margin-top:.6rem;padding:.5rem .7rem;background:#f0fdf4;border:1px solid #bbf7d0;border-radius:6px;font-size:.73rem;color:#15803d;line-height:1.5}}
    .check-card.warn .fix-tip{{background:#fffbeb;border-color:#fde68a;color:#92400e}}
    .check-card.fail .fix-tip{{background:#fef2f2;border-color:#fecaca;color:#991b1b}}
    /* pills */
    .pill{{display:inline-block;padding:.18rem .6rem;border-radius:20px;font-size:.67rem;font-weight:700;letter-spacing:.3px}}
    .pill.pass{{background:var(--green-light);color:var(--green)}}
    .pill.warn{{background:var(--yellow-light);color:var(--yellow)}}
    .pill.fail{{background:var(--red-light);color:var(--red)}}
    /* section titles */
    .section-title{{font-size:.75rem;font-weight:700;color:var(--text);text-transform:uppercase;letter-spacing:.8px;margin-bottom:.8rem;display:flex;align-items:center;gap:.5rem}}
    .section-title::after{{content:'';flex:1;height:1px;background:var(--border)}}
    /* tables */
    .tbl-wrap{{background:var(--surface);border:1px solid var(--border);border-radius:12px;overflow:hidden;margin-bottom:1.5rem}}
    table{{width:100%;border-collapse:collapse}}
    thead tr{{background:var(--surface2)}}
    th{{padding:.6rem 1rem;text-align:left;font-size:.68rem;font-weight:600;color:var(--muted);text-transform:uppercase;letter-spacing:.5px;border-bottom:1px solid var(--border)}}
    td{{padding:.6rem 1rem;border-bottom:1px solid var(--border);font-size:.82rem}}
    tr:last-child td{{border-bottom:none}}
    tr:hover td{{background:var(--surface2)}}
    .cat-dot{{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:.5rem;opacity:.7}}
    .bar-wrap{{background:#e2e8f0;border-radius:4px;height:6px;width:90px;overflow:hidden;display:inline-block;vertical-align:middle}}
    .bar-fill{{background:var(--indigo);height:100%;border-radius:4px}}
    .num{{font-variant-numeric:tabular-nums}}
    /* hourly chart */
    .hourly-chart{{background:var(--surface);border:1px solid var(--border);border-radius:12px;padding:1.1rem 1.3rem;margin-bottom:1.5rem}}
    .h-row{{display:flex;align-items:flex-end;gap:3px;height:50px;padding-bottom:2px}}
    .h-col{{display:flex;flex-direction:column;align-items:center;gap:2px;flex:1}}
    .h-bar{{background:var(--indigo);border-radius:2px 2px 0 0;width:100%;min-height:2px;opacity:.7;transition:.3s}}
    .h-label{{font-size:.6rem;color:var(--muted)}}
    /* two col */
    .two{{display:grid;grid-template-columns:1fr 1fr;gap:1.2rem}}
    @media(max-width:960px){{.layout{{grid-template-columns:1fr}}.sidebar{{display:none}}.two{{grid-template-columns:1fr}}}}
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

  <!-- ── Sidebar ── -->
  <aside class="sidebar">
    <div class="brand">
      <div class="brand-icon">🔍</div>
      <div><div class="brand-name">DataQuality</div><div class="brand-sub">Platform v3.0</div></div>
    </div>
    <div>
      <div class="nav-label">Navigation</div>
      <div class="nav-item active">📊 Dashboard</div>
      <a class="nav-item" href="/docs">📖 API Docs</a>
      <a class="nav-item" href="{BASE}" target="_blank">↗ Data Source</a>
    </div>
    <div class="score-box">
      <div class="score-num">{score_pct}%</div>
      <div class="score-label">{s['passed']}/{s['total_checks']} checks passed</div>
      <div class="status-pill {'sp-ok' if s['failed']==0 else 'sp-warn' if s['failed']<=1 else 'sp-fail'}">{'✓ HEALTHY' if s['failed']==0 else '⚠ DEGRADED' if s['failed']<=1 else '✕ FAILING'}</div>
    </div>
    <div>
      <div class="nav-label">Check Status</div>
      {"".join(f'<div class="mini-check"><span style="font-size:.8rem">{CHECK_META.get(r["check"],{{}}).get("icon","🔍")} {CHECK_META.get(r["check"],{{}}).get("label",r["check"])}</span><span class="pill {"pass" if r["status"]=="PASS" else "warn" if r["status"]=="WARN" else "fail"}">{r["status"]}</span></div>' for r in data["checks"])}
    </div>
    <div class="sidebar-footer">
      Last run: {data['run_at'][11:19]} UTC<br/>
      Rows: {data['dataset']['rows']:,} · Cols: {len(data['dataset']['columns'])}<br/>
      Issues found: {total_issues:,}<br/>
      Cache TTL: {CACHE_TTL//60} min
    </div>
  </aside>

  <!-- ── Main ── -->
  <main class="main">
    <div class="topbar">
      <div>
        <div class="page-title">Data Quality Dashboard</div>
        <div class="page-sub">{data['run_at'][:19]} UTC · Analysing {data['dataset']['rows']:,} e-commerce transactions</div>
      </div>
      <div class="src-chip">
        <span class="live-dot"></span>
        Live ·&nbsp;<a href="{BASE}" target="_blank">books.toscrape.com</a>
      </div>
    </div>

    <div class="rbar">
      <span>🕐 Auto-refresh in <span id="cd">60</span>s</span>
      <span style="color:var(--muted)">{source}</span>
    </div>

    {issues_banner}

    <!-- KPI Row -->
    <div class="cards">
      <div class="card"><div class="card-val">{s['total_checks']}</div><div class="card-label">Total Checks</div></div>
      <div class="card"><div class="card-val" style="color:var(--green)">{s['passed']}</div><div class="card-label">Passed</div></div>
      <div class="card"><div class="card-val" style="color:{hc}">{s['failed']}</div><div class="card-label">Failed / Warned</div></div>
      <div class="card"><div class="card-val">{data['dataset']['rows']:,}</div><div class="card-label">Rows Analysed</div></div>
      <div class="card"><div class="card-val" style="color:var(--red)">{total_issues:,}</div><div class="card-label">Issues Found</div></div>
      <div class="card"><div class="card-val" style="color:var(--indigo)">{score_pct}%</div><div class="card-label">Quality Score</div></div>
    </div>

    <!-- Check Cards -->
    <div class="section-title">Quality Checks · Detailed Breakdown</div>
    <div class="checks-grid">{check_cards}</div>

    <!-- Hourly distribution -->
    <div class="section-title">Transaction Volume · 24h Distribution</div>
    <div class="hourly-chart">
      <div style="font-size:.75rem;color:var(--muted);margin-bottom:.6rem">Orders per hour (last 48h window)</div>
      <div class="h-row">{hour_bars}</div>
    </div>

    <!-- Category + Bad Records -->
    <div class="two">
      <div>
        <div class="section-title">Revenue by Category · Issue Rate</div>
        <div class="tbl-wrap">
          <table>
            <thead><tr><th>Category</th><th>Orders</th><th>Revenue</th><th>Avg $</th><th>Share</th><th>Issues</th></tr></thead>
            <tbody>{cat_rows}</tbody>
          </table>
        </div>
      </div>
      <div>
        <div class="section-title">Sample Flagged Records</div>
        <div class="tbl-wrap">
          <table>
            <thead><tr><th>Txn ID</th><th>User</th><th>Product</th><th>Cat</th><th>Amount</th><th>Status</th></tr></thead>
            <tbody>{bad_rows if bad_rows else '<tr><td colspan="6" style="text-align:center;color:var(--muted);padding:1.5rem">✓ No bad records found</td></tr>'}</tbody>
          </table>
        </div>
      </div>
    </div>

    <!-- Injected defects explanation -->
    <div class="section-title">Pipeline Defect Injection · What's Being Tested</div>
    <div class="tbl-wrap">
      <table>
        <thead><tr><th>Defect Type</th><th>Count</th><th>Check That Catches It</th><th>Real-World Cause</th></tr></thead>
        <tbody>
          <tr><td>🔴 Duplicate Transaction IDs</td><td class="num" style="color:var(--red);font-weight:700">8 rows</td><td><span style="font-family:monospace;font-size:.78rem;color:var(--indigo)">DuplicateCheck</span></td><td style="color:var(--muted);font-size:.78rem">Double-submit bug, network retry without idempotency key</td></tr>
          <tr><td>🟡 Bulk / Bot Orders</td><td class="num" style="color:var(--yellow);font-weight:700">6 rows</td><td><span style="font-family:monospace;font-size:.78rem;color:var(--indigo)">AnomalyCheck</span></td><td style="color:var(--muted);font-size:.78rem">Automated scraper, fraud, or test account purchasing in bulk</td></tr>
          <tr><td>🟡 Out-of-Stock Purchases</td><td class="num" style="color:var(--yellow);font-weight:700">~5% of rows</td><td><span style="font-family:monospace;font-size:.78rem;color:var(--indigo)">RangeCheck</span></td><td style="color:var(--muted);font-size:.78rem">Race condition between inventory update and order placement</td></tr>
          <tr><td>🔲 Missing Fields</td><td class="num" style="color:var(--muted)">Varies</td><td><span style="font-family:monospace;font-size:.78rem;color:var(--indigo)">NullCheck</span></td><td style="color:var(--muted);font-size:.78rem">Upstream schema change or partial API response</td></tr>
          <tr><td>🕐 Stale Records</td><td class="num" style="color:var(--muted)">Varies</td><td><span style="font-family:monospace;font-size:.78rem;color:var(--indigo)">FreshnessCheck</span></td><td style="color:var(--muted);font-size:.78rem">ETL pipeline delay, backfill job, or delayed event processing</td></tr>
        </tbody>
      </table>
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
