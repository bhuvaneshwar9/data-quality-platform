"""
Ships quality check results and pipeline logs to Elasticsearch.
Falls back to local JSON logging if ES is not available.
"""

import json, os, logging
from datetime import datetime

try:
    from elasticsearch import Elasticsearch
    ES_AVAILABLE = True
except ImportError:
    ES_AVAILABLE = False

ES_HOST  = os.getenv("ES_HOST", "http://localhost:9200")
ES_INDEX = "data-quality-logs"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def get_client():
    if not ES_AVAILABLE:
        return None
    try:
        es = Elasticsearch(ES_HOST)
        if es.ping():
            return es
    except Exception:
        pass
    return None


def ship_log(event_type: str, payload: dict):
    doc = {
        "@timestamp": datetime.utcnow().isoformat(),
        "event_type": event_type,
        "host":       os.uname().nodename if hasattr(os, "uname") else "windows",
        **payload,
    }

    es = get_client()
    if es:
        es.index(index=ES_INDEX, body=doc)
        logger.info(f"Shipped to Elasticsearch: {event_type}")
    else:
        # Fallback: write to local NDJSON log
        os.makedirs("logs", exist_ok=True)
        with open("logs/quality_events.ndjson", "a") as f:
            f.write(json.dumps(doc) + "\n")
        logger.info(f"Logged locally (ES unavailable): {event_type}")


def log_check_result(check_name: str, passed: bool, metric: float, rows_affected: int):
    ship_log("quality_check", {
        "check_name":    check_name,
        "passed":        passed,
        "metric":        metric,
        "rows_affected": rows_affected,
    })


def log_pipeline_run(pipeline: str, rows: int, duration_s: float, status: str):
    ship_log("pipeline_run", {
        "pipeline":   pipeline,
        "rows":       rows,
        "duration_s": duration_s,
        "status":     status,
    })
