-- PostgreSQL schema: data lineage + quality tracking

CREATE SCHEMA IF NOT EXISTS lineage;
CREATE SCHEMA IF NOT EXISTS quality;

-- ── Data lineage ─────────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS lineage.pipeline_runs (
    run_id          UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_name   VARCHAR(128)    NOT NULL,
    source          VARCHAR(255),
    destination     VARCHAR(255),
    rows_read       BIGINT          DEFAULT 0,
    rows_written    BIGINT          DEFAULT 0,
    status          VARCHAR(32)     DEFAULT 'RUNNING',  -- RUNNING, SUCCESS, FAILED
    started_at      TIMESTAMP       DEFAULT NOW(),
    finished_at     TIMESTAMP,
    error_message   TEXT
);

CREATE TABLE IF NOT EXISTS lineage.dataset_versions (
    version_id      UUID            PRIMARY KEY DEFAULT gen_random_uuid(),
    dataset_name    VARCHAR(128)    NOT NULL,
    version         VARCHAR(64)     NOT NULL,
    location        VARCHAR(255),
    schema_hash     VARCHAR(64),
    row_count       BIGINT,
    created_at      TIMESTAMP       DEFAULT NOW(),
    run_id          UUID            REFERENCES lineage.pipeline_runs(run_id)
);

-- ── Quality results ───────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS quality.check_results (
    id              BIGSERIAL       PRIMARY KEY,
    run_timestamp   TIMESTAMP       DEFAULT NOW(),
    dataset_name    VARCHAR(128),
    check_name      VARCHAR(128)    NOT NULL,
    passed          BOOLEAN         NOT NULL,
    severity        VARCHAR(32),
    metric_value    NUMERIC(18,6),
    threshold       NUMERIC(18,6),
    rows_affected   BIGINT          DEFAULT 0,
    message         TEXT
);

CREATE TABLE IF NOT EXISTS quality.quality_sla (
    dataset_name    VARCHAR(128)    NOT NULL,
    check_name      VARCHAR(128)    NOT NULL,
    threshold       NUMERIC(18,6)   NOT NULL,
    severity        VARCHAR(32)     DEFAULT 'WARNING',
    PRIMARY KEY (dataset_name, check_name)
);

-- ── RBAC ──────────────────────────────────────────────────────────────────────

CREATE ROLE IF NOT EXISTS data_reader;
CREATE ROLE IF NOT EXISTS data_writer;
CREATE ROLE IF NOT EXISTS quality_admin;

GRANT USAGE ON SCHEMA lineage, quality TO data_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA lineage TO data_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA quality TO data_reader;

GRANT USAGE ON SCHEMA lineage, quality TO data_writer;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA lineage TO data_writer;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA quality TO data_writer;

GRANT ALL PRIVILEGES ON SCHEMA lineage, quality TO quality_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA lineage TO quality_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA quality TO quality_admin;

-- ── Useful views ──────────────────────────────────────────────────────────────

CREATE OR REPLACE VIEW quality.latest_check_summary AS
SELECT
    dataset_name,
    check_name,
    passed,
    severity,
    metric_value,
    rows_affected,
    run_timestamp
FROM quality.check_results
WHERE run_timestamp = (SELECT MAX(run_timestamp) FROM quality.check_results);

CREATE OR REPLACE VIEW quality.check_pass_rate AS
SELECT
    check_name,
    COUNT(*)                                        AS total_runs,
    SUM(CASE WHEN passed THEN 1 ELSE 0 END)         AS passed,
    ROUND(AVG(CASE WHEN passed THEN 1.0 ELSE 0 END) * 100, 2) AS pass_rate_pct
FROM quality.check_results
GROUP BY check_name
ORDER BY pass_rate_pct;
