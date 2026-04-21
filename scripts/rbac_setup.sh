#!/usr/bin/env bash
# Create PostgreSQL users and assign RBAC roles

set -euo pipefail

DB_URL="${DATABASE_URL:-postgres://postgres:postgres@localhost/dq_platform}"

echo "Setting up RBAC..."

psql "$DB_URL" <<EOF
-- Create application users
DO \$\$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'pipeline_user') THEN
        CREATE USER pipeline_user WITH PASSWORD 'pipeline_pass_2026';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'analyst_user') THEN
        CREATE USER analyst_user WITH PASSWORD 'analyst_pass_2026';
    END IF;
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'admin_user') THEN
        CREATE USER admin_user WITH PASSWORD 'admin_pass_2026';
    END IF;
END
\$\$;

-- Assign roles
GRANT data_writer TO pipeline_user;
GRANT data_reader  TO analyst_user;
GRANT quality_admin TO admin_user;

SELECT rolname, rolcanlogin FROM pg_roles
WHERE rolname IN ('pipeline_user','analyst_user','admin_user');
EOF

echo "RBAC setup complete"
