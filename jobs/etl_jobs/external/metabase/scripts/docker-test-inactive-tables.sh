#!/usr/bin/env bash
# docker-test-inactive-tables.sh — Integration test proving that deleted tables
# appear as inactive via the Metabase metadata API (remove_inactive=false).
#
# Phases:
#   1. Verify preconditions (table exists and is active)
#   2. Drop the table from the sample database
#   3. Sync Metabase and wait for deletion to be reflected
#   4. Assert the table is now inactive
#   5. Cleanup — restore the table for other tests
#
# Prerequisites: docker-setup.sh must have been run first.
# Usage: bash scripts/docker-test-inactive-tables.sh

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
METABASE_URL="http://localhost:3000"
ADMIN_EMAIL="admin@test.com"
ADMIN_PASSWORD="Testpass1"
COMPOSE_FILE="docker/docker-compose.yml"

MAX_WAIT_SYNC=60   # seconds to wait for sync (longer than docker-setup's 30s)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
info()  { echo "==> $*"; }
ok()    { echo "  ✓ $*"; }
fail()  { echo "  ✗ $*" >&2; exit 1; }

# Parse JSON with Python (guaranteed available in this project)
json_get() {
    python3 -c "import sys, json; data=json.load(sys.stdin); print($1)"
}

# ---------------------------------------------------------------------------
# Phase 1: Verify preconditions
# ---------------------------------------------------------------------------
info "Phase 1: Verifying preconditions..."

# Login
TOKEN=$(curl -sf "${METABASE_URL}/api/session" \
    -H "Content-Type: application/json" \
    -d "{
        \"username\": \"${ADMIN_EMAIL}\",
        \"password\": \"${ADMIN_PASSWORD}\"
    }" | json_get "data['id']")

if [ -z "$TOKEN" ] || [ "$TOKEN" = "None" ]; then
    fail "Could not obtain session token — is Metabase running?"
fi

AUTH_HEADER="X-Metabase-Session: ${TOKEN}"

# Get Sample DB ID
DB_ID=$(curl -sf "${METABASE_URL}/api/database" \
    -H "$AUTH_HEADER" \
    | json_get "[d['id'] for d in data.get('data', data) if d['name'] == 'Sample DB'][0]")

if [ -z "$DB_ID" ] || [ "$DB_ID" = "None" ]; then
    fail "Could not find Sample DB — has docker-setup.sh been run?"
fi

# Get table ID from /api/table (use next() to safely handle empty list)
TABLE_ID=$(curl -sf "${METABASE_URL}/api/table" \
    -H "$AUTH_HEADER" \
    | json_get "next(iter([t['id'] for t in data if t.get('name') == 'old_user_stats' and t.get('db_id') == ${DB_ID}]), '')")

if [ -z "$TABLE_ID" ] || [ "$TABLE_ID" = "None" ]; then
    fail "Table old_user_stats not found in Metabase — run 'just docker-setup' first"
fi

# SQL check: table is active in Metabase app DB
ACTIVE_FLAG=$(docker compose -f "$COMPOSE_FILE" exec -T postgres \
    psql -U metabase -d metabase -t -A \
    -c "SELECT active FROM metabase_table WHERE name = 'old_user_stats' AND db_id = ${DB_ID};")

if [ "$ACTIVE_FLAG" != "t" ]; then
    fail "Table old_user_stats is not active in Metabase DB (expected 't', got '${ACTIVE_FLAG}')"
fi
ok "Table active in Metabase DB (SQL: active=$ACTIVE_FLAG)"

# API check: table is active via metadata endpoint
API_ACTIVE=$(curl -sf "${METABASE_URL}/api/database/${DB_ID}/metadata?skip_fields=true&remove_inactive=false" \
    -H "$AUTH_HEADER" \
    | json_get "[t['active'] for t in data.get('tables', []) if t['name'] == 'old_user_stats'][0]")

if [ "$API_ACTIVE" != "True" ]; then
    fail "Table old_user_stats is not active via API (expected 'True', got '${API_ACTIVE}')"
fi
ok "Table active via API (remove_inactive=false: active=$API_ACTIVE)"

# ---------------------------------------------------------------------------
# Phase 2: Drop the table
# ---------------------------------------------------------------------------
info "Phase 2: Dropping table from sample database..."

docker compose -f "$COMPOSE_FILE" exec -T sample-db \
    psql -U sample -d sample -c "DROP TABLE IF EXISTS analytics.old_user_stats;"

ok "Table analytics.old_user_stats dropped"

# ---------------------------------------------------------------------------
# Phase 3: Sync and wait for deletion to be reflected
# ---------------------------------------------------------------------------
info "Phase 3: Syncing Metabase and waiting for table to become inactive..."

curl -sf -X POST "${METABASE_URL}/api/database/${DB_ID}/sync_schema" \
    -H "$AUTH_HEADER" > /dev/null

elapsed=0
while [ $elapsed -lt $MAX_WAIT_SYNC ]; do
    FOUND=$(curl -sf "${METABASE_URL}/api/table" -H "$AUTH_HEADER" \
        | json_get "[t['id'] for t in data if t.get('name') == 'old_user_stats' and t.get('db_id') == ${DB_ID}]")

    if [ "$FOUND" = "[]" ]; then
        ok "Table disappeared from active table list (${elapsed}s)"
        break
    fi
    sleep 2
    elapsed=$((elapsed + 2))
done

if [ $elapsed -ge $MAX_WAIT_SYNC ]; then
    fail "Table old_user_stats did not become inactive within ${MAX_WAIT_SYNC}s"
fi

# ---------------------------------------------------------------------------
# Phase 4: Assert table is inactive
# ---------------------------------------------------------------------------
info "Phase 4: Asserting table is inactive..."

TEST_PASSED=true

# SQL check: table exists in Metabase app DB with active=false
ACTIVE_FLAG=$(docker compose -f "$COMPOSE_FILE" exec -T postgres \
    psql -U metabase -d metabase -t -A \
    -c "SELECT active FROM metabase_table WHERE name = 'old_user_stats' AND db_id = ${DB_ID};")

if [ "$ACTIVE_FLAG" != "f" ]; then
    echo "  ✗ Table old_user_stats is not inactive in Metabase DB (expected 'f', got '${ACTIVE_FLAG}')" >&2
    TEST_PASSED=false
else
    ok "Table found in Metabase app DB with active=false"
fi

# API check: table should also be returned by the metadata API with remove_inactive=false.
# The test FAILS if the table is in the app DB but missing from the API — that means
# remove_inactive=false does not actually include inactive tables.
API_TABLES=$(curl -sf "${METABASE_URL}/api/database/${DB_ID}/metadata?skip_fields=true&remove_inactive=false" \
    -H "$AUTH_HEADER" \
    | json_get "[t['name'] for t in data.get('tables', []) if t['name'] == 'old_user_stats']")

if [ "$API_TABLES" = "[]" ]; then
    echo "  ✗ Table found in Metabase app DB but NOT returned by metadata API with remove_inactive=false" >&2
    TEST_PASSED=false
else
    ok "Table returned by metadata API with remove_inactive=false"
fi

# ---------------------------------------------------------------------------
# Phase 5: Cleanup — restore the table
# ---------------------------------------------------------------------------
info "Phase 5: Restoring table for other tests..."

docker compose -f "$COMPOSE_FILE" exec -T sample-db \
    psql -U sample -d sample -q <<'SQL'
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE TABLE IF NOT EXISTS analytics.old_user_stats (
    user_id INTEGER PRIMARY KEY,
    booking_cnt INTEGER,
    total_amount DECIMAL(10,2),
    last_booking_date DATE,
    user_type VARCHAR(50)
);
INSERT INTO analytics.old_user_stats (user_id, booking_cnt, total_amount, last_booking_date, user_type)
VALUES
    (1, 5, 150.00, '2026-01-15', 'regular'),
    (2, 12, 480.50, '2026-02-20', 'power'),
    (3, 1, 25.00, '2026-03-01', 'new')
ON CONFLICT (user_id) DO NOTHING;
SQL

# Trigger sync
curl -sf -X POST "${METABASE_URL}/api/database/${DB_ID}/sync_schema" \
    -H "$AUTH_HEADER" > /dev/null

# Wait for table to reappear
elapsed=0
while [ $elapsed -lt $MAX_WAIT_SYNC ]; do
    FOUND=$(curl -sf "${METABASE_URL}/api/table" -H "$AUTH_HEADER" \
        | json_get "[t['id'] for t in data if t.get('name') == 'old_user_stats' and t.get('db_id') == ${DB_ID}]")

    if [ "$FOUND" != "[]" ]; then
        ok "Table reappeared in active table list (${elapsed}s)"
        break
    fi
    sleep 2
    elapsed=$((elapsed + 2))
done

if [ $elapsed -ge $MAX_WAIT_SYNC ]; then
    fail "Table old_user_stats did not reappear within ${MAX_WAIT_SYNC}s"
fi

# Verify restored: table is active again
API_ACTIVE=$(curl -sf "${METABASE_URL}/api/database/${DB_ID}/metadata?skip_fields=true&remove_inactive=false" \
    -H "$AUTH_HEADER" \
    | json_get "[t['active'] for t in data.get('tables', []) if t['name'] == 'old_user_stats'][0]")

if [ "$API_ACTIVE" != "True" ]; then
    fail "Table old_user_stats should be active again (expected 'True', got '${API_ACTIVE}')"
fi
ok "Table restored and active again"

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
if [ "$TEST_PASSED" = true ]; then
    echo "============================================================"
    echo " Inactive Tables Test: PASSED"
    echo "============================================================"
    echo " All assertions verified:"
    echo "   ✓ Table initially active in Metabase DB and API"
    echo "   ✓ After deletion + sync: table found in Metabase app DB with active=false"
    echo "   ✓ After deletion + sync: table returned by metadata API with remove_inactive=false"
    echo "   ✓ Table restored and active again"
    echo "============================================================"
else
    echo "============================================================"
    echo " Inactive Tables Test: FAILED"
    echo "============================================================"
    echo " Assertions:"
    echo "   ✓ Table initially active in Metabase DB and API"
    echo "   ✓ After deletion + sync: table found in Metabase app DB with active=false"
    echo "   ✗ After deletion + sync: table NOT returned by metadata API with remove_inactive=false"
    echo "   ✓ Table restored and active again"
    echo "============================================================"
    exit 1
fi
