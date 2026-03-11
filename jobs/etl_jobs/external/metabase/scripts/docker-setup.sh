#!/usr/bin/env bash
# docker-setup.sh — Idempotent bootstrap for local Metabase development environment.
#
# Takes running Docker containers to a fully seeded Metabase instance in 6 phases:
#   1. Wait for Metabase to be ready
#   2. Admin account setup (or login if already set up)
#   3. Connect sample-db as a Metabase database
#   4. Create test tables in sample-db
#   5. Sync Metabase and wait for tables to appear
#   6. Create test cards (native SQL + query builder)
#
# Usage: bash scripts/docker-setup.sh

set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
METABASE_URL="http://localhost:3000"
ADMIN_EMAIL="admin@test.com"
ADMIN_PASSWORD="Testpass1"
ADMIN_FIRST_NAME="Admin"
ADMIN_LAST_NAME="User"
COMPOSE_FILE="docker/docker-compose.yml"

MAX_WAIT_METABASE=90   # seconds to wait for Metabase readiness
MAX_WAIT_SYNC=30       # seconds to wait for table sync

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
info()  { echo "==> $*"; }
ok()    { echo "  ✓ $*"; }
skip()  { echo "  • $* (already exists)"; }
fail()  { echo "  ✗ $*" >&2; exit 1; }

# Parse JSON with Python (guaranteed available in this project)
json_get() {
    python3 -c "import sys, json; data=json.load(sys.stdin); print($1)"
}

# ---------------------------------------------------------------------------
# Phase 1: Wait for Metabase
# ---------------------------------------------------------------------------
info "Phase 1: Waiting for Metabase to be ready..."

elapsed=0
while [ $elapsed -lt $MAX_WAIT_METABASE ]; do
    if curl -sf "${METABASE_URL}/api/session/properties" > /dev/null 2>&1; then
        ok "Metabase is ready (${elapsed}s)"
        break
    fi
    sleep 2
    elapsed=$((elapsed + 2))
done

if [ $elapsed -ge $MAX_WAIT_METABASE ]; then
    fail "Metabase did not become ready within ${MAX_WAIT_METABASE}s"
fi

# ---------------------------------------------------------------------------
# Phase 2: Admin setup (idempotent)
# ---------------------------------------------------------------------------
info "Phase 2: Setting up admin account..."

SETUP_TOKEN=$(curl -sf "${METABASE_URL}/api/session/properties" \
    | json_get "data.get('setup-token', '')")

if [ -n "$SETUP_TOKEN" ] && [ "$SETUP_TOKEN" != "None" ]; then
    # First-time setup
    RESPONSE=$(curl -sf "${METABASE_URL}/api/setup" \
        -H "Content-Type: application/json" \
        -d "{
            \"token\": \"${SETUP_TOKEN}\",
            \"user\": {
                \"email\": \"${ADMIN_EMAIL}\",
                \"password\": \"${ADMIN_PASSWORD}\",
                \"first_name\": \"${ADMIN_FIRST_NAME}\",
                \"last_name\": \"${ADMIN_LAST_NAME}\",
                \"site_name\": \"Metabase Dev\"
            },
            \"prefs\": {
                \"site_name\": \"Metabase Dev\",
                \"site_locale\": \"en\",
                \"allow_tracking\": false
            }
        }")
    TOKEN=$(echo "$RESPONSE" | json_get "data.get('id', '')")
    ok "Admin account created (${ADMIN_EMAIL})"
else
    # Already set up — log in
    RESPONSE=$(curl -sf "${METABASE_URL}/api/session" \
        -H "Content-Type: application/json" \
        -d "{
            \"username\": \"${ADMIN_EMAIL}\",
            \"password\": \"${ADMIN_PASSWORD}\"
        }")
    TOKEN=$(echo "$RESPONSE" | json_get "data['id']")
    skip "Admin account"
fi

if [ -z "$TOKEN" ] || [ "$TOKEN" = "None" ]; then
    fail "Could not obtain session token"
fi

AUTH_HEADER="X-Metabase-Session: ${TOKEN}"

# ---------------------------------------------------------------------------
# Phase 3: Connect sample-db (idempotent)
# ---------------------------------------------------------------------------
info "Phase 3: Connecting sample database..."

EXISTING_DB=$(curl -sf "${METABASE_URL}/api/database" \
    -H "$AUTH_HEADER" \
    | json_get "[d['id'] for d in data.get('data', data) if d['name'] == 'Sample DB']")

if [ "$EXISTING_DB" = "[]" ]; then
    curl -sf "${METABASE_URL}/api/database" \
        -H "$AUTH_HEADER" \
        -H "Content-Type: application/json" \
        -d '{
            "engine": "postgres",
            "name": "Sample DB",
            "details": {
                "host": "sample-db",
                "port": 5432,
                "dbname": "sample",
                "user": "sample",
                "password": "sample"
            }
        }' > /dev/null
    ok "Sample DB connected"
else
    skip "Sample DB connection"
fi

# Get the database ID for later use
DB_ID=$(curl -sf "${METABASE_URL}/api/database" \
    -H "$AUTH_HEADER" \
    | json_get "[d['id'] for d in data.get('data', data) if d['name'] == 'Sample DB'][0]")

if [ -z "$DB_ID" ] || [ "$DB_ID" = "None" ]; then
    fail "Could not find Sample DB ID"
fi

# ---------------------------------------------------------------------------
# Phase 4: Create test tables (idempotent via IF NOT EXISTS / ON CONFLICT)
# ---------------------------------------------------------------------------
info "Phase 4: Creating test tables..."

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

ok "Test tables and data created"

# ---------------------------------------------------------------------------
# Phase 5: Sync Metabase and wait for tables to appear
# ---------------------------------------------------------------------------
info "Phase 5: Triggering Metabase sync..."

curl -sf -X POST "${METABASE_URL}/api/database/${DB_ID}/sync_schema" \
    -H "$AUTH_HEADER" > /dev/null

# Wait for old_user_stats to appear in Metabase table list
elapsed=0
TABLE_ID=""
while [ $elapsed -lt $MAX_WAIT_SYNC ]; do
    TABLE_ID=$(curl -sf "${METABASE_URL}/api/table" \
        -H "$AUTH_HEADER" \
        | json_get "str([t['id'] for t in data if t.get('name') == 'old_user_stats' and t.get('db_id') == ${DB_ID}][0])" 2>/dev/null || echo "")

    if [ -n "$TABLE_ID" ] && [ "$TABLE_ID" != "" ] && [ "$TABLE_ID" != "None" ]; then
        break
    fi
    sleep 2
    elapsed=$((elapsed + 2))
done

if [ -z "$TABLE_ID" ] || [ "$TABLE_ID" = "None" ]; then
    fail "Table old_user_stats did not appear in Metabase within ${MAX_WAIT_SYNC}s"
fi

ok "Sync complete — old_user_stats is table ID ${TABLE_ID}"

# ---------------------------------------------------------------------------
# Phase 6: Create test cards (idempotent — check by name)
# ---------------------------------------------------------------------------
info "Phase 6: Creating test cards..."

EXISTING_CARDS=$(curl -sf "${METABASE_URL}/api/card" \
    -H "$AUTH_HEADER" \
    | json_get "[c['name'] for c in data]")

# --- Native SQL card ---
if echo "$EXISTING_CARDS" | grep -q "Test Native Card"; then
    skip "Test Native Card"
else
    NATIVE_CARD=$(curl -sf "${METABASE_URL}/api/card" \
        -H "$AUTH_HEADER" \
        -H "Content-Type: application/json" \
        -d "{
            \"name\": \"Test Native Card\",
            \"display\": \"table\",
            \"visualization_settings\": {},
            \"dataset_query\": {
                \"type\": \"native\",
                \"native\": {
                    \"query\": \"SELECT user_id, booking_cnt, total_amount FROM analytics.old_user_stats WHERE user_type = {{user_type}}\",
                    \"template-tags\": {
                        \"user_type\": {
                            \"id\": \"b0a3d8a0-1234-5678-9abc-def012345678\",
                            \"type\": \"text\",
                            \"name\": \"user_type\",
                            \"display-name\": \"User Type\"
                        }
                    }
                },
                \"database\": ${DB_ID}
            }
        }")
    NATIVE_CARD_ID=$(echo "$NATIVE_CARD" | json_get "data['id']")
    ok "Test Native Card created (ID: ${NATIVE_CARD_ID})"
fi

# --- Query Builder card ---
if echo "$EXISTING_CARDS" | grep -q "Test Query Builder Card"; then
    skip "Test Query Builder Card"
else
    QB_CARD=$(curl -sf "${METABASE_URL}/api/card" \
        -H "$AUTH_HEADER" \
        -H "Content-Type: application/json" \
        -d "{
            \"name\": \"Test Query Builder Card\",
            \"display\": \"table\",
            \"visualization_settings\": {},
            \"dataset_query\": {
                \"type\": \"query\",
                \"query\": {
                    \"source-table\": ${TABLE_ID}
                },
                \"database\": ${DB_ID}
            }
        }")
    QB_CARD_ID=$(echo "$QB_CARD" | json_get "data['id']")
    ok "Test Query Builder Card created (ID: ${QB_CARD_ID})"
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
# Fetch final card IDs for summary
ALL_CARD_IDS=$(curl -sf "${METABASE_URL}/api/card" \
    -H "$AUTH_HEADER" \
    | json_get "','.join(str(c['id']) for c in data)")

echo ""
echo "============================================================"
echo " Metabase Local Environment Ready!"
echo "============================================================"
echo ""
echo " URL:        ${METABASE_URL}"
echo " Login:      ${ADMIN_EMAIL} / ${ADMIN_PASSWORD}"
echo " Database:   Sample DB (ID: ${DB_ID})"
echo " Table:      analytics.old_user_stats (ID: ${TABLE_ID})"
echo " Cards:      ${ALL_CARD_IDS}"
echo ""
echo " Next steps:"
echo "   just docker-prepare-migration  # rename table/column"
echo "   just docker-test-migrate       # run migration dry-run"
echo "   just docker-open               # open Metabase in browser"
echo "============================================================"
