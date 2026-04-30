#!/usr/bin/env bash
# reset-demo.sh — wipe and reseed the demo so each business day starts
# from a known clean state. Run via cron at 03:00 UTC (or whatever
# off-hours window suits your audience).
#
# Behaviour:
#   1. Stop kilter + remove its named volumes (clean slate).
#   2. Restart with the demo overlay applied so Caddy stays connected.
#   3. Wait for /healthz, then seed bootstrap demo-admin + mock dataset.
#
# Idempotent. Safe to run by hand for a manual reset before a high-stakes
# demo session.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

# Both compose files MUST be referenced or `docker compose` will only
# operate on the base stack and Caddy's network attachment will be torn
# down on the next `down -v`. Keep this list in lockstep with the deploy
# command in HOSTED_DEMO.md §2.4.
COMPOSE_FILES=(-f docker-compose.yml -f infra/demo/docker-compose.demo.yml)

echo "[reset-demo] $(date -u +%FT%TZ) starting"

# Tear down the kilter service (volumes included) but leave caddy running
# so the TLS cert isn't lost. `down -v` on the full stack would also wipe
# Caddy's /data volume which holds the LE cert + ACME account key — that
# would re-issue on every reset and hit the LE rate limit fast.
docker compose "${COMPOSE_FILES[@]}" stop kilter || true
docker compose "${COMPOSE_FILES[@]}" rm -f kilter || true

# Remove only the kilter-owned volumes. Compose names them
# `<project>_<volume>` where <project> is COMPOSE_PROJECT_NAME or the
# basename of $ROOT.
PROJECT="${COMPOSE_PROJECT_NAME:-$(basename "$ROOT")}"
for v in kilter-data kilter-uploads kilter-exports kilter-messages; do
    docker volume rm "${PROJECT}_${v}" >/dev/null 2>&1 || true
done

# Recreate the kilter container (deps will reattach to caddy's network).
docker compose "${COMPOSE_FILES[@]}" up -d --no-deps kilter

# Wait for /healthz. The container starts in <5s on a small VM so 30s
# is plenty; bail out with a clear message if something's wrong.
for i in $(seq 1 30); do
    if docker compose "${COMPOSE_FILES[@]}" exec -T kilter \
            curl -fsS -o /dev/null http://127.0.0.1:8000/healthz 2>/dev/null; then
        break
    fi
    if [ "$i" -eq 30 ]; then
        echo "[reset-demo] FAIL: /healthz never came up after 30s"
        docker compose "${COMPOSE_FILES[@]}" logs --tail 50 kilter
        exit 1
    fi
    sleep 1
done

# Seed the demo-admin user and capture a fresh enrollment URL. We re-mint
# the token every reset so a leaked enrollment link from yesterday can't
# claim today's account.
docker compose "${COMPOSE_FILES[@]}" exec -T kilter python -c "
import sqlite3, secrets
from datetime import datetime
conn = sqlite3.connect('/data/kilter.db')
tok = secrets.token_urlsafe(16)
conn.execute(
    \"INSERT INTO users (username, display_name, role, active, created_at, \"
    \"created_by, enrollment_token) VALUES \"
    \"('demo-admin', 'Demo Admin', 'admin', 1, ?, 'system', ?) \"
    \"ON CONFLICT(username) DO UPDATE SET enrollment_token=excluded.enrollment_token, \"
    \"totp_secret=NULL, totp_enrolled_at=NULL\",
    (datetime.utcnow().isoformat(), tok))
conn.commit()
print(f'[reset-demo] enrollment URL: /enroll?user=demo-admin&token={tok}')
"

# Generate the mock 10-account / 10-business-day dataset.
docker compose "${COMPOSE_FILES[@]}" exec -T kilter python scripts/_generate_mock_data.py

echo "[reset-demo] $(date -u +%FT%TZ) done"
