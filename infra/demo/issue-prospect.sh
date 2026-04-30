#!/usr/bin/env bash
# issue-prospect.sh — generate a fresh demo-admin enrollment link and
# print an email body ready to copy-paste to a prospect.
#
# Run this just before sending the link to a prospect. It rotates the
# enrollment token (so an old link from a previous prospect can't claim
# the same account), and emits the URL + a sample email body that
# already has the right hostname filled in.
#
# Why a separate script (vs. running reset-demo.sh): reset wipes the
# whole dataset, which is a >5s operation and clobbers any in-flight
# prospect demo. Issue-prospect just rotates the admin token in place.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

COMPOSE_FILES=(-f docker-compose.yml -f infra/demo/docker-compose.demo.yml)

# Load env so we know the public hostname and basic-auth user.
if [ -f .env ]; then
    set -a; . ./.env; set +a
fi
HOSTNAME="${DEMO_HOSTNAME:-demo.kilter.example.com}"
BASIC_USER="${DEMO_BASIC_AUTH_USER:-demo}"
BASIC_PASS="${1:-}"   # caller passes the basic-auth password as $1; we
                       # don't read it from anywhere because the bcrypt
                       # hash is one-way and we can't recover the plaintext.

# Rotate the enrollment token. Failing to set ON CONFLICT is intentional:
# we want to ALSO null out totp_secret so the demo-admin's old TOTP code
# stops working — the prospect re-enrolls.
TOKEN_LINE=$(docker compose "${COMPOSE_FILES[@]}" exec -T kilter python -c "
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
print(tok)
" | tail -1 | tr -d '\r')

if [ -z "$TOKEN_LINE" ]; then
    echo "FAIL: could not mint a fresh enrollment token. Is the kilter container up?"
    docker compose "${COMPOSE_FILES[@]}" ps kilter
    exit 1
fi

ENROLL_URL="https://${HOSTNAME}/enroll?user=demo-admin&token=${TOKEN_LINE}"
DEMO_URL="https://${HOSTNAME}"

echo
echo "==== Prospect access ===================================="
echo "URL:           $DEMO_URL"
echo "Basic-auth:    $BASIC_USER  /  ${BASIC_PASS:-<the password you set in .env>}"
echo "Enrollment:    $ENROLL_URL"
echo
echo "==== Email body (copy/paste below) ======================"
cat <<EOF

Hi [name],

Here's the live demo of Kilter as promised:

  URL:      $DEMO_URL
  Username: $BASIC_USER
  Password: ${BASIC_PASS:-[password]}

When you reach the sign-in page, the demo-admin enrollment link is:

  $ENROLL_URL

Scan the QR with Microsoft Authenticator, log in, and you're in. The
demo resets at 03:00 UTC each night, so feel free to break things.

If you'd like a 30-minute walk-through live, reply with a window that
suits.

Cheers,
[your name]

EOF
echo "========================================================="
echo
echo "Note: this rotates demo-admin's TOTP. The prior prospect's"
echo "Authenticator entry will stop working — that's intentional."
