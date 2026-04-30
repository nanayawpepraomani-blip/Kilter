#!/usr/bin/env bash
# check-deploy.sh — pre-flight checks for the hosted demo.
#
# Run BEFORE the first `docker compose up`. Catches the four mistakes
# that account for ~all "first deploy didn't come up cleanly" tickets:
#
#   1. DNS hasn't propagated — Caddy will fail Let's Encrypt and enter
#      rate-limit backoff; you wait an hour.
#   2. .env missing required values, or KILTER_SECRET_KEY left blank.
#   3. DEMO_BASIC_AUTH_HASH not in bcrypt format.
#   4. Ports 80/443 already bound by another process.
#
# All non-fatal failures print a clear "FIX:" line. If everything
# passes, exits 0 and prints "ready to deploy".

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

ERR=0
err()  { echo "  FAIL: $*"; ERR=$((ERR+1)); }
ok()   { echo "  ok:   $*"; }
note() { echo "        $*"; }

echo "==> .env present + readable"
if [ ! -f .env ]; then
    err ".env missing"
    note "FIX: cp infra/demo/.env.demo.example .env  &&  chmod 600 .env"
else
    ok ".env exists"
fi

echo "==> Required env vars set"
# Source .env in a subshell so we don't pollute the user's shell.
need_vars=(KILTER_SECRET_KEY DEMO_HOSTNAME ACME_EMAIL DEMO_BASIC_AUTH_USER DEMO_BASIC_AUTH_HASH)
if [ -f .env ]; then
    set -a; . ./.env; set +a
    for v in "${need_vars[@]}"; do
        val="${!v:-}"
        if [ -z "$val" ]; then
            err "$v is unset or empty"
        elif [ "$v" = "DEMO_BASIC_AUTH_HASH" ] && [[ "$val" == *"REPLACE_ME"* ]]; then
            err "$v still has the placeholder value"
            note "FIX: docker run --rm caddy:2.8-alpine caddy hash-password --plaintext 'YOUR-PASSWORD'"
        elif [ "$v" = "DEMO_BASIC_AUTH_HASH" ] && ! [[ "$val" =~ ^\$2[ayb]?\$[0-9]{2}\$.{53}$ ]]; then
            err "$v doesn't look like a bcrypt hash (expected \$2a\$14\$… 60 chars)"
        else
            ok "$v set"
        fi
    done
fi

echo "==> DNS resolves to this host"
if [ -n "${DEMO_HOSTNAME:-}" ]; then
    resolved=$(dig +short "$DEMO_HOSTNAME" 2>/dev/null | tail -1 || true)
    if [ -z "$resolved" ]; then
        err "$DEMO_HOSTNAME has no A record"
        note "FIX: add an A record pointing to this host's public IP, wait 5 min for propagation"
    else
        # Compare against this host's external IP. Multiple ways to discover
        # it; ifconfig.me is the most universal (no auth, plain text).
        my_ip=$(curl -fsS --max-time 5 https://ifconfig.me 2>/dev/null || echo "")
        if [ -z "$my_ip" ]; then
            note "(could not determine this host's public IP — skipping IP-match check)"
            ok "$DEMO_HOSTNAME resolves to $resolved"
        elif [ "$resolved" = "$my_ip" ]; then
            ok "$DEMO_HOSTNAME resolves to $resolved (matches this host)"
        else
            err "$DEMO_HOSTNAME resolves to $resolved but this host is $my_ip"
            note "FIX: update the A record, or run check-deploy.sh from the host the DNS points at"
        fi
    fi
fi

echo "==> Ports 80/443 free"
for port in 80 443; do
    # ss is on every modern Ubuntu; netstat fallback just in case.
    if ss -tln 2>/dev/null | awk '{print $4}' | grep -qE ":${port}$"; then
        err "port $port already bound"
        note "FIX: stop whatever's on it (often Apache/Nginx) — 'systemctl stop apache2'"
    elif netstat -tln 2>/dev/null | awk '{print $4}' | grep -qE ":${port}$"; then
        err "port $port already bound"
    else
        ok "port $port free"
    fi
done

echo "==> Docker daemon reachable"
if docker info >/dev/null 2>&1; then
    ok "docker info works"
else
    err "docker daemon not reachable from this user"
    note "FIX: ensure docker is running, or add this user to the 'docker' group"
fi

echo "==> Compose v2 available"
if docker compose version >/dev/null 2>&1; then
    ok "$(docker compose version --short 2>/dev/null) installed"
else
    err "docker compose v2 not installed"
    note "FIX: apt install docker-compose-plugin"
fi

echo
if [ "$ERR" -eq 0 ]; then
    echo "ready to deploy. run:"
    echo "  docker compose -f docker-compose.yml -f infra/demo/docker-compose.demo.yml up -d --build"
else
    echo "$ERR check(s) failed — fix the FAIL lines above, re-run check-deploy.sh"
    exit 1
fi
