#!/usr/bin/env bash
# htpasswd-add.sh — add or remove a per-prospect basic-auth credential.
#
# The default Caddyfile uses a single shared {DEMO_BASIC_AUTH_USER} +
# password. For tighter control (audit trail per prospect, easy
# revocation), switch to import_file mode in Caddyfile:
#
#     basic_auth { import_file /etc/caddy/htpasswd }
#
# Then use this script to manage entries:
#
#   ./infra/demo/htpasswd-add.sh add    acme-bank   'plaintext-pwd'
#   ./infra/demo/htpasswd-add.sh remove acme-bank
#   ./infra/demo/htpasswd-add.sh list
#
# After add/remove, the script reloads Caddy in-place so the change
# takes effect without dropping any in-flight TLS connection.

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

# The htpasswd file lives on a host-side bind mount that the caddy
# container reads. We keep it next to the Caddyfile so a single
# `git pull && docker compose up -d` carries the change.
HTPASSWD_FILE="$ROOT/infra/demo/htpasswd"
COMPOSE_FILES=(-f docker-compose.yml -f infra/demo/docker-compose.demo.yml)

cmd="${1:-}"
case "$cmd" in
    add)
        user="${2:-}"; pass="${3:-}"
        if [ -z "$user" ] || [ -z "$pass" ]; then
            echo "usage: $0 add <user> <plaintext-password>"
            exit 1
        fi
        # Caddy ships its own bcrypt hasher in the same image we deploy.
        # Pinning the version keeps reproducibility.
        hash=$(docker run --rm caddy:2.8-alpine \
                caddy hash-password --plaintext "$pass" | tr -d '\r')
        if [ -z "$hash" ]; then
            echo "FAIL: could not generate hash"
            exit 1
        fi
        # Strip any prior line for this user, then append the new hash.
        if [ -f "$HTPASSWD_FILE" ]; then
            # Use a temp file so an Ctrl-C mid-write doesn't leave a half-
            # truncated htpasswd that locks every prospect out.
            tmp=$(mktemp)
            grep -v "^${user} " "$HTPASSWD_FILE" > "$tmp" || true
            mv "$tmp" "$HTPASSWD_FILE"
        fi
        printf '%s %s\n' "$user" "$hash" >> "$HTPASSWD_FILE"
        chmod 600 "$HTPASSWD_FILE"
        echo "added: $user"
        ;;
    remove)
        user="${2:-}"
        if [ -z "$user" ]; then
            echo "usage: $0 remove <user>"
            exit 1
        fi
        if [ ! -f "$HTPASSWD_FILE" ]; then
            echo "no htpasswd file yet — nothing to remove"
            exit 0
        fi
        if ! grep -q "^${user} " "$HTPASSWD_FILE"; then
            echo "no entry for: $user"
            exit 0
        fi
        tmp=$(mktemp)
        grep -v "^${user} " "$HTPASSWD_FILE" > "$tmp" || true
        mv "$tmp" "$HTPASSWD_FILE"
        chmod 600 "$HTPASSWD_FILE"
        echo "removed: $user"
        ;;
    list)
        if [ ! -f "$HTPASSWD_FILE" ]; then
            echo "(no htpasswd file)"
            exit 0
        fi
        awk '{print $1}' "$HTPASSWD_FILE"
        exit 0
        ;;
    *)
        cat <<EOF
usage:
  $0 add    <user> <plaintext-password>   add or rotate a credential
  $0 remove <user>                        revoke
  $0 list                                 show all current users

Per-prospect basic-auth — see HOSTED_DEMO.md §4.
EOF
        exit 1
        ;;
esac

# Reload Caddy in-place. This re-reads the htpasswd file without
# dropping the listening socket — TLS handshakes mid-flight are safe.
if docker compose "${COMPOSE_FILES[@]}" ps caddy 2>/dev/null | grep -q "running"; then
    docker compose "${COMPOSE_FILES[@]}" exec -T caddy \
        caddy reload --config /etc/caddy/Caddyfile >/dev/null
    echo "caddy reloaded"
else
    echo "(caddy not running; change will apply on next start)"
fi
