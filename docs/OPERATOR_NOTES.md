# Operator Notes — Pre-Pilot Hygiene

Short list of one-time tasks the operator runs before handing the
container off to a pilot bank. Each step is idempotent and safe to
re-run.

## 1. Promote the encryption key to an environment variable

On first run, `secrets_vault.py` generates a `.kilter_secret_key` file
next to the app and prints a warning to stderr. That's a dev convenience
— in production, the key MUST live in the host's secret store and be
injected as `KILTER_SECRET_KEY` at deploy time. A key on disk inside
the image or volume increases the blast radius of a stolen volume
snapshot.

### Steps

```bash
# 1. Read the key off the dev host (where you first booted the app).
cat /path/to/Kilter/.kilter_secret_key
# Output is a single line ending with '='. Copy it.

# 2. Stash it in your secret store. Examples:
#    - Vault:   vault kv put secret/kilter KILTER_SECRET_KEY="<paste>"
#    - AWS SM:  aws secretsmanager create-secret --name kilter/secret-key \
#                  --secret-string "<paste>"
#    - 1Password CLI: op item create category="API Credential" \
#                  title="Kilter Encryption Key" credential="<paste>"
#    - Plain .env on the deploy host (acceptable for single-node
#      pilot, NOT for any multi-host environment):
#        echo "KILTER_SECRET_KEY=<paste>" >> /opt/kilter/.env
#        chmod 600 /opt/kilter/.env

# 3. Restart the container with the env var set. Verify in logs that
#    the "[secrets_vault] Generated a new encryption key" warning is
#    GONE — that line means the env var didn't make it through.

# 4. Once you're confident the env var is working, delete the on-disk
#    key file from the host.
shred -u /path/to/Kilter/.kilter_secret_key   # POSIX
# or simply: rm /path/to/Kilter/.kilter_secret_key
```

If you skip this and ship `.kilter_secret_key` to the pilot bank, the
DPA Annex A's "encryption keys held in OS environment / HSM, never in
source" claim is false. Don't ship without doing this.

## 2. Sweep stale backup files

The dev workflow occasionally drops `kilter.db.backup-*` files when
running mock-data regenerations. None of these belong in a pilot
container. Already-removed at the time of writing — but verify on
every host before shipping:

```bash
find /opt/kilter -name "kilter.db.backup-*" -print -delete
```

## 3. Confirm the seed admin's enrollment token has been consumed

If the bootstrap admin hasn't completed enrollment, their
`enrollment_token` is still live in the database. Anyone who reads the
token can complete enrollment as the admin.

```bash
docker compose exec kilter python -c "
import sqlite3
conn = sqlite3.connect('/data/kilter.db')
rows = conn.execute(
    'SELECT username, enrollment_token, totp_enrolled_at FROM users'
).fetchall()
for u, tok, enrolled in rows:
    print(f'  {u}: enrolled={bool(enrolled)} pending_token={bool(tok)}')
"
```

If anything other than the bootstrap admin shows `pending_token=True`
without a recent `enrolled` flip, regenerate the enrollment link
through the admin UI.

## 4. Idle timeout sanity check

`KILTER_SESSION_IDLE_MINUTES` defaults to 30 minutes. For higher-trust
environments (well-hardened laptops, no shared workstations) you can
extend to 60 or 90; for shared workstations or kiosks, drop to 10–15.
Set in `.env` and restart:

```bash
echo "KILTER_SESSION_IDLE_MINUTES=15" >> /opt/kilter/.env
docker compose up -d
```

## 5. Tail-test the audit log

After a pilot dry-run, confirm the audit log captured what you'd want
post-incident:

```sql
-- last 50 events
SELECT timestamp, action, actor, json_extract(details, '$.reason') AS reason
FROM audit_log ORDER BY id DESC LIMIT 50;

-- failed logins (now categorised by reason: ldap:* | totp)
SELECT timestamp, actor, json_extract(details, '$.reason') AS reason
FROM audit_log WHERE action='login_failed'
ORDER BY id DESC LIMIT 20;
```

If `login_failed` doesn't appear after a deliberately bad attempt,
something is wired wrong — fix before shipping.

## 6. Verify the cards module is ready (if you're enabling cards)

Cards stream tables are created at `init_db()` time. Confirm they
exist + the seeded card-switch profiles are present:

```sql
-- Tables exist
SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'card_%';
-- expect: card_settlement_files, card_settlement_records

-- Seeded profiles
SELECT name FROM csv_format_profiles
WHERE name LIKE 'Card switch%' OR name LIKE 'MTN MoMo%'
   OR name LIKE 'M-Pesa%'      OR name LIKE 'Telcel%'
   OR name LIKE 'Airtel%';
```

If any are missing, run `init_db()` from a Python shell against the
DB — it's idempotent and only adds missing rows.

Before the first cards ingest, set the `currency` column on the
`Card switch acquirer settlement` and `Card switch issuer settlement`
profiles (they default to NULL because the source files use ISO
numeric currency codes like `936` rather than alpha codes; binding
the profile to a single-currency wallet or hard-setting the currency
prevents per-row bucket failures).

## 7. Mobile-money operator profile sanity check

Each pre-seeded operator profile is wired to a different sign
convention:

| Profile | Sign convention | Notes |
|---|---|---|
| M-Pesa Safaricom | `paid_in_withdrawn` | Two-column amount shape; sign_column = "Withdrawn" |
| Telcel Cash organisation statement | `paid_in_withdrawn` | Same shape; xlsx native (zip-magic detected) |
| MTN MoMo agent statement | `cr_dr_column` | Type column carries CR/DR |
| MTN MoMo operator B2W | `positive_credit` | Signed amount; negative = bank debit |
| MTN MoMo operator W2B | `positive_credit` | Same; positive = funds in |
| Airtel Money agent statement | `cr_dr_column` | CR/DR column convention |

If you bind any profile to a wallet account, the wallet's currency
fills in for rows where the operator CSV has a blank Currency column —
that fallback chain prevents spurious "no currency, no bucket" failures
on partial extracts.
