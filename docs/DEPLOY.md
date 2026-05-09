# Deploying Kilter

Single-node container deployment for pilot banks. Designed to run inside
the bank's own infrastructure — on a VM, a Docker host, or a Kubernetes
cluster. Kilter never phones home.

## Prerequisites

| | Minimum | Recommended |
|---|---|---|
| OS | Linux x86_64 with Docker 24+ | Same |
| RAM | 2 GB | 4 GB |
| Disk | 20 GB free for `/var/lib/docker/volumes` | 50 GB+ |
| CPU | 2 vCPUs | 4 vCPUs |
| Network | Inbound HTTPS from authorised users | Same, behind reverse proxy |

Docker Compose v2 (`docker compose`, not `docker-compose`).

## 1. First-run setup

### 1.1 Generate the encryption key

The encryption key protects TOTP secrets and SMTP credentials at rest.
Generate it once and store it in your secret manager (Vault / AWS Secrets
Manager / 1Password / `pass`). **Never commit this value to a repo.**

```bash
docker run --rm python:3.12-slim sh -c \
  "pip install -q cryptography==47.0.0 && python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'"
```

Output looks like:

```
PpWvB1fX...44 chars total...lYg=
```

Save it. If you lose it, every TOTP secret in the database becomes
unrecoverable garbage and every user has to re-enrol.

### 1.2 Configure `.env`

Copy the template and fill in the key:

```bash
cp .env.example .env
$EDITOR .env
```

`.env`:

```
KILTER_SECRET_KEY=PpWvB1fX...your-44-char-key...lYg=
TZ=Africa/Accra
```

### 1.3 Install the license file

Kilter requires a `kilter.lic` license file issued by Timeless Nypo Tech.
Place it in the root of the project directory (same folder as `app.py`):

```
kilter.lic          ← place here
app.py
...
```

The license file is a JSON document. It encodes the licensed organisation,
the expiry date, and the deployment hostname. Example:

```json
{
  "product": "Kilter",
  "licensee": "Example Bank",
  "issued": "2026-05-09",
  "expires": "2027-05-09",
  "hostname": "kilter-prod-01",
  "sig": "<HMAC-SHA256 signature — do not edit>"
}
```

> **Important:** the signature binds the license to the exact hostname
> string and expiry date. Editing any field invalidates it. If you need a
> new hostname or renewal, contact **timelessnypotech@outlook.com**.

**Grace period.** If no license file is present, Kilter enters a 14-day
grace period and logs a warning on startup. After 14 days the application
will refuse to start. Set `KILTER_DEV=1` in `.env` to bypass all checks
during internal development (never set this in production).

### 1.4 Start the stack

```bash
docker compose up -d --build
docker compose logs -f
```

First start takes 60-90 seconds: image build + DB schema creation +
scheduler spin-up. You'll see something like:

```
[scheduler] started: 6 jobs scheduled
INFO:     Uvicorn running on http://0.0.0.0:8000
```

### 1.5 Bootstrap admin enrolment

On first start the database is empty. The seeder creates a single admin
user named `admin` with a one-time enrolment token printed in the logs.
Look for:

```
[seed] Bootstrap admin enrolment: http://localhost:8000/enroll/<token>
```

Open that URL in a browser, scan the QR with Microsoft Authenticator,
log in. The token becomes invalid after the first successful enrolment.

If you missed the log line:

```bash
docker compose exec kilter \
  python -c "import sqlite3; print(sqlite3.connect('/data/kilter.db').execute('SELECT username, enrollment_token FROM users WHERE enrollment_token IS NOT NULL').fetchall())"
```

## 2. Reverse proxy and TLS

Kilter listens on port 8000 inside the container, exposed to `127.0.0.1`
on the host. Front it with a reverse proxy that terminates TLS and
forwards `X-Forwarded-For` / `X-Forwarded-Proto`.

### Caddy (easiest)

```caddy
kilter.bank.local {
    reverse_proxy 127.0.0.1:8000
    encode zstd gzip
}
```

### nginx

```nginx
server {
    listen 443 ssl http2;
    server_name kilter.bank.local;

    ssl_certificate     /etc/ssl/certs/kilter.crt;
    ssl_certificate_key /etc/ssl/private/kilter.key;

    client_max_body_size 50m;          # match Kilter's internal cap

    location / {
        proxy_pass         http://127.0.0.1:8000;
        proxy_set_header   Host              $host;
        proxy_set_header   X-Real-IP         $remote_addr;
        proxy_set_header   X-Forwarded-For   $proxy_add_x_forwarded_for;
        proxy_set_header   X-Forwarded-Proto $scheme;
        proxy_read_timeout 120s;
    }
}
```

After the first 24 hours of stable HTTPS, browsers honour Kilter's HSTS
header (preload-eligible) and will refuse plain HTTP — exactly what you
want.

## 2.1 Reconciliation streams — what files go where

Kilter runs three reconciliation streams through the same operator UI
+ engine. Each has a well-defined intake path:

| Stream | Intake | Notes |
|---|---|---|
| Nostro / GL | Drop SWIFT MT940/950 `.out` into `messages/swift/`, Flexcube xlsx into `messages/flexcube/`. Or upload manually at `/intake/upload`. | Watched-folder scanner picks up + pairs by registered cash account. |
| Mobile money | Same `messages/flexcube/` folder using a wallet-bound BYO CSV profile (M-Pesa, MTN MoMo agent / B2W / W2B, Airtel, Telcel Cash all pre-seeded). Or `/intake/upload` with the profile selected. | Profiles seeded by `db._seed_mobile_money_profiles` on first run; bind to a wallet account at `/byo-formats`. |
| Cards | `POST /cards/files` (or the upload modal at `/cards`). Switch settlement files arrive as TSV with pre-masked PAN. | Visa Base II / Mastercard IPM binary parsers stubbed pending sample data — see [docs/CARDS_DESIGN.md](CARDS_DESIGN.md). |

The 300 MB request-body cap with chunked-write streaming is sized for
real-world card-switch settlement files (250+ MB common at issuer
scale). SWIFT statements + mobile-money exports stay well under 5 MB
each but ride the same cap.

## 3. Routine operations

### Inspecting logs

```bash
docker compose logs -f kilter           # live tail
docker compose logs --tail 1000 kilter  # last 1000 lines
```

Logs rotate at 10 MB × 5 files (compose-managed). For longer retention
ship them to your SIEM via the host's log forwarder.

### Backing up

The only state worth backing up is the volume `kilter-data` (the SQLite file).

**Built-in backup job (recommended).** Kilter ships a `db_backup` scheduler
job pre-enabled at 02:00 UTC. It uses SQLite's online backup API — safe while
the DB is live under WAL mode. Backups go to `./backups/` inside the container
by default. To redirect to a host-mounted volume set the env var:

```
KILTER_BACKUP_DIR=/backups
```

and mount the path in `docker-compose.yml`:

```yaml
volumes:
  - /host/path/backups:/backups
```

The job keeps 7 daily snapshots by default; edit `keep_days` in the job's
params JSON via Scheduler Admin to change retention.

**Manual / external backup (alternative).** If you prefer host-level snapshots:

```bash
# SQLite WAL makes a hot copy safe, but a brief pause guarantees consistency.
docker compose pause kilter
docker run --rm \
  -v kilter_kilter-data:/data:ro \
  -v $(pwd):/backup \
  alpine sh -c "cp /data/kilter.db /backup/kilter-$(date +%Y%m%d-%H%M).db"
docker compose unpause kilter
```

Either way, encrypt the `.db` file at rest and ship it offsite — it contains
encrypted TOTP secrets, so treat it like a credential store.

### Restoring

```bash
docker compose stop kilter
docker run --rm \
  -v kilter_kilter-data:/data \
  -v $(pwd):/backup \
  alpine sh -c "cp /backup/kilter-YYYYMMDD-HHMM.db /data/kilter.db"
docker compose start kilter
```

### Upgrading to a new release

```bash
git pull
docker compose build --pull
docker compose up -d
docker compose logs -f
```

The DB schema is idempotent (`init_db` runs at startup and applies any
new migrations in-place). No manual migration step.

### Resetting a user's MFA

Admin in the UI: **Users → [name] → Reset MFA**. This clears the TOTP
secret and issues a fresh one-time enrolment token; the user gets the
new QR-code link to scan.

If the admin themselves is locked out:

```bash
docker compose exec kilter \
  python -c "
import sqlite3, secrets
conn = sqlite3.connect('/data/kilter.db')
tok = secrets.token_urlsafe(16)
conn.execute('UPDATE users SET totp_secret=NULL, enrollment_token=? WHERE username=?', (tok, 'admin'))
conn.commit()
print(f'Re-enrol at: http://your-host/enroll/{tok}')
"
```

### Rotating the encryption key

Sensitive but routine. Document a maintenance window. The procedure is:

1. Mint the new key with the same `Fernet.generate_key()` snippet.
2. Stop the container: `docker compose stop kilter`.
3. Run a re-encryption job that decrypts every secret column with the
   old key and re-encrypts with the new key. The hook is at
   `secrets_vault.rotate_key` (not yet wired to a CLI route — schedule
   that work before your first rotation).
4. Update `.env` with the new key.
5. `docker compose up -d`.

Until that CLI lands, key rotation requires a custom script. Do not
attempt to rotate without it — you will silently corrupt secrets.

### Monitoring

Healthcheck endpoint: `GET /healthz` — returns `{"status":"ok"}` with
HTTP 200 when uvicorn is up.

```bash
curl -fsS http://127.0.0.1:8000/healthz
```

Wire this to your monitoring tool (Prometheus blackbox, Datadog HTTP
check, simple cron + alert). The compose file already includes a Docker
healthcheck driving container restarts on hard failure.

## 4. Security posture

What the container does for you out of the box:

- Runs as non-root (UID 10001).
- Drops all Linux capabilities; `no-new-privileges` set.
- Read-only root filesystem; only mounted volumes and `/tmp` are
  writable.
- Logs capped at 10 MB × 5 (no disk-fill DoS via stdout spam).
- Internal 300 MB request-body cap (covers cards-side settlement files; SWIFT/Flex stay tiny).
- Security headers (HSTS, CSP, X-Frame-Options, X-Content-Type-Options,
  Referrer-Policy, Permissions-Policy) on every response.
- Rate limit on `/login` (10 attempts/minute per IP).

What you still need to do:

- Terminate TLS at your reverse proxy. Kilter speaks plain HTTP
  internally on purpose — the proxy owns certs.
- Restrict inbound traffic to the proxy only (firewall / security
  group). Port 8000 should never be exposed publicly.
- Keep `.env` readable only by the deploy user (`chmod 600`).
- Move `KILTER_SECRET_KEY` out of `.env` and into your secret manager
  with deploy-time injection once you have one.
- Snapshot the data volume daily and test a restore quarterly.

## 5. Common problems

**Container restarts in a loop with `Permission denied`**
The named volume picked up wrong ownership from a prior run. Fix:

```bash
docker compose down
docker volume rm kilter_kilter-data kilter_kilter-messages \
                 kilter_kilter-uploads kilter_kilter-exports
docker compose up -d --build
```

(This deletes data — only do it on a fresh install.)

**`InvalidToken` exceptions in the logs after rotating `.env`**
You changed `KILTER_SECRET_KEY` without re-encrypting. Restore the old
key and run rotation properly (Section 3, *Rotating the encryption key*).

**Web UI loads but `/login` is 429**
You're hitting the rate limiter. By design — wait 60 seconds or look
for an automation accidentally pounding `/login`.

**Healthcheck flips between `healthy` and `unhealthy`**
Usually the scheduler running a long job blocks the worker. Check
`docker compose logs --tail 200 kilter` for stack traces. If the
scheduler is the cause, raise the healthcheck `start_period` and
`timeout` and consider running uvicorn with `--workers 2`.

> ⚠️ **Single-worker constraint for TOTP.** Kilter's TOTP replay cache
> (which prevents a 6-digit code from being used twice within its 30-second
> window) is stored in-process. If you run `uvicorn --workers N` with N > 1,
> each worker has its own cache — a code used against worker 1 can be
> replayed against worker 2. **Run single-process (`--workers 1`) until a
> Redis-backed replay store is added.** For the vast majority of deployments
> (single host, bank-internal network) one worker handles the load comfortably.

## 5b. MySQL Deployment

Set `DATABASE_URL=mysql://kilter:password@localhost:3306/kilter` in `.env`.

Install the MySQL driver:
```bash
.venv/bin/pip install mysql-connector-python
```

Create the database:
```sql
CREATE DATABASE kilter CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER 'kilter'@'localhost' IDENTIFIED BY 'strong-password';
GRANT ALL PRIVILEGES ON kilter.* TO 'kilter'@'localhost';
```

The app runs the same schema DDL on first start. MySQL-specific notes:
- Triggers (audit_log immutability) are created automatically
- WAL mode and PRAGMA statements are ignored for MySQL
- All timestamps remain ISO-8601 strings (no TIMESTAMP columns)

See [docs/MYSQL.md](MYSQL.md) for the full MySQL guide including migration from SQLite, connection pooling, and backup.

---

## Environment Variables Reference

| Variable | Required | Default | Description |
|---|---|---|---|
| `KILTER_SECRET_KEY` | **Yes** | — | Fernet key for encrypting TOTP secrets and SMTP passwords |
| `KILTER_DB_PATH` | No | `kilter.db` | Path to the SQLite database file |
| `DATABASE_URL` | No | — | MySQL URI: `mysql://user:pass@host:3306/dbname` (overrides SQLite) |
| `KILTER_SESSION_IDLE_MINUTES` | No | `30` | Idle session timeout in minutes |
| `KILTER_REQUIRE_APPROVAL` | No | `false` | Enable two-person approval gate for match decisions |
| `KILTER_CARDS_REQUIRED_STAGES` | No | `auth,clearing,settlement` | Comma-separated card stages required for a "matched" result |
| `KILTER_LDAP_URL` | No | — | LDAP server URL: `ldaps://dc.yourbank.com:636` |
| `KILTER_LDAP_BASE_DN` | No | — | LDAP search base: `DC=yourbank,DC=com` |
| `KILTER_LDAP_CA_CERTS_FILE` | No | — | Path to CA bundle for LDAP TLS verification |

---

## 6. Next steps after pilot conversion

These are out of scope for the initial pilot but worth planning:

- **External secret manager.** Vault Agent / AWS Secrets Manager
  injection rather than `.env` on disk.
- **Object storage for `messages/`.** S3 / MinIO instead of a local
  volume so the spool can survive host loss.
- **Postgres backend.** SQLite is fine for a single-tenant pilot;
  Postgres makes sense once you cross 5+ concurrent operators or
  want streaming replication.
- **AD/LDAP integration.** Per-user opt-in: set `KILTER_LDAP_URL` and
  related env vars, then flip individual users from `local` to `ldap`
  in the Users admin page. TOTP stays as the second factor. See
  [docs/LDAP.md](LDAP.md) for the env-var reference, AD vs OpenLDAP
  examples, and the troubleshooting matrix.
- **Centralised audit-log shipping.** Forward the `audit_log` table to
  the bank's SIEM (Splunk / Elastic / Sentinel).
