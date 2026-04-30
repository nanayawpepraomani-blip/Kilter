# Kilter — Operations Runbook

## Daily Checklist

- [ ] Check `uvicorn.log` for ERRORs
- [ ] Confirm nightly jobs ran: **Admin → Scheduler** → check last_run_status
- [ ] Check SLA alert channels: any open items breaching thresholds?
- [ ] Verify database size is within disk budget: `du -sh kilter.db`

---

## Backup Procedure

### SQLite (default)

```bash
# Consistent hot backup while the app is running (WAL mode safe)
sqlite3 /var/lib/kilter/kilter.db ".backup /var/backups/kilter/kilter.db.$(date +%Y%m%d)"

# Verify the backup
sqlite3 /var/backups/kilter/kilter.db.$(date +%Y%m%d) "PRAGMA integrity_check"
```

Schedule with cron (daily at 02:00):
```cron
0 2 * * * kilter sqlite3 /var/lib/kilter/kilter.db ".backup /var/backups/kilter/kilter.db.$(date +\%Y\%m\%d)" && find /var/backups/kilter -name "kilter.db.*" -mtime +30 -delete
```

### MySQL

```bash
mysqldump --single-transaction --routines --triggers \
  -u kilter -p kilter > /var/backups/kilter/kilter_$(date +%Y%m%d).sql
```

---

## Restore Procedure

### SQLite

```bash
systemctl stop kilter
cp /var/backups/kilter/kilter.db.20260101 /var/lib/kilter/kilter.db
systemctl start kilter
```

### MySQL

```bash
mysql -u kilter -p kilter < /var/backups/kilter/kilter_20260101.sql
```

---

## User Management

### Create a new user

1. Go to **Admin → Users**
2. Click **New user**, fill in username, role, auth source
3. Copy the enrollment URL from the success message
4. Send the URL to the user via secure channel (Signal, encrypted email)
5. User scans the QR code with Microsoft Authenticator

### Reset a user's TOTP (lost phone)

1. Go to **Admin → Users**
2. Click **Recovery codes** next to the user — generates 8 new single-use codes
3. Give the codes to the user securely (they can log in using a code instead of TOTP)
4. Once the user is in, they can re-enroll their new device

### Deactivate a user

1. Go to **Admin → Users**
2. Click **Deactivate** — immediately revokes all active sessions

---

## Scheduler Job Management

Jobs run automatically. To inspect or trigger manually:

1. Go to **Admin → Scheduler**
2. Click a job to see its run history
3. Click **Run now** to trigger immediately

Key jobs:
| Job | Schedule | What it does |
|---|---|---|
| `scan` | Every 5 min | Picks up new files from `/messages/swift` and `/messages/flexcube` |
| `daily_close` | 01:00 UTC | Closes open sessions older than 24h |
| `sla_check` | 08:00 UTC | Sends aging alerts to configured channels |
| `daily_breaks_report` | 07:30 UTC | Emails the daily open-items summary |

---

## Monitoring

Watch these signals:

| Signal | Where to look | Action if bad |
|---|---|---|
| App errors | `uvicorn.log` | Check traceback; restart if OOM |
| Failed logins | Admin → Activity, filter `login_failed` | Investigate if >10/hour from one IP |
| Scheduler failures | Admin → Scheduler, `last_run_status=error` | Check `last_run_output`; fix file permissions or SMTP creds |
| Disk usage | `du -sh kilter.db uploads/ exports/` | Archive old exports; prune processed messages |
| SLA breaches | SLA alert channels | Escalate to ops team lead |

---

## Troubleshooting

### App won't start

```bash
.venv/bin/python -c "import app" 2>&1  # Check for import errors
```

Common causes:
- Missing `KILTER_SECRET_KEY` → generate one and add to `.env`
- Port 8000 already in use → `lsof -i:8000` to find what's using it
- DB permission error → `chmod 600 kilter.db; chown kilter:kilter kilter.db`

### Login fails for all users

- Check if enrollment was completed (users must scan QR code first)
- Verify time sync: TOTP requires clocks within 30s. Run `timedatectl` on the server
- Check rate limiting: after 10 failed attempts in a minute, wait 60s

### File scanner isn't picking up files

- Check file permissions: `/messages/swift/` must be readable by the app user
- Check filename format: SWIFT files must be `.out` or `.xml`; Flex files `.xlsx` or `.csv`
- Check for errors in the scan job: Admin → Scheduler → scan → last run output

### Audit log errors

- The audit_log table is write-only (immutable triggers). Any tool that tries to UPDATE or DELETE audit_log rows will get `ABORT: audit_log rows are immutable`. This is by design.

---

## Emergency Contacts Template

Copy and fill in before going live:

```
Kilter admin account: admin / (in vault)
DB server (if MySQL): host=  port=3306  db=kilter
Backup location: /var/backups/kilter/
Server access: ssh kilter@<ip>
On-call contact: <name> <phone>
Vendor support: <email>
```
