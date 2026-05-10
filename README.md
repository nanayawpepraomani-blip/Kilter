# Kilter

Modern nostro + GL reconciliation for correspondent-banking treasuries.

**Self-hosted · Audit-first · Format-agnostic** (MT940/950, camt.053/054,
any core-banking GL extract).

---

## What it does

For each registered cash account, Kilter pairs the correspondent bank's
SWIFT statement (MT940/950 or ISO 20022 camt.053/054) against the
core-banking GL's posting journal, proposes matches across four tiers,
and lets ops confirm or reject — with every click logged.

- **Intake** — drop SWIFT `.out`/`.xml` and core `.xlsx` into a folder; the
  scanner parses, routes, and pairs by registered account.
- **Tiered matcher** — strict (ref + amount + date), ref-hit with amount
  variance, no-ref same-day, ±1-day weekend timing.
- **Review queue** — keyboard-friendly Confirm / Reject / Skip / Swap.
- **Open items ledger** — unmatched breaks roll forward across sessions
  until cleared, written off, or reviewed.
- **Audit log** — every login, decision, export, and config change in one
  immutable table; CSV export for regulatory review.
- **Roles** — `admin`, `ops`, `audit`, `internal_control`. MFA (TOTP) by
  default — works with Microsoft Authenticator, Google Authenticator, Authy,
  or any TOTP app. Access-area scoping per user.
- **Scheduled automation** — 7 built-in jobs: intake scan, daily-close,
  SLA alerts (Teams / email), daily breaks report, month-end certificates,
  nightly DB backup, and weekly session cleanup. All configurable in the UI.

## Requirements

- Python 3.13+
- ~8 GB RAM (SQLite default; Postgres supported for HA — ask)
- 100 GB disk for 2 years of statements + audit log
- Outbound: 443 (Teams webhook), 587 (SMTP), optionally 1521 (Oracle for
  Flexcube pull)

## Quick start

```bash
# 1. Install (production deps only)
python -m venv .venv && source .venv/bin/activate    # or .venv\Scripts\activate on Windows
pip install -r requirements.txt

# 2. First run — creates schema + bootstraps an admin enrollment token
python -m uvicorn app:app --host 127.0.0.1 --port 8000

# Watch the console — first start prints something like:
#   FIRST-RUN ADMIN ENROLLMENT
#   Open: http://localhost:8000/enroll?user=admin&token=<token>

# 3. Open that URL in a browser, scan the QR with Microsoft Authenticator,
#    then log in at /login with username `admin` + the 6-digit code.

# 4. The dashboard shows a four-step welcome card. Follow it:
#    a. Register your first nostro account (Cash accounts page)
#    b. Drop a SWIFT statement into messages/swift/
#    c. Drop the matching core-banking xlsx into messages/flexcube/
#    d. Click "Scan now" or wait for the 15-min auto-scan
```

## Layout

```
.
├── app.py                  # FastAPI entry — routes, middleware, auth
├── auth.py                 # TOTP, sessions, role gating
├── db.py                   # SQLite schema, migrations, seeding
├── scanner.py              # messages/ folder watcher + ingestion driver
├── ingest.py               # File → session pipeline
├── recon_engine.py         # Tiered match proposer + resolver
├── reconcile.py            # Legacy reconciler (kept for reference)
├── open_items.py           # Rolling-ledger break tracking
├── certificates.py         # Month-end sign-off workflow
├── scheduler.py            # In-process cron (scan / close / SLA / report)
├── sla.py                  # Teams + email alert dispatchers
├── swift_core.py           # MT940/950 parser
├── swift_loader.py         # MT loader → engine shape
├── iso20022_loader.py      # camt.053/054 parser + loader
├── account_meta.py         # SWIFT/Flex metadata extractors
├── templates/              # Jinja2 — every UI page, all manuals
├── tests/                  # pytest — parser + engine + auth
├── scripts/                # Operator utilities + demo seeders
│   └── ecobank/            # Ecobank-specific Oracle extractors (NOT shipped)
├── messages/               # Intake — swift/, flexcube/, plus processed/, unloaded/
├── exports/                # Generated reports (xlsx, CSV)
├── uploads/                # Manual-upload staging
└── demo/                   # Pitch decks + demo script (Markdown + reveal.js HTML)
```

## Testing

```bash
pip install -r requirements-dev.txt
python -m pytest tests/ -v
```

Smoke tests in [scripts/_smoke_camt.py](scripts/_smoke_camt.py) — sanity
check the camt parser end-to-end without pytest.

## Generating demo data

```bash
# Wipes kilter.db and reseeds with 10 mock nostros + 10 days of mock
# SWIFT + Flex files. Useful for screenshots and sales demos.
rm kilter.db && python -m uvicorn app:app --port 8000 &
python scripts/_generate_mock_data.py --copy-to-messages
python -c "from scanner import scan; scan(user='system')"
```

See [scripts/_generate_mock_data.py](scripts/_generate_mock_data.py) for
the account list ("Meridian Trust Bank" — fictional) and tier mix.

## Security

- TLS termination via reverse proxy (nginx / IIS / Apache); see
  `templates/manual_setup.html` for the recommended config.
- Login is rate-limited (10/min per source IP).
- Default-disabled `/docs`, `/redoc`, `/openapi.json` to avoid
  unauthenticated API enumeration.
- Conservative CSP, HSTS, X-Frame-Options, Permissions-Policy on every
  response.
- Backups: kilter.db should be backed up nightly out-of-tree; do NOT
  retain backups in the deploy directory (they contain TOTP secrets).

A formal security review was conducted on 2026-04-25; outstanding
findings are tracked separately. See `SECURITY.md` (when added) for the
disclosure policy.

## License

Proprietary commercial — see [LICENSE](LICENSE). Pilot evaluation use is
permitted under specific terms; production use requires a signed
commercial license.

**Contact:** Timeless Nypo Tech — timelessnypotech@outlook.com | https://www.kilter-app.com

## Status

**v1.0 — production-ready.** Pre-launch security review complete. All blocking
findings resolved. Suitable for full ops-team rollout.
between minor versions until 1.0.
