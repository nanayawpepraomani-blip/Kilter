# Architecture — Kilter

**Audience:** the bank's enterprise architect / security architect.
**Purpose:** answer "where does my data go, who can touch it, and what
trust boundaries exist?" without requiring them to read source.

---

## 1. Deployment topology

Kilter is **shipped as a self-hosted container**. The default
deployment is single-node, on-premises or in the customer's private
cloud. There is no Kilter-operated cloud unless the customer
specifically opts into a managed-hosting addendum (rare for banks).

```
                   ┌──────────────────────────────────────────────────┐
                   │            CUSTOMER NETWORK (BANK)               │
                   │                                                   │
   Operator        │   ┌──────────────┐         ┌─────────────────┐   │
   browser  ──TLS──┼──►│ Reverse proxy│──HTTP──►│  Kilter         │   │
                   │   │ (nginx/Caddy)│         │  container      │   │
                   │   └──────────────┘         │  - uvicorn      │   │
                   │                            │  - SQLite       │   │
                   │                            │  - scheduler    │   │
                   │                            └────────┬────────┘   │
                   │                                     │            │
                   │                              ┌──────▼──────┐     │
                   │                              │ Persistent  │     │
                   │                              │ volumes:    │     │
                   │                              │  /data      │     │
                   │                              │  /messages  │     │
                   │                              │  /uploads   │     │
                   │                              │  /exports   │     │
                   │                              └─────────────┘     │
                   │           ┌──────────────────┐                   │
                   │           │ Active Directory │ ◄─LDAPS (optional)│
                   │           └──────────────────┘                   │
                   │           ┌──────────────────┐                   │
                   │           │ SMTP relay       │ ◄─optional        │
                   │           └──────────────────┘                   │
                   │                                                   │
                   └──────────────────────────────────────────────────┘
                                ▲
                                │ All inbound and outbound
                                │ flows stay within the
                                │ customer's network.
                                │ No phone-home from Kilter.
```

## 2. Components

| Layer | Component | Purpose | Persistence |
|---|---|---|---|
| Edge | Reverse proxy (customer-supplied) | TLS termination, WAF if any, rate limiting | Customer's existing logs |
| App | uvicorn + FastAPI | HTTP API + Jinja2 server-rendered UI | Logs to stdout (rotated by Docker) |
| App | Background scheduler (in-process daemon thread) | Polls `/messages` for new statement files; runs daily SLA digest | Same as above |
| App | Cards module (`cards_loaders/`, `cards_engine.py`, `cards_ingest.py`) | PCI-safe ingest of issuer/acquirer/switch settlement files; N-way matching engine on `scheme_ref` | `card_settlement_files` + `card_settlement_records` tables; full PAN never persists |
| App | Mobile-money operator profiles (seeded in `db._seed_mobile_money_profiles`) | Pre-built BYO-CSV profiles for M-Pesa, MTN MoMo (agent + B2W + W2B), Airtel Money, Telcel Cash | `csv_format_profiles` table |
| Auth | TOTP (pyotp) | Second factor on every login | Encrypted in `users.totp_secret` |
| Auth | LDAP (ldap3) — optional | Password layer (per-user opt-in) | None on Kilter side; binds to AD on each login |
| Auth | Session tokens (`secrets.token_urlsafe`) | Bearer auth via `X-Session-Token` header | `user_sessions` table |
| Data | SQLite (WAL mode) | Application database | `/data/kilter.db` volume |
| Data | Filesystem | Statement files in/out, exports | `/messages`, `/uploads`, `/exports` volumes |
| Crypto | Fernet (cryptography 47) | At-rest encryption of TOTP secrets and SMTP credentials | Key sourced from `KILTER_SECRET_KEY` env var |

## 3. Data flow — reconciliation

Kilter runs three reconciliation streams through the same engine and
audit pipeline. Different ingest seams, identical persistence + audit
contract once parsed.

```
                            ┌─────────── stream 1: nostro / GL ───────────┐
   SWIFT MT/camt file ──────┤                                              │
   Flexcube xlsx ───────────┘                                              │
                                                                           │
                            ┌─────────── stream 2: mobile money ──────────┤
   M-Pesa / MTN MoMo /      │                                              │
   Telcel Cash / Airtel ────┘                                              │
   operator CSV (B2W / W2B)                                                │
                                                                           │
                            ┌─────────── stream 3: card scheme ───────────┤
   Visa / Mastercard /                                                    │
   Verve / GhIPSS settlement TSV                                          │
   (pre-masked PAN; full PAN never enters the system)                    │
                                                                           │
                                          /messages spool                  │
                                                │                          │
                                          scheduler ─► ingest              │
                                                │                          │
                                                ▼                          │
                                       parsed canonical txns               │
                                                │                          │
                              ┌─────────────────┴─────────────────┐        │
                              ▼                                   ▼        │
                       recon_engine                          cards_engine  │
                       (4-tier matching                     (N-way join on │
                        for nostro + mobile                  scheme_ref;   │
                        money — same shape)                 PCI-safe)      │
                              │                                   │        │
                              ▼                                   ▼        │
                       candidate set                         match groups  │
                              │                                   │        │
                ┌─────────────┴─────────────┐                     │        │
                ▼                           ▼                     │        │
           confirmed                  ambiguous                   │        │
         (auto-committed)           (review queue)                │        │
                │                           │                     │        │
                └─────────────┬─────────────┘                     │        │
                              ▼                                   │        │
                  reconciliation_certificates                     │        │
                  (maker/checker/approver)                        │        │
                              │                                   │        │
                              ▼                                   ▼        │
                       signed certificate              card recon status   │
                  (frozen snapshot, immutable)        persisted; protected │
                              │                      states (disputed /   │
                              │                      written_off) win     │
                              │                      over engine output   │
                              │                                   │       │
                              └────────────────┬──────────────────┘       │
                                               ▼                          │
                                        audit_log (append-only)           │
                                               │                          │
                                               ▼                          │
                                         exports/                         │
                                  (xlsx for nostro/mobile,                │
                                   CSV match groups for cards)            │
```

No transaction data leaves the customer environment. Aggregated,
anonymised operational metrics (parser throughput, match rates) may be
collected only with the customer's explicit opt-in per the DPA.

### 3.1 PCI scope reduction (cards stream only)

The cards stream has additional PCI-DSS-driven invariants that bind
the rest of the data flow:

- **Loaders mask at the seam.** `cards_loaders/csv_generic.py` handles
  the common case of pre-masked PAN columns (`484680******1168`)
  directly; for the rare case of a full PAN slipping through,
  `pci_safety.mask_pan` validates Luhn + length and returns
  `(first6, last4)` only — the full value is discarded immediately.
- **The schema has no full-PAN column.** `card_settlement_records`
  carries `pan_first6` + `pan_last4` only, both nullable. There is
  no path to insert a full PAN.
- **SAD is refused.** `pci_safety.refuse_if_sad_present` rejects any
  record whose column names suggest CVV / track / PIN data per
  DSS §3.2 — defence in depth for accidental exports.
- **Free-text fields are scanned + redacted.** Merchant name and
  notes pass through `pci_safety.redact_pan` before persistence.
  Embedded full PANs become `first6***last4`.

## 4. Trust boundaries

| Boundary | Direction | Authentication | Encryption |
|---|---|---|---|
| Browser ↔ reverse proxy | inbound | TLS 1.2+ | yes (customer cert) |
| Reverse proxy ↔ container | inbound (loopback) | none (proxy is trusted) | no — within host |
| Container ↔ AD/LDAP | outbound | service account + per-user bind | LDAPS / TLS required |
| Container ↔ SMTP relay | outbound | SMTP AUTH if configured; password encrypted in DB | STARTTLS where supported by the relay |
| Container ↔ Flexcube DB (Oracle) | outbound, **optional** | DB credentials in env var | TNS / TCP — bank-owned |
| Container ↔ filesystem volumes | n/a | n/a | customer-provided FDE recommended |

## 5. Authentication and authorisation

### Authentication factors

- **First factor (per-user choice):**
  - `auth_source = 'local'` (default): no first factor — TOTP-only.
    Reserved for the bootstrap admin and emergency-access accounts.
  - `auth_source = 'ldap'`: password verified against AD via LDAPS.
- **Second factor (always):** TOTP via Microsoft Authenticator
  (RFC 6238, SHA-1, 6 digits, 30s period, ±30s drift window).
- **Session:** opaque 256-bit token in `X-Session-Token`, server-side
  state, revocable by an admin and on role/auth-source change.

### Session lifecycle

| Event | Effect |
|---|---|
| Successful login | New row in `user_sessions`; absolute expiry 8h, idle expiry 30 min |
| Each authenticated request | `last_used_at` slides forward |
| Idle > 30 min | Token rejected; user re-authenticates |
| Absolute > 8h | Token rejected |
| Admin deactivates user | All user's tokens revoked |
| Admin changes role or auth_source | All user's tokens revoked |
| Logout | Caller's token revoked |

### Authorisation model

- **Role:** `admin`, `ops`, `audit`, `internal_control`. Enforced via
  the `require_role()` dependency on every state-changing endpoint.
- **Access-area scope:** each account is tagged with an `access_area`
  (e.g., region, business unit). A user's session has an active scope
  list; reads are filtered to that scope, writes are pre-checked
  against it. Helpers: `_scope_clause`, `_assert_account_in_scope`.
- **Four-eyes on certificates:** prepare (ops) → review
  (internal_control) → sign (admin). Same person can't sign their own
  preparation; the role checks enforce that mechanically.

## 6. Encryption

| Data | At rest | In transit |
|---|---|---|
| TOTP secrets | Fernet (AES-128-CBC + HMAC-SHA256) | TLS to client; never sent server→client after enrollment |
| SMTP password (notification channels) | Fernet | STARTTLS to the relay |
| Session tokens | None — tokens are random 256-bit secrets, equivalent to plaintext passwords for this purpose | TLS to client |
| Statement file content | None at the application layer; customer-side FDE recommended | TLS for upload |
| Audit log content | None — already non-sensitive after redaction (no payment-card data, no special-category data) | TLS |

The encryption key (`KILTER_SECRET_KEY`) is a single 256-bit Fernet
key, sourced from environment variable, secret manager (Vault / AWS
SM / 1Password), or — only on dev — a `.kilter_secret_key` file
generated on first run with mode 0600. Operators are expected to
promote dev-host keys to env-var-only before pilot start; the
[OPERATOR_NOTES.md](../docs/OPERATOR_NOTES.md) covers the procedure.

## 7. Logging and audit

- **Application logs:** stdout, captured by Docker's json-file driver
  with rotation (10 MB × 5). Customer ships these to their SIEM via
  the host's existing log pipeline.
- **Audit log:** dedicated SQLite table `audit_log`, append-only by
  convention. Captures authentication events, MFA enrollment, session
  issuance/revocation, configuration changes, certificate transitions,
  reconciliation decisions. Each row carries actor, action, timestamp,
  and a JSON `details` blob.
- **Failed-login telemetry:** `audit_log.action = 'login_failed'` rows
  carry `details.reason` (`ldap:bind_failed`, `ldap:user_not_found`,
  `ldap:service_bind_failed`, `totp`) so SOC analysts can distinguish
  password-spray from TOTP-misconfig from infrastructure outage.

## 8. Hardening posture

What ships hardened:

- Non-root container user (UID 10001).
- All Linux capabilities dropped; `no-new-privileges` set.
- Read-only root filesystem; only the four mounted volumes plus
  `/tmp` (tmpfs, 64 MB) are writable.
- HSTS (preload-eligible), CSP, X-Frame-Options: DENY, X-Content-
  Type-Options, Referrer-Policy, Permissions-Policy on every
  response.
- Auto-generated OpenAPI docs disabled in production.
- 300 MB request-body cap; uploads stream chunked to disk so worker RAM isn't proportional to file size. Reverse-proxy further-cap recommended.
- 10/min rate limit on `/login`.
- Pinned dependency versions in `requirements.txt`; no `*` ranges.

What is the customer's responsibility:

- Reverse-proxy TLS configuration and cipher suite policy.
- Network segmentation around the container.
- Volume-level encryption (full-disk encryption at rest).
- SIEM ingestion of stdout logs.
- Backup encryption and offsite shipment.
- Patch cadence of the underlying Docker host.

## 9. What's intentionally not in this picture

- **Service mesh / sidecar.** Single-container scope. A future
  multi-tenant SaaS path would introduce mTLS via Istio/Linkerd; that's
  not in the pilot.
- **Public/internet-facing surface.** Kilter is internal-use-only by
  design.
- **Multi-region replication.** Single-node SQLite. A future Postgres
  backend would unlock streaming replication.
- **HSM-backed key.** `KILTER_SECRET_KEY` is held in env / secret
  manager, not an HSM. Banks that mandate HSM-rooted keys can be
  accommodated via a wrapping shim that decrypts the key inside the
  container at startup; needs scoping per deployment.
