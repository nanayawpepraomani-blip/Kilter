# Threat Model — Kilter

STRIDE-style. Tight rather than exhaustive — covers what an attacker
realistically tries against an internal financial-operations app, and
what's in the way of each. Re-review after every external pentest.

---

## Assets

In rough order of value to an attacker:

1. **Reconciliation decisions and signed certificates** — falsifying
   one creates a window to cover up a real-money discrepancy.
2. **TOTP secrets and (where LDAP is enabled) the ability to log in
   as a privileged user.**
3. **Audit log integrity** — a tampered log lets all of the above
   go undetected.
4. **Account-level reconciliation data** — counterparty names,
   transaction narratives, account numbers. Sensitive but not the
   primary target.
5. **Service availability** — DoS denying month-end sign-off.

## Trust boundaries

1. Internet ↔ bank network (out of scope; bank-owned).
2. Authenticated browser ↔ Kilter HTTP surface (the primary surface).
3. Kilter container ↔ Active Directory.
4. Kilter container ↔ persistent volumes.
5. Operator console (host shell) ↔ Kilter container.

## Threats and mitigations

### Spoofing

| # | Threat | Mitigation | Residual |
|---|---|---|---|
| S1 | Attacker steals an operator's TOTP code (e.g., shoulder-surfing the laptop) | TOTP codes valid for ±30 s; idle session timeout 30 min so a stolen code only opens a short window; rate limit on `/login` (10/min/IP) | Low — assumes no real-time relay attack |
| S2 | Attacker steals an operator's session token (XSS, malicious browser extension) | CSP `default-src 'self'`; `X-Frame-Options: DENY`; HSTS; tokens never persisted in `localStorage` of cross-origin pages | Medium — XSS would still be game-over within one session |
| S3 | Attacker poisons the LDAP search filter via crafted username | `escape_filter_chars` from ldap3 applied; multi-match search rejected | Low |
| S4 | Attacker spoofs an admin during the bootstrap-enrollment window | Enrollment token is a 16-byte URL-safe random; one-time-use; cleared on completion | Low — assumes secure delivery of the link |

### Tampering

| # | Threat | Mitigation | Residual |
|---|---|---|---|
| T1 | Modify a signed reconciliation certificate after the fact | `status='signed'` is a guard in every transition; `snapshot_json` freezes the figures; audit_log records the signing event | Low — relies on integrity of the DB file |
| T2 | Modify a confirmed match decision to cover a break | Decisions append to `audit_log`; the matching row in the candidate table records actor, action, timestamp; no API path overwrites a confirmed decision | Low |
| T3 | Modify the audit log itself | Audit log is a regular SQLite table with no UI write path; an attacker with shell access to the container *can* modify it | High if shell access is achieved — partly compensated by SIEM shipping (logs leave the host as soon as they're written) |
| T4 | Substitute the Fernet key in transit between secret store and container | `KILTER_SECRET_KEY` env var is delivered through the customer's secret-manager mechanism; we don't own this trust chain | Customer-side |

### Repudiation

| # | Threat | Mitigation | Residual |
|---|---|---|---|
| R1 | Operator denies signing a certificate they signed | Audit log + SQL record + four-eyes flow (prepare/review/sign on different roles) | Low |
| R2 | Operator denies a failed login attempt that triggered a security review | `audit_log.action = 'login_failed'` rows carry the timestamp and the user-agent | Low |

### Information disclosure

| # | Threat | Mitigation | Residual |
|---|---|---|---|
| I1 | TOTP secret leaks from the database | Encryption at rest with Fernet; key held outside the DB file; `is_encrypted` flag for migration tracking | Medium if both DB volume and `KILTER_SECRET_KEY` leak together |
| I2 | TOTP secret leaks via the enrollment flow | Server-side keying — secret never re-sent to the client after `/enroll/start`; manual-entry key shown for typing only | Low |
| I3 | A user reads another user's reconciliation data | Per-user access-area scope enforced on list endpoints (`active_scope`) and resource endpoints (`_assert_account_in_scope`); admin-style "no scope" requires the admin role | Low |
| I4 | API docs (`/docs`, `/openapi.json`) reveal endpoint surface | Disabled in production | Low |
| I5 | Stack traces or DB errors leak in responses | Global exception handler returns generic 500; SQLite errors specifically intercepted before propagation | Low |
| I6 | Login response distinguishes "wrong password" from "wrong TOTP" | All login failures return the same generic 401; audit log records the actual reason internally | Low |

### Denial of service

| # | Threat | Mitigation | Residual |
|---|---|---|---|
| D1 | Brute-force `/login` | `slowapi` rate limit at 10/min/IP; `RateLimitExceeded` returns 429 | Low |
| D2 | Oversized upload exhausts disk | 300 MB request-body cap in middleware + inline chunked-write enforcement (catches chunked-transfer abuse where Content-Length is omitted); streaming write keeps worker RAM flat; reverse-proxy `client_max_body_size` recommended | Low |
| D3 | Slowloris-style request | Reverse proxy is the customer's responsibility; documented in DEPLOY.md | Customer-side |
| D4 | Long-running parser ties up the worker | Single-tenant scale; healthcheck with restart-on-failure | Low |
| D5 | Disk-fill via stdout log spam | Docker json-file driver caps at 10 MB × 5 | Low |

### Elevation of privilege

| # | Threat | Mitigation | Residual |
|---|---|---|---|
| E1 | Ops user signs a certificate (should be admin only) | `require_role('admin')` on `/certificates/{id}/sign`; same for prepare/review | Low |
| E2 | User in scope "EU" reads accounts in scope "US" | `active_scope` list drives `_scope_clause` and `_assert_account_in_scope` | Low |
| E3 | Admin demotes themselves accidentally and can't recover | `update_user` refuses self-demote and self-deactivate; emergency CLI in OPERATOR_NOTES.md to re-promote via SQL | Low |
| E4 | Container break-out (kernel exploit) | Drop-all-caps, `no-new-privileges`, read-only rootfs, non-root UID — full kernel exploit is in scope but expensive | Low |
| E5 | An admin moves themselves to LDAP before LDAP is configured, bricks the whole system | Self-flip lockout guard requires `KILTER_LDAP_URL` to be set | Low |

## Assumptions

If any of these break, the model needs re-review:

1. The reverse proxy in front of Kilter handles TLS correctly and
   doesn't strip or duplicate `X-Forwarded-For`.
2. The customer's AD enforces a non-trivial password policy (length,
   rotation, lockout).
3. The encryption key is stored in a secret manager or env var, not
   on the same volume as the DB.
4. The container host is patched on a reasonable cadence and not
   shared with adversary-controlled workloads.
5. Operators do not share credentials, and the bank's SIEM ingests
   the application stdout logs.

## Known residual risks (accepted)

- **DB-file shell access ⇒ audit-log tamper.** A compromised admin or
  container break-out can modify `audit_log` rows directly. The
  compensation is real-time SIEM shipping: by the time the row is in
  Splunk/Elastic/Sentinel, tampering with the local DB doesn't
  re-write what the SIEM saw. Customers who need on-DB tamper-evidence
  can mount the DB on append-only storage or run it through an
  external write-ahead log shipping layer; out of scope for the pilot.
- **Single Fernet key.** A single key encrypts every protected
  column. Compromise ⇒ all secrets are readable. Compensations: key
  is never on the same volume as the data, never in source, rotation
  workflow exists (`secrets_vault.rotate_key`). Multi-key envelope
  encryption is on the post-pilot roadmap.
- **Self-pentest only at this stage.** A formal third-party
  engagement is scheduled (see [PENTEST_SUMMARY.md](PENTEST_SUMMARY.md)).
  Until that closes out, claims here are best-effort, not attested.
