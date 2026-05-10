# Security FAQ — Kilter

Pre-filled answers to the most common security-questionnaire items
(SIG-Lite, CAIQ-Lite, bank-internal vendor templates). Use this as the
source of truth — when the bank hands you their bespoke questionnaire,
answer from here and update both this file and theirs if anything is
out of date.

> **Status flags used below**
>
> ✅ = implemented today
> ⚠️ = partial / mitigated, with a roadmap step
> ❌ = not implemented; honest no
> 🟡 = customer-side responsibility

---

## A. Company and product

**A.1 — Legal entity?**
Timeless Nypo Tech (Ghana). Incorporation in progress; pilot contracts can
proceed in parallel via the founder.

**A.2 — Product description in one paragraph?**
Kilter is a self-hosted reconciliation platform that ingests SWIFT
MT940/MT950 and ISO 20022 camt.053/054 statements alongside the bank's
internal ledger extract, parses both, runs a 4-tier matching engine,
queues ambiguous candidates for human review, enforces a maker /
checker / approver workflow on month-end certificates, and stores
everything in an append-only audit trail. The Software is licensed for
on-premises or private-cloud deployment.

**A.3 — Reference customers?**
Pilot phase. Earlier production deployment was internal at the
founder's prior employer; no permission to name them. Reference rights
are part of the design-partner discount in the pilot agreement.

**A.4 — Open-source dependencies?**
Pinned versions, all permissively licensed (MIT, BSD, Apache 2):
FastAPI, uvicorn, SQLAlchemy core / SQLite stdlib, Jinja2, pyotp,
cryptography, ldap3, slowapi, qrcode, openpyxl, python-multipart.
Full list in `requirements.txt`. No copyleft (GPL/AGPL) dependencies
in the runtime path.

**A.5 — Sub-processors?**
For on-premises deployments: none. The app runs entirely within the
customer's network. For optional features that may use external
sub-processors (SMTP relay, hosted demo URL), see [DPA Annex
B](../legal/04_dpa.md).

## B. Architecture and data flow

**B.1 — Where is customer data stored?**
On the customer's filesystem only. The default deployment writes the
SQLite database to a volume mounted into the container. ✅

**B.2 — Does Kilter phone home or send telemetry?**
No. The container makes no outbound connections except those the
customer explicitly configures (LDAP to the bank's AD, SMTP to the
bank's relay, optional Flexcube Oracle pull). ✅

**B.3 — Does Kilter store special-category personal data?**
No. The data model covers transaction metadata, account references,
counterparty names, and Authorised User identifiers. We refuse to
accept payment-card numbers, government identifiers, biometrics, or
health data. The DPA Section 4.3 makes this explicit.

**B.4 — Multi-tenancy?**
Single-tenant per deployment. There is no shared database between
customers. ✅

## C. Authentication and access control

**C.1 — MFA?**
TOTP (RFC 6238) via Microsoft Authenticator, mandatory for every
user, every login. ±30s drift window. ✅

**C.2 — Password layer?**
Two modes per user (`auth_source` field):
- `local` — TOTP-only, used for the bootstrap admin and emergency
  accounts.
- `ldap` — Active Directory password verified via LDAPS bind, then
  TOTP. ✅
The `local` path's defensive position is documented in
[ARCHITECTURE.md](ARCHITECTURE.md) §5.

**C.3 — SSO?**
LDAP / Active Directory today. SAML 2.0 / OIDC: ❌ not yet — we'll
add whichever the first paying customer specifies.

**C.4 — Session management?**
Server-side opaque tokens (256-bit), absolute expiry 8 h, idle expiry
30 min (configurable via `KILTER_SESSION_IDLE_MINUTES`). Tokens
revoked on logout, on role change, on auth-source flip, and on user
deactivation. ✅

**C.5 — Role-based access control?**
Four roles: `admin`, `ops`, `audit`, `internal_control`. Maker /
checker / approver enforced on certificate workflow. ✅

**C.6 — Per-user data segmentation?**
Yes. Each account is tagged with an `access_area`; users have a scope
list; reads are filtered, writes are pre-checked. Documented in
[ARCHITECTURE.md](ARCHITECTURE.md) §5. ✅

**C.7 — Privileged-access management?**
Admin role is the only one that can create users, change roles, or
sign certificates. Customer-side PAM (CyberArk / etc.) wraps the
container host as usual. 🟡

**C.8 — Account lockout / brute-force defence?**
Rate limit on `/login` (10 attempts / minute / IP). Failed-login
audit-log rows carry the failure category for SIEM alerting. ✅

## D. Cryptography

**D.1 — Encryption at rest?**
Fernet (AES-128-CBC + HMAC-SHA256, 256-bit key) for TOTP secrets and
SMTP credentials. Application-level rather than full-disk — encrypted
backups remain encrypted even if exfiltrated. The customer's
filesystem-level encryption (FDE) covers the rest. ✅ (app-level)
🟡 (FDE).

**D.2 — Encryption in transit?**
TLS terminated at the customer's reverse proxy. HSTS preload-eligible
header on every response. Internal proxy → container link is plain HTTP
on loopback (within the host); recommendation in DEPLOY.md is to
constrain that to localhost. ✅ + 🟡

**D.3 — Key management?**
`KILTER_SECRET_KEY` env var, sourced from the customer's secret
manager (Vault / AWS SM / 1Password / equivalent). On dev only, a
0600-mode `.kilter_secret_key` file is generated as a fallback;
[OPERATOR_NOTES.md](../docs/OPERATOR_NOTES.md) documents the
promotion procedure. ✅

**D.4 — Key rotation?**
`secrets_vault.rotate_key()` re-encrypts every protected column.
Wired but not yet exposed via a CLI; planned before first paid
customer. ⚠️

**D.5 — HSM?**
Not today. A wrapper that decrypts `KILTER_SECRET_KEY` from an HSM at
container startup is feasible per-deployment if a customer mandates
it. Out of scope for the pilot. ❌

## E. Vulnerability and patch management

**E.1 — Pinned dependencies?**
Yes, exact versions. `requirements.txt` is read-only between
deliberate bumps. ✅

**E.2 — Vulnerability scanning?**
`pip-audit` (or equivalent) is run before each release. ✅
Container scanning (Trivy / Snyk / Anchore) is the customer's
existing pipeline; the slim Python 3.12 base image is intentionally
boring. 🟡

**E.3 — Patch SLA?**
Critical: patched and released within 7 calendar days of a confirmed
exploit. High: 30 days. Medium / low: next scheduled release. Customer
applies via a routine `docker compose build --pull && up -d`. ✅

**E.4 — Penetration testing?**
Internal pentest pass on 2026-04-23 to 2026-04-26; all critical /
high / medium / low findings remediated. External third-party
engagement scheduled Q4 2026. See
[PENTEST_SUMMARY.md](PENTEST_SUMMARY.md). ⚠️ (internal only today)

## F. Logging, monitoring, and incident response

**F.1 — Audit logging?**
Append-only `audit_log` table. Captures authentication events
(success and failure with categorised reason), MFA enrollment,
session lifecycle, configuration changes, certificate transitions,
reconciliation decisions. ✅

**F.2 — SIEM integration?**
Container stdout is captured by the host's Docker logging driver
(json-file with rotation). Customer ships to their SIEM via the host's
existing log pipeline. ✅ (we provide the data) 🟡 (customer pipes it)

**F.3 — Real-time alerting?**
Customer-side. Recommendations in
[ARCHITECTURE.md](ARCHITECTURE.md) §7 for queries to wire up
(repeat-failed-login, ambiguous-match-spike, certificate signed off
hours).

**F.4 — Incident-response plan?**
Documented commitments are in [DPA Section 8](../legal/04_dpa.md):
72-hour breach-notification clock from confirmed awareness; named
on-call contact; cooperation with the customer's investigation. ✅

## G. Backup, business continuity, disaster recovery

**G.1 — Backups?**
The customer backs up the `kilter-data` volume (single SQLite file).
Procedure documented in [DEPLOY.md](../docs/DEPLOY.md) §3. We don't
hold customer data, so we don't back it up. 🟡

**G.2 — RPO / RTO?**
For on-prem: customer-defined. The application restores cleanly from
a copied DB file in seconds. We commit to an RTO of 4 hours for
incidents we cause (e.g., a botched release rollback) once we have a
managed-service offering; for pilot, the customer owns RTO/RPO end-
to-end.

**G.3 — Tested restore?**
The compose-pause-snapshot procedure in DEPLOY.md is the documented
pattern; quarterly restore testing is the recommended cadence. 🟡

## H. Personnel and supply chain

**H.1 — Background checks on personnel?**
DPA Annex A: "background checks where local law permits; written
confidentiality undertakings; data-protection training on hire and at
least annually." ✅ as policy; small-team practice today.

**H.2 — Code review?**
Every change is reviewed before merge. ✅

**H.3 — Source-code provenance?**
Repository is private; commits are signed. Pinned dependencies; no
upstream CDN script tags in production templates. ✅

**H.4 — Build supply chain?**
Container builds from `python:3.12-slim` (Debian). No private
internal mirror today; building from upstream PyPI / Debian. ⚠️ —
a customer-managed build pipeline can substitute their own approved
base if they require it.

## I. Data subject rights and privacy

**I.1 — Data subject access requests?**
Kilter is a Processor, the customer is the Controller (DPA Section 2).
Requests come to the customer; we assist per DPA Section 5.3 within
the timelines they require.

**I.2 — Data deletion on termination?**
Within 30 days of contract termination; certified in writing on
request. DPA Section 5.5. ✅

**I.3 — Cross-border transfer?**
On-premises deployments don't transfer data. If the customer opts
into a Kilter-managed deployment, default region is configured with
the customer (AWS Frankfurt for Europe, AWS Cape Town for Africa).
DPA Section 7. 🟡

## J. Compliance and certifications

**J.1 — SOC 2?**
Not yet. Earliest realistic Type I: 12 months from first paying
customer (~2027 Q1). Customer-funded acceleration is possible. ❌
today.

**J.2 — ISO 27001?**
Not yet. Defer until ≥3 paying customers + dedicated security lead.
❌ today.

**J.3 — PCI-DSS?**
Kilter is **architected to keep your reconciliation system out of
PCI-DSS storage scope**, even when you reconcile card-scheme settlement
files. Three structural commitments:

1. **No full PAN, ever.** Loaders mask at the parser seam into
   `pan_first6` + `pan_last4` only. The schema (`card_settlement_records`)
   has no column that could hold a full PAN — there's no path to insert
   one. PCI-DSS v4 §3.4.1 explicitly permits storing first-6 + last-4
   separately or together without scope; that's the line we ship at.
2. **Sensitive Authentication Data refused.** CVV / CVV2, full track
   data, PIN blocks (DSS §3.2) get rejected at ingest by
   `pci_safety.refuse_if_sad_present` — the loader raises before the
   record reaches the records list.
3. **Free-text fields are PAN-redacted.** Merchant name and notes
   columns get a Luhn-validated PAN sweep on ingest; embedded full
   PANs become `first6***last4` before persistence
   (`pci_safety.redact_pan`).

What this means for your QSA:
- You can run Kilter's cards module in your CDE-adjacent zone. The
  data stored in Kilter (first6+last4 only) is out of cardholder-data
  scope per DSS v4.
- If your security policy requires column-level encryption on
  `pan_last4` even though it's out-of-scope, the existing Fernet
  vault primitive can extend to those columns; not yet wired in stock.
- If your policy requires tokenisation of `pan_last4` into a
  deterministic hash, the schema accommodates it without changes.

What it does **not** do:
- Cards module does not implement DSS network-segmentation controls —
  that's the bank's deployment responsibility (covered in DEPLOY.md
  pilot security pack).
- The Visa Base II / Mastercard IPM binary parsers are stubbed pending
  scheme-published synthetic test files (Visa V.I.P., Mastercard PUF) —
  see [docs/CARDS_DESIGN.md](../docs/CARDS_DESIGN.md). Until those land,
  cards ingest is via tab-separated switch reports + the BYO format
  profile machinery.

**J.4 — GDPR / regional data-protection laws?**
The DPA covers GDPR and equivalent regional regimes (e.g. NDPR,
Kenya DPA 2019, POPIA, and other national DPAs). See
[DPA Section 1](../legal/04_dpa.md). ✅

**J.5 — Central-bank outsourcing rules (FCA, ECB, regional regulators)?**
Kilter is shipped as software; the bank operates it. So the bank's
outsourcing controls apply to its own operations team. We accommodate
specific clauses (right-to-audit, pre-approval of subcontractors,
etc.) in the MSA per-deal. ✅ as a posture.

---

## K. Common follow-ups (banks always ask)

- **"Do you have a SIG Core (1,500+ questions) version of this?"**
  Not pre-filled. Once a bank confirms they require the full SIG, we
  fill in 4–6 days; price the engagement.
- **"What's your bug-bounty / vulnerability-disclosure program?"**
  Email `timelessnypotech@outlook.com` or visit https://www.kilter-app.com — coordinated disclosure. No bounty $$
  during pilot; will introduce one after the first paid customer.
- **"Can you fill out our internal vendor-risk questionnaire?"**
  Yes — we use this FAQ as the source of truth. Allow 5 business
  days for first turnaround; subsequent revisions usually 1–2 days.
- **"Will you sign our standard MSA / DPA?"**
  Yes, with redlines. Our [MSA](../legal/02_msa.md) and
  [DPA](../legal/04_dpa.md) are starting points; we expect bank-side
  redlines and engage in good faith.
- **"What's the worst-case outcome if your container is compromised?"**
  See [THREAT_MODEL.md](THREAT_MODEL.md), particularly the "Known
  residual risks (accepted)" section. The largest accepted risk is
  audit-log tamper after container shell-access; compensated by SIEM
  shipping.
