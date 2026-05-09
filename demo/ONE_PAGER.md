---
title: "Kilter — Reconciliation, automated."
subtitle: "Self-hosted reconciliation across nostro, mobile money, and card-scheme settlements."
geometry: margin=0.75in
fontsize: 10pt
mainfont: "Helvetica"
documentclass: article
header-includes:
  - \pagenumbering{gobble}
  - \setlength{\parskip}{0.4em}
  - \setlength{\parindent}{0pt}
---

**Timeless Nypo Tech  ·  timelessnypotech@outlook.com**

---

# Reconciliation, automated.

**Kilter** is self-hosted reconciliation software for banks and payments
businesses. Drop in your statements — SWIFT (MT940/950, camt.053/054),
mobile-money operator feeds (M-Pesa, MTN MoMo, Airtel Money, Telcel Cash),
or card-scheme settlement files (Visa, Mastercard, Verve, GhIPSS) — and
your internal ledger extract. Kilter parses, matches, surfaces the
ambiguous cases for a human, and produces audit-grade artefacts your
finance and compliance teams already recognise.

## Three reconciliation streams, one platform

| Stream | Source | Bank-side | Use case |
|---|---|---|---|
| **Nostro / GL** | MT940 / MT950 / camt.053 / camt.054 | Flexcube / T24 / Finacle GL extract | Daily correspondent recon, month-end close |
| **Mobile money** | Operator B2W & W2B CSVs | Bank GL | Wallet-bank settlement, multi-operator coverage |
| **Card scheme** | Visa / Mastercard / switch settlement files | Bank issuing / acquiring GL | Daily switch settlements, PCI-DSS-out-of-scope storage |

Same operator UI, same audit log, same role model, same deployment.
A team trained on nostro can immediately work the mobile-money or cards
queue. **Replaces 2-3 separate reconciliation tools at typical mid-tier
bank cost.**

## The problem

Treasury and payments back offices reconcile **5–50 nostro accounts**
every business day, plus a daily card-switch settlement, plus weekly or
daily mobile-money operator feeds. The work is mostly manual: open
statements in one window, the GL in another, eyeball matches, paste into
Excel, repeat. Most banks spend **6–10 person-hours per account per month**
on nostro alone — and run *separate* tools (or just spreadsheets) for
cards and mobile money. Mistakes get caught late or never; sign-off
slips past the close window; auditors flag it.

## What Kilter does

**Parse anything banks actually send.** SWIFT MT940/950, ISO 20022
camt.053/054 (including SWIFT Alliance Access envelopes), Flexcube xlsx
exports, Oracle DB pulls, mobile-money operator CSVs (Paid In / Withdrawn
or signed-amount), card-switch tab-separated reports with pre-masked PAN,
and any custom CSV/xlsx via a column-mapping wizard. **1–2 minutes per
file**, not 48 hours.

**Match aggressively, surface only what's ambiguous.** 4-tier matching
engine for nostro (75–85% auto-match at tier 1 in typical pilot data).
N-way matching engine for cards (auth → clearing → settlement on
scheme-ref). Bring-your-own-tolerance per account (date, amount, FX bps).
Operators decide; engine learns the patterns.

**Sign off with audit-grade artefacts.** Maker / checker / approver
workflow on the month-end certificate. Frozen snapshots — once signed,
the figures never silently shift. Append-only audit log of every
decision, every login, every change.

## PCI-DSS posture (cards module)

Kilter is designed to keep the cards module **out of PCI-DSS storage
scope**: full PAN never persists, parsers mask at the seam into
`first6 + last4` only, no Sensitive Authentication Data (CVV, track,
PIN) is ever accepted, free-text fields are scanned and redacted on
ingest. Customers who need a tighter posture can tokenise `pan_last4`
into a deterministic hash; the schema supports it.

## What you get on day 1

- **Self-hosted container.** Runs on your VM or in your private cloud.
  We never see your data. No phone-home.
- **MFA on every login.** TOTP via Microsoft Authenticator out of the
  box; Active Directory password layer (LDAPS) optional per-user.
- **Encrypted at rest.** TOTP secrets and SMTP credentials. Fernet,
  key in your secret manager, never in source.
- **Pen-tested, hardened.** Read-only rootfs, dropped capabilities,
  rate-limited login, 300 MB streaming-upload cap (covers a 250 MB
  card-switch settlement file), security headers, scope-enforced
  authorisation. Full pack at `security/` for your CISO.
- **Pre-seeded operator profiles.** M-Pesa Safaricom, Telcel Cash,
  MTN MoMo (agent + B2W + W2B), Airtel Money, GhIPSS card switch — bind
  to a wallet account in the BYO formats UI and start ingesting same day.

## Pricing — paid customers

**USD $4–8k per reconciliation account, per year.** Counts apply across
all three streams: a nostro account, a wallet account, and a card-acquirer
account each consume one licence slot. Volume discounts beyond 10
accounts. Implementation USD $3–10k one-time depending on file-format
quirks. Annual prepay; net 30. Customer-side hosting only — you bring
the infrastructure, we bring the software.

## Pilot — free for 60–90 days

A real evaluation, not a sales prop:

- 10 nostro + 10 GL accounts, your real (or anonymised) data, your VM.
  Add a wallet operator feed and / or a card-switch file in week 3 to
  exercise the multi-stream value.
- Success criteria pinned upfront — typically 75% tier-1 auto-match,
  50% reduction in close-cycle time, no critical defects.
- If we hit them, you convert at a **50% discount on year 1, 25% on
  year 2** as a design-partner customer.
- If we don't, walk away. We delete your data on request and keep nothing.

---

**Reply with a 30-minute window in the next two weeks.**
We'll bring the demo URL and your file format. You bring two questions
and a sceptic.
