# Legal Documents — Sign-off Status

> This file tracks which legal documents have been reviewed by a qualified
> Ghana lawyer and are ready for execution with customer banks. Update on
> every status change.

## Why this matters

5 of 7 documents are still marked `DRAFT for lawyer review`. **No paid
customer contract can be signed using a DRAFT.** This is the single
remaining external gate to go-live.

Estimated cost to clear all five: GHS 7,000–18,000 (~USD 500–1,200) via a
Ghana lawyer per `legal/README.md` execution-order notes. Estimated
turnaround: 1–2 weeks.

## Status board

| # | Document | Customer-facing? | Status |
|---|----------|------------------|--------|
| 01 | [Memorandum & Articles](01_memorandum_and_articles.md) | No (incorporation) | ⚠️ Required before company registration. Not blocking pilot sign-up. |
| 02 | [Master Services Agreement (MSA)](02_msa.md) | **Yes — paid customers** | 🔴 **DRAFT.** Blocks paid contract execution. |
| 03 | [Pilot Agreement](03_pilot_agreement.md) | **Yes — pilot banks** | 🔴 **DRAFT.** Updated 2026-05-10 to reflect $5K paid pilot model + 50%/25% design-partner discount stacking. Needs lawyer review of new clauses. |
| 04 | [Data Processing Addendum (DPA)](04_dpa.md) | **Yes — pilot + paid** | 🔴 **DRAFT.** Incorporated by reference into 02 and 03 — blocks both. |
| 05 | [Privacy Policy](05_privacy_policy.md) | Public (website) | 🔴 **DRAFT.** Website URL fixed (was `kilter.com`, now `www.kilter-app.com`). Needs lawyer sign-off on Ghana DPA framing. |
| 06 | [Terms of Service](06_terms_of_service.md) | Public (website) | 🔴 **DRAFT.** Website URL fixed. Needs lawyer sign-off. |
| 07 | [Legal README](README.md) | Internal | 🟢 Reference doc, no sign-off required. |

## What's blocking what

```
┌────────────────────────────────────────────────────────────────┐
│  Public marketing site (www.kilter-app.com)                    │
│  ✅ LIVE — but Privacy Policy + ToS are still DRAFT.           │
│     Visitors can read them but they bind nobody until signed.  │
└────────────────────────────────────────────────────────────────┘
                            ↓
┌────────────────────────────────────────────────────────────────┐
│  First pilot bank sign-up                                      │
│  ⚠️  BLOCKED until Pilot Agreement (03) + DPA (04) are        │
│      lawyer-reviewed and finalised.                            │
└────────────────────────────────────────────────────────────────┘
                            ↓
┌────────────────────────────────────────────────────────────────┐
│  First paid customer                                           │
│  ⚠️  BLOCKED until MSA (02) + DPA (04) are lawyer-reviewed.   │
│      Pilot → Paid conversion requires signed MSA.              │
└────────────────────────────────────────────────────────────────┘
```

## Next action

Engage a Ghana lawyer with a brief covering all five drafts. Templates
intentionally written so a lawyer can mark them up (rather than write
from scratch) — should keep billing time under 8 hours total.

Suggested brief:

> "Five drafts attached: MSA, Pilot Agreement, DPA, Privacy Policy, ToS.
> Public-facing software product (Kilter, self-hosted reconciliation for
> banks). Need Ghana-compliant execution-ready versions. Particular
> attention to: (a) Pilot Agreement section 2 — $5K paid pilot fee
> structure, (b) section 5.2/5.3 — pilot fee credit stacks with
> design-partner 50%/25% discount, (c) DPA controller/processor split
> for on-prem deployment."

## Last updated

2026-05-10 — `legal/03_pilot_agreement.md` re-architected from free pilot
to paid pilot model (commits `ae2b2f9`); all other drafts unchanged
since initial creation.
