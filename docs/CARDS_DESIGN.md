# Cards module — design notes

This is the design baseline for the cards reconciliation module. It
captures the scoping decisions made when the foundation landed (PCI-safe
schema + redaction primitives + loader interface) so the next
implementation pass doesn't re-litigate them.

## Why a separate module

Cards reconciliation overlaps conceptually with nostro/GL recon — two
sides matching on a reference key with tolerance for fees and FX —
but the **data model, file formats, and compliance posture** are
different enough that bolting them onto the cash-nostro engine would
dilute both.

Specifically:
- **PCI-DSS scope**. Card data brings storage / transmission /
  encryption controls that nostro doesn't. Mixing the two means every
  GL recon table is suddenly in scope.
- **3-way matching**. Card flows have authorization → clearing →
  settlement, three records that must reconcile. The 4-tier engine
  does 1:1 / 1:N / M:N pairs; 3-way is an extension, not a
  generalisation.
- **Volume**. A medium issuer easily produces 100k+ settlement records
  per day. SQLite still copes, but the schema + indexes need to be
  set up for it from day one.
- **Lifecycle**. Card records can be disputed (chargebacks) months
  after settlement; nostro entries don't have that pattern. The
  `recon_status` enum carries 'disputed' and 'written_off' specifically
  for this.

## PCI strategy — stay out of scope

Kilter's cards module is designed to be **PCI-DSS-out-of-scope for
storage**. The minimum-viable scope-reduction posture:

1. **Full PAN never persists.** All loaders mask at the parser layer
   into `(pan_first6, pan_last4)`. The schema doesn't have a column
   that could hold a full PAN.
2. **Truncation rule.** PCI-DSS v4 §3.4.1 permits storing first 6 +
   last 4 separately or together without scope. We store both because
   that's the maximum permitted, which gives matching algorithms
   enough to disambiguate transactions on the same merchant + amount.
3. **Sensitive Authentication Data refusal.** CVV / track / PIN
   data must never be stored post-authorization (DSS §3.2). The
   `pci_safety.refuse_if_sad_present` helper rejects records carrying
   any of those field names. Settlement files don't include SAD anyway,
   but defence in depth matters when bank ops accidentally exports
   from the wrong system.
4. **Free-text scanning.** Merchant-name and notes fields can leak
   PANs in human-typed comments. `redact_pan(text)` replaces any
   embedded full-PAN-shaped substring (Luhn-validated) with
   `first6***last4` before persistence; `refuse_if_pan(record)` is the
   stricter version that rejects rather than redacts.
5. **Logs and exports.** Application logs must NEVER print full PANs.
   The redaction helpers should be applied at log boundaries when
   the cards UI lands.

What this DOESN'T cover (out of scope, intentionally):
- Encryption at rest beyond what the existing Fernet vault does for
  TOTP / SMTP creds. If a customer's policy requires column-level
  encryption for `pan_first6`, that's an additive build using the
  existing `secrets_vault` primitive.
- Tokenization. Customers who want to remove `pan_last4` entirely can
  hash it into a deterministic token at parser time; not yet wired.
- Network segmentation. PCI-DSS requires the cardholder data
  environment to be segmented; that's the customer's deployment
  responsibility, not Kilter's. Documented in `docs/DEPLOY.md` as
  part of the pilot security pack.

## Scheme priority

For the first paid customer (most likely a regional African
mid-market issuer) the priority order is:

1. **Visa Base II issuer settlement.** Highest African footprint via
   Visa for issuers; tooling exists at most banks already; sample
   files available through V.I.P. (Visa Implementation Partner) test
   environments.
2. **Mastercard IPM issuer settlement.** Second-highest footprint;
   similar shape (binary, fixed-position TC items) so the architecture
   from #1 ports cleanly. Sample files via Mastercard PUF.
3. **Local switch (GhIPSS / NIBSS Verve).** Usually CSV-shaped, so
   the existing BYO format profile machinery handles them via
   `cards_loaders/csv_generic.py`. No bespoke parser needed.
4. **Acquirer settlement.** Same schemes, different role. Mostly the
   same parsers with `role='acquirer'`; the differences are in the
   matching logic (different fee structures, different chargeback
   flows), not the file format.
5. **ATM cassette / dispense reconciliation.** Different domain
   conceptually (cash vs card), often handled by a separate vendor
   today. Defer until a customer specifically asks.

## Why parsers aren't built yet

Visa Base II and Mastercard IPM are binary fixed-position formats
with byte-offset specifications running to dozens of fields each.
Building these speculatively against published spec PDFs without
real test files is high-risk:

- Regional variants and optional sub-fields shift offsets;
- Padding conventions vary by issuer;
- Bit-mapped sub-elements (Mastercard PDS) are unforgiving — a
  one-byte slip silently corrupts fees, rates, or merchant IDs;
- Verification requires checksum totals against the file header,
  which can't be reproduced without authentic content.

The right move is to wait for **scheme-published synthetic test data**
(Visa V.I.P., Mastercard PUF) when the first cards pilot is signed,
calibrate the parsers against those, and only then push to production.
Until then `cards_loaders/visa_base_ii.py` and
`cards_loaders/mastercard_ipm.py` raise `NotImplementedError` with a
pointer to this doc.

For schemes that publish CSV (most West African switches),
`cards_loaders/csv_generic.py` rides the existing BYO format profile
machinery — that's already callable from tests.

## Schema contracts

Two tables, both in `db.py`:

| Table | Purpose | Notable rules |
|---|---|---|
| `card_settlement_files` | One row per ingested file. Carries scheme, role, dates, file_id, total. | sha256 UNIQUE so re-ingesting the same file is idempotent (matches existing `ingested_files` pattern). |
| `card_settlement_records` | Individual cleared transactions. | Composite UNIQUE on `(file_id, record_index)`; indexes on `scheme_ref`, `settlement_date`, `pan_last4`, `recon_status` to support the matching engine and operator queries. |

PCI-relevant columns:
- `pan_first6`, `pan_last4` — both nullable (some formats don't expose
  even the truncated parts; that's fine).
- No column accepts a full PAN. There's no schema path to insert one.

## Matching engine extension

The existing 4-tier nostro engine doesn't translate directly to cards
because the join keys differ. A separate `cards_engine.py` will
handle:

- **Issuer 3-way match**: authorization → clearing → settlement on
  `scheme_ref` (TRR/ARN/Banknet ref).
- **Acquirer 2-way match**: scheme settlement → merchant disbursement,
  joined on merchant_id + scheme_ref.
- **Fee reconciliation**: comparing recorded `fee_total` against the
  bank's expected interchange + scheme fee schedule per MCC + region.

For the pilot we only need the first one. Acquirer and fee recon are
v2.

## What this foundation pass shipped

- Schema for `card_settlement_files` + `card_settlement_records`
  (db.py, post-pilot migration block adds the columns to existing
  installs).
- `pci_safety.py` — Luhn-validated PAN detector, `mask_pan`,
  `redact_pan`, `refuse_if_pan`, `refuse_if_sad_present`,
  `RefusedPanError`.
- `cards_loaders/__init__.py` — package contract and ParsedFile
  NamedTuple.
- `cards_loaders/visa_base_ii.py` — stub with calibration
  requirements documented.
- `cards_loaders/mastercard_ipm.py` — stub with calibration
  requirements documented.
- `cards_loaders/csv_generic.py` — generic-CSV adapter that turns
  BYO-loaded transactions into card_settlement_records shape, with
  PCI redaction at the seam.
- This document.

## What's NOT in this foundation pass

- Cards-side ingest endpoint (`POST /cards/files`).
- Cards UI (a `/cards` page analogous to `/mobile-money`).
- 3-way matching engine.
- Chargeback / dispute workflow.
- Operator dashboard's cards tile.
- The actual Visa Base II / Mastercard IPM byte parsers — see "Why
  parsers aren't built yet" above.

These are deliberate cuts; pick them up when the first cards pilot
provides a real file (and ideally, a Visa V.I.P. or Mastercard PUF
synthetic file alongside).
