# Kilter — Pitch (Pilot Bank)

**Audience:** Treasury / Operations / Payment & Settlement Control leadership at a correspondent-banking-active bank.
**Length:** ~12 minutes + live demo.
**Format:** 12 slides. One idea per slide. Commercial terms, industry benchmarks, competitive positioning, and design-partner pricing are pre-filled. Per-meeting items (prospect bank, date, screenshots, pre-parsed sample files) are marked in-line — swap in before presenting.

---

## Slide 1 — Title

> **Kilter**
> Modern reconciliation across nostro, mobile money, and card-scheme settlements.
> Self-hosted · Audit-first · Format-agnostic (MT940/950, camt.053/054, mobile-money operator CSV, card-switch settlement files, any core GL extract).
>
> *Presented by Kilter · For \[prospect bank] · \[meeting date]*

**Talk track (20 s):** "Kilter is a reconciliation platform for banks and payments businesses. It started in nostro, but it's now three reconciliation streams in one: nostro, mobile money, and card scheme settlements. Thirty minutes from now you'll know whether it's worth running for one month on one of your accounts — your choice of stream."

---

## Slide 2 — The problem every payments-active bank has

Four pressures most banks are feeling at once:

- **Reconciliation tooling is fragmented.** Nostro runs on a 1990s-era product still common at mid-tier banks. Mobile-money runs on Excel. Card-switch settlements run on a third tool, often the switch vendor's. **Three teams, three tools, three audit trails.**
- **Month-end close is still manual.** Ops exports from your core, pulls SWIFT statements, eyeballs matches in Excel. Industry practitioners commonly report **40–80 hours per cycle** per stream just to get the books to reconcile.
- **Audit cannot trace decisions.** When finance asks *"who cleared this item on the 14th?"* the answer is a spreadsheet filename and a person's memory — and that's per stream, three times over.
- **Cards put more of the bank in PCI scope every year.** Each tool that touches a settlement file with PAN data drags a new system into scope. The compliance bill compounds.

**Talk track (60 s):** "Most banks aren't reconciling one thing — they're reconciling three: nostro, mobile money, and cards. They're using three different tools, three audit trails, and three different teams. We compressed all three into one platform with one operator UI, one audit log, and a PCI-DSS-out-of-scope storage posture for the cards module so card recon doesn't pull more of your stack into compliance scope."

---

## Slide 3 — What Kilter is, in one sentence

**One platform that pairs the bank-side ledger against the counter-party stream — SWIFT, mobile-money operator, or card scheme — proposes matches in tiers, and lets ops confirm or reject with every click logged.**

Three reconciliation streams, one operator UI:

| Stream | What you drop in | Bank-side counterpart |
|---|---|---|
| **Nostro / GL** | MT940 / MT950 / camt.053 / camt.054 | Flexcube / T24 / Finacle GL extract |
| **Mobile money** | Operator CSV (M-Pesa Paid In/Withdrawn, MTN MoMo B2W & W2B, Airtel Money, Telcel Cash) | Bank wallet account GL |
| **Card scheme** | Visa / Mastercard / Verve / GhIPSS settlement file (TSV with pre-masked PAN) | Bank issuing or acquiring GL |

Four user-visible parts (same for all three streams):

1. **Intake** — drop the file or let the watched folder pick it up. SHA-256 dedup; one file is one session.
2. **Review queue** — ranked candidates per unmatched transaction; confirm in one click, keyboard-friendly.
3. **Scoped dashboard** — each user picks the access areas they work in (a branch, a cluster, or all).
4. **Export + audit** — xlsx report for finance, immutable audit log for compliance, separate match-groups CSV for the cards stream.

---

## Slide 4 — The matching engine (this is the "smart" part)

Four tiers, strongest first. The engine **proposes**; the human **decides**.

| Tier | What matches | Example |
|------|--------------|---------|
| **1 · Strict** | Reference + amount + date | 1:1, no judgement call |
| **2 · Ref hit, amounts differ** | Same ref, amount off by fees/FX | Wire fee deduction |
| **3 · Amount + date** | No ref, but same amount same day | Statement without a narrative |
| **4 · Amount, date ±1 day** | Same amount, next-day booking | Weekend / cut-off timing |

Nothing auto-matches without a human confirm. Tier 1 still needs one click — but it's *one* click, not a forensic exercise.

**Talk track (60 s):** "The engine isn't replacing the ops team. It's doing the *searching* — the part that burns hours — so the team can do the *deciding* — the part that actually needs judgement."

---

## Slide 5 — Format support (why this plugs into your bank)

**What you drop in — across all three streams:**

| Stream | Source | Formats supported |
|---|---|---|
| Nostro | Correspondent statements (SWIFT FIN) | MT940 end-of-day, MT950 statement |
| Nostro | Correspondent statements (SWIFT MX / ISO 20022) | camt.053 (EoD), camt.054 (intraday) — including SWIFT Alliance Access envelopes |
| Nostro | Core-banking GL | Flexcube `.xlsx` export (direct Oracle pull optional); T24 / Finacle / Equation via column-mapping wizard |
| Mobile money | Operator B2W / W2B feed | M-Pesa Safaricom statement, MTN MoMo (agent + B2W + W2B), Airtel Money agent, Telcel Cash organisation statement (xlsx native) |
| Cards | Issuer / acquirer settlement | Switch tab-separated reports with pre-masked PAN; GhIPSS / Cardlink / NIBSS Verve CSV |
| Cards | Scheme clearing | Visa Base II + Mastercard IPM parsers stubbed pending V.I.P. / PUF synthetic samples (a deliberate cut — see CARDS_DESIGN.md) |
| Any | Custom CSV / xlsx | BYO format profile machinery — column-mapping wizard, no code changes |

**What comes out:** daily breaks workbook + month-end certificate (nostro), match-groups CSV with mismatched-first sort (cards), per-record drill-in CSV exports — all in formats your ops team already recognises.

*\[FILL: if you've already run the parser against the prospect's own file samples, cite it here — "Parsed 7 of your camt.053 files on receipt, zero errors. Card-switch settlement file ingested 2,000 rows in 1.4 s, all PCI-safe."]*

---

## Slide 6 — The review experience

*\[Screenshot placeholder: review queue card — tier banner, SWIFT side vs GL side, Confirm/Reject buttons. Capture from the live Kilter instance before presenting; see /review.html after processing at least one session.]*

- One candidate at a time — no Excel scrolling.
- **Confirm / Reject / Skip** on keyboard.
- "Swap" — if the engine's top pick is wrong, the next-ranked candidate surfaces without re-running.
- "Queue cleared → Download reconciliation xlsx" when the day is done.

---

## Slide 7 — Access-area scoping (for the "just my branch" crowd)

*\[Screenshot placeholder: topbar "Active area" picker open. Capture from the live Kilter instance — click the area picker in the header.]*

- Top-bar picker on every page. Single area (most ops) or multi-select (regional supervisors).
- Scope filters Dashboard / Cash accounts / Sessions — users only see work relevant to them.
- Areas are configurable: branch codes, business lines, currency pools — whatever your bank's structure needs.

---

## Slide 7b — Cards module: PCI scope reduction by design

The cards stream is built to keep your reconciliation system **out of
PCI-DSS storage scope**. Three architectural commitments:

1. **Full PAN never persists.** Loaders mask at the seam into
   `pan_first6` + `pan_last4` only. The schema has no column that
   could hold a full PAN — there's no path to insert one.
2. **Sensitive Authentication Data is refused.** CVV, track data, PIN
   blocks — DSS §3.2 — get rejected at ingest. Settlement files
   shouldn't include them; this is defence in depth for the day a bank
   ops team accidentally exports from the wrong system.
3. **Free-text fields are scanned and redacted.** Merchant names and
   notes get a Luhn-validated PAN sweep on the way in. Any embedded
   full PAN becomes `first6***last4` before persistence.

**Why this matters commercially:** every reconciliation tool in your
stack that touches a settlement file with full-PAN data drags that
system into PCI-DSS scope. Kilter doesn't. One less system in scope
means a smaller compliance bill at your next QSA review.

The N-way matching engine joins on `scheme_ref` (Visa TRR, Mastercard
Banknet, switch RRN) across files — auth, clearing, settlement — and
classifies each group as matched / mismatched / unmatched. Operator-set
states (`disputed`, `written_off`) win over the engine's classification.

---

## Slide 7c — Mobile money: bank-to-wallet and wallet-to-bank

Across Africa, mobile-money operator integrations have outgrown the
Excel sheet. A typical mid-tier bank now reconciles:

- **Bank-to-wallet (B2W)** — outbound pulls from the bank's settlement
  account to wallet customers (salary disbursement, supplier payments).
- **Wallet-to-bank (W2B)** — inbound pushes from wallet customers to
  bank accounts (bill pay, top-ups).

Both produce daily operator-side CSVs that need to reconcile to the
bank's GL. Different formats per operator; some include FX columns,
some don't; column counts shift between releases.

**Kilter ships pre-seeded operator profiles:** M-Pesa Safaricom (two-
column Paid In / Withdrawn shape), MTN MoMo (separate B2W and W2B
profiles with `_extra` passthrough for the wide-format FX columns),
Airtel Money (CR/DR column convention), Telcel Cash (two-column
organisation-statement shape, xlsx native). Bind a profile to a wallet
account in the BYO formats UI and start ingesting same day.

**Talk track (45 s):** "If you're already reconciling MTN MoMo or
Telcel Cash by hand or with a vendor add-on, the saving here is direct.
Same matching engine, same audit trail, same operator UI as your nostro
team is already using. The pilot extends naturally — start on nostro,
add MoMo in week 3."

---

## Slide 8 — Security, roles, audit

**Four roles, principle of least privilege:**
- `admin` — everything, including ingest and user management
- `ops` — reconcile, confirm/reject, export
- `audit` / `internal_control` — read-only access to the activity log and exports

**Every action is logged:** every login, every match decision, every export, every scope change, every account registration. Same `audit_log` table powers the Activity page and the CSV auditors download.

**MFA:** TOTP, compatible with Microsoft Authenticator / Google Authenticator / Authy. Enrollment via one-time token, no SMS dependency.

**Deployment:** **self-hosted on your infrastructure**. No nostro data leaves your perimeter. Windows or Linux, SQLite default (hundreds of millions of rows on one SSD), swap for Postgres if you need HA.

---

## Slide 9 — Proof: show me it works on my files

Most vendors show you a polished demo on their own data. We invert it.

**Send us 2–3 of your real SWIFT messages (MT940/950 or camt.053/054) and one GL extract. The converter runs in 1–2 minutes per file — you get back a readable xlsx with every field parsed, before this meeting ends, before you sign anything, before a pilot scope is agreed.**

Why that matters: the parser is where every reconciliation pilot dies. SWIFT Alliance Access envelopes, PRCD-instead-of-OPBD balances, missing `BookgDt` on camt.054 notifications, the idiosyncratic narration fields every core-banking extract produces — Kilter's loaders already handle them. If they don't handle something in *your* files, you'll know in minutes, not in month three of an implementation.

**What's been parsed already (Kilter's test corpus today):**

| Format | Files | Transactions | Notes |
|---|---:|---:|---|
| MT940 (end-of-day) | 221 | 1,033 | Real correspondent statements, multi-currency |
| MT950 (statement) | 14 | 123 | Including fee-only statements |
| camt.053 (ISO 20022 EoD) | 3 | 105 | Including SWIFT Alliance Access `Saa:DataPDU` envelopes |
| camt.054 (intraday notify) | 3 | 3 | Balance-less notifications |

*\[Presenter note: hand the prospect the `readable_messages.xlsx` export as a takeaway — live evidence the parser is real, not slideware.]*

**Launch-customer terms:** Kilter is selecting a small number of design-partner banks for the first production deployments. In exchange for being named as a reference at go-live *and* a 30-minute weekly feedback session during the pilot, design partners get **50% off year-one licensing and 25% off year-two** — locked in at sign-up, not revisited at renewal.

**Talk track (45 s):** "Here's what every vendor in this category can't do: take your file *right now* and show you a working parse before this meeting ends. We can — the converter runs in 1–2 minutes per file because the parser is built and tested against real-world message messiness, not just the ISO spec. If you've brought sample files, let's run it now."

---

## Slide 10 — The pilot we're proposing

Low-risk, parallel-run, reversible. Here's the shape:

**Scope (week 1–2):** 1 nostro account · 1 currency · 1 month of daily statements.
**Optional extension (week 3–4):** add a mobile-money operator feed *or* a card-switch settlement file to demonstrate multi-stream value on your data — same operator UI, same audit log, same pilot terms.
**Runs in parallel** with your existing tool(s) — no workflow change, no migration risk.
**Ops commitment:** one champion who works the review queue ~30 min/day during the pilot window.
**Our commitment:** on-site setup, format validation against your actual files (including your mobile-money operator's CSV shape and your card switch's settlement format), weekly check-in, full audit log for your internal control / audit team.

**What we measure together:**
1. Tier 1 auto-match rate on *your* volume (nostro stream).
2. Match-group resolution rate (cards stream, if exercised).
3. Cycle-time delta on *your* close, across all enabled streams.
4. Break ageing — how long items stay unmatched compared to today.

**Commercial:**

- **Pilot — free of charge.** Your investment is your ops champion's ~30 min/day during the pilot window plus a weekly 30-minute feedback call with the Kilter team. No licence fee, no setup charge, no data-migration bill.
- **Production — per-nostro-account annual licence.** Unlimited users, unlimited transaction volume within the licensed accounts. Indicative range **USD $4,000–$8,000 per nostro account per year**, tiered by total account count (lower price at higher volumes). Firm quote issued once pilot scope is agreed.
- **Deployment — included.** On-prem installation on your infrastructure, 12 months of standard support, and unlimited parser updates covering new correspondent bank formats.

---

## Slide 11 — Why Kilter vs. the incumbents

| | SmartStream TLM / IntelliMatch | Trintech Cadency | Legacy MT-only tool | **Kilter** |
|---|---|---|---|---|
| Deployment | Cloud / hosted | Cloud-first | On-prem (often EoL) | **Self-hosted on your infra** |
| Data residency | Theirs | Theirs | Yours | **Yours** |
| Audit trail | Add-on module | Yes | Partial | **Default, immutable** |
| Modern SWIFT MX (camt.053/054) | Yes | Yes | **No** | **Yes** |
| Mobile-money operator feeds | Custom add-on | Custom add-on | **No** | **Pre-seeded profiles for major operators** |
| Card-scheme settlements | Separate product | Separate product | **No** | **Same platform, PCI-safe** |
| PCI-DSS storage scope | Vendor-dependent | Vendor-dependent | **Out** | **Out (first6+last4 only, never full PAN)** |
| Time-to-pilot | 3–6 months | 2–4 months | n/a | **~2 weeks from files-in-hand** |
| Pricing shape | 6- / 7-figure annual | 6-figure annual | Maintenance only | **Per-account annual, mid-5 figures typical, all streams included** |

We're not trying to be a like-for-like SmartStream replacement. We're the answer for a bank that wants **modern workflow + on-prem data residency + audit trail without an add-on SKU + one operator UI across nostro, mobile money, and cards instead of three separate tools.**

---

## Slide 12 — Ask

Three things, in order:

1. **Share 2–3 real camt.053 / MT940 files and one GL extract.** We parse them live during the meeting — 1–2 minutes per file — and you walk out with a read-only readable xlsx.
2. **Scope the pilot.** Pick 1 account / 1 month. We'll draft the pilot plan within a week of receiving the files.
3. **Commit a go/no-go review date — 6 weeks from pilot start.** If the tier-1 auto-match rate, cycle-time, and audit trail meet the success criteria agreed at week 0, we move to production scope. If not, the pilot ends and your workflow is unchanged.

> **Demo next.** Live walkthrough of the review queue, ingest, and audit log. (~10 minutes.)

---

## Appendix — Things you can hand them after the meeting

- One-page format-compatibility sheet (this deck's slide 5, pulled out).
- Security & compliance one-pager (deployment topology, data-residency statement, MFA details).
- Pilot-plan template (scope definition, success criteria, timeline, roles).
- The `readable_messages.xlsx` parsed from their sample files, if they provided any during the meeting.
