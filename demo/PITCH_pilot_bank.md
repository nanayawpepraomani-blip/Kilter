# Kilter — Pitch (Pilot Bank)

**Audience:** Treasury / Operations / Payment & Settlement Control leadership at a correspondent-banking-active bank.
**Length:** ~12 minutes + live demo.
**Format:** 12 slides. One idea per slide. Commercial terms, industry benchmarks, competitive positioning, and design-partner pricing are pre-filled. Per-meeting items (prospect bank, date, screenshots, pre-parsed sample files) are marked in-line — swap in before presenting.

---

## Slide 1 — Title

> **Kilter**
> Modern nostro + GL reconciliation.
> Self-hosted · Audit-first · Format-agnostic (MT940/950, camt.053/054, any core GL extract).
>
> *Presented by Kilter · For \[prospect bank] · \[meeting date]*

**Talk track (20 s):** "Kilter is a nostro-and-GL reconciliation platform built for correspondent-banking treasuries. Thirty minutes from now you'll know whether it's worth running for one month on one of your accounts."

---

## Slide 2 — The problem every correspondent treasury has

Three pressures most banks are feeling at once:

- **Your reconciliation tool is aging.** Most banks still run Corona 7.9 or an equivalent late-90s product. No vendor support, no Windows 11 hardening, UI that assumes a 1024×768 screen.
- **Month-end close is still manual.** Ops exports from your core, pulls SWIFT statements, eyeballs matches in Excel. Industry practitioners commonly report **40–80 hours per cycle** for a mid-size FX desk just to get the books to reconcile.
- **Audit cannot trace decisions.** When finance asks *"who cleared this item on the 14th?"* the answer is a spreadsheet filename and a person's memory.

**Talk track (45 s):** "We didn't invent these pressures — they're industry-wide. What we built is the answer that fits a bank that wants something modern but doesn't want to hand its nostro data to SmartStream's cloud."

---

## Slide 3 — What Kilter is, in one sentence

**Pairs SWIFT messages with core-banking GL transactions, proposes matches in tiers, lets ops confirm or reject — with every click logged.**

Four user-visible parts:

1. **Intake** — drop SWIFT `.out` or `.xml` and the core-banking `.xlsx` into a folder. The scanner picks up, parses, routes.
2. **Review queue** — ranked candidates per unmatched transaction; confirm in one click, keyboard-friendly.
3. **Scoped dashboard** — each user picks the access areas they work in (a branch, a cluster, or all).
4. **Export + audit** — xlsx report for finance, immutable audit log for compliance.

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

**What you drop in:**

| Source | Formats supported |
|---|---|
| Correspondent statements (SWIFT FIN) | MT940 end-of-day, MT950 statement |
| Correspondent statements (SWIFT MX / ISO 20022) | camt.053 (EoD), camt.054 (intraday) |
| Core-banking GL | Flexcube `.xlsx` export (direct Oracle pull optional) |
| Other cores (T24, Finacle, Equation) | Extract to xlsx; mapping is a config file, not a rewrite |

**What comes out:** daily breaks workbook + month-end certificate in the format your ops team already recognises.

*\[FILL: if you've already run the parser against the prospect's own file samples, cite it here — "Parsed 7 of your camt.053 files on receipt, zero errors."]*

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

**Scope:** 1 nostro account · 1 currency · 1 month of daily statements.
**Runs in parallel** with your existing tool — no workflow change, no migration risk.
**Ops commitment:** one champion who works the review queue ~30 min/day during the pilot window.
**Our commitment:** on-site setup, format validation against your actual files, weekly check-in, full audit log for your internal control / audit team.

**What we measure together:**
1. Tier 1 auto-match rate on *your* volume.
2. Cycle-time delta on *your* close.
3. Break ageing — how long items stay unmatched compared to today.

**Commercial:**

- **Pilot — free of charge.** Your investment is your ops champion's ~30 min/day during the pilot window plus a weekly 30-minute feedback call with the Kilter team. No licence fee, no setup charge, no data-migration bill.
- **Production — per-nostro-account annual licence.** Unlimited users, unlimited transaction volume within the licensed accounts. Indicative range **USD $4,000–$8,000 per nostro account per year**, tiered by total account count (lower price at higher volumes). Firm quote issued once pilot scope is agreed.
- **Deployment — included.** On-prem installation on your infrastructure, 12 months of standard support, and unlimited parser updates covering new correspondent bank formats.

---

## Slide 11 — Why Kilter vs. the incumbents

| | SmartStream TLM / IntelliMatch | Trintech Cadency | Corona 7.9 | **Kilter** |
|---|---|---|---|---|
| Deployment | Cloud / hosted | Cloud-first | On-prem (EoL) | **Self-hosted on your infra** |
| Data residency | Theirs | Theirs | Yours | **Yours** |
| Audit trail | Add-on module | Yes | Partial | **Default, immutable** |
| Modern SWIFT MX (camt.053/054) | Yes | Yes | **No** | **Yes** |
| Time-to-pilot | 3–6 months | 2–4 months | n/a | **~2 weeks from files-in-hand** |
| Pricing shape | 6- / 7-figure annual | 6-figure annual | Maintenance only | **Per-account annual, mid-5 figures typical** |

We're not trying to be a like-for-like SmartStream replacement. We're the answer for a treasury that wants **modern workflow + on-prem data residency + an audit trail that does not need an add-on SKU.**

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
