# Kilter — Pitch

**Audience:** mixed (ops + leadership)
**Length:** ~10 minutes + live demo
**Format:** 10 slides. One idea per slide. Numbers marked **\[FILL]** are placeholders — swap in your real figures before presenting.

---

## Slide 1 — Title

> **Kilter**
> A modern replacement for the legacy reconciliation stack — and more.
>
> Same nostro recon, plus mobile-money and card-scheme reconciliation in one platform.
>
> Self-hosted · Vendor-neutral · Audit trail by default.
>
> *\[Presenter name · Date]*

**Talk track (20 s):** "Most mid-tier banks still run a 1990s reconciliation tool — end-of-life, no vendor support, MT-only. Kilter is the purpose-built replacement: same job, 2026-era UX, every action audited. The same platform also handles mobile-money operator feeds (MTN MoMo, Telcel Cash, M-Pesa, Airtel) and card-switch settlement files. One operator UI, one audit log, three reconciliation streams."

---

## Slide 2 — Why now

Three pressures converging:

- **The legacy reconciliation tool is end-of-life** — no active vendor support, no Windows 11 hardening, UI unchanged since its launch.
- **Month-end reconciliation is manual** — ops exports from the core (Flexcube / T24 / Finacle / equivalent), pulls SWIFT messages, eyeballs matches in Excel. **\[FILL: hours per cycle]**.
- **Audit has no breadcrumbs** — when finance asks "who cleared this item on the 14th?" the answer is a spreadsheet filename.

**Talk track (45 s):** "The problem isn't the legacy tool itself — it's that legacy tool plus manual Excel stitching is how books actually close. That's three risks in one: a tool you can't patch, hours you can't reclaim, and decisions you can't trace."

---

## Slide 3 — What Kilter does

One sentence: **Pairs the bank-side ledger against the counter-party stream — SWIFT, mobile-money operator, or card scheme — proposes matches in tiers, and lets ops confirm or reject with every click logged.**

Three streams, same interface:

| Stream | Source | Bank-side |
|---|---|---|
| **Nostro / GL** (legacy-tool replacement) | MT940 / MT950 / camt.053 / camt.054 | Core-banking GL extract (xlsx / CSV / DB) |
| **Mobile money** | MTN MoMo (B2W & W2B), Telcel Cash, Airtel, M-Pesa CSV | Wallet account on the core |
| **Card scheme** | Switch settlement TSV with masked PAN; Visa/Mastercard binary stubbed pending scheme samples | Issuing / acquiring GL on the core |

Four user-visible parts (same for all three):
1. **Intake** — drop the file or let the scanner auto-ingest from `messages/` (or for cards, `POST /cards/files`).
2. **Review queue** — ranked candidates per unmatched transaction; confirm in one click.
3. **Scoped dashboard** — each user picks the access areas they work in (a branch, a cluster, or all).
4. **Export + audit** — xlsx report for finance, full audit log for compliance, match-groups CSV for cards.

---

## Slide 4 — The matching engine (this is the "smart" part)

Four tiers, strongest first:

| Tier | What matches | Example |
|------|--------------|---------|
| **1 · Strict** | Reference + amount + date | 1:1, no judgement call |
| **2 · Ref hit, amounts differ** | Same ref, amount off by fees/FX | Usually a wire fee deduction |
| **3 · Amount + date** | No ref, but same amount on same day | Statement without a narrative |
| **4 · Amount, date ±1 day** | Same amount, next-day booking | Weekend / cut-off timing |

What ops still does: **decides** on tier 2-4. What the engine does: **proposes** and ranks.

**Auto-confirmation rules:** admins can define priority-ordered rules — "auto-confirm tier-1 matches where amount is exact and references overlap" — and the engine applies them silently at ingest. Ops only sees what the rules can't resolve. Rules are audited like every other action.

**Talk track (60 s):** "The engine isn't doing the ops team's job — it's doing the *searching* so ops can do the *deciding*. Tier 1 is the 'free' matches. With auto-rules turned on, those tier-1 matches don't even reach the queue — they confirm themselves and the ops team opens to a smaller, harder set where judgement is genuinely needed."

---

## Slide 5 — The review experience

*\[Screenshot placeholder: review.html queue card showing tier banner, SWIFT side vs Flexcube side, Confirm/Reject buttons]*

- One candidate at a time — no Excel scrolling.
- **Confirm / Reject / Skip** keyboard-friendly.
- "Swap" — if the engine's top pick is wrong, surface the next candidate without re-running.
- "Queue cleared → Download reconciliation xlsx" when you're done.

---

## Slide 6 — Access-area scoping (for the "I just need my branch" crowd)

*\[Screenshot placeholder: topbar "Active area: ▾" control open]*

- Top-bar picker, present on every page.
- Single area (most common) or multi-select (regional supervisors).
- Scope filters Dashboard / Cash accounts / Sessions — users only see work relevant to them.
- Areas are configurable per deployment — import the taxonomy from your existing tool or define your own.

**Talk track (30 s):** "Ops asked: 'I only work on Branch 001 — don't show me 102 other areas.' Done. Leadership asked: 'I want to see the whole bank.' Leave it on All areas — same screen, different scope."

---

## Slide 7 — Roles, controls & audit

**Four roles:**
- `admin` — everything, including ingest and user management.
- `ops` — reconcile, confirm/reject, export.
- `audit` / `internal_control` — read-only access to the activity log and exports.

**Two-person approval gate (optional):** when enabled, an ops decision moves to `pending_approval` before confirming. A second user with `admin` or `internal_control` role approves or rejects. Self-approval is blocked by the system. Configurable per deployment — no code change required.

**Session immutability:** once a month-end certificate is signed off, the underlying session locks. No one — not even an admin — can alter match decisions after sign-off without a fresh session.

**Everything is logged, immutably:** every login, decision, approval, export, scope change, and account registration. The audit log table has database-level triggers blocking deletes and updates — immutable by construction, not by policy.

**MFA:** TOTP, Microsoft Authenticator compatible. Enrollment via one-time token. Single-use recovery codes issued at enrollment — a lost phone doesn't lock users out permanently. Replay prevention: each code is accepted once within its 30-second window.

---

## Slide 8 — What the numbers look like

*\[FILL with your pilot data before presenting. Example shape:]*

| Metric | Legacy tool + Excel today | Kilter pilot |
|---|---:|---:|
| Month-end close cycle | **\[FILL] hrs** | **\[FILL] hrs** |
| Items auto-proposed (tier 1) | 0 | **\[FILL] %** |
| "Who cleared this?" answered in | *minutes, if logged* | **< 1 second** |
| Tool support contract | *end-of-life* | **in-house** |

If you don't have pilot numbers yet, replace this slide with: **"Pilot plan: 1 currency, 1 branch, 1 month. Measure before-and-after on the table above."**

---

## Slide 9 — What's built vs. what's next

**Built and working today:**
- **Nostro / GL stream:** intake (manual + auto-scan), 4-tier matching engine with admin-configurable auto-confirmation rules, review queue, xlsx export, daily breaks workbook, month-end certificate with maker/checker sign-off, session locking after sign-off, statement opening/closing balance validation at ingest.
- **Mobile-money stream:** five pre-seeded operator profiles (M-Pesa, Telcel Cash, MTN MoMo agent + B2W + W2B, Airtel Money), wallet-account intake, dedicated `/mobile-money` view.
- **Cards stream:** PCI-safe ingest (`/cards/files`), N-way matching engine on `scheme_ref`, auth/clearing/settlement stage tagging, `incomplete` status when required stages are missing, mismatched-first match groups view, CSV exports, switch settlement profile (Visa/Mastercard binary parsers stubbed pending scheme sample data).
- **Platform:** TOTP MFA with recovery codes + replay prevention, two-person approval gate (optional), four roles, immutable audit log (DB-level triggers), activity export, access-area scoping, BYO format wizard, 300 MB streaming uploads, AD/LDAP integration, MySQL + SQLite database support, SLA escalation with snooze/acknowledge on open items.

**Next (in priority order):**
1. **Pilot** — 1 branch, 1 month, parallel-run against the existing tool. Extend to mobile money or cards in week 3.
2. **Visa Base II + Mastercard IPM binary parsers** — pending V.I.P. / PUF synthetic sample data from scheme (see CARDS_DESIGN.md).
3. **Audit-log SIEM export** — forward `audit_log` to Splunk / Sentinel / Elastic for banks that require centralised security monitoring.

---

## Slide 10 — Ask

Three things, in order:

1. **Green-light a 1-branch / 1-month pilot** — low-risk, parallel to the existing tool, measures the numbers on slide 8.
2. **Name an ops champion** — the person who lives in the review queue during the pilot.
3. **Commit a go/no-go review date** — *\[FILL: ~6 weeks from pilot start]*.

> **Demo next.** (See `demo/SCRIPT.md` for the walkthrough.)
