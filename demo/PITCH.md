# Kilter — Pitch

**Audience:** mixed (ops + leadership)
**Length:** ~10 minutes + live demo
**Format:** 10 slides. One idea per slide. Numbers marked **\[FILL]** are placeholders — swap in your real figures before presenting.

---

## Slide 1 — Title

> **Kilter**
> A modern replacement for Corona 7.9 reconciliation.
>
> Built in-house · Runs on our stack · Audit trail by default.
>
> *\[Presenter name · Date]*

**Talk track (20 s):** "For 15+ years our month-end reconciliation has depended on Corona 7.9. Kilter is the purpose-built replacement — same job, 2026-era UX, and every action leaves an audit trail."

---

## Slide 2 — Why now

Three pressures converging:

- **Corona 7.9 is end-of-life** — no vendor support, no Windows 11 hardening, UI unchanged since its launch.
- **Month-end reconciliation is manual** — ops exports from Flexcube, pulls SWIFT messages, eyeballs matches in Excel. **\[FILL: hours per cycle]**.
- **Audit has no breadcrumbs** — when finance asks "who cleared this item on the 14th?" the answer is a spreadsheet filename.

**Talk track (45 s):** "The problem isn't Corona itself — it's that Corona plus manual Excel stitching is how we actually close the books. That's three risks in one: a tool we can't patch, hours we can't reclaim, and decisions we can't trace."

---

## Slide 3 — What Kilter does

One sentence: **Pairs SWIFT messages with Flexcube transactions, proposes matches in tiers, and lets ops confirm or reject — with every click logged.**

Four user-visible parts:
1. **Intake** — drop the SWIFT `.out` and Flexcube `.xlsx` files, or let the scanner auto-ingest from `messages/`.
2. **Review queue** — ranked candidates per unmatched transaction; confirm in one click.
3. **Scoped dashboard** — each user picks the access areas they work in (a branch, a cluster, or all).
4. **Export + audit** — xlsx report for finance, full audit log for compliance.

---

## Slide 4 — The matching engine (this is the "smart" part)

Four tiers, strongest first:

| Tier | What matches | Example |
|------|--------------|---------|
| **1 · Strict** | Reference + amount + date | 1:1, no judgement call |
| **2 · Ref hit, amounts differ** | Same ref, amount off by fees/FX | Usually a wire fee deduction |
| **3 · Amount + date** | No ref, but same amount on same day | Statement without a narrative |
| **4 · Amount, date ±1 day** | Same amount, next-day booking | Weekend / cut-off timing |

What ops still does: **decides** on tier 2-4. What the engine does: **proposes** and ranks. Nothing auto-matches without a human confirm.

**Talk track (60 s):** "The engine isn't doing the ops team's job — it's doing the *searching* so ops can do the *deciding*. Tier 1 is the 'free' matches. Tier 2-4 is where judgement lives, and that's exactly the work we want humans on."

---

## Slide 5 — The review experience

*\[Screenshot placeholder: review.html queue card showing tier banner, SWIFT side vs Flexcube side, Confirm/Reject buttons]*

- One candidate at a time — no Excel scrolling.
- **Confirm / Reject / Skip** keyboard-friendly.
- "Swap" — if the engine's top pick is wrong, surface the next candidate without re-running.
- "Queue cleared → Download reconciliation xlsx" when you're done.

---

## Slide 6 — Access-area scoping (for the "I just need my branch" crowd)

*\[Screenshot placeholder: topbar "Active area: BANK OF GHANA ▾" control open]*

- Top-bar picker, present on every page.
- Single area (most common) or multi-select (regional supervisors).
- Scope filters Dashboard / Cash accounts / Sessions — users only see work relevant to them.
- Backed by the same taxonomy Corona uses (103 areas seeded from Corona 7.9).

**Talk track (30 s):** "Ops asked: 'I only work on Branch 001 — don't show me 102 other areas.' Done. Leadership asked: 'I want to see the whole bank.' Leave it on All areas — same screen, different scope."

---

## Slide 7 — Roles & audit

**Four roles:**
- `admin` — everything, including ingest and user management.
- `ops` — reconcile, confirm/reject, export.
- `audit` / `internal_control` — read-only access to the activity log and exports.

**Everything is logged:**
- Every login, every decision, every export, every scope change, every account registration.
- Same `audit_log` table powers the Activity page and the CSV download auditors get.

**MFA:** TOTP, Microsoft Authenticator compatible. Enrollment via one-time token.

---

## Slide 8 — What the numbers look like

*\[FILL with your pilot data before presenting. Example shape:]*

| Metric | Corona + Excel today | Kilter pilot |
|---|---:|---:|
| Month-end close cycle | **\[FILL] hrs** | **\[FILL] hrs** |
| Items auto-proposed (tier 1) | 0 | **\[FILL] %** |
| "Who cleared this?" answered in | *minutes, if logged* | **< 1 second** |
| Tool support contract | *end-of-life* | **in-house** |

If you don't have pilot numbers yet, replace this slide with: **"Pilot plan: 1 currency, 1 branch, 1 month. Measure before-and-after on the table above."**

---

## Slide 9 — What's built vs. what's next

**Built and working today:**
- Intake (manual upload + auto-scan), tiered matching engine, review queue, xlsx export.
- MFA login, four roles, audit log, activity export.
- Access-area scoping, discovered-accounts inbox.

**Next (in priority order):**
1. **Pilot** — 1 branch, 1 month, parallel-run against Corona.
2. **RBAC for access areas** — admins restrict which areas a user *can* pick (today it's convenience-only).
3. **Scheduled scan** — replace the "admin clicks Scan" with a cron.
4. **AD/LDAP integration** — retire the local user table once IT provisions a service account.

---

## Slide 10 — Ask

Three things, in order:

1. **Green-light a 1-branch / 1-month pilot** — low-risk, parallel to Corona, measures the numbers on slide 8.
2. **Name an ops champion** — the person who lives in the review queue during the pilot.
3. **Commit a go/no-go review date** — *\[FILL: ~6 weeks from pilot start]*.

> **Demo next.** (See `demo/SCRIPT.md` for the walkthrough.)
