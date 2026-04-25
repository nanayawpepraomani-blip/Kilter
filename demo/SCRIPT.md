# Kilter — Live Demo Script

**Audience:** mixed (ops + leadership). Stay concrete — show screens, not code.
**Length:** ~10 minutes.
**Environment:** local, `uvicorn app:app --reload` on `http://127.0.0.1:8000`.

---

## Pre-flight checklist (5 min before you start)

Do these in order. If any step fails, fix before presenting — don't improvise live.

- [ ] **Server running.** `uvicorn app:app --reload` → dashboard loads at `http://127.0.0.1:8000`.
- [ ] **Admin logged in in Tab 1.** You're on the Dashboard showing 4 sessions.
- [ ] **Ops user ready in Tab 2.** Sign in as `ops_demo` (TOTP via Authenticator). Leave the Dashboard open.
- [ ] **Active area set to "All areas"** on both tabs (topbar → Cancel if the picker is open).
- [ ] **One session with a pending queue item.** If everything is already cleared, upload a fresh SWIFT/Flex pair from `uploads/` so you have something to confirm live.
- [ ] **Browser zoom at 110%.** Makes the screen readable from the back of the room.
- [ ] **Close every unrelated tab.** The bookmark bar is visible in screenshots; hide it (`Ctrl+Shift+B`) if you prefer.
- [ ] **Recovery plan.** If a page breaks, switch to the other tab and narrate while you recover. Never refresh the presenter tab mid-demo.

---

## Timing map

| Beat | Duration | Scene |
|------|---------|-------|
| 0:00 | 0:45 | Scene 1 · The dashboard (admin) |
| 0:45 | 1:30 | Scene 2 · Ingest a pair |
| 2:15 | 2:30 | Scene 3 · Review queue — confirm, reject, swap |
| 4:45 | 1:15 | Scene 4 · Switch to ops — what role-based UI looks like |
| 6:00 | 1:30 | Scene 5 · Active-area picker (the "mixed audience" hook) |
| 7:30 | 1:30 | Scene 6 · Export xlsx + audit log |
| 9:00 | 1:00 | Close + Q&A |

---

## Scene 1 — The dashboard (0:00 – 0:45)

**Show:** Tab 1, logged in as admin, `/` dashboard.

**Point at, in this order:**
- Top four stat cards: *Pending decisions, Confirmed matches, Open sessions, Registered cash accounts*.
- Recent sessions table — 4 sessions, one per currency (GBP/USD/EUR/GHS).
- The sidebar: Workspace / Intake / Admin groups.

**Say it like this:**
> "This is the landing page. Four numbers that matter for month-end — what's pending, what we've cleared, how many sessions still have open work, and how many cash accounts are wired up. The table below is the last four reconciliation sessions. Each one is a SWIFT statement matched against a Flexcube account for one currency."

---

## Scene 2 — Ingest a pair (0:45 – 2:15)

**Show:** `/#upload` or drag into the Manual upload card.

**Do:**
1. Drag the SWIFT `.out` file into the SWIFT slot.
2. Drag the Flexcube `.xlsx` into the Flex slot.
3. Click Upload.
4. Wait for the status line → *"Session #5: Registered · N pending, X unmatched SWIFT, Y unmatched Flex."*

**Say it like this:**
> "Two files — SWIFT messages from the bank, and the Flexcube extract for the matching account. Kilter sha-256s both files so you can't accidentally ingest the same statement twice. Behind the scenes the matcher proposed pairs across four tiers — the ones it's sure about, and the ones that need a human. That's what we do next."

**If the upload fails** (duplicate hash, wrong format): switch to an existing session with `pending > 0` and narrate: *"Here's one already ingested — same flow from here on."*

---

## Scene 3 — Review queue (2:15 – 4:45) · **the money shot**

**Show:** Click **Review →** on a session with pending items.

**Do, narrating as you go:**

1. **Point at the tier banner** — *"This is a Tier 1 strict match. The engine has a reference number hit, amount matches, date matches. 99% of the time ops will click Confirm without thinking — that's the point."*
2. **Click Confirm.** The next card appears.
3. **Navigate to a Tier 2 or 3 card** (skip Confirms until you land on one). Say: *"Here's where humans come in. Same reference but the amount is off by 40 basis points — that's a wire fee. The engine surfaces it, ops makes the call."*
4. **Click Swap** (if the card offers alternatives) — *"If we don't like the top pick, the engine has ranked alternatives ready. No re-running the matcher."*
5. **Click Reject** on a bad candidate — *"And if it's actually wrong, reject. The SWIFT row goes back to unmatched; it shows up in the exception report."*
6. **Confirm a couple more** until you see **"✓ Queue cleared."** Point at the green card: *"That's what ops wants to see at the end of a session."*

**Watch for:** the "Download reconciliation xlsx" button on the cleared card. Don't click it yet — that's Scene 6.

---

## Scene 4 — Role-based UI (4:45 – 6:00)

**Show:** Switch to Tab 2 (logged in as `ops_demo`).

**Point at:**
- Sidebar — only **Workspace** group (Dashboard, Cash accounts, Sessions). No Intake, no Admin.
- Bottom-left: *Ops Demo User · ops*.
- Top of Dashboard — the same four stat cards as admin.

**Say it like this:**
> "Same app, different role. Ops sees the reconciliation tools and nothing else. No user management, no activity log, no manual upload — that's admin territory. We've got four roles total: admin, ops, audit, and internal control. Audit and IC can read the activity log; they can't touch a match."

---

## Scene 5 — Active-area picker (6:00 – 7:30) · **the mixed-audience hook**

**Show:** Top-right of the page — *Active area: All areas ▾*.

**Do:**
1. **Click the picker.** A panel opens with a search box and a checkbox list.
2. **Type "bank"** in the search — point at BANK OF GHANA filtering in.
3. **Check BANK OF GHANA, click Apply.** Page reloads.
4. Point at the top-right badge: now reads *Active area: BANK OF GHANA*.
5. Go to Sessions — only BoG sessions visible.
6. Go back to the picker, search "branch", check **BRANCH 001 HOFF** and **BRANCH 002 TEMA**, Apply.
7. Badge now reads *Active area: 2 areas* with a tooltip listing them.

**Say it like this (tailor to the room):**
> "For ops — most of you will pick one area when you log in and live there all day. For supervisors — you can pick several at once and see across your cluster. For leadership — leave it on All areas and you've got the whole bank. The picker is backed by the same 103 access areas Corona uses, so there's no new taxonomy to learn."

**Click Cancel / set back to All areas before moving on** — scene 6 needs the unscoped view.

---

## Scene 6 — Export + audit (7:30 – 9:00)

**Show:** Tab 1 (admin). Session detail page with the queue cleared.

**Do:**
1. **Click Export xlsx** in the topbar. File downloads.
2. **Open the xlsx** — point at the sheet names: *Matched / Unmatched SWIFT / Unmatched Flex / Balance*.
3. Close the xlsx. Sidebar → **Activity log**.
4. Point at the most recent rows — *login*, *session_create*, *decision*, *export*, *access_scope_change*.
5. Use the filters: **Action = export**, apply. Say: *"When audit asks 'who pulled a report in March', the answer is two clicks."*
6. Click **Export CSV** on the activity page. *"And the audit log itself exports — auditors get the raw trail."*

**Say it like this:**
> "Three things ship with every reconciliation session. The xlsx report for finance — same format they're already using. The session's audit trail — who matched what, when. And a running activity log across every user, every action. The question 'prove who did this' has a one-line answer now."

---

## Close (9:00 – 10:00)

**Back to the Dashboard. Say:**
> "That's the loop: ingest, review, export, audited. We built it on our stack, MFA by default, role-based, and scoped to the way each team actually works. What I'd like next is a green light to run a 1-branch, 1-month pilot in parallel with Corona — we measure the real numbers, and we come back with a go/no-go in six weeks."

**Pause. Invite questions.**

---

## If something breaks (live recovery)

| Symptom | Recovery |
|---|---|
| Page shows JSON (looks like `{"id": ...}`) | Wrong link — go back, use the sidebar. Don't refresh. |
| `{"detail":"Missing session token"}` | Your session expired. Switch to the other tab; narrate while you re-login later. |
| Upload hangs > 15 s | Say "in production this is async" and switch to an existing session. |
| Queue is empty on every session | Open the register tab — show matched history instead. |
| Server 500 | Switch to the already-loaded tab. Don't refresh; talk through it. After the demo, check `uvicorn.err`. |

**Golden rule:** never refresh the presenter tab. Narrate. Continue.

---

## After the demo — leave-behinds

- `demo/PITCH.md` (the slide source).
- Link to the running instance *\[FILL: URL after pilot deploy]*.
- Contact: *\[FILL: who owns the product, who owns intake]*.
