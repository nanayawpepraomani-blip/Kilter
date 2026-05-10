"""
ingest.py
=========

One place to turn a (swift_path, flex_path) pair into a reconciled session.
Both the web upload endpoint and the folder-drop scanner call ingest_pair()
so they can't drift apart.

Flow:
    1. Compute SHA-256 of both files. If either hash is already in
       ingested_files, raise DuplicateFileError — same content can only be
       loaded once. Prevents a file dropped twice into messages/ from
       producing two reconciliation sessions.
    2. Load rows via reconcile.load_swift / load_flexcube.
    3. Extract account metadata (SWIFT panel, Flex row scan).
    4. Validate: currency must match, Flex must be single-account.
    5. Run the proposer engine and greedy resolver.
    6. Persist session, txns, candidates, assignments, ingested_files,
       and an audit log entry — all in one transaction.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from account_meta import extract_flex_meta, extract_swift_meta
from db import get_conn
from iso20022_loader import extract_camt_meta_raw, load_camt_raw
from recon_engine import propose_candidates, resolve, propose_splits, propose_many_to_many
from byo_csv_loader import CsvProfile, load_csv as load_byo_csv


def _load_flex_via_profile(conn, flex_path: Path, profile_id: int) -> list[dict]:
    """Load `flex_path` as CSV using the given format profile. Raises
    IngestError on profile/parse problems so the caller surfaces a 400.

    Currency / account fallback chain:
        1. The currency column on each row (loader emits it directly).
        2. profile.currency (loader picks this up when no column).
        3. The bound account's currency (this function patches it in
           when the loader emitted ''). Same for ac_no when the
           profile lacks a column mapping.

    Without this fallback, every BYO file would have to either include a
    currency column or have the currency repeated in the profile —
    annoying when the profile is bound to a single account.
    """
    row = conn.execute(
        "SELECT * FROM csv_format_profiles WHERE id=? AND active=1",
        (profile_id,),
    ).fetchone()
    if row is None:
        raise IngestError(f"CSV profile {profile_id} not found or inactive.")
    profile_dict = dict(row)
    profile = CsvProfile.from_db(profile_dict)

    # Look up the bound account once for fallback. accounts has no
    # ac_branch column today — the BYO file is expected to carry it
    # in narration/data, or omit it entirely.
    bound_account = None
    if profile_dict.get('account_id'):
        acc_row = conn.execute(
            "SELECT id, flex_ac_no, currency FROM accounts WHERE id=?",
            (profile_dict['account_id'],),
        ).fetchone()
        if acc_row is None:
            raise IngestError(
                f"Profile references account_id {profile_dict['account_id']} "
                f"which no longer exists.")
        bound_account = dict(acc_row)

    content = flex_path.read_bytes()
    try:
        result = load_byo_csv(content, profile)
    except ValueError as exc:
        raise IngestError(f"BYO CSV profile rejected the file: {exc}") from exc
    if not result.txns and result.errors:
        first_msg = result.errors[0][1] if result.errors else "no rows parsed"
        raise IngestError(
            f"BYO CSV produced no rows. First error on row "
            f"{result.errors[0][0]}: {first_msg}")

    # Apply fallbacks. Empty cells stay empty if no fallback source —
    # downstream account-resolve will then bail with a clean error
    # rather than silently ingesting headless data.
    fallback_ccy = bound_account['currency'] if bound_account else None
    fallback_ac_no = bound_account['flex_ac_no'] if bound_account else None
    if fallback_ccy or fallback_ac_no:
        for t in result.txns:
            if not t.get('ccy') and fallback_ccy:
                t['ccy'] = fallback_ccy
            if not t.get('ac_no') and fallback_ac_no:
                t['ac_no'] = fallback_ac_no

    # Final guard: if no row has a currency by now, the engine can't
    # bucket. Tell the operator instead of producing a half-ingested mess.
    if result.txns and not any(t.get('ccy') for t in result.txns):
        raise IngestError(
            "BYO file has no currency: profile currency is empty, no "
            "currency column is mapped, and the profile isn't bound to "
            "an account. Edit the profile to add one of these.")
    return result.txns
from reconcile import load_flexcube, load_swift, read_balance_sheet
from swift_loader import extract_swift_meta_raw, load_swift_raw
from open_items import (
    carry_forward_match, load_tolerance, load_match_tiers,
    seed_open_items_for_session,
)


class IngestError(Exception):
    """Something is wrong with the file(s). Message is user-facing."""


class DuplicateFileError(IngestError):
    def __init__(self, which: str, sha: str, prior_session_id: int | None):
        self.which = which
        self.sha = sha
        self.prior_session_id = prior_session_id
        location = f"session {prior_session_id}" if prior_session_id else "prior upload"
        super().__init__(f"{which} file already ingested in {location}")


# Tolerance for the continuity check: file's stated opening balance must
# match account.last_closing_balance to within this many units of currency.
# 0.01 = one cent / pesewa — tight by design. Per-account override lives
# on the tolerance_rules row (Phase 3 wires the override; Phase 2 just
# anchors the constant).
CONTINUITY_TOLERANCE = 0.01


class AlreadySeededError(IngestError):
    """Re-loading a proof on an already-anchored account would silently
    rewrite the chain. Refused by default — admin must clear the anchor
    explicitly via /accounts/{id}/clear-anchor first."""
    def __init__(self, account_label: str, account_id: int, current_anchor: float):
        self.account_label = account_label
        self.account_id = account_id
        self.current_anchor = current_anchor
        super().__init__(
            f"Account {account_label!r} (id={account_id}) is already seeded "
            f"with closing balance {current_anchor:,.2f}. To re-seed, an "
            f"admin must first clear the anchor.")


class NotSeededError(IngestError):
    """A delta arrived for an account that's been registered but never
    seeded. The continuity chain has no starting point — operator must
    either seed a proof first or accept the file as the implicit start
    via the force-accept flow."""
    def __init__(self, account_label: str, account_id: int):
        self.account_label = account_label
        self.account_id = account_id
        super().__init__(
            f"Account {account_label!r} (id={account_id}) has no balance "
            "anchor — load a proof via /accounts/{id}/seed-proof before "
            "sending daily delta files.")


class ContinuityBreakError(IngestError):
    """The delta file's stated opening balance doesn't match the
    account's anchor. Refused by default — anchor stays put. An admin
    can force-accept the delta via /sessions/{id}/force-accept after
    investigating the cause; the override is audit-logged."""
    def __init__(self, *, expected: float, actual: float,
                 last_closing_date: int):
        self.expected = expected
        self.actual = actual
        self.delta = round(actual - expected, 4)
        self.last_closing_date = last_closing_date
        super().__init__(
            f"Continuity break: expected opening balance "
            f"{expected:,.2f} (anchor as of {last_closing_date}), "
            f"got {actual:,.2f} — delta {self.delta:,.2f}. "
            "File rejected; the previous closing balance stands. An "
            "admin can force-accept after investigating.")


@dataclass
class IngestResult:
    session_id: int
    swift_rows: int
    flex_rows: int
    candidates_proposed: int
    pending_assignments: int
    unmatched_swift: int
    unmatched_flex: int
    account_registered: bool
    account_label: str | None
    swift_account: str | None
    flex_ac_no: str | None
    currency: str | None
    # Rolling-ledger side effects from this ingest — zero when account not registered.
    open_items_seeded: int = 0
    open_items_cleared: int = 0


def _load_swift_auto(path: Path) -> tuple[list, dict]:
    """Dispatch by extension: raw MT940/MT950 .out files go through
    swift_core's native parser; ISO 20022 camt.053/camt.054 .xml or
    .txt files go through iso20022_loader (BoG's RTGS feed emits .txt
    on disk); pre-parsed .xlsx files continue to flow through
    reconcile.load_swift for back-compat with ops workflows."""
    suffix = path.suffix.lower()
    if suffix == '.out':
        return load_swift_raw(path), extract_swift_meta_raw(path)
    if suffix in ('.xml', '.txt'):
        return load_camt_raw(path), extract_camt_meta_raw(path)
    if suffix in ('.xlsx', '.xlsm'):
        return load_swift(path), extract_swift_meta(path)
    raise IngestError(
        f"Unsupported SWIFT file type '{suffix}'. "
        "Use .out (MT940/MT950), .xml or .txt (camt.053/camt.054), or .xlsx.")


def sha256_of(path: Path) -> str:
    h = hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda: f.read(1 << 16), b''):
            h.update(chunk)
    return h.hexdigest()


def _validate_statement_balance(
    opening_amount, opening_sign,
    closing_amount, closing_sign,
    swift_txns: list,
) -> dict | None:
    """Return {valid, expected, actual, delta} if both balances are present,
    else None. Credits increase the balance; debits decrease it.
    delta = expected_closing - actual_closing; 0.0 is a perfect match."""
    if opening_amount is None or closing_amount is None:
        return None
    try:
        opening = float(opening_amount) * (1 if opening_sign == 'C' else -1)
        net = 0.0
        for t in swift_txns:
            amt = float(t.get('amount') or 0)
            net += amt if t.get('sign') == 'C' else -amt
        expected = opening + net
        actual = float(closing_amount) * (1 if closing_sign == 'C' else -1)
        delta = round(expected - actual, 4)
        return {'valid': abs(delta) < 0.01, 'expected': expected, 'actual': actual, 'delta': delta}
    except (TypeError, ValueError):
        return None


def ingest_pair(swift_path: Path, flex_path: Path, user: str,
                swift_filename: str | None = None,
                flex_filename: str | None = None,
                flex_profile_id: int | None = None) -> IngestResult:
    """Ingest one (SWIFT, Flex) pair.

    flex_profile_id — when set, the Flex file is treated as a CSV and
    parsed through byo_csv_loader using the named profile, instead of
    the default Flexcube xlsx loader. The rest of the pipeline is
    unchanged because byo_csv_loader emits the same canonical txn shape.
    """
    swift_filename = swift_filename or swift_path.name
    flex_filename = flex_filename or flex_path.name

    swift_sha = sha256_of(swift_path)
    flex_sha = sha256_of(flex_path)

    conn = get_conn()
    try:
        # Duplicate check up front — fail before expensive parsing.
        dup = conn.execute("SELECT session_id FROM ingested_files WHERE sha256=?",
                           (swift_sha,)).fetchone()
        if dup:
            raise DuplicateFileError('SWIFT', swift_sha, dup['session_id'])
        dup = conn.execute("SELECT session_id FROM ingested_files WHERE sha256=?",
                           (flex_sha,)).fetchone()
        if dup:
            raise DuplicateFileError('Flexcube', flex_sha, dup['session_id'])

        try:
            swift_txns, swift_meta = _load_swift_auto(swift_path)
            if flex_profile_id is not None:
                flex_txns = _load_flex_via_profile(conn, flex_path, flex_profile_id)
            else:
                flex_txns = load_flexcube(flex_path)
            flex_meta = extract_flex_meta(flex_txns)
        except IngestError:
            raise
        except Exception as exc:
            raise IngestError(f"Failed to parse files: {exc}") from exc

        if flex_meta.get('multi_account'):
            raise IngestError(
                f"Flexcube file contains multiple AC_NOs: {flex_meta['all_accounts']}. "
                "Reconciliation expects one account per file.")
        s_ccy = swift_meta.get('currency')
        f_ccy = flex_meta.get('currency')
        if s_ccy and f_ccy and s_ccy != f_ccy:
            raise IngestError(
                f"Currency mismatch: SWIFT is {s_ccy} but Flexcube is {f_ccy}.")

        # Resolve the account *before* running the engine so per-account
        # tolerance rules apply to candidate proposal, not just to the UI.
        account_match = None
        s_acc = swift_meta.get('account')
        f_acc = flex_meta.get('ac_no')
        ccy = s_ccy or f_ccy
        if s_acc and f_acc and ccy:
            row = conn.execute(
                "SELECT * FROM accounts WHERE swift_account=? AND flex_ac_no=? "
                "AND currency=? AND active=1",
                (s_acc, f_acc, ccy),
            ).fetchone()
            if row:
                account_match = dict(row)

        tol = load_tolerance(conn, account_match['id'] if account_match else None)
        candidates = propose_candidates(swift_txns, flex_txns, tol=tol)
        resolution = resolve(candidates, swift_txns, flex_txns)

        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO sessions (created_at, created_by, swift_filename, flex_filename, "
            "swift_account, swift_currency, swift_statement_ref, flex_ac_no, flex_ac_branch, "
            "flex_currency, account_id, account_label, flex_profile_id, "
            "opening_balance, opening_balance_amount, opening_balance_sign, opening_balance_date, "
            "closing_balance, closing_balance_amount, closing_balance_sign, closing_balance_date) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?, ?,?,?,?, ?,?,?,?)",
            (now, user, swift_filename, flex_filename,
             swift_meta.get('account'), swift_meta.get('currency'),
             swift_meta.get('statement_ref'),
             flex_meta.get('ac_no'), flex_meta.get('ac_branch'), flex_meta.get('currency'),
             account_match['id'] if account_match else None,
             account_match['label'] if account_match else None,
             flex_profile_id,
             swift_meta.get('opening_balance'),
             swift_meta.get('opening_balance_amount'),
             swift_meta.get('opening_balance_sign'),
             swift_meta.get('opening_balance_date'),
             swift_meta.get('closing_balance'),
             swift_meta.get('closing_balance_amount'),
             swift_meta.get('closing_balance_sign'),
             swift_meta.get('closing_balance_date')),
        )
        session_id = cur.lastrowid

        cur.executemany(
            "INSERT INTO swift_txns (session_id, row_number, value_date, amount, sign, "
            "origin, type, status, book_date, our_ref, their_ref, booking_text_1, "
            "booking_text_2) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [(session_id, s['_row_number'], s['value_date'], s['amount'], s['sign'],
              s['origin'], s['type'], s['status'], s['book_date'], s['our_ref'],
              s['their_ref'], s['booking_text_1'], s['booking_text_2'])
             for s in swift_txns],
        )
        cur.executemany(
            "INSERT INTO flex_txns (session_id, row_number, trn_ref, ac_branch, ac_no, "
            "booking_date, value_date, type, narration, amount, ccy, module, "
            "external_ref, user_id) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            [(session_id, f['_row_number'], f['trn_ref'], f['ac_branch'], f['ac_no'],
              f['booking_date'], f['value_date'], f['type'], f['narration'],
              f['amount'], f['ccy'], f['module'], f['external_ref'], f['user_id'])
             for f in flex_txns],
        )
        cur.executemany(
            "INSERT INTO candidates (session_id, swift_row, flex_row, tier, reason, amount_diff) "
            "VALUES (?,?,?,?,?,?)",
            [(session_id, c.swift_row, c.flex_row, c.tier, c.reason, c.amount_diff)
             for c in candidates],
        )
        cur.executemany(
            "INSERT INTO assignments (session_id, swift_row, flex_row, tier, reason, "
            "amount_diff, status) VALUES (?,?,?,?,?,?, 'pending')",
            [(session_id, a.swift_row, a.flex_row, a.tier, a.reason, a.amount_diff)
             for a in resolution.assignments],
        )

        # --- Split matching (1:N / N:1) ---
        # Runs on the rows the 1:1 proposer couldn't pair. Each split becomes
        # N assignments (one per row on the multi side) sharing a split_group_id
        # so the UI can group and accept/reject the whole aggregate at once.
        # Ref-gated, so no amount-only coincidences slip through.
        import uuid
        split_cands = propose_splits(
            swift_txns, flex_txns,
            set(resolution.unmatched_swift), set(resolution.unmatched_flex),
            tol=tol,
        )
        split_rows = []
        for sc in split_cands:
            grp = uuid.uuid4().hex[:12]
            per_row_diff = sc.amount_diff / max(len(sc.swift_rows), len(sc.flex_rows))
            if len(sc.swift_rows) == 1:
                # 1:N — one SWIFT row paired against N flex rows.
                sw = sc.swift_rows[0]
                for fx in sc.flex_rows:
                    split_rows.append((session_id, sw, fx, sc.tier, sc.reason,
                                       per_row_diff, 'pending', 'split', grp))
            else:
                # N:1 — N SWIFT rows paired against one flex row.
                fx = sc.flex_rows[0]
                for sw in sc.swift_rows:
                    split_rows.append((session_id, sw, fx, sc.tier, sc.reason,
                                       per_row_diff, 'pending', 'split', grp))
        if split_rows:
            cur.executemany(
                "INSERT INTO assignments (session_id, swift_row, flex_row, tier, reason, "
                "amount_diff, status, source, split_group_id) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                split_rows,
            )

        # --- Many-to-many matching (tier 6) ---
        # Runs after the 1:N / N:1 split pass, on whatever remains. Catches
        # the genuinely-aggregated case where both sides bundled multiple
        # txns into a single conceptual settlement — neither tier 1-4 nor
        # tier 5 covers this. Strict gating (date band + sign mirror + pool
        # cap) keeps false positives bounded.
        consumed_swift_so_far = {r[1] for r in split_rows}
        consumed_flex_so_far  = {r[2] for r in split_rows}
        m2n_unmatched_swift = set(resolution.unmatched_swift) - consumed_swift_so_far
        m2n_unmatched_flex  = set(resolution.unmatched_flex)  - consumed_flex_so_far
        m2n_cands = propose_many_to_many(
            swift_txns, flex_txns,
            m2n_unmatched_swift, m2n_unmatched_flex,
            tol=tol,
        )
        m2n_rows = []
        for sc in m2n_cands:
            grp = uuid.uuid4().hex[:12]
            denom = max(len(sc.swift_rows), len(sc.flex_rows))
            per_row_diff = sc.amount_diff / denom
            # Cross-product persistence: every (swift, flex) pair in the
            # subset emits an assignment row sharing the split_group_id.
            # The UI groups by split_group_id and treats accept/reject
            # atomically so the operator can't half-confirm an aggregate.
            for sw in sc.swift_rows:
                for fx in sc.flex_rows:
                    m2n_rows.append((session_id, sw, fx, sc.tier, sc.reason,
                                     per_row_diff, 'pending', 'split', grp))
        if m2n_rows:
            cur.executemany(
                "INSERT INTO assignments (session_id, swift_row, flex_row, tier, reason, "
                "amount_diff, status, source, split_group_id) "
                "VALUES (?,?,?,?,?,?,?,?,?)",
                m2n_rows,
            )

        cur.executemany(
            "INSERT INTO ingested_files (sha256, kind, original_filename, session_id, "
            "ingested_at, ingested_by) VALUES (?,?,?,?,?,?)",
            [(swift_sha, 'swift', swift_filename, session_id, now, user),
             (flex_sha, 'flexcube', flex_filename, session_id, now, user)],
        )
        cur.execute(
            "INSERT INTO audit_log (session_id, action, actor, timestamp, details) "
            "VALUES (?, 'session_created', ?, ?, ?)",
            (session_id, user, now, json.dumps({
                "swift_rows": len(swift_txns),
                "flex_rows": len(flex_txns),
                "candidates": len(candidates),
                "assignments": len(resolution.assignments),
                "swift_sha256": swift_sha,
                "flex_sha256": flex_sha,
            })),
        )

        # Rolling-ledger: clear prior carry-forward items that find a
        # counterpart in this session, then seed anything still one-sided.
        # Only runs when the session is tied to a registered account — an
        # untagged session has no account to key the ledger against.
        carried = {'cleared_against_swift': 0, 'cleared_against_flex': 0}
        seeded = 0
        if account_match is not None:
            carried = carry_forward_match(conn, session_id, tol)
            seeded = seed_open_items_for_session(conn, session_id)

        # Compute final unmatched counts from DB after all matching passes
        # (1:1 engine + split + M:N + carry-forward). Earlier in-memory
        # subtraction was stale whenever carry-forward cleared additional rows.
        assigned_swift_final = {r[0] for r in conn.execute(
            "SELECT swift_row FROM assignments WHERE session_id=? AND swift_row IS NOT NULL",
            (session_id,)).fetchall()}
        assigned_flex_final = {r[0] for r in conn.execute(
            "SELECT flex_row FROM assignments WHERE session_id=? AND flex_row IS NOT NULL",
            (session_id,)).fetchall()}
        unmatched_swift_after = [r['row_number'] for r in conn.execute(
            "SELECT row_number FROM swift_txns WHERE session_id=?", (session_id,)).fetchall()
            if r['row_number'] not in assigned_swift_final]
        unmatched_flex_after = [r['row_number'] for r in conn.execute(
            "SELECT row_number FROM flex_txns WHERE session_id=?", (session_id,)).fetchall()
            if r['row_number'] not in assigned_flex_final]

        # Balance validation: verify that the SWIFT statement's opening +
        # net txns = closing balance. Stored per-session so the review UI
        # can show a green/red badge without re-computing on every load.
        bv = _validate_statement_balance(
            swift_meta.get('opening_balance_amount'),
            swift_meta.get('opening_balance_sign'),
            swift_meta.get('closing_balance_amount'),
            swift_meta.get('closing_balance_sign'),
            swift_txns,
        )
        if bv is not None:
            conn.execute(
                "UPDATE sessions SET balance_valid=?, balance_delta=? WHERE id=?",
                (1 if bv['valid'] else 0, bv['delta'], session_id),
            )

        # Auto-match: run rule engine on pending assignments.
        # Import lazily to avoid circular imports.
        from auto_match_engine import apply_auto_rules
        apply_auto_rules(conn, session_id, actor='system_auto')

        conn.commit()

        return IngestResult(
            session_id=session_id,
            swift_rows=len(swift_txns),
            flex_rows=len(flex_txns),
            candidates_proposed=len(candidates),
            pending_assignments=len(resolution.assignments) + len(split_rows),
            unmatched_swift=len(unmatched_swift_after),
            unmatched_flex=len(unmatched_flex_after),
            account_registered=account_match is not None,
            account_label=account_match['label'] if account_match else None,
            swift_account=swift_meta.get('account'),
            flex_ac_no=flex_meta.get('ac_no'),
            currency=ccy,
            open_items_seeded=seeded,
            open_items_cleared=carried['cleared_against_swift'] + carried['cleared_against_flex'],
        )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Phase 2 — Day-0 proof seeding for one-sided accounts.
# ---------------------------------------------------------------------------

def _split_flex_for_self_match(flex_txns: list[dict]) -> tuple[list[dict], list[dict]]:
    """Reshape a flex-shape txn list (CR + DR mixed) into the two arrays
    the matching engine expects: a swift-side carrying the DR legs and
    a flex-side carrying the CR legs. The engine matches them as if
    they were two banks' views of the same trades, which is exactly
    what we want for one-sided GL self-matching: settled C/D pairs
    cancel each other; unmatched residue becomes open items.

    Row numbers restart at 1 on each side so swift_txns and flex_txns
    table writes don't collide on (session_id, row_number).

    ⚠️  IMPORTANT VOCABULARY NOTE  ⚠️
    On one-sided GLs there is NO SWIFT side — both DR and CR come from
    Flexcube. The "swift_side" / `swift_txns` storage here is purely an
    engine-reuse convention so the proposer doesn't need a one-sided
    fork. Anywhere this data surfaces to the operator (UI labels,
    exports, banners, error messages, AI/assistant chat replies) it
    MUST be called "DR side" / "Flexcube · DR" / "matched DRs".
    Using "SWIFT" externally on a one-sided session is wrong and
    confuses the ops team. See the same warning in recon_engine.py's
    module docstring."""
    swift_side: list[dict] = []
    flex_side: list[dict] = []
    s_rn = f_rn = 0
    for f in flex_txns:
        if f['type'] == 'DR':
            s_rn += 1
            swift_side.append({
                '_source':       'swift',
                '_row_number':   s_rn,
                '_used':         False,
                'value_date':    f['value_date'],
                'amount':        f['amount'],
                'sign':          'D',                          # SWIFT 'D' mirrors Flex 'CR'
                'origin':        'Our',
                'type':          f.get('module', '') or 'Other',
                'status':        'Open',
                'book_date':     f.get('booking_date', f['value_date']),
                'our_ref':       f['trn_ref'],
                'their_ref':     f.get('external_ref', '') or '',
                'booking_text_1': (f.get('narration', '') or '')[:80],
                'booking_text_2': f.get('narration', '') or '',
            })
        elif f['type'] == 'CR':
            f_rn += 1
            flex_side.append({**f, '_row_number': f_rn})
    return swift_side, flex_side


def ingest_proof_seed(flex_path: Path, account_id: int, user: str,
                      flex_filename: str | None = None) -> IngestResult:
    """Day-0 load (load-only): parse the proof xlsx, persist its rows,
    and stamp the account's last_closing_balance so the next delta has
    something to chain off. The matching engine, internal self-match,
    and open_items seeding are deferred to a separate `run_matching`
    pass that ops triggers explicitly via the review page.

    Hard-fails when:
      - the account is unknown / inactive
      - the account is already seeded (use clear-anchor first)
      - the file's content hash is already in ingested_files
      - the proof file mixes currencies, or carries a currency that
        doesn't match the account.
    """
    from proof_loader import compute_seed_balance, load_proof

    flex_filename = flex_filename or flex_path.name
    flex_sha = sha256_of(flex_path)

    conn = get_conn()
    try:
        account = conn.execute(
            "SELECT * FROM accounts WHERE id=? AND active=1", (account_id,)
        ).fetchone()
        if account is None:
            raise IngestError(f"Account id={account_id} not found or inactive.")
        account = dict(account)
        if account.get('last_closing_balance') is not None:
            raise AlreadySeededError(
                account['label'], account_id,
                account['last_closing_balance'])

        dup = conn.execute(
            "SELECT session_id FROM ingested_files WHERE sha256=?", (flex_sha,)
        ).fetchone()
        if dup:
            raise DuplicateFileError('Flexcube', flex_sha, dup['session_id'])

        try:
            flex_txns = load_proof(flex_path)
        except Exception as exc:
            raise IngestError(f"Failed to parse proof file: {exc}") from exc
        if not flex_txns:
            raise IngestError(
                f"Proof file '{flex_filename}' has no usable rows.")

        # Currency consistency: every row should share the account's
        # currency. Reject mixed-currency proofs — seeds are per-account.
        ccies = {t['ccy'] for t in flex_txns if t.get('ccy')}
        if ccies and account['currency'] not in ccies:
            raise IngestError(
                f"Proof currency {sorted(ccies)} doesn't match the "
                f"account's currency {account['currency']!r}.")
        if len(ccies) > 1:
            raise IngestError(
                f"Proof file mixes currencies {sorted(ccies)}. "
                "Each seed must be single-currency.")

        closing, max_value_date = compute_seed_balance(flex_txns)
        # Min value date — drives the displayed Statement period. Without
        # this, a proof spanning Dec 2025 → Apr 2026 would render as
        # "2026-04-30 → 2026-04-30" (single day) which misrepresents
        # what the file actually covers.
        min_value_date = min(
            (t['value_date'] for t in flex_txns if t.get('value_date')),
            default=max_value_date,
        )

        # Reshape CR/DR into the swift/flex split the engine expects
        # so a future run_matching pass can self-match internal pairs.
        # We persist the rows in this shape immediately; engine work
        # is deferred until the operator clicks Run matching.
        swift_side, flex_side = _split_flex_for_self_match(flex_txns)

        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        cur = conn.cursor()
        # session_kind='seed' so reports / dashboards can filter the
        # seed session out of recon-volume metrics. swift_filename is
        # an empty string (NOT NULL constraint) — sentinel for no SWIFT.
        # Status starts 'open' — switches to 'closed' when ops closes
        # the session after run_matching + review.
        cur.execute(
            "INSERT INTO sessions (created_at, created_by, swift_filename, "
            "flex_filename, status, session_kind, "
            "flex_ac_no, flex_currency, account_id, account_label, "
            "opening_balance_amount, opening_balance_sign, opening_balance_date, "
            "closing_balance_amount, closing_balance_sign, closing_balance_date, "
            "flex_opening_balance, flex_closing_balance, flex_balance_as_of, "
            "flex_balance_currency) "
            "VALUES (?,?,?,?,'open','seed',?,?,?,?, ?,?,?, ?,?,?, ?,?,?,?)",
            (now, user, '', flex_filename,
             account['flex_ac_no'], account['currency'],
             account_id, account['label'],
             0.0, 'C', min_value_date,
             abs(closing), 'C' if closing >= 0 else 'D', max_value_date,
             0.0, closing, max_value_date, account['currency']),
        )
        session_id = cur.lastrowid

        if swift_side:
            cur.executemany(
                "INSERT INTO swift_txns (session_id, row_number, value_date, "
                "amount, sign, origin, type, status, book_date, our_ref, "
                "their_ref, booking_text_1, booking_text_2) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [(session_id, s['_row_number'], s['value_date'], s['amount'],
                  s['sign'], s['origin'], s['type'], s['status'], s['book_date'],
                  s['our_ref'], s['their_ref'], s['booking_text_1'],
                  s['booking_text_2'])
                 for s in swift_side],
            )
        if flex_side:
            cur.executemany(
                "INSERT INTO flex_txns (session_id, row_number, trn_ref, ac_branch, "
                "ac_no, booking_date, value_date, type, narration, amount, ccy, "
                "module, external_ref, user_id) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [(session_id, f['_row_number'], f['trn_ref'], f['ac_branch'],
                  f['ac_no'], f['booking_date'], f['value_date'], f['type'],
                  f['narration'], f['amount'], f['ccy'], f['module'],
                  f['external_ref'], f['user_id'])
                 for f in flex_side],
            )

        cur.execute(
            "INSERT INTO ingested_files (sha256, kind, original_filename, "
            "session_id, ingested_at, ingested_by) VALUES (?,?,?,?,?,?)",
            (flex_sha, 'flexcube', flex_filename, session_id, now, user),
        )

        # Anchor the account so subsequent delta loads can chain off it.
        cur.execute(
            "UPDATE accounts SET last_closing_balance=?, last_closing_date=?, "
            "last_session_id=? WHERE id=?",
            (closing, max_value_date, session_id, account_id),
        )

        cur.execute(
            "INSERT INTO audit_log (session_id, action, actor, timestamp, details) "
            "VALUES (?, 'proof_loaded', ?, ?, ?)",
            (session_id, user, now, json.dumps({
                "account_id": account_id,
                "flex_rows": len(flex_txns),
                "dr_legs": len(swift_side),
                "cr_legs": len(flex_side),
                "closing_balance": closing,
                "as_of_date": max_value_date,
                "flex_sha256": flex_sha,
            })),
        )
        conn.commit()

        return IngestResult(
            session_id=session_id,
            swift_rows=len(swift_side),
            flex_rows=len(flex_side),
            candidates_proposed=0,
            pending_assignments=0,
            unmatched_swift=len(swift_side),
            unmatched_flex=len(flex_side),
            account_registered=True,
            account_label=account['label'],
            swift_account=account.get('swift_account'),
            flex_ac_no=account['flex_ac_no'],
            currency=account['currency'],
            open_items_seeded=0,   # deferred to run_matching
        )
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Phase 3 — daily Flex-only delta for one-sided accounts.
# ---------------------------------------------------------------------------

def _load_continuity_tolerance(conn, account_id: int) -> float:
    """Per-account continuity tolerance, defaulting to the 0.01 constant
    when the account has no tolerance_rules row. Kept tight by design:
    a balance chain that accepts more than a cent of drift per day
    accumulates enough error to mask a real problem within a quarter."""
    row = conn.execute(
        "SELECT continuity_tol_abs FROM tolerance_rules WHERE account_id=?",
        (account_id,),
    ).fetchone()
    if row is None or row['continuity_tol_abs'] is None:
        return CONTINUITY_TOLERANCE
    return float(row['continuity_tol_abs'])


def ingest_flex_only(flex_path: Path, account_id: int, user: str,
                     flex_filename: str | None = None,
                     flex_profile_id: int | None = None,
                     force_accept: bool = False,
                     force_reason: str | None = None) -> IngestResult:
    """Day-N delta load (load-only) for a one-sided account. Persists
    rows, runs the inline continuity check, advances the account
    anchor. The matching engine, carry-forward, and open_items seeding
    are deferred to a separate `run_matching` pass that ops triggers
    explicitly via the review page.

    Continuity contract (still inline — structural, not match-related):
      * Account must already be seeded (last_closing_balance not NULL).
        Otherwise NotSeededError — load a proof first.
      * The file's `balances` sheet must be present and its
        opening_balance must equal the account's anchor within
        per-account tolerance (default 0.01). Otherwise
        ContinuityBreakError.
      * On success the account's anchor advances to the file's
        closing_balance.

    force_accept=True bypasses the continuity check (admin override
    after investigating a break). The file's opening is then written
    into the session as-is, the anchor jumps to the file's closing
    regardless of the gap, and the override is audit-logged with the
    operator's reason.
    """
    flex_filename = flex_filename or flex_path.name
    flex_sha = sha256_of(flex_path)

    conn = get_conn()
    try:
        account = conn.execute(
            "SELECT * FROM accounts WHERE id=? AND active=1", (account_id,)
        ).fetchone()
        if account is None:
            raise IngestError(f"Account id={account_id} not found or inactive.")
        account = dict(account)
        if account.get('account_recon_type') != 'one_sided':
            raise IngestError(
                f"Account {account['label']!r} is not one-sided. Use the "
                "two-sided pair-ingest flow instead.")

        dup = conn.execute(
            "SELECT session_id FROM ingested_files WHERE sha256=?", (flex_sha,)
        ).fetchone()
        if dup:
            raise DuplicateFileError('Flexcube', flex_sha, dup['session_id'])

        # Parse the file (default xlsx loader, or BYO profile if specified).
        try:
            if flex_profile_id is not None:
                flex_txns = _load_flex_via_profile(conn, flex_path, flex_profile_id)
            else:
                flex_txns = load_flexcube(flex_path)
            flex_meta = extract_flex_meta(flex_txns)
        except IngestError:
            raise
        except Exception as exc:
            raise IngestError(f"Failed to parse Flex file: {exc}") from exc

        if not flex_txns:
            raise IngestError(f"Flex file {flex_filename!r} has no usable rows.")
        if flex_meta.get('multi_account'):
            raise IngestError(
                f"Flex file contains multiple AC_NOs: {flex_meta['all_accounts']}. "
                "Reconciliation expects one account per file.")
        if flex_meta.get('ac_no') and flex_meta['ac_no'] != account['flex_ac_no']:
            raise IngestError(
                f"File's AC_NO {flex_meta['ac_no']!r} doesn't match the "
                f"registered account's flex_ac_no {account['flex_ac_no']!r}.")
        if flex_meta.get('currency') and flex_meta['currency'] != account['currency']:
            raise IngestError(
                f"File currency {flex_meta['currency']!r} doesn't match "
                f"account currency {account['currency']!r}.")

        # Read the optional `balances` sheet — drives the continuity check.
        flex_balance = None
        if flex_profile_id is None and flex_path.suffix.lower() in ('.xlsx', '.xlsm'):
            try:
                flex_balance = read_balance_sheet(flex_path)
            except Exception:
                flex_balance = None

        # Continuity gate (still inline — structural check).
        anchor = account.get('last_closing_balance')
        if anchor is None:
            raise NotSeededError(account['label'], account_id)
        if flex_balance is None:
            if not force_accept:
                raise IngestError(
                    f"File {flex_filename!r} has no `balances` sheet — "
                    "Kilter can't run the continuity check without one. "
                    "Update the extract script to emit a balances sheet, "
                    "or use the force-accept flow.")
        else:
            tol = _load_continuity_tolerance(conn, account_id)
            actual = float(flex_balance['opening_balance'])
            expected = float(anchor)
            if not force_accept and abs(actual - expected) > tol:
                raise ContinuityBreakError(
                    expected=expected, actual=actual,
                    last_closing_date=account.get('last_closing_date') or 0)

        # Reshape CR/DR into the swift/flex split shape so a future
        # run_matching pass can self-match within-day pairs and
        # carry-forward against the open_items ledger.
        swift_side, flex_side = _split_flex_for_self_match(flex_txns)

        # Compute the value-date range so the review page's Statement
        # period column reads correctly. Falls back to the balance
        # sheet's as_of_date when the file has no usable value dates,
        # then to today's date as last resort so the column never
        # renders empty.
        value_dates = [t['value_date'] for t in flex_txns if t.get('value_date')]
        as_of = (flex_balance.get('as_of_date') if flex_balance else None) or 0
        period_start = min(value_dates) if value_dates else as_of
        period_end   = max(value_dates) if value_dates else as_of

        now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        cur = conn.cursor()
        # opening_balance_amount/sign captured from the file's stated
        # opening (which we just continuity-checked) so the review-page
        # "Opening balance" tile renders the same number we matched the
        # anchor against, with date pinned to period_start.
        opening_amt = (flex_balance['opening_balance']
                        if flex_balance is not None else anchor)
        closing_amt = (flex_balance['closing_balance']
                        if flex_balance is not None
                        else _compute_running_anchor(anchor, flex_txns))
        cur.execute(
            "INSERT INTO sessions (created_at, created_by, swift_filename, "
            "flex_filename, status, session_kind, "
            "flex_ac_no, flex_ac_branch, flex_currency, account_id, account_label, "
            "flex_profile_id, "
            "opening_balance_amount, opening_balance_sign, opening_balance_date, "
            "closing_balance_amount, closing_balance_sign, closing_balance_date, "
            "flex_opening_balance, flex_closing_balance, flex_balance_as_of, "
            "flex_balance_currency) "
            "VALUES (?,?,?,?,'open','flex_delta',?,?,?,?,?,?, ?,?,?, ?,?,?, ?,?,?,?)",
            (now, user, '', flex_filename,
             flex_meta.get('ac_no'), flex_meta.get('ac_branch'),
             flex_meta.get('currency'), account['id'], account['label'],
             flex_profile_id,
             abs(opening_amt) if opening_amt is not None else None,
             ('C' if (opening_amt or 0) >= 0 else 'D'),
             period_start,
             abs(closing_amt) if closing_amt is not None else None,
             ('C' if (closing_amt or 0) >= 0 else 'D'),
             period_end,
             flex_balance['opening_balance'] if flex_balance else None,
             flex_balance['closing_balance'] if flex_balance else None,
             flex_balance['as_of_date']      if flex_balance else None,
             flex_balance['currency']        if flex_balance else None),
        )
        session_id = cur.lastrowid

        if swift_side:
            cur.executemany(
                "INSERT INTO swift_txns (session_id, row_number, value_date, "
                "amount, sign, origin, type, status, book_date, our_ref, "
                "their_ref, booking_text_1, booking_text_2) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [(session_id, s['_row_number'], s['value_date'], s['amount'],
                  s['sign'], s['origin'], s['type'], s['status'], s['book_date'],
                  s['our_ref'], s['their_ref'], s['booking_text_1'],
                  s['booking_text_2'])
                 for s in swift_side],
            )
        if flex_side:
            cur.executemany(
                "INSERT INTO flex_txns (session_id, row_number, trn_ref, ac_branch, "
                "ac_no, booking_date, value_date, type, narration, amount, ccy, "
                "module, external_ref, user_id) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [(session_id, f['_row_number'], f['trn_ref'], f['ac_branch'],
                  f['ac_no'], f['booking_date'], f['value_date'], f['type'],
                  f['narration'], f['amount'], f['ccy'], f['module'],
                  f['external_ref'], f['user_id'])
                 for f in flex_side],
            )

        cur.execute(
            "INSERT INTO ingested_files (sha256, kind, original_filename, "
            "session_id, ingested_at, ingested_by) VALUES (?,?,?,?,?,?)",
            (flex_sha, 'flexcube', flex_filename, session_id, now, user),
        )

        # Advance the account anchor. force-accept moves it to whatever
        # the file's closing says, so the operator owns the consequence.
        new_anchor = (flex_balance['closing_balance']
                      if flex_balance is not None
                      else _compute_running_anchor(account['last_closing_balance'],
                                                    flex_txns))
        new_anchor_date = (flex_balance['as_of_date']
                            if flex_balance is not None
                            else max((t['value_date'] for t in flex_txns), default=0))
        cur.execute(
            "UPDATE accounts SET last_closing_balance=?, last_closing_date=?, "
            "last_session_id=? WHERE id=?",
            (new_anchor, new_anchor_date, session_id, account['id']),
        )

        cur.execute(
            "INSERT INTO audit_log (session_id, action, actor, timestamp, details) "
            "VALUES (?, ?, ?, ?, ?)",
            (session_id,
             'flex_delta_force_accepted' if force_accept else 'flex_delta_loaded',
             user, now, json.dumps({
                "account_id": account['id'],
                "flex_rows": len(flex_txns),
                "dr_legs": len(swift_side),
                "cr_legs": len(flex_side),
                "anchor_before": anchor,
                "anchor_after":  new_anchor,
                "anchor_delta":  round(new_anchor - anchor, 4) if (anchor is not None and new_anchor is not None) else None,
                "flex_sha256": flex_sha,
                "force_accept": bool(force_accept),
                "force_reason": force_reason,
                "opening_balance": flex_balance['opening_balance'] if flex_balance else None,
                "closing_balance": flex_balance['closing_balance'] if flex_balance else None,
            })),
        )
        conn.commit()

        return IngestResult(
            session_id=session_id,
            swift_rows=len(swift_side),
            flex_rows=len(flex_side),
            candidates_proposed=0,
            pending_assignments=0,
            unmatched_swift=len(swift_side),
            unmatched_flex=len(flex_side),
            account_registered=True,
            account_label=account['label'],
            swift_account=account.get('swift_account'),
            flex_ac_no=account['flex_ac_no'],
            currency=account['currency'],
            open_items_seeded=0,    # deferred to run_matching
            open_items_cleared=0,   # deferred to run_matching
        )
    finally:
        conn.close()


def _compute_running_anchor(prev_anchor: float, flex_txns: list[dict]) -> float:
    """Fallback closing for the rare force-accept-without-balance-sheet
    case: prev_anchor + ΣCR − ΣDR. Best-effort; the operator owns the
    result by force-accepting without metadata."""
    delta = 0.0
    for t in flex_txns:
        if t.get('type') == 'CR':
            delta += float(t.get('amount') or 0)
        elif t.get('type') == 'DR':
            delta -= float(t.get('amount') or 0)
    return round((prev_anchor or 0.0) + delta, 2)


# ---------------------------------------------------------------------------
# run_matching — operator-triggered matching pass.
#
# Loads the rows already persisted on a session, runs the engine + splits +
# M:N + carry-forward + auto-rules + open_items seeding. Idempotent in the
# sense that calling it twice on a clean session has the same end state as
# calling it once; calling it after a partial run picks up where it left
# off (any pre-existing pending assignments stay in place — we only emit
# new ones for rows the engine hasn't already paired).
#
# This is the heavy operation in the system. The ingest paths above are
# load-only so the operator gets immediate feedback when files land; the
# matching engine runs only when the operator explicitly chooses to.
# ---------------------------------------------------------------------------

@dataclass
class MatchingResult:
    session_id: int
    candidates_proposed: int
    pending_assignments: int
    splits_proposed: int
    m2n_proposed: int
    open_items_carried: int
    open_items_seeded: int
    auto_confirmed: int
    started_at: str
    finished_at: str
    elapsed_seconds: float


def run_matching(session_id: int, user: str = 'system', *,
                  carry_forward: bool = False,
                  splits: bool = False,
                  m2n: bool = False,
                  auto_rules: bool = True,
                  seed_residue: bool = True) -> MatchingResult:
    """Run the matching engine for a session that's already had its rows
    loaded by ingest_*. Designed to be called explicitly via POST
    /sessions/{id}/run-matching after the operator reviews the loaded
    data and configures their matching tiers.

    Stage flags (all default to OFF except tier-driven matching itself,
    auto_rules, and seed_residue):
      tiers       : ALWAYS runs — propose_candidates over the loaded
                    pool using whatever rows are in match_tiers for
                    this account. If no enabled tiers exist for this
                    account/recon_type, this stage is a no-op.
      carry_forward : Clear today's free rows against PRIOR open items
                    via ref+amount linkage. Off by default — operator
                    triggers separately via POST /sessions/{id}/carry-forward
                    after they've reviewed the in-session matches.
      splits      : Tier-5 1:N / N:1 aggregate matches. Off by default
                    (currently uses hardcoded engine logic, not yet
                    user-configurable via match_tiers).
      m2n         : Tier-6 M:N aggregate matches. Off by default for
                    the same reason as splits.
      auto_rules  : Apply auto-categorization rules from
                    auto_categorization_rules. On by default — these
                    are user-configured (not hardcoded).
      seed_residue: Seed today's still-unmatched rows as new open items
                    for tomorrow's deltas to clear. On by default — the
                    open-items pipeline depends on this.

    Behaviour for already-matched sessions: row-already-claimed checks
    skip pairs whose swift or flex side has an existing assignment, so
    re-running this on a fully-resolved session is a no-op. On a
    partially-reviewed session, it'll only propose new assignments for
    rows that don't yet have one.
    """
    import time as _time
    import uuid as _uuid

    started = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    t0 = _time.time()

    conn = get_conn()
    try:
        sess = conn.execute(
            "SELECT id, account_id, session_kind, status, "
            "       matching_started_at, matching_finished_at "
            "FROM sessions WHERE id=?",
            (session_id,),
        ).fetchone()
        if sess is None:
            raise IngestError(f"Session id={session_id} not found.")
        if sess['status'] == 'closed':
            raise IngestError(
                f"Session {session_id} is closed — re-open it before running "
                "the matching engine again.")
        # In-flight guard: if another matching run is already in progress
        # on this session (started_at set, finished_at NULL), refuse
        # rather than start a parallel pass. The UI shows the in-flight
        # timer either way, so this just prevents duplicate work.
        if (sess['matching_started_at']
                and not sess['matching_finished_at']):
            raise IngestError(
                f"Matching is already in progress on session {session_id} "
                f"(started {sess['matching_started_at']}). Wait for it to "
                "finish, or close the session if it's stuck.")
        # Stamp started_at in its own commit so other clients (and a
        # post-navigation reload) immediately see the in-flight state
        # and can reconnect to the timer.
        conn.execute(
            "UPDATE sessions SET matching_started_at=?, "
            "matching_finished_at=NULL WHERE id=?",
            (started, session_id),
        )
        conn.commit()

        # Pull rows in the dict shape the engine expects.
        swift_rows = [dict(r) for r in conn.execute(
            "SELECT * FROM swift_txns WHERE session_id=?", (session_id,)
        ).fetchall()]
        flex_rows = [dict(r) for r in conn.execute(
            "SELECT * FROM flex_txns WHERE session_id=?", (session_id,)
        ).fetchall()]

        # The engine expects '_row_number'. swift_txns/flex_txns store
        # row_number — alias it on the way in.
        for r in swift_rows: r['_row_number'] = r['row_number']
        for r in flex_rows:  r['_row_number'] = r['row_number']

        engine_tol = load_tolerance(conn, sess['account_id'])

        # Carry-forward (opt-in). When enabled, runs FIRST so prior
        # session's high-confidence ref-linked open items claim today's
        # rows before tier-driven matching can grab them at lower
        # confidence. Off by default — operator triggers separately via
        # POST /sessions/{id}/carry-forward after reviewing the
        # in-session tier matches.
        carried = {'cleared_against_swift': 0, 'cleared_against_flex': 0}
        if carry_forward and sess['account_id'] is not None:
            carried = carry_forward_match(conn, session_id, engine_tol)

        # Skip pairs whose either side is already claimed by an existing
        # assignment (idempotency on partial re-runs + the carry-forward
        # rows we just wrote).
        claimed_swift = {r[0] for r in conn.execute(
            "SELECT swift_row FROM assignments WHERE session_id=?",
            (session_id,)).fetchall()}
        claimed_flex = {r[0] for r in conn.execute(
            "SELECT flex_row FROM assignments WHERE session_id=?",
            (session_id,)).fetchall()}
        free_swift = [r for r in swift_rows if r['row_number'] not in claimed_swift]
        free_flex  = [r for r in flex_rows  if r['row_number'] not in claimed_flex]

        # Tier set comes from match_tiers table. For one-sided sessions
        # the seeded defaults ship T3/T4 disabled (per Ecobank Ghana ops
        # policy — amount+date alone is unsafe on busy GLs). Operators
        # can enable per-account via the visual rule builder. For two-
        # sided sessions all 4 default tiers are enabled. The previous
        # hardcoded `enabled_tiers={1,2}` short-circuit is now data-
        # driven through the match_tiers rows.
        kind = sess['session_kind'] if 'session_kind' in sess.keys() else 'recon'
        tier_recon_type = 'one_sided' if kind in ('seed', 'flex_delta') else 'two_sided'
        match_tiers = load_match_tiers(conn, sess['account_id'], tier_recon_type)
        candidates = propose_candidates(free_swift, free_flex,
                                        tol=engine_tol,
                                        tiers=match_tiers,
                                        recon_type=tier_recon_type)
        resolution = resolve(candidates, free_swift, free_flex)

        cur = conn.cursor()
        if candidates:
            cur.executemany(
                "INSERT INTO candidates (session_id, swift_row, flex_row, tier, reason, amount_diff) "
                "VALUES (?,?,?,?,?,?)",
                [(session_id, c.swift_row, c.flex_row, c.tier, c.reason, c.amount_diff)
                 for c in candidates],
            )
        if resolution.assignments:
            # Auto-confirm enforcement (Phase 3): assignments produced by
            # a tier marked auto_confirm=1 in match_tiers go straight to
            # 'confirmed' instead of 'pending', with decided_by/decided_at
            # set to the run-matching actor. Operator review is bypassed
            # for these — only safe at high-confidence tiers (per
            # Ecobank Ghana ops policy: T1 strict ref+amount). The
            # match_tiers row controls per-tier eligibility.
            now_decided = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
            assignment_rows = []
            auto_confirmed_count = 0
            for a in resolution.assignments:
                if a.auto_confirm:
                    assignment_rows.append((
                        session_id, a.swift_row, a.flex_row, a.tier, a.reason,
                        a.amount_diff, 'confirmed', 'system_auto_tier', now_decided,
                    ))
                    auto_confirmed_count += 1
                else:
                    assignment_rows.append((
                        session_id, a.swift_row, a.flex_row, a.tier, a.reason,
                        a.amount_diff, 'pending', None, None,
                    ))
            cur.executemany(
                "INSERT INTO assignments (session_id, swift_row, flex_row, tier, reason, "
                "amount_diff, status, decided_by, decided_at) VALUES (?,?,?,?,?,?,?,?,?)",
                assignment_rows,
            )
            if auto_confirmed_count:
                cur.execute(
                    "INSERT INTO audit_log (session_id, action, actor, timestamp, details) "
                    "VALUES (?, 'auto_confirmed_tier1', 'system_auto_tier', ?, ?)",
                    (session_id, now_decided,
                     json.dumps({"count": auto_confirmed_count,
                                 "source": "engine_auto_confirm"})),
                )

        # Tier 5 splits + Tier 6 M:N — opt-in. Both still use hardcoded
        # engine logic (not yet user-configurable via match_tiers), so
        # they're off by default to keep the operator in control.
        split_rows: list[tuple] = []
        m2n_rows: list[tuple] = []
        if splits:
            unmatched_swift = set(resolution.unmatched_swift)
            unmatched_flex  = set(resolution.unmatched_flex)
            split_cands = propose_splits(swift_rows, flex_rows,
                                           unmatched_swift, unmatched_flex,
                                           tol=engine_tol)
            for sc in split_cands:
                grp = _uuid.uuid4().hex[:12]
                per_row_diff = sc.amount_diff / max(len(sc.swift_rows), len(sc.flex_rows))
                if len(sc.swift_rows) == 1:
                    sw = sc.swift_rows[0]
                    for fx in sc.flex_rows:
                        split_rows.append((session_id, sw, fx, sc.tier, sc.reason,
                                           per_row_diff, 'pending', 'split', grp))
                else:
                    fx = sc.flex_rows[0]
                    for sw in sc.swift_rows:
                        split_rows.append((session_id, sw, fx, sc.tier, sc.reason,
                                           per_row_diff, 'pending', 'split', grp))
            if split_rows:
                cur.executemany(
                    "INSERT INTO assignments (session_id, swift_row, flex_row, tier, "
                    "reason, amount_diff, status, source, split_group_id) "
                    "VALUES (?,?,?,?,?,?,?,?,?)",
                    split_rows,
                )

        if m2n:
            unmatched_swift = set(resolution.unmatched_swift)
            unmatched_flex  = set(resolution.unmatched_flex)
            consumed_swift = {r[1] for r in split_rows}
            consumed_flex  = {r[2] for r in split_rows}
            m2n_unmatched_swift = unmatched_swift - consumed_swift
            m2n_unmatched_flex  = unmatched_flex  - consumed_flex
            m2n_cands = propose_many_to_many(swift_rows, flex_rows,
                                               m2n_unmatched_swift, m2n_unmatched_flex,
                                               tol=engine_tol)
            for sc in m2n_cands:
                grp = _uuid.uuid4().hex[:12]
                denom = max(len(sc.swift_rows), len(sc.flex_rows))
                per_row_diff = sc.amount_diff / denom
                for sw in sc.swift_rows:
                    for fx in sc.flex_rows:
                        m2n_rows.append((session_id, sw, fx, sc.tier, sc.reason,
                                         per_row_diff, 'pending', 'split', grp))
            if m2n_rows:
                cur.executemany(
                    "INSERT INTO assignments (session_id, swift_row, flex_row, tier, "
                    "reason, amount_diff, status, source, split_group_id) "
                    "VALUES (?,?,?,?,?,?,?,?,?)",
                    m2n_rows,
                )

        # Seed today's still-unmatched residue as new open items so
        # tomorrow's delta can carry them forward.
        seeded = 0
        if seed_residue and sess['account_id'] is not None:
            seeded = seed_open_items_for_session(conn, session_id)

        # Auto-match rules — apply user-defined rules from
        # auto_categorization_rules. Opt-out via auto_rules=False.
        # These are user-configured (not engine-hardcoded).
        auto_confirmed = 0
        if auto_rules:
            from auto_match_engine import apply_auto_rules
            auto = apply_auto_rules(conn, session_id, actor='system_auto')
            auto_confirmed = (auto or {}).get('auto_confirmed', 0)

        finished = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
        elapsed = _time.time() - t0
        cur.execute(
            "INSERT INTO audit_log (session_id, action, actor, timestamp, details) "
            "VALUES (?, 'session_matched', ?, ?, ?)",
            (session_id, user, finished, json.dumps({
                "candidates_proposed": len(candidates),
                "pending_assignments": len(resolution.assignments),
                "splits_proposed":     len(split_rows),
                "m2n_proposed":        len(m2n_rows),
                "open_items_carried":
                    carried.get('cleared_against_swift', 0) +
                    carried.get('cleared_against_flex',  0),
                "open_items_seeded":   seeded,
                "auto_confirmed":      auto_confirmed,
                "started_at":          started,
                "finished_at":         finished,
                "elapsed_seconds":     round(elapsed, 2),
            })),
        )
        # Stamp finished_at on the session row alongside the audit log
        # entry — single commit so the UI sees both at once.
        cur.execute(
            "UPDATE sessions SET matching_finished_at=? WHERE id=?",
            (finished, session_id),
        )
        conn.commit()

        return MatchingResult(
            session_id=session_id,
            candidates_proposed=len(candidates),
            pending_assignments=len(resolution.assignments),
            splits_proposed=len(split_rows),
            m2n_proposed=len(m2n_rows),
            open_items_carried=(carried.get('cleared_against_swift', 0) +
                                  carried.get('cleared_against_flex',  0)),
            open_items_seeded=seeded,
            auto_confirmed=auto_confirmed,
            started_at=started,
            finished_at=finished,
            elapsed_seconds=round(elapsed, 2),
        )
    except Exception:
        # If the engine crashes mid-run, clear started_at so a stuck
        # in-flight marker doesn't permanently block re-running. We
        # use a separate connection to ensure the rollback of the
        # primary connection doesn't undo this cleanup write.
        try:
            cleanup = get_conn()
            try:
                cleanup.execute(
                    "UPDATE sessions SET matching_started_at=NULL "
                    "WHERE id=? AND matching_finished_at IS NULL",
                    (session_id,))
                cleanup.commit()
            finally:
                cleanup.close()
        except Exception:
            pass  # best-effort cleanup; don't mask the original error
        raise
    finally:
        conn.close()
