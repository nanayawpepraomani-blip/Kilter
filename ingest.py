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
from datetime import datetime
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
from reconcile import load_flexcube, load_swift
from swift_loader import extract_swift_meta_raw, load_swift_raw
from open_items import (
    carry_forward_match, load_tolerance, seed_open_items_for_session,
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

        now = datetime.utcnow().isoformat()
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

        # Adjust the unmatched counts to reflect split + M:N consumption so
        # the surfaced numbers match what the review queue actually shows.
        consumed_swift = {r[1] for r in split_rows} | {r[1] for r in m2n_rows}
        consumed_flex  = {r[2] for r in split_rows} | {r[2] for r in m2n_rows}
        unmatched_swift_after = [r for r in resolution.unmatched_swift
                                 if r not in consumed_swift]
        unmatched_flex_after  = [r for r in resolution.unmatched_flex
                                 if r not in consumed_flex]

        # Rolling-ledger: clear prior carry-forward items that find a
        # counterpart in this session, then seed anything still one-sided.
        # Only runs when the session is tied to a registered account — an
        # untagged session has no account to key the ledger against.
        carried = {'cleared_against_swift': 0, 'cleared_against_flex': 0}
        seeded = 0
        if account_match is not None:
            carried = carry_forward_match(conn, session_id, tol)
            seeded = seed_open_items_for_session(conn, session_id)

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
