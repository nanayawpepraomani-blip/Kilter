"""Phase 2 tests — Day-0 proof seeding.

Pins:
  * Happy path: anchor advances, session_kind='seed', open items seeded
  * Re-seed rejected (AlreadySeededError)
  * Currency mismatch rejected
  * Unknown account rejected
  * Duplicate file rejected (sha-based)
  * Self-match within the proof: paired CR/DR cancel, only residue
    becomes open items
  * audit_log entry written
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pytest
from openpyxl import Workbook


PROOF_HEADER = [
    'Account', 'Value date', 'Curr.', 'Amount', 'S',
    '', 'Origin', 'Type', 'Status', 'age', 'Book. date',
    'Our reference 1', 'Their reference 1',
    'Booking text 1', 'Booking text 2',
]


@pytest.fixture
def fresh_db(tmp_path, monkeypatch):
    """Build a clean DB at a tmp path so tests don't trample real data."""
    db = tmp_path / 'test.db'
    monkeypatch.setenv('KILTER_DB_PATH', str(db))
    # Force module-level constants in db.py + scanner.py to re-read the env.
    import db as db_mod
    monkeypatch.setattr(db_mod, 'DB_PATH', db, raising=False)
    db_mod.init_db()
    return db


def _insert_one_sided_account(db_path: Path, *, label='BTW GL',
                               flex_ac_no='1441000601589',
                               currency='GHS') -> int:
    """One-sided account fixture: swift_account is empty-string sentinel
    (preserves the UNIQUE constraint without needing a real BIC pair)."""
    conn = sqlite3.connect(db_path)
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    conn.execute(
        "INSERT INTO accounts (label, swift_account, flex_ac_no, currency, "
        "active, created_at, created_by, access_area, account_recon_type) "
        "VALUES (?, '', ?, ?, 1, ?, 'system', 'MOBILE MONEY', 'one_sided')",
        (label, flex_ac_no, currency, now),
    )
    acct_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit(); conn.close()
    return acct_id


def _proof_row(value_date, amount, sign, ref, narration='wallet credit'):
    return [
        'BANK TO WALLET GL ACCOUNT', value_date, 'GHS', amount, sign,
        amount, 'Our', 'Other', 'Open', 0,
        datetime(2026, 4, 29) if isinstance(value_date, int) else value_date,
        ref, None, narration[:40], narration,
    ]


def _write_proof(path: Path, rows: list[list], *, currency='GHS') -> None:
    wb = Workbook()
    ws = wb.active
    ws.append(PROOF_HEADER)
    ws.append([None] * len(PROOF_HEADER))
    for r in rows:
        if currency != 'GHS':
            r = list(r); r[2] = currency
        ws.append(r)
    wb.save(path)


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

def test_seed_anchors_account(fresh_db, tmp_path):
    """Day-0 seed (load-only): 1 CR + 1 DR persisted, anchor advances
    to (CR_amount − DR_amount). No matching, no open_items at load
    time — those are deferred to a later run_matching call."""
    from ingest import ingest_proof_seed

    acct_id = _insert_one_sided_account(fresh_db)
    proof = tmp_path / 'btw_proof.xlsx'
    _write_proof(proof, [
        _proof_row(20260429, 100.0, 'C', 'CR_REF'),
        _proof_row(20260430,  30.0, 'D', 'DR_REF'),
    ])

    result = ingest_proof_seed(proof, acct_id, user='ops')
    # Load-only contract: rows persisted, no engine work yet.
    assert result.swift_rows == 1
    assert result.flex_rows == 1
    assert result.pending_assignments == 0
    assert result.open_items_seeded == 0
    assert result.account_registered

    conn = sqlite3.connect(fresh_db); conn.row_factory = sqlite3.Row
    acct = dict(conn.execute(
        "SELECT * FROM accounts WHERE id=?", (acct_id,)).fetchone())
    sess = dict(conn.execute(
        "SELECT * FROM sessions WHERE id=?", (result.session_id,)).fetchone())
    open_count = conn.execute(
        "SELECT COUNT(*) FROM open_items WHERE account_id=?",
        (acct_id,)).fetchone()[0]
    audit = conn.execute(
        "SELECT action FROM audit_log WHERE session_id=? AND action='proof_loaded'",
        (result.session_id,)).fetchone()
    conn.close()

    # Anchor: 100 (CR) − 30 (DR) = 70.
    assert acct['last_closing_balance'] == 70.0
    assert acct['last_closing_date'] == 20260430
    assert acct['last_session_id'] == result.session_id
    # Session metadata captures the seed shape; status starts 'open'
    # so ops can run matching + close explicitly.
    assert sess['session_kind'] == 'seed'
    assert sess['flex_opening_balance'] == 0.0
    assert sess['flex_closing_balance'] == 70.0
    assert sess['status'] == 'open'
    # No open items at load time.
    assert open_count == 0
    # Audit log captured the load.
    assert audit is not None


def test_seed_self_matches_paired_legs_after_run_matching(fresh_db, tmp_path):
    """Proof contains a CR and DR with the same ref + amount. After
    run_matching, the pair self-matches (Tier 1 within the proof), so
    neither becomes an open item.

    Note: seeded match_tiers ship DISABLED — the operator opts in via
    the Matching tiers UI before running. This test enables Tier 1
    explicitly to verify the engine still produces the expected pair
    when the tier IS enabled."""
    from ingest import ingest_proof_seed, run_matching

    acct_id = _insert_one_sided_account(fresh_db)
    proof = tmp_path / 'btw_proof.xlsx'
    _write_proof(proof, [
        _proof_row(20260429, 100.0, 'C', 'PAIRED-REF'),
        _proof_row(20260429, 100.0, 'D', 'PAIRED-REF'),
    ])

    load_result = ingest_proof_seed(proof, acct_id, user='ops')
    # Load-only at this point — no assignments yet.
    assert load_result.pending_assignments == 0
    assert load_result.open_items_seeded == 0

    # Enable Tier 1 (Strong match) for the one_sided default tier set so
    # the engine has a rule to fire on. Mirrors the Matching tiers UI
    # action of clicking the "Enabled" toggle on the seeded T1 row.
    conn = sqlite3.connect(fresh_db); conn.row_factory = sqlite3.Row
    conn.execute(
        "UPDATE match_tiers SET enabled=1 "
        "WHERE account_id IS NULL AND recon_type='one_sided' AND legacy_tier=1"
    )
    conn.commit(); conn.close()

    # Now run the matching engine.
    match = run_matching(load_result.session_id, user='ops')
    assert match.pending_assignments == 1   # the paired match
    assert match.open_items_seeded == 0     # no residue → nothing to seed
    assert match.elapsed_seconds >= 0       # timer captured

    conn = sqlite3.connect(fresh_db); conn.row_factory = sqlite3.Row
    acct = dict(conn.execute(
        "SELECT * FROM accounts WHERE id=?", (acct_id,)).fetchone())
    open_count = conn.execute(
        "SELECT COUNT(*) FROM open_items WHERE account_id=?",
        (acct_id,)).fetchone()[0]
    conn.close()
    assert acct['last_closing_balance'] == 0.0
    assert open_count == 0


def test_seed_negative_closing(fresh_db, tmp_path):
    """The real BTW pattern: many small credits, a few large debits.
    Closing comes out negative — should be stored exactly as-is so the
    next delta's opening can match against the same negative anchor."""
    from ingest import ingest_proof_seed

    acct_id = _insert_one_sided_account(fresh_db)
    proof = tmp_path / 'btw_proof.xlsx'
    rows = [
        _proof_row(20260429, 100.0, 'C', f'CR_{i}') for i in range(10)
    ] + [
        _proof_row(20260427, 5000.0, 'D', 'BIG_DR'),
    ]
    _write_proof(proof, rows)

    result = ingest_proof_seed(proof, acct_id, user='ops')
    conn = sqlite3.connect(fresh_db); conn.row_factory = sqlite3.Row
    acct = dict(conn.execute(
        "SELECT last_closing_balance FROM accounts WHERE id=?",
        (acct_id,)).fetchone())
    conn.close()
    # 10 × 100 = 1000 CR; 1 × 5000 DR; net = 1000 − 5000 = −4000.
    assert acct['last_closing_balance'] == -4000.0


# ---------------------------------------------------------------------------
# Error cases
# ---------------------------------------------------------------------------

def test_reseed_rejected(fresh_db, tmp_path):
    """Re-seeding a seeded account raises AlreadySeededError. Anchor
    stays at the original value."""
    from ingest import AlreadySeededError, ingest_proof_seed

    acct_id = _insert_one_sided_account(fresh_db)
    p1 = tmp_path / 'first.xlsx'
    _write_proof(p1, [_proof_row(20260429, 1.0, 'C', 'REF1')])
    ingest_proof_seed(p1, acct_id, user='ops')

    p2 = tmp_path / 'second.xlsx'
    _write_proof(p2, [_proof_row(20260430, 999.0, 'C', 'REF2')])
    with pytest.raises(AlreadySeededError) as exc_info:
        ingest_proof_seed(p2, acct_id, user='ops')
    # Error carries enough context for the operator to know what to do.
    assert exc_info.value.account_id == acct_id
    assert exc_info.value.current_anchor == 1.0

    # Anchor unchanged.
    conn = sqlite3.connect(fresh_db); conn.row_factory = sqlite3.Row
    acct = dict(conn.execute(
        "SELECT last_closing_balance FROM accounts WHERE id=?",
        (acct_id,)).fetchone())
    conn.close()
    assert acct['last_closing_balance'] == 1.0


def test_unknown_account_rejected(fresh_db, tmp_path):
    from ingest import IngestError, ingest_proof_seed
    p = tmp_path / 'p.xlsx'
    _write_proof(p, [_proof_row(20260429, 1.0, 'C', 'X')])
    with pytest.raises(IngestError, match='not found'):
        ingest_proof_seed(p, 99999, user='ops')


def test_currency_mismatch_rejected(fresh_db, tmp_path):
    """Account is GHS, proof is USD — refuse rather than silently
    anchor the account at a USD balance under a GHS label."""
    from ingest import IngestError, ingest_proof_seed
    acct_id = _insert_one_sided_account(fresh_db, currency='GHS')
    p = tmp_path / 'p.xlsx'
    _write_proof(p, [_proof_row(20260429, 1.0, 'C', 'X')], currency='USD')
    with pytest.raises(IngestError, match="doesn't match"):
        ingest_proof_seed(p, acct_id, user='ops')


def test_duplicate_file_rejected(fresh_db, tmp_path):
    """Same content hash as a prior ingest → DuplicateFileError. Anchor
    is set by the FIRST ingest; the second ingest doesn't re-anchor."""
    from ingest import DuplicateFileError, ingest_proof_seed
    acct_id_a = _insert_one_sided_account(fresh_db, label='A')
    acct_id_b = _insert_one_sided_account(fresh_db, label='B',
                                            flex_ac_no='OTHER')
    p = tmp_path / 'p.xlsx'
    _write_proof(p, [_proof_row(20260429, 1.0, 'C', 'X')])
    ingest_proof_seed(p, acct_id_a, user='ops')
    with pytest.raises(DuplicateFileError):
        ingest_proof_seed(p, acct_id_b, user='ops')


def test_empty_proof_rejected(fresh_db, tmp_path):
    """A proof with valid header but no usable rows shouldn't anchor
    the account at zero — that's a different operation (clear-anchor)."""
    from ingest import IngestError, ingest_proof_seed
    acct_id = _insert_one_sided_account(fresh_db)
    p = tmp_path / 'p.xlsx'
    _write_proof(p, [])  # header + blank, no data rows
    with pytest.raises(IngestError, match='no usable rows'):
        ingest_proof_seed(p, acct_id, user='ops')
