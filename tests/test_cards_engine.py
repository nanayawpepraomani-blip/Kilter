"""Tests for cards_engine — the scheme_ref-join match engine.

What we're pinning:
    - Two files contributing to the same scheme_ref + identical
      settlement amounts → 'matched'.
    - Two files with diverging amounts (beyond tolerance) → 'mismatched'.
    - Single file contributing alone → 'unmatched'.
    - apply_match_status writes the computed status into the records,
      stamps matched_at + matched_by for matched groups, and leaves
      'disputed' / 'written_off' rows untouched (operator wins).
    - Filter args (scheme, settlement_date_from/_to) narrow the population.
    - Re-running recompute is idempotent.
"""

import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


@pytest.fixture
def fresh_app(tmp_path, monkeypatch):
    db_path = tmp_path / "kilter-engine.db"
    monkeypatch.setenv("KILTER_DB_PATH", str(db_path))
    monkeypatch.setenv("KILTER_SECRET_KEY", "")
    monkeypatch.setenv("KILTER_CARDS_REQUIRED_STAGES", "")
    monkeypatch.chdir(tmp_path)
    for mod in list(sys.modules):
        if mod in ('app', 'db', 'auth', 'secrets_vault', 'scanner', 'scheduler',
                   'recon_engine', 'ingest', 'open_items', 'byo_csv_loader',
                   'cards_ingest', 'cards_engine'):
            sys.modules.pop(mod, None)
    import db as db_module
    db_module.init_db()
    yield db_path


def _seed_file(conn, *, scheme='visa', role='issuer',
                settlement_date='2026-04-15', sha=None,
                ingested_by='test') -> int:
    sha = sha or f'sha-{datetime.now(timezone.utc).replace(tzinfo=None).timestamp()}-{role}-{settlement_date}'
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    cur = conn.execute(
        "INSERT INTO card_settlement_files "
        "(sha256, scheme, role, processing_date, settlement_date, "
        " record_count, total_amount, currency, ingested_at, ingested_by) "
        "VALUES (?, ?, ?, ?, ?, 0, 0, 'GHS', ?, ?)",
        (sha, scheme, role, settlement_date, settlement_date, now, ingested_by),
    )
    return cur.lastrowid


def _seed_record(conn, *, file_id, scheme_ref, amount,
                  pan_last4='1111', currency='GHS', recon_status='unmatched',
                  settlement_date='2026-04-15', record_index=None) -> int:
    if record_index is None:
        record_index = (conn.execute(
            "SELECT COALESCE(MAX(record_index), 0) FROM card_settlement_records "
            "WHERE file_id=?", (file_id,)).fetchone()[0]) + 1
    cur = conn.execute(
        "INSERT INTO card_settlement_records "
        "(file_id, record_index, pan_first6, pan_last4, scheme_ref, "
        " amount_settlement, currency_settlement, settlement_date, "
        " fee_total, recon_status) "
        "VALUES (?, ?, '484680', ?, ?, ?, ?, ?, 0, ?)",
        (file_id, record_index, pan_last4, scheme_ref, amount, currency,
         settlement_date, recon_status),
    )
    return cur.lastrowid


# ---------------------------------------------------------------------------
# compute_match_groups — classification
# ---------------------------------------------------------------------------

def test_two_files_same_amount_classify_as_matched(fresh_app):
    """Acquirer + issuer file both carry the same scheme_ref with the
    same settlement amount → matched."""
    from db import get_conn
    from cards_engine import compute_match_groups
    conn = get_conn()
    f1 = _seed_file(conn, role='acquirer', sha='a')
    f2 = _seed_file(conn, role='issuer', sha='b')
    _seed_record(conn, file_id=f1, scheme_ref='RRN001', amount=100.00)
    _seed_record(conn, file_id=f2, scheme_ref='RRN001', amount=100.00)
    conn.commit()

    groups = compute_match_groups(conn)
    conn.close()
    assert len(groups) == 1
    g = groups[0]
    assert g.scheme_ref == 'RRN001'
    assert g.file_count == 2
    assert g.record_count == 2
    assert g.status == 'matched'
    assert g.amount_spread == 0.0


def test_amount_within_tolerance_still_matched(fresh_app):
    """0.01-cent rounding gap stays inside the default tolerance."""
    from db import get_conn
    from cards_engine import compute_match_groups, DEFAULT_TOLERANCE
    conn = get_conn()
    f1 = _seed_file(conn, role='acquirer', sha='a')
    f2 = _seed_file(conn, role='issuer', sha='b')
    _seed_record(conn, file_id=f1, scheme_ref='RRN002', amount=100.00)
    _seed_record(conn, file_id=f2, scheme_ref='RRN002', amount=100.00 + DEFAULT_TOLERANCE)
    conn.commit()
    groups = compute_match_groups(conn)
    conn.close()
    assert groups[0].status == 'matched'


def test_two_files_diverging_amounts_classify_as_mismatched(fresh_app):
    from db import get_conn
    from cards_engine import compute_match_groups
    conn = get_conn()
    f1 = _seed_file(conn, role='acquirer', sha='a')
    f2 = _seed_file(conn, role='issuer', sha='b')
    _seed_record(conn, file_id=f1, scheme_ref='RRN003', amount=100.00)
    _seed_record(conn, file_id=f2, scheme_ref='RRN003', amount=98.50)   # 1.50 short
    conn.commit()
    groups = compute_match_groups(conn)
    conn.close()
    assert len(groups) == 1
    assert groups[0].status == 'mismatched'
    assert groups[0].amount_spread == 1.50


def test_single_file_contribution_classify_as_unmatched(fresh_app):
    """A scheme_ref appearing only in one file — auth ingested but
    settlement file not yet uploaded, or a solo reversal."""
    from db import get_conn
    from cards_engine import compute_match_groups
    conn = get_conn()
    f1 = _seed_file(conn, role='acquirer', sha='a')
    _seed_record(conn, file_id=f1, scheme_ref='RRN004', amount=50.00)
    _seed_record(conn, file_id=f1, scheme_ref='RRN004', amount=50.00)  # 2 rows, same file
    conn.commit()
    groups = compute_match_groups(conn)
    conn.close()
    assert len(groups) == 1
    assert groups[0].status == 'unmatched'
    assert groups[0].record_count == 2
    assert groups[0].file_count == 1


def test_three_files_all_match_classify_as_matched(fresh_app):
    """The canonical 3-way: auth + clearing + settlement."""
    from db import get_conn
    from cards_engine import compute_match_groups
    conn = get_conn()
    f1 = _seed_file(conn, role='acquirer', sha='a')
    f2 = _seed_file(conn, role='switch', sha='b')
    f3 = _seed_file(conn, role='issuer', sha='c')
    for f in (f1, f2, f3):
        _seed_record(conn, file_id=f, scheme_ref='RRN005', amount=200.00)
    conn.commit()
    groups = compute_match_groups(conn)
    conn.close()
    assert len(groups) == 1
    g = groups[0]
    assert g.status == 'matched'
    assert g.file_count == 3
    assert g.record_count == 3


def test_filter_by_scheme(fresh_app):
    from db import get_conn
    from cards_engine import compute_match_groups
    conn = get_conn()
    fv = _seed_file(conn, scheme='visa', sha='v')
    fm = _seed_file(conn, scheme='mastercard', sha='m')
    _seed_record(conn, file_id=fv, scheme_ref='V1', amount=10)
    _seed_record(conn, file_id=fm, scheme_ref='M1', amount=20)
    conn.commit()
    visa_only = compute_match_groups(conn, scheme='visa')
    mc_only = compute_match_groups(conn, scheme='mastercard')
    conn.close()
    assert {g.scheme_ref for g in visa_only} == {'V1'}
    assert {g.scheme_ref for g in mc_only} == {'M1'}


def test_filter_by_settlement_date_range(fresh_app):
    from db import get_conn
    from cards_engine import compute_match_groups
    conn = get_conn()
    f1 = _seed_file(conn, sha='a', settlement_date='2026-04-10')
    f2 = _seed_file(conn, sha='b', settlement_date='2026-04-20')
    _seed_record(conn, file_id=f1, scheme_ref='OLD', amount=10,
                 settlement_date='2026-04-10')
    _seed_record(conn, file_id=f2, scheme_ref='NEW', amount=10,
                 settlement_date='2026-04-20')
    conn.commit()
    only_new = compute_match_groups(conn, settlement_date_from='2026-04-15')
    only_old = compute_match_groups(conn, settlement_date_to='2026-04-15')
    conn.close()
    assert {g.scheme_ref for g in only_new} == {'NEW'}
    assert {g.scheme_ref for g in only_old} == {'OLD'}


def test_groups_ignore_blank_scheme_ref(fresh_app):
    """Records with empty scheme_ref are unjoinable — the engine
    excludes them from groups entirely (the cards loader already
    drops these at ingest, this is defence in depth)."""
    from db import get_conn
    from cards_engine import compute_match_groups
    conn = get_conn()
    f1 = _seed_file(conn, sha='a')
    _seed_record(conn, file_id=f1, scheme_ref='', amount=10)
    _seed_record(conn, file_id=f1, scheme_ref='REAL', amount=20)
    conn.commit()
    groups = compute_match_groups(conn)
    conn.close()
    assert len(groups) == 1
    assert groups[0].scheme_ref == 'REAL'


# ---------------------------------------------------------------------------
# apply_match_status — persistence
# ---------------------------------------------------------------------------

def test_apply_writes_status_back_to_records(fresh_app):
    from db import get_conn
    from cards_engine import compute_match_groups, apply_match_status
    conn = get_conn()
    f1 = _seed_file(conn, role='acquirer', sha='a')
    f2 = _seed_file(conn, role='issuer', sha='b')
    r1 = _seed_record(conn, file_id=f1, scheme_ref='M1', amount=100)
    r2 = _seed_record(conn, file_id=f2, scheme_ref='M1', amount=100)
    conn.commit()

    groups = compute_match_groups(conn)
    result = apply_match_status(conn, groups, actor='alice')
    assert result.matched == 1
    assert result.records_updated == 2

    rows = conn.execute(
        "SELECT id, recon_status, matched_at, matched_by "
        "FROM card_settlement_records ORDER BY id"
    ).fetchall()
    conn.close()
    for r in rows:
        assert r['recon_status'] == 'matched'
        assert r['matched_at'] is not None
        assert r['matched_by'] == 'alice'


def test_apply_preserves_disputed_and_written_off(fresh_app):
    """Operator-set states must never be overwritten — even if the
    engine would compute a different status, 'disputed' / 'written_off'
    win."""
    from db import get_conn
    from cards_engine import compute_match_groups, apply_match_status
    conn = get_conn()
    f1 = _seed_file(conn, role='acquirer', sha='a')
    f2 = _seed_file(conn, role='issuer', sha='b')
    rd = _seed_record(conn, file_id=f1, scheme_ref='X1', amount=100,
                      recon_status='disputed')
    rw = _seed_record(conn, file_id=f1, scheme_ref='X2', amount=200,
                      recon_status='written_off')
    rn = _seed_record(conn, file_id=f2, scheme_ref='X1', amount=100)
    rn2 = _seed_record(conn, file_id=f2, scheme_ref='X2', amount=200)
    conn.commit()

    groups = compute_match_groups(conn)
    result = apply_match_status(conn, groups, actor='alice')

    # 4 rows total, 2 protected → only the 2 non-protected get updated.
    assert result.records_protected == 2
    assert result.records_updated == 2

    statuses = {r['id']: r['recon_status'] for r in conn.execute(
        "SELECT id, recon_status FROM card_settlement_records").fetchall()}
    conn.close()
    assert statuses[rd] == 'disputed'
    assert statuses[rw] == 'written_off'
    assert statuses[rn] == 'matched'
    assert statuses[rn2] == 'matched'


def test_apply_writes_null_matched_fields_for_non_matched_groups(fresh_app):
    """Only 'matched' status stamps matched_at + matched_by. mismatched
    and unmatched leave those columns NULL so audit can distinguish a
    real reconciliation from a passive 'still waiting' state."""
    from db import get_conn
    from cards_engine import compute_match_groups, apply_match_status
    conn = get_conn()
    f1 = _seed_file(conn, role='acquirer', sha='a')
    f2 = _seed_file(conn, role='issuer', sha='b')
    _seed_record(conn, file_id=f1, scheme_ref='X1', amount=100)
    _seed_record(conn, file_id=f2, scheme_ref='X1', amount=80)   # mismatch
    _seed_record(conn, file_id=f1, scheme_ref='X2', amount=200)  # solo
    conn.commit()

    apply_match_status(conn, compute_match_groups(conn))
    rows = conn.execute(
        "SELECT scheme_ref, recon_status, matched_at, matched_by "
        "FROM card_settlement_records ORDER BY scheme_ref, file_id"
    ).fetchall()
    conn.close()
    for r in rows:
        if r['recon_status'] == 'matched':
            assert r['matched_at'] is not None
            assert r['matched_by'] is not None
        else:
            assert r['matched_at'] is None
            assert r['matched_by'] is None


def test_apply_is_idempotent(fresh_app):
    from db import get_conn
    from cards_engine import compute_match_groups, apply_match_status
    conn = get_conn()
    f1 = _seed_file(conn, role='acquirer', sha='a')
    f2 = _seed_file(conn, role='issuer', sha='b')
    _seed_record(conn, file_id=f1, scheme_ref='I1', amount=10)
    _seed_record(conn, file_id=f2, scheme_ref='I1', amount=10)
    conn.commit()

    g1 = compute_match_groups(conn); r1 = apply_match_status(conn, g1)
    g2 = compute_match_groups(conn); r2 = apply_match_status(conn, g2)
    conn.close()

    assert r1.matched == r2.matched == 1
    assert r1.records_updated == 2
    # Second pass: rows are already 'matched' so UPDATE still touches
    # them (no WHERE-status filter on re-write), but the count stays
    # consistent — the test pins that re-running doesn't crash or
    # produce divergent state.
    assert r2.records_updated == 2
