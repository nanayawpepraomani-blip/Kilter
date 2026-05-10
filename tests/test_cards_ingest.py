"""Tests for cards_ingest.ingest_card_settlement.

What we're pinning:
    - Happy path: a CSV settlement file lands as one card_settlement_files
      row + N card_settlement_records rows in a single transaction.
    - sha256 dedup: re-uploading the same bytes is rejected as
      DuplicateCardFileError, with the prior file_id surfaced.
    - Bad scheme/role values are rejected before any I/O.
    - Profile-id required: there's no binary parser yet, so a CSV
      ingest without a profile_id raises CardsIngestError pointing to
      CARDS_DESIGN.md.
    - Records carry only first6 / last4 — no full PAN ever lands.

Uses the same fresh_app fixture pattern as test_byo_intake — a tmp DB
populated by db.init_db() and seeded via direct sqlite3 inserts.
"""

import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


# Public Visa test PAN — Luhn-valid, used in the cards test suite.
LUHN_PAN_16 = '4111111111111111'


# ---------------------------------------------------------------------------
# Shared fixtures (same shape as test_byo_intake.fresh_app).
# ---------------------------------------------------------------------------

@pytest.fixture
def fresh_app(tmp_path, monkeypatch):
    db_path = tmp_path / "kilter-cards.db"
    monkeypatch.setenv("KILTER_DB_PATH", str(db_path))
    monkeypatch.setenv("KILTER_SECRET_KEY", "")
    monkeypatch.chdir(tmp_path)
    for mod in list(sys.modules):
        if mod in ('app', 'db', 'auth', 'secrets_vault', 'scanner',
                   'scheduler', 'recon_engine', 'ingest', 'open_items',
                   'byo_csv_loader', 'cards_ingest'):
            sys.modules.pop(mod, None)
    import db as db_module
    db_module.init_db()
    yield db_path


def _seed_csv_profile(db_path: Path) -> int:
    """Seed a flex-side CSV profile that maps to the columns used in the
    test files below. Returns the new profile id."""
    column_map = {
        'amount': 'Amount', 'value_date': 'Date', 'booking_date': 'Date',
        'ref': 'Ref', 'narration': 'Memo',
        'type': None, 'currency': 'Ccy',
        'ac_no': None, 'ac_branch': None,
    }
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute(
        "INSERT INTO csv_format_profiles (name, side, delimiter, header_row, "
        "skip_rows, date_format, currency, column_map, sign_convention, "
        "account_id, created_by, created_at, active) "
        "VALUES ('cards-test', 'flex', ',', 1, 0, '%Y-%m-%d', 'USD', ?, "
        "        'positive_credit', NULL, 'system', ?, 1)",
        (json.dumps(column_map), now),
    )
    pid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit(); conn.close()
    return pid


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------

def test_ingest_persists_file_row_and_records(fresh_app, tmp_path):
    pid = _seed_csv_profile(fresh_app)
    csv = tmp_path / "issuer.csv"
    csv.write_bytes(
        b"Date,Ref,Memo,Amount,Ccy\n"
        b"2026-04-15,RRN001,Acme purchase,125.50,USD\n"
        b"2026-04-15,RRN002,Beta refund,-12.00,USD\n"
    )
    from cards_ingest import ingest_card_settlement
    result = ingest_card_settlement(
        file_path=csv, scheme='visa', role='issuer',
        settlement_date='2026-04-15', currency='USD',
        ingested_by='alice', original_filename='issuer.csv',
        profile_id=pid,
    )
    assert result.record_count == 2
    assert result.scheme == 'visa'
    assert result.role == 'issuer'

    from db import get_conn
    conn = get_conn()
    file_row = conn.execute(
        "SELECT * FROM card_settlement_files WHERE id=?", (result.file_id,),
    ).fetchone()
    assert file_row['scheme'] == 'visa'
    assert file_row['record_count'] == 2
    assert file_row['ingested_by'] == 'alice'
    assert file_row['sha256']
    rows = conn.execute(
        "SELECT * FROM card_settlement_records WHERE file_id=? ORDER BY record_index",
        (result.file_id,),
    ).fetchall()
    conn.close()
    assert len(rows) == 2
    assert rows[0]['scheme_ref'] == 'RRN001'
    assert rows[0]['amount_settlement'] == 125.50
    assert rows[0]['recon_status'] == 'unmatched'


def test_ingest_dedups_on_sha256(fresh_app, tmp_path):
    """Re-uploading the same bytes returns DuplicateCardFileError with
    the prior file_id, mirroring the cash-side ingested_files pattern."""
    pid = _seed_csv_profile(fresh_app)
    csv = tmp_path / "issuer.csv"
    csv.write_bytes(
        b"Date,Ref,Memo,Amount,Ccy\n"
        b"2026-04-15,RRN001,Acme,100,USD\n"
    )
    from cards_ingest import ingest_card_settlement, DuplicateCardFileError

    first = ingest_card_settlement(
        file_path=csv, scheme='visa', role='issuer',
        settlement_date='2026-04-15', currency='USD',
        ingested_by='alice', profile_id=pid,
    )
    with pytest.raises(DuplicateCardFileError) as exc:
        ingest_card_settlement(
            file_path=csv, scheme='visa', role='issuer',
            settlement_date='2026-04-15', currency='USD',
            ingested_by='bob', profile_id=pid,
        )
    assert exc.value.prior_file_id == first.file_id


def test_ingest_rejects_invalid_scheme(fresh_app, tmp_path):
    pid = _seed_csv_profile(fresh_app)
    csv = tmp_path / "x.csv"
    csv.write_bytes(b"Date,Ref,Memo,Amount,Ccy\n2026-04-15,R,m,1,USD\n")
    from cards_ingest import ingest_card_settlement, CardsIngestError
    with pytest.raises(CardsIngestError, match="scheme"):
        ingest_card_settlement(
            file_path=csv, scheme='paypal', role='issuer',
            settlement_date='2026-04-15', currency='USD',
            ingested_by='alice', profile_id=pid,
        )


def test_ingest_rejects_invalid_role(fresh_app, tmp_path):
    pid = _seed_csv_profile(fresh_app)
    csv = tmp_path / "x.csv"
    csv.write_bytes(b"Date,Ref,Memo,Amount,Ccy\n2026-04-15,R,m,1,USD\n")
    from cards_ingest import ingest_card_settlement, CardsIngestError
    with pytest.raises(CardsIngestError, match="role"):
        ingest_card_settlement(
            file_path=csv, scheme='visa', role='processor',
            settlement_date='2026-04-15', currency='USD',
            ingested_by='alice', profile_id=pid,
        )


def test_ingest_requires_profile_id(fresh_app, tmp_path):
    """Until binary parsers ship, profile_id is mandatory. The error
    message must point operators at CARDS_DESIGN.md."""
    csv = tmp_path / "x.csv"
    csv.write_bytes(b"Date,Ref,Memo,Amount,Ccy\n2026-04-15,R,m,1,USD\n")
    from cards_ingest import ingest_card_settlement, CardsIngestError
    with pytest.raises(CardsIngestError, match="CARDS_DESIGN"):
        ingest_card_settlement(
            file_path=csv, scheme='visa', role='issuer',
            settlement_date='2026-04-15', currency='USD',
            ingested_by='alice', profile_id=None,
        )


def test_ingest_rejects_unknown_profile(fresh_app, tmp_path):
    csv = tmp_path / "x.csv"
    csv.write_bytes(b"Date,Ref,Memo,Amount,Ccy\n2026-04-15,R,m,1,USD\n")
    from cards_ingest import ingest_card_settlement, CardsIngestError
    with pytest.raises(CardsIngestError, match="profile"):
        ingest_card_settlement(
            file_path=csv, scheme='visa', role='issuer',
            settlement_date='2026-04-15', currency='USD',
            ingested_by='alice', profile_id=99999,
        )


def test_ingest_drops_records_without_scheme_ref(fresh_app, tmp_path):
    """A row without a scheme_ref is unmatchable. The parser drops it;
    the file row's record_count reflects the kept records, not the
    source row count."""
    pid = _seed_csv_profile(fresh_app)
    csv = tmp_path / "issuer.csv"
    csv.write_bytes(
        b"Date,Ref,Memo,Amount,Ccy\n"
        b"2026-04-15,RRN001,kept,100,USD\n"
        b"2026-04-15,,dropped no ref,50,USD\n"
    )
    from cards_ingest import ingest_card_settlement
    result = ingest_card_settlement(
        file_path=csv, scheme='visa', role='issuer',
        settlement_date='2026-04-15', currency='USD',
        ingested_by='alice', profile_id=pid,
    )
    assert result.record_count == 1
    assert result.skipped_records == 1


def test_ingest_redacts_pan_in_narration(fresh_app, tmp_path):
    """A row whose narration carries a full PAN is redacted at parser
    level — the persisted merchant_name contains first6***last4 and
    NEVER the full PAN."""
    pid = _seed_csv_profile(fresh_app)
    csv = tmp_path / "issuer.csv"
    csv.write_bytes(
        f"Date,Ref,Memo,Amount,Ccy\n"
        f"2026-04-15,RRN001,Refund on {LUHN_PAN_16},100,USD\n".encode()
    )
    from cards_ingest import ingest_card_settlement
    result = ingest_card_settlement(
        file_path=csv, scheme='visa', role='issuer',
        settlement_date='2026-04-15', currency='USD',
        ingested_by='alice', profile_id=pid,
    )
    from db import get_conn
    conn = get_conn()
    row = conn.execute(
        "SELECT merchant_name FROM card_settlement_records WHERE file_id=?",
        (result.file_id,),
    ).fetchone()
    conn.close()
    assert LUHN_PAN_16 not in (row['merchant_name'] or '')
    assert '411111***1111' in row['merchant_name']
