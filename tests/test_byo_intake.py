"""Tests for the BYO intake post-pilot fixes:

1. Profile validation no longer requires a currency source IF the profile
   binds to an account.
2. _match_csv_profile resolves filename glob → profile, including the
   "no match" and "multi-match" guard cases.
3. Scanner picks up .csv files dropped in messages/flexcube/ when a
   profile's filename_pattern matches.
4. Ingest currency / ac_no fallback chain works: column → profile →
   bound account.

These tests run against pure helper functions (no FastAPI startup) plus
direct DB-fixture tests for the fallback chain.
"""

import os
import shutil
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# _match_csv_profile — pattern resolution
# ---------------------------------------------------------------------------

def test_match_csv_profile_single_hit():
    from scanner import _match_csv_profile
    profiles = [
        {'id': 1, 'name': 'acme', 'filename_pattern': 'acme_gl_*.csv'},
        {'id': 2, 'name': 'beta', 'filename_pattern': '*.tsv'},
    ]
    hit = _match_csv_profile('acme_gl_20260415.csv', profiles)
    assert hit is not None
    assert hit['id'] == 1


def test_match_csv_profile_no_match_returns_none():
    from scanner import _match_csv_profile
    profiles = [{'id': 1, 'name': 'a', 'filename_pattern': 'acme_*.csv'}]
    assert _match_csv_profile('something_else.csv', profiles) is None


def test_match_csv_profile_multi_match_returns_none():
    """Two patterns both matching the same filename is operator error.
    The scanner refuses rather than guessing; bad-pattern profiles get
    routed to UNLOADED with a clear reason."""
    from scanner import _match_csv_profile
    profiles = [
        {'id': 1, 'name': 'a', 'filename_pattern': 'acme_*.csv'},
        {'id': 2, 'name': 'b', 'filename_pattern': '*.csv'},
    ]
    assert _match_csv_profile('acme_gl.csv', profiles) is None


def test_match_csv_profile_case_insensitive():
    """Filenames on Windows can arrive in any case; pattern matching
    must be case-insensitive so profiles work cross-platform."""
    from scanner import _match_csv_profile
    profiles = [{'id': 1, 'name': 'a', 'filename_pattern': 'ACME_*.csv'}]
    assert _match_csv_profile('acme_gl.csv', profiles) is not None
    profiles = [{'id': 1, 'name': 'a', 'filename_pattern': 'acme_*.CSV'}]
    assert _match_csv_profile('ACME_GL.csv', profiles) is not None


def test_match_csv_profile_empty_pattern_skipped():
    """A profile with no pattern (manual-upload-only) must not match
    anything in the scanner — preventing accidental routing through a
    profile the operator never asked to be auto-applied."""
    from scanner import _match_csv_profile
    profiles = [
        {'id': 1, 'name': 'a', 'filename_pattern': None},
        {'id': 2, 'name': 'b', 'filename_pattern': ''},
    ]
    assert _match_csv_profile('anything.csv', profiles) is None


# ---------------------------------------------------------------------------
# _apply_profile_fallbacks_to_meta — fills holes from bound account
# ---------------------------------------------------------------------------

def test_apply_profile_fallbacks_fills_missing_ccy_and_ac_no():
    from scanner import _apply_profile_fallbacks_to_meta
    profile = {
        'currency': None,
        'bound_ac_no': '99001',
        'bound_currency': 'GHS',
    }
    meta = {'ac_no': None, 'currency': None}
    out = _apply_profile_fallbacks_to_meta(meta, profile)
    assert out['ac_no'] == '99001'
    assert out['currency'] == 'GHS'


def test_apply_profile_fallbacks_does_not_overwrite_present_values():
    from scanner import _apply_profile_fallbacks_to_meta
    profile = {
        'currency': 'EUR',
        'bound_ac_no': '99001',
        'bound_currency': 'GHS',
    }
    meta = {'ac_no': '12345', 'currency': 'USD'}
    out = _apply_profile_fallbacks_to_meta(meta, profile)
    assert out['ac_no'] == '12345'
    assert out['currency'] == 'USD'


def test_apply_profile_fallbacks_prefers_profile_currency_over_bound():
    """Profile-level currency overrides bound-account currency. Lets
    operators bind a profile to one account but explicitly state a
    different currency if needed (rare but legal)."""
    from scanner import _apply_profile_fallbacks_to_meta
    profile = {
        'currency': 'EUR',
        'bound_ac_no': '99001',
        'bound_currency': 'GHS',
    }
    meta = {'ac_no': '99001', 'currency': None}
    out = _apply_profile_fallbacks_to_meta(meta, profile)
    assert out['currency'] == 'EUR'


# ---------------------------------------------------------------------------
# Ingest fallback chain — full-stack via temp DB
# ---------------------------------------------------------------------------

@pytest.fixture
def fresh_app(tmp_path, monkeypatch):
    db_path = tmp_path / "kilter-byo.db"
    monkeypatch.setenv("KILTER_DB_PATH", str(db_path))
    monkeypatch.setenv("KILTER_SECRET_KEY", "")
    monkeypatch.chdir(tmp_path)

    for mod in list(sys.modules):
        if mod in ('app', 'db', 'auth', 'secrets_vault', 'scanner', 'scheduler',
                   'recon_engine', 'ingest', 'open_items', 'byo_csv_loader'):
            sys.modules.pop(mod, None)

    import db as db_module
    db_module.init_db()
    yield db_path


def _seed_account_and_profile(db_path: Path, *, with_currency_in_profile: bool,
                                with_currency_column: bool,
                                bind_to_account: bool) -> tuple[int, int]:
    """Seed an account + a CSV profile under various currency-source
    permutations. Returns (account_id, profile_id)."""
    import json
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    conn.execute(
        "INSERT INTO accounts (label, swift_account, flex_ac_no, currency, "
        "active, created_at, created_by, access_area) "
        "VALUES ('Acme USD', 'SW001', 'FX001', 'USD', 1, ?, 'system', 'TREASURY')",
        (now,),
    )
    acct_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    column_map = {
        'amount': 'Amount', 'value_date': 'Date',
        'ref': 'Ref', 'narration': 'Memo',
        'type': None, 'currency': 'Ccy' if with_currency_column else None,
        'ac_no': None, 'ac_branch': None, 'booking_date': None,
    }
    conn.execute(
        "INSERT INTO csv_format_profiles (name, side, delimiter, header_row, "
        "skip_rows, date_format, currency, column_map, sign_convention, "
        "account_id, created_by, created_at, active) "
        "VALUES ('test-profile', 'flex', ',', 1, 0, '%Y-%m-%d', ?, ?, "
        "        'positive_credit', ?, 'system', ?, 1)",
        ('USD' if with_currency_in_profile else None,
         json.dumps(column_map),
         acct_id if bind_to_account else None,
         now),
    )
    prof_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit(); conn.close()
    return acct_id, prof_id


def test_load_via_profile_uses_bound_account_currency(fresh_app, tmp_path):
    """Profile has no currency field, no currency column, but is bound
    to a USD account → loaded txns should carry ccy='USD'."""
    db_path = fresh_app
    _seed_account_and_profile(db_path,
                                with_currency_in_profile=False,
                                with_currency_column=False,
                                bind_to_account=True)
    csv_path = tmp_path / "acme.csv"
    csv_path.write_bytes(
        b"Date,Ref,Memo,Amount\n2026-04-01,REF1,salary,1500.00\n"
    )
    from db import get_conn
    from ingest import _load_flex_via_profile
    conn = get_conn()
    prof_id = conn.execute(
        "SELECT id FROM csv_format_profiles WHERE name='test-profile'"
    ).fetchone()['id']
    txns = _load_flex_via_profile(conn, csv_path, prof_id)
    conn.close()
    assert len(txns) == 1
    assert txns[0]['ccy'] == 'USD'


def test_load_via_profile_uses_bound_account_ac_no(fresh_app, tmp_path):
    """Profile has no ac_no column → bound account's flex_ac_no fills in."""
    db_path = fresh_app
    _seed_account_and_profile(db_path,
                                with_currency_in_profile=False,
                                with_currency_column=False,
                                bind_to_account=True)
    csv_path = tmp_path / "acme.csv"
    csv_path.write_bytes(
        b"Date,Ref,Memo,Amount\n2026-04-01,REF1,salary,1500.00\n"
    )
    from db import get_conn
    from ingest import _load_flex_via_profile
    conn = get_conn()
    prof_id = conn.execute(
        "SELECT id FROM csv_format_profiles WHERE name='test-profile'"
    ).fetchone()['id']
    txns = _load_flex_via_profile(conn, csv_path, prof_id)
    conn.close()
    assert txns[0]['ac_no'] == 'FX001'


def test_load_via_profile_raises_when_no_currency_source(fresh_app, tmp_path):
    """If profile has no currency, no column, AND no bound account, the
    ingest layer must reject before we end up with bucket-less txns."""
    db_path = fresh_app
    _seed_account_and_profile(db_path,
                                with_currency_in_profile=False,
                                with_currency_column=False,
                                bind_to_account=False)
    csv_path = tmp_path / "acme.csv"
    csv_path.write_bytes(
        b"Date,Ref,Memo,Amount\n2026-04-01,REF1,salary,1500.00\n"
    )
    from db import get_conn
    from ingest import _load_flex_via_profile, IngestError
    conn = get_conn()
    prof_id = conn.execute(
        "SELECT id FROM csv_format_profiles WHERE name='test-profile'"
    ).fetchone()['id']
    with pytest.raises(IngestError, match="currency"):
        _load_flex_via_profile(conn, csv_path, prof_id)
    conn.close()


def test_load_via_profile_currency_column_wins_over_account(fresh_app, tmp_path):
    """When a row has a currency column AND the profile is bound to a
    different-currency account, the row's value wins. The bound account
    is a fallback, not an override."""
    import json
    db_path = fresh_app
    _seed_account_and_profile(db_path,
                                with_currency_in_profile=False,
                                with_currency_column=True,
                                bind_to_account=True)
    csv_path = tmp_path / "acme.csv"
    csv_path.write_bytes(
        b"Date,Ref,Memo,Amount,Ccy\n2026-04-01,REF1,salary,1500.00,EUR\n"
    )
    from db import get_conn
    from ingest import _load_flex_via_profile
    conn = get_conn()
    prof_id = conn.execute(
        "SELECT id FROM csv_format_profiles WHERE name='test-profile'"
    ).fetchone()['id']
    txns = _load_flex_via_profile(conn, csv_path, prof_id)
    conn.close()
    assert txns[0]['ccy'] == 'EUR'   # column value, not USD from bound account
