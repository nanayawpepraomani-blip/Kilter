"""Tests for the session-source feature: persisting the BYO profile id
on each session and filtering /sessions by source.

What we're pinning:
    - flex_profile_id is stored on the session row when ingesting via a
      CSV profile, and is NULL when ingesting via the default xlsx
      loader.
    - GET /sessions returns flex_profile_id + flex_profile_name fields
      so the UI can render chips and the source column.
    - GET /sessions?flex_profile_id=N returns only sessions ingested via
      that profile.
    - GET /sessions?flex_profile_id=default returns only the legacy
      xlsx-loaded sessions.
    - Bogus flex_profile_id values are rejected with 400.
"""

import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


@pytest.fixture
def app_with_seed(tmp_path, monkeypatch):
    """Fresh DB + a test admin + an account + a CSV profile + three
    sessions: one via the profile, two via the default xlsx loader."""
    db_path = tmp_path / "kilter-source.db"
    monkeypatch.setenv("KILTER_DB_PATH", str(db_path))
    monkeypatch.setenv("KILTER_SECRET_KEY", "")
    monkeypatch.chdir(tmp_path)

    for mod in list(sys.modules):
        if mod in ('app', 'db', 'auth', 'secrets_vault', 'scanner', 'scheduler',
                   'recon_engine', 'ingest', 'open_items', 'byo_csv_loader'):
            sys.modules.pop(mod, None)

    from fastapi.testclient import TestClient
    import app as app_module
    import db as db_module
    db_module.init_db()

    conn = db_module.get_conn()
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    conn.execute(
        "INSERT INTO users (username, display_name, role, active, created_at, "
        "created_by, totp_secret, totp_enrolled_at) VALUES "
        "('test-admin', 'Test Admin', 'admin', 1, ?, 'system', 'JBSWY3DPEHPK3PXP', ?)",
        (now, now),
    )
    # One account
    conn.execute(
        "INSERT INTO accounts (label, swift_account, flex_ac_no, currency, "
        "active, created_at, created_by, access_area) "
        "VALUES ('Acme USD', 'SW001', 'FX001', 'USD', 1, ?, 'system', 'TREASURY')",
        (now,),
    )
    # One profile
    column_map = {
        'amount': 'Amount', 'value_date': 'Date',
        'ref': 'Ref', 'narration': 'Memo',
        'type': None, 'currency': None,
        'ac_no': None, 'ac_branch': None, 'booking_date': None,
    }
    conn.execute(
        "INSERT INTO csv_format_profiles (name, side, delimiter, header_row, "
        "skip_rows, date_format, currency, column_map, sign_convention, "
        "created_by, created_at, active) "
        "VALUES ('Acme GL', 'flex', ',', 1, 0, '%Y-%m-%d', 'USD', ?, "
        "        'positive_credit', 'system', ?, 1)",
        (json.dumps(column_map), now),
    )
    profile_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    # Three sessions: 2 default (NULL profile), 1 via the profile.
    for i, prof in enumerate([None, None, profile_id], start=1):
        conn.execute(
            "INSERT INTO sessions (created_at, created_by, swift_filename, "
            "flex_filename, flex_profile_id) "
            "VALUES (?, 'test-admin', ?, ?, ?)",
            (now, f"swift-{i}.out", f"flex-{i}.csv" if prof else f"flex-{i}.xlsx", prof),
        )
    from auth import issue_session
    sess = issue_session(conn, "test-admin")
    conn.commit(); conn.close()

    client = TestClient(app_module.app)
    client.headers.update({"X-Session-Token": sess['token']})
    yield client, profile_id


def test_sessions_lists_all_when_no_filter(app_with_seed):
    client, _ = app_with_seed
    r = client.get("/sessions")
    assert r.status_code == 200
    assert len(r.json()) == 3


def test_sessions_includes_profile_fields(app_with_seed):
    """Every row carries flex_profile_id + flex_profile_name (NULL for
    default-loaded sessions). The UI relies on these for the chip
    toolbar and the source column."""
    client, profile_id = app_with_seed
    rows = client.get("/sessions").json()
    by_filename = {s['flex_filename']: s for s in rows}
    # The CSV-loaded session has the profile fields populated.
    csv_row = by_filename['flex-3.csv']
    assert csv_row['flex_profile_id'] == profile_id
    assert csv_row['flex_profile_name'] == 'Acme GL'
    # The xlsx-loaded sessions have NULL profile fields.
    xlsx_row = by_filename['flex-1.xlsx']
    assert xlsx_row['flex_profile_id'] is None
    assert xlsx_row['flex_profile_name'] is None


def test_filter_by_profile_id_returns_only_that_profile(app_with_seed):
    client, profile_id = app_with_seed
    r = client.get(f"/sessions?flex_profile_id={profile_id}")
    body = r.json()
    assert len(body) == 1
    assert body[0]['flex_profile_id'] == profile_id


def test_filter_default_returns_only_xlsx_loaded(app_with_seed):
    """The 'default' sentinel filters to sessions with flex_profile_id
    IS NULL — i.e. the legacy/default xlsx ingest path."""
    client, _ = app_with_seed
    r = client.get("/sessions?flex_profile_id=default")
    body = r.json()
    assert len(body) == 2
    assert all(s['flex_profile_id'] is None for s in body)


def test_filter_unknown_profile_id_returns_empty(app_with_seed):
    """A profile id that exists in the column type but matches no
    session → empty list, not an error."""
    client, _ = app_with_seed
    r = client.get("/sessions?flex_profile_id=99999")
    assert r.status_code == 200
    assert r.json() == []


def test_filter_bogus_value_rejected(app_with_seed):
    """flex_profile_id must be an integer or the string 'default';
    anything else gets a 400 instead of silently ignoring."""
    client, _ = app_with_seed
    r = client.get("/sessions?flex_profile_id=banana")
    assert r.status_code == 400
    assert "flex_profile_id" in r.json()['detail']
