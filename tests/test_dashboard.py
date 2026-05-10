"""End-to-end tests for the dashboard KPI / case-load / by-account
endpoints.

What we're pinning:
    - /dashboard/kpis returns the right structural shape and computes
      tier-1 rate, oldest open age, and SLA-breached count from real DB
      rows (not from mocks).
    - /dashboard/case-load groups pending assignments by assignee with
      correct overdue counts.
    - /dashboard/by-account orders accounts by descending open-item count.
    - Empty-state behaviour (no sessions, no open items) returns clean
      defaults rather than NULL or 500.
"""

import os
import sys
import sqlite3
from datetime import date, datetime, timedelta, timezone

import pytest


@pytest.fixture
def app_with_temp_db(tmp_path, monkeypatch):
    """Spin up the app pointed at a fresh DB. Same pattern as
    test_case_management — temp dir + fresh module imports."""
    db_path = tmp_path / "kilter-test.db"
    monkeypatch.setenv("KILTER_DB_PATH", str(db_path))
    monkeypatch.setenv("KILTER_SECRET_KEY", "")
    monkeypatch.chdir(tmp_path)

    for mod in list(sys.modules):
        if mod in ('app', 'db', 'auth', 'secrets_vault', 'scanner', 'scheduler',
                   'recon_engine', 'ingest', 'open_items'):
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
    from auth import issue_session
    sess = issue_session(conn, "test-admin")
    conn.commit()
    conn.close()

    client = TestClient(app_module.app)
    client.headers.update({"X-Session-Token": sess['token']})
    yield client, db_path


# ---------------------------------------------------------------------------
# /dashboard/kpis
# ---------------------------------------------------------------------------

def test_kpis_empty_state(app_with_temp_db):
    """Fresh DB: no decisions, no open items. Endpoint must return clean
    null/zero defaults rather than NULL-from-SQL or a 500."""
    client, _ = app_with_temp_db
    r = client.get("/dashboard/kpis")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body['tier1_rate'] is None
    assert body['oldest_open_days'] is None
    assert body['sla_breached'] == 0
    assert body['total_open_items'] == 0
    assert body['total_decided_14d'] == 0


def test_kpis_tier1_rate_computed_correctly(app_with_temp_db):
    """8 confirmed assignments — 6 at tier 1, 2 at tier 4. Expected
    tier-1 rate is 75.0% (6/8). Pinned because a regression here means
    the headline KPI on the dashboard misleads ops."""
    client, db_path = app_with_temp_db
    conn = sqlite3.connect(db_path)
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    conn.execute("INSERT INTO sessions (created_at, created_by, swift_filename, "
                 "flex_filename) VALUES (?, 'test', 'a', 'b')", (now,))
    sess_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    for i, tier in enumerate([1, 1, 1, 1, 1, 1, 4, 4]):
        conn.execute(
            "INSERT INTO assignments (session_id, swift_row, flex_row, tier, "
            "reason, amount_diff, status, source, decided_at) "
            "VALUES (?, ?, ?, ?, 'r', 0.0, 'confirmed', 'engine', ?)",
            (sess_id, i, i, tier, now),
        )
    conn.commit(); conn.close()

    r = client.get("/dashboard/kpis")
    body = r.json()
    assert body['tier1_rate'] == 75.0
    assert body['total_decided_14d'] == 8
    assert body['tier_counts'] == {'1': 6, '4': 2}


def test_kpis_oldest_open_days(app_with_temp_db):
    """Insert two open items at 5 days and 30 days old. KPI must report
    30 — the MAX, not the MIN or average."""
    client, db_path = app_with_temp_db
    conn = sqlite3.connect(db_path)
    five_days_ago = (datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=5)).isoformat()
    thirty_days_ago = (datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(days=30)).isoformat()
    # Distinct src_row_number values needed — UNIQUE constraint covers
    # (account_id, source_side, src_session_id, src_row_number).
    for i, opened in enumerate([five_days_ago, thirty_days_ago]):
        conn.execute(
            "INSERT INTO open_items (account_id, source_side, src_session_id, "
            "src_row_number, value_date, amount, sign, ref, narration, status, opened_at) "
            "VALUES (1, 'swift', 1, ?, 20260420, 100.0, 'C', 'X', 'n', 'open', ?)",
            (i, opened),
        )
    conn.commit(); conn.close()

    r = client.get("/dashboard/kpis")
    body = r.json()
    assert body['oldest_open_days'] == 30
    assert body['total_open_items'] == 2


def test_kpis_sla_breached_count(app_with_temp_db):
    """Three pending assignments: one due yesterday (breach), one due
    today (not breach), one with no due date (not breach). Endpoint
    must report exactly 1."""
    client, db_path = app_with_temp_db
    conn = sqlite3.connect(db_path)
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    conn.execute("INSERT INTO sessions (created_at, created_by, swift_filename, "
                 "flex_filename) VALUES (?, 'test', 'a', 'b')", (now,))
    sess_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    today = date.today().isoformat()
    for i, due in enumerate([yesterday, today, None]):
        conn.execute(
            "INSERT INTO assignments (session_id, swift_row, flex_row, tier, "
            "reason, amount_diff, status, source, due_date) "
            "VALUES (?, ?, ?, 1, 'r', 0.0, 'pending', 'engine', ?)",
            (sess_id, i, i, due),
        )
    conn.commit(); conn.close()

    r = client.get("/dashboard/kpis")
    assert r.json()['sla_breached'] == 1


# ---------------------------------------------------------------------------
# /dashboard/case-load
# ---------------------------------------------------------------------------

def test_case_load_groups_and_counts(app_with_temp_db):
    """Three pending assignments: alice has 2 (one overdue, one urgent),
    bob has 1, one is unassigned. Endpoint groups correctly."""
    client, db_path = app_with_temp_db
    conn = sqlite3.connect(db_path)
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    conn.execute("INSERT INTO sessions (created_at, created_by, swift_filename, "
                 "flex_filename) VALUES (?, 'test', 'a', 'b')", (now,))
    sess_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    rows = [
        ('alice', yesterday, 'normal'),    # overdue
        ('alice', None,      'urgent'),    # high priority
        ('bob',   None,      'normal'),
        (None,    None,      'normal'),    # unassigned
    ]
    for i, (assignee, due, prio) in enumerate(rows):
        conn.execute(
            "INSERT INTO assignments (session_id, swift_row, flex_row, tier, "
            "reason, amount_diff, status, source, assignee, due_date, priority) "
            "VALUES (?, ?, ?, 1, 'r', 0.0, 'pending', 'engine', ?, ?, ?)",
            (sess_id, i, i, assignee, due, prio),
        )
    conn.commit(); conn.close()

    r = client.get("/dashboard/case-load")
    body = r.json()
    by_assignee = {row['assignee']: row for row in body}
    assert by_assignee['alice']['pending'] == 2
    assert by_assignee['alice']['overdue'] == 1
    assert by_assignee['alice']['high_priority'] == 1
    assert by_assignee['bob']['pending'] == 1
    assert by_assignee['(unassigned)']['pending'] == 1


# ---------------------------------------------------------------------------
# /dashboard/by-account
# ---------------------------------------------------------------------------

def test_by_account_orders_by_count_desc(app_with_temp_db):
    """Three accounts with 5 / 3 / 1 open items respectively. Endpoint
    must return them ordered by count descending."""
    client, db_path = app_with_temp_db
    conn = sqlite3.connect(db_path)
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    # Create three accounts, then open items against them.
    for i, label in enumerate(['Citibank USD', 'JPM EUR', 'BoNY GBP'], start=1):
        conn.execute(
            "INSERT INTO accounts (id, label, swift_account, flex_ac_no, currency, "
            "active, created_at, created_by, access_area) "
            "VALUES (?, ?, ?, ?, ?, 1, ?, 'system', 'TREASURY')",
            (i, label, f"SW{i}", f"FX{i}", 'USD' if i == 1 else 'EUR' if i == 2 else 'GBP', now),
        )
    counts = {1: 5, 2: 3, 3: 1}
    for acct_id, n in counts.items():
        for j in range(n):
            conn.execute(
                "INSERT INTO open_items (account_id, source_side, src_session_id, "
                "src_row_number, value_date, amount, sign, ref, narration, "
                "status, opened_at) "
                "VALUES (?, 'swift', 1, ?, 20260420, 100.0, 'C', 'X', 'n', 'open', ?)",
                (acct_id, j, now),
            )
    conn.commit(); conn.close()

    r = client.get("/dashboard/by-account")
    body = r.json()
    assert [row['count'] for row in body] == [5, 3, 1]
    assert body[0]['label'] == 'Citibank USD'
