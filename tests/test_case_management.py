"""End-to-end test for the case-management PATCH endpoint.

What we're pinning:
    - Schema migrations actually add the assignee/due_date/priority
      columns when starting fresh.
    - PATCH /assignments/{id}/case writes all three fields and returns
      the updated row.
    - Validation rejects bogus priority values and malformed dates.
    - The audit log records the change.
    - Empty-string semantics: '' clears the field; None leaves unchanged.

Implementation note: we set KILTER_DB_PATH to a temp file BEFORE importing
the app so init_db() and ensure_dirs() write to a throwaway location.
"""

import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


@pytest.fixture
def app_with_temp_db(tmp_path, monkeypatch):
    """Start the app pointed at a fresh DB. Inserts a session + an
    assignment so we have something to PATCH."""
    db_path = tmp_path / "kilter-test.db"
    monkeypatch.setenv("KILTER_DB_PATH", str(db_path))
    monkeypatch.setenv("KILTER_SECRET_KEY", "")  # let it auto-generate
    monkeypatch.chdir(tmp_path)

    # Force a fresh import so the new env var takes effect
    for mod in list(sys.modules):
        if mod in ('app', 'db', 'auth', 'secrets_vault', 'scanner', 'scheduler',
                   'recon_engine', 'ingest', 'open_items'):
            sys.modules.pop(mod, None)

    from fastapi.testclient import TestClient
    import app as app_module          # triggers FastAPI app construction
    import db as db_module
    db_module.init_db()                # schema + migrations on the temp DB

    # Seed: one user (admin), one session, one assignment.
    conn = db_module.get_conn()
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    conn.execute(
        "INSERT INTO users (username, display_name, role, active, created_at, "
        "created_by, totp_secret, totp_enrolled_at) VALUES "
        "('test-admin', 'Test Admin', 'admin', 1, ?, 'system', 'JBSWY3DPEHPK3PXP', ?)",
        (now, now),
    )
    conn.execute(
        "INSERT INTO sessions (created_at, created_by, swift_filename, flex_filename) "
        "VALUES (?, 'test-admin', 'a.mt940', 'b.xlsx')",
        (now,),
    )
    sess_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO assignments (session_id, swift_row, flex_row, tier, reason, "
        "amount_diff, status, source) VALUES (?, 1, 1, 1, 'test', 0.0, 'pending', 'engine')",
        (sess_id,),
    )
    asg_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    # Issue a session token directly so we can authenticate in the client.
    from auth import issue_session
    sess = issue_session(conn, "test-admin", user_agent="pytest")
    conn.commit()
    conn.close()

    client = TestClient(app_module.app)
    client.headers.update({"X-Session-Token": sess['token']})
    yield client, asg_id, sess_id


def test_patch_case_writes_all_three_fields(app_with_temp_db):
    client, asg_id, _ = app_with_temp_db
    r = client.patch(f"/assignments/{asg_id}/case", json={
        "assignee": "alice",
        "due_date": "2026-05-30",
        "priority": "high",
    })
    assert r.status_code == 200, r.text
    body = r.json()
    assert body['assignee'] == "alice"
    assert body['due_date'] == "2026-05-30"
    assert body['priority'] == "high"


def test_patch_case_empty_string_clears_field(app_with_temp_db):
    client, asg_id, _ = app_with_temp_db
    # First set
    client.patch(f"/assignments/{asg_id}/case",
                 json={"assignee": "alice", "due_date": "2026-05-30"})
    # Then clear with empty strings
    r = client.patch(f"/assignments/{asg_id}/case",
                     json={"assignee": "", "due_date": ""})
    assert r.status_code == 200
    body = r.json()
    assert body['assignee'] is None
    assert body['due_date'] is None


def test_patch_case_rejects_invalid_priority(app_with_temp_db):
    client, asg_id, _ = app_with_temp_db
    r = client.patch(f"/assignments/{asg_id}/case",
                     json={"priority": "bogus"})
    assert r.status_code == 400
    assert "priority" in r.json()['detail'].lower()


def test_patch_case_rejects_malformed_date(app_with_temp_db):
    client, asg_id, _ = app_with_temp_db
    r = client.patch(f"/assignments/{asg_id}/case",
                     json={"due_date": "not-a-date"})
    assert r.status_code == 400


def test_patch_case_rejects_unknown_assignment(app_with_temp_db):
    client, _, _ = app_with_temp_db
    r = client.patch("/assignments/99999/case", json={"priority": "high"})
    assert r.status_code == 404


def test_patch_case_emits_audit_log(app_with_temp_db, tmp_path):
    client, asg_id, _ = app_with_temp_db
    client.patch(f"/assignments/{asg_id}/case",
                 json={"priority": "urgent"})
    db_path = os.environ['KILTER_DB_PATH']
    conn = sqlite3.connect(db_path)
    rows = conn.execute(
        "SELECT action, actor, details FROM audit_log "
        "WHERE action='case_updated' ORDER BY id DESC LIMIT 1"
    ).fetchall()
    assert rows
    details = json.loads(rows[0][2])
    assert details['priority'] == 'urgent'
    assert details['assignment_id'] == asg_id
