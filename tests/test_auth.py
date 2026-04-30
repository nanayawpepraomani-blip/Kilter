"""Auth helper tests — TOTP secret generation, enrollment-token shape,
session lifecycle (issue / resolve / idle-timeout / revoke)."""

import re
import sqlite3
from datetime import datetime, timedelta

import pyotp

import auth as auth_module
from auth import (
    ISSUER, generate_totp_secret, generate_enrollment_token, verify_totp,
    issue_session, resolve_session, revoke_session, SESSION_LIFETIME,
)


def _make_session_db():
    """In-memory DB with the columns resolve_session reads. Mirrors the
    real schema; if the migration adds more columns the tests follow."""
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("""
        CREATE TABLE user_sessions (
            token         TEXT PRIMARY KEY,
            username      TEXT NOT NULL,
            created_at    TEXT NOT NULL,
            expires_at    TEXT NOT NULL,
            user_agent    TEXT,
            revoked_at    TEXT,
            last_used_at  TEXT
        )
    """)
    return conn


def test_issuer_is_kilter_not_ecobank():
    """Pinned post-rename: vendor-neutral issuer name shows in user
    authenticators. Regressing this leaks the original deployment to
    every pilot bank's users."""
    assert ISSUER == 'Kilter'


def test_totp_secret_is_base32():
    s = generate_totp_secret()
    # pyotp's random_base32 default is 32 chars.
    assert len(s) >= 16
    assert re.fullmatch(r'[A-Z2-7]+', s), "TOTP secret must be RFC 4648 base32"


def test_totp_secret_is_random():
    """Sanity — two consecutive calls must not collide."""
    a = generate_totp_secret()
    b = generate_totp_secret()
    assert a != b


def test_enrollment_token_unique_and_urlsafe():
    a = generate_enrollment_token()
    b = generate_enrollment_token()
    assert a != b
    # url-safe base64: only A-Z a-z 0-9 _ -
    assert re.fullmatch(r'[A-Za-z0-9_-]+', a)
    assert len(a) >= 16


def test_verify_totp_accepts_current_code():
    secret = generate_totp_secret()
    code = pyotp.TOTP(secret).now()
    assert verify_totp(secret, code) is True


def test_verify_totp_rejects_wrong_code():
    secret = generate_totp_secret()
    assert verify_totp(secret, '000000') is False
    assert verify_totp(secret, '') is False
    assert verify_totp(secret, 'abcdef') is False


def test_verify_totp_rejects_empty_secret():
    """Defensive — never let an unenrolled user log in even if validation
    is skipped upstream."""
    code = pyotp.TOTP(generate_totp_secret()).now()
    assert verify_totp('', code) is False


# ---------------------------------------------------------------------------
# Session lifecycle
# ---------------------------------------------------------------------------

def test_issue_then_resolve_returns_username():
    conn = _make_session_db()
    sess = issue_session(conn, "alice", user_agent="pytest")
    assert resolve_session(conn, sess['token']) == "alice"


def test_resolve_returns_none_for_unknown_or_empty_token():
    conn = _make_session_db()
    assert resolve_session(conn, "") is None
    assert resolve_session(conn, "not-a-real-token") is None


def test_revoked_session_does_not_resolve():
    conn = _make_session_db()
    sess = issue_session(conn, "alice")
    revoke_session(conn, sess['token'])
    assert resolve_session(conn, sess['token']) is None


def test_session_idle_timeout_blocks_stale_session(monkeypatch):
    """A session unused for longer than SESSION_IDLE_TIMEOUT must be
    rejected, even if its absolute expiry is hours away."""
    monkeypatch.setattr(auth_module, "SESSION_IDLE_TIMEOUT", timedelta(minutes=1))
    conn = _make_session_db()
    sess = issue_session(conn, "alice")
    # Backdate last_used_at by 2 minutes — past the idle window.
    stale = (datetime.utcnow() - timedelta(minutes=2)).isoformat()
    conn.execute("UPDATE user_sessions SET last_used_at=? WHERE token=?",
                 (stale, sess['token']))
    conn.commit()
    assert resolve_session(conn, sess['token']) is None


def test_session_idle_window_slides_with_use(monkeypatch):
    """Continuous use should keep a session alive — every successful
    resolve touches last_used_at, so a session in use never times out
    from idleness, only from absolute expiry."""
    monkeypatch.setattr(auth_module, "SESSION_IDLE_TIMEOUT", timedelta(minutes=10))
    conn = _make_session_db()
    sess = issue_session(conn, "alice")
    # Resolve a few times — each call should leave the session valid.
    for _ in range(3):
        assert resolve_session(conn, sess['token']) == "alice"
    # last_used_at must have advanced from the issue time.
    row = conn.execute(
        "SELECT created_at, last_used_at FROM user_sessions WHERE token=?",
        (sess['token'],),
    ).fetchone()
    assert row['last_used_at'] is not None


def test_legacy_session_without_last_used_at_still_resolves(monkeypatch):
    """Pre-migration rows have last_used_at = NULL. They must keep
    working until they're touched at least once, otherwise deploying the
    column would force every active operator to log in again."""
    monkeypatch.setattr(auth_module, "SESSION_IDLE_TIMEOUT", timedelta(minutes=1))
    conn = _make_session_db()
    # Insert a legacy-shape row by hand: last_used_at left NULL.
    now = datetime.utcnow()
    expires = now + SESSION_LIFETIME
    conn.execute(
        "INSERT INTO user_sessions (token, username, created_at, expires_at) "
        "VALUES (?, ?, ?, ?)",
        ("legacy-tok", "alice", now.isoformat(), expires.isoformat()),
    )
    conn.commit()
    assert resolve_session(conn, "legacy-tok") == "alice"


def test_absolutely_expired_session_does_not_resolve():
    conn = _make_session_db()
    now = datetime.utcnow()
    past = now - timedelta(hours=1)
    conn.execute(
        "INSERT INTO user_sessions (token, username, created_at, expires_at, last_used_at) "
        "VALUES (?, ?, ?, ?, ?)",
        ("expired-tok", "alice", past.isoformat(), past.isoformat(), past.isoformat()),
    )
    conn.commit()
    assert resolve_session(conn, "expired-tok") is None
