"""
auth.py
=======

MFA (TOTP, Microsoft Authenticator compatible) + session-token issuance.

Design:
    * Users enroll once by scanning a QR code into Microsoft Authenticator.
      The enrollment_token is a one-time secret generated when the user is
      created (or seeded for the bootstrap admin). After successful
      enrollment the token is cleared so the link becomes unusable.
    * Login requires (username, 6-digit TOTP code). There is no password
      yet — AD will own that when it lands; the TOTP layer doesn't care
      what happens before it.
    * A successful login issues an opaque session token stored in
      user_sessions (so admins can revoke it), and returned to the client
      to send back as X-Session-Token on subsequent requests.

TOTP parameters match Microsoft Authenticator defaults: SHA1, 6 digits,
30s period. Verification window is ±1 step (±30s) to tolerate clock drift.

Session lifetime: 8 hours. No rolling refresh — re-auth daily keeps the
audit trail clean ("who was logged in at 14:30?" → look at user_sessions).
"""

from __future__ import annotations

import base64
import hashlib
import secrets
from datetime import datetime, timedelta
from io import BytesIO

import pyotp
import threading as _threading
import time as _time
import qrcode
import qrcode.image.pil


ISSUER = "Kilter"
SESSION_LIFETIME = timedelta(hours=8)
# Idle timeout — a session that hasn't been used in this long is treated
# as expired, even if its absolute lifetime hasn't elapsed. Default 30
# minutes matches typical bank-internal-app norms; override per
# deployment via KILTER_SESSION_IDLE_MINUTES.
import os as _os
SESSION_IDLE_TIMEOUT = timedelta(minutes=int(_os.environ.get('KILTER_SESSION_IDLE_MINUTES', '30')))
TOTP_WINDOW = 1  # accept current code ± 1 step (30s either side)

# ---------------------------------------------------------------------------
# TOTP replay cache — prevents the same code being accepted twice within its
# validity window (current step ± TOTP_WINDOW × 30 s).  Keyed by
# (username, code); entries expire after 90 s (3 × step size).
# ---------------------------------------------------------------------------
_REPLAY_CACHE: dict = {}
_REPLAY_LOCK = _threading.Lock()
_REPLAY_TTL = 90.0  # seconds


def _check_and_mark_used(username: str, code: str) -> bool:
    """Return True if code was already used (replay). Mark it used otherwise."""
    key = (username, code)
    now = _time.monotonic()
    with _REPLAY_LOCK:
        # Prune stale entries inline to keep memory bounded.
        stale = [k for k, exp in _REPLAY_CACHE.items() if now >= exp]
        for k in stale:
            del _REPLAY_CACHE[k]
        if key in _REPLAY_CACHE:
            return True
        _REPLAY_CACHE[key] = now + _REPLAY_TTL
        return False


# ---------------------------------------------------------------------------
# TOTP helpers
# ---------------------------------------------------------------------------

def generate_totp_secret() -> str:
    """32-char base32 secret. pyotp's default is compatible with MS Authenticator."""
    return pyotp.random_base32()


def verify_totp(secret: str, code: str, username: str = '') -> bool:
    """Constant-time verification with clock-drift tolerance and replay prevention.

    Accepts both plaintext (legacy rows) and Fernet-encrypted secrets. The
    DB schema has TOTP secrets encrypted at rest from go-live onward; the
    fallback exists so existing pre-encryption rows keep working until the
    next enrollment / rotation re-writes them encrypted.

    Pass username so a valid code cannot be replayed within its validity window.
    Without username the replay guard is skipped (acceptable only during enrollment
    verification, where the code is consumed once and the token invalidated)."""
    if not secret or not code:
        return False
    code = code.replace(' ', '').strip()
    if not code.isdigit() or len(code) != 6:
        return False
    try:
        from secrets_vault import decrypt
        plaintext = decrypt(secret)
        if not pyotp.TOTP(plaintext).verify(code, valid_window=TOTP_WINDOW):
            return False
        if username and _check_and_mark_used(username, code):
            return False  # replay detected
        return True
    except Exception:
        return False


def provisioning_uri(secret: str, username: str) -> str:
    """The otpauth:// URI the QR code encodes. Microsoft Authenticator
    reads the issuer as the account label, so keep it stable."""
    return pyotp.TOTP(secret).provisioning_uri(name=username, issuer_name=ISSUER)


def qr_data_url(secret: str, username: str) -> str:
    """Return a data:image/png;base64,... URL for the enrollment QR. Inlined
    into the HTML — no file storage, no extra fetch."""
    uri = provisioning_uri(secret, username)
    img = qrcode.make(uri, image_factory=qrcode.image.pil.PilImage, box_size=6, border=2)
    buf = BytesIO()
    img.save(buf, format='PNG')
    return "data:image/png;base64," + base64.b64encode(buf.getvalue()).decode('ascii')


# ---------------------------------------------------------------------------
# Session tokens (kept in the user_sessions DB table so admins can revoke)
# ---------------------------------------------------------------------------

def issue_session(conn, username: str, user_agent: str | None = None) -> dict:
    """Insert a row into user_sessions and return the token + expiry."""
    token = secrets.token_urlsafe(32)
    now = datetime.utcnow()
    expires = now + SESSION_LIFETIME
    conn.execute(
        "INSERT INTO user_sessions (token, username, created_at, expires_at, "
        "user_agent, last_used_at) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (token, username, now.isoformat(), expires.isoformat(), user_agent, now.isoformat()),
    )
    return {'token': token, 'expires_at': expires.isoformat()}


def resolve_session(conn, token: str) -> str | None:
    """Return the username for a valid, unexpired, non-revoked, non-idle
    token, else None. Touches `last_used_at` on every successful resolve
    so the idle window slides forward — a session in continuous use never
    times out from idleness, only from absolute expiry."""
    if not token:
        return None
    row = conn.execute(
        "SELECT username, expires_at, revoked_at, last_used_at "
        "FROM user_sessions WHERE token=?",
        (token,),
    ).fetchone()
    if row is None or row['revoked_at']:
        return None
    now = datetime.utcnow()
    try:
        exp = datetime.fromisoformat(row['expires_at'])
    except (ValueError, TypeError):
        return None
    if now >= exp:
        return None
    # Idle-timeout enforcement. Sessions that pre-date the column have
    # last_used_at = NULL — treat them as freshly used to avoid a wave of
    # forced logouts at deploy time.
    last_used_raw = row['last_used_at'] if 'last_used_at' in row.keys() else None
    if last_used_raw:
        try:
            last_used = datetime.fromisoformat(last_used_raw)
            if now - last_used >= SESSION_IDLE_TIMEOUT:
                return None
        except (ValueError, TypeError):
            pass
    # Slide the idle window. Cheap UPDATE; the table is keyed on token.
    try:
        conn.execute(
            "UPDATE user_sessions SET last_used_at=? WHERE token=?",
            (now.isoformat(), token),
        )
        conn.commit()
    except Exception:
        # Not load-bearing for the auth decision — fail open on the touch.
        pass
    return row['username']


def revoke_session(conn, token: str) -> None:
    conn.execute(
        "UPDATE user_sessions SET revoked_at=? WHERE token=? AND revoked_at IS NULL",
        (datetime.utcnow().isoformat(), token),
    )


def revoke_all_sessions_for(conn, username: str) -> None:
    """Called when an admin deactivates or changes a user — nuke their active sessions."""
    conn.execute(
        "UPDATE user_sessions SET revoked_at=? "
        "WHERE username=? AND revoked_at IS NULL",
        (datetime.utcnow().isoformat(), username),
    )


# ---------------------------------------------------------------------------
# Enrollment tokens (one-time, used during initial QR-code setup)
# ---------------------------------------------------------------------------

def generate_enrollment_token() -> str:
    return secrets.token_urlsafe(16)


# ---------------------------------------------------------------------------
# MFA Recovery codes — single-use backup codes issued at enrollment.
# Each code is 12 chars from an unambiguous charset (no 0/O/I/1) formatted
# as XXXX-XXXX-XXXX. Stored as SHA-256 hashes; plaintext shown only once.
# ---------------------------------------------------------------------------

_RECOVERY_CHARSET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'


def _hash_recovery_code(code: str) -> str:
    return hashlib.sha256(code.encode()).hexdigest()


def generate_recovery_codes(count: int = 8) -> list[str]:
    codes = []
    for _ in range(count):
        raw = ''.join(secrets.choice(_RECOVERY_CHARSET) for _ in range(12))
        codes.append(f"{raw[:4]}-{raw[4:8]}-{raw[8:]}")
    return codes


def store_recovery_codes(conn, username: str, codes: list[str]) -> None:
    """Replace existing recovery codes with a fresh set (hashed)."""
    now = datetime.utcnow().isoformat()
    conn.execute("DELETE FROM user_recovery_codes WHERE username=?", (username,))
    rows = [(username, _hash_recovery_code(c.replace('-', '')), now) for c in codes]
    conn.executemany(
        "INSERT INTO user_recovery_codes (username, code_hash, created_at) VALUES (?,?,?)",
        rows,
    )


def consume_recovery_code(conn, username: str, code: str) -> bool:
    """Try to consume a recovery code. Returns True and marks it used if valid."""
    h = _hash_recovery_code(code.replace('-', '').upper())
    row = conn.execute(
        "SELECT id FROM user_recovery_codes "
        "WHERE username=? AND code_hash=? AND used_at IS NULL",
        (username, h),
    ).fetchone()
    if row is None:
        return False
    conn.execute(
        "UPDATE user_recovery_codes SET used_at=? WHERE id=?",
        (datetime.utcnow().isoformat(), row['id']),
    )
    return True
