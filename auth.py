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
import secrets
from datetime import datetime, timedelta
from io import BytesIO

import pyotp
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
# TOTP helpers
# ---------------------------------------------------------------------------

def generate_totp_secret() -> str:
    """32-char base32 secret. pyotp's default is compatible with MS Authenticator."""
    return pyotp.random_base32()


def verify_totp(secret: str, code: str) -> bool:
    """Constant-time verification with clock-drift tolerance.

    Accepts both plaintext (legacy rows) and Fernet-encrypted secrets. The
    DB schema has TOTP secrets encrypted at rest from go-live onward; the
    fallback exists so existing pre-encryption rows keep working until the
    next enrollment / rotation re-writes them encrypted."""
    if not secret or not code:
        return False
    code = code.replace(' ', '').strip()
    if not code.isdigit() or len(code) != 6:
        return False
    try:
        from secrets_vault import decrypt
        plaintext = decrypt(secret)
        return pyotp.TOTP(plaintext).verify(code, valid_window=TOTP_WINDOW)
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
