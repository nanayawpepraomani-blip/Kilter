"""Auth helper tests — TOTP secret generation, enrollment-token shape."""

import re

import pyotp

from auth import (
    ISSUER, generate_totp_secret, generate_enrollment_token, verify_totp,
)


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
