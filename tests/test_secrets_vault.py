"""Tests for the encryption-at-rest helpers.

What we're pinning:
    - encrypt → decrypt round-trips for arbitrary UTF-8 strings
    - is_encrypted correctly distinguishes Fernet ciphertext from plaintext
    - Empty / None inputs pass through unchanged on both sides
    - Wrong-key decrypt raises (no silent garbage)
    - The legacy-plaintext fallback in decrypt() returns input verbatim,
      so already-deployed plaintext rows keep working until re-written.
"""

import os

import pytest
from cryptography.fernet import Fernet, InvalidToken

import secrets_vault as sv


@pytest.fixture(autouse=True)
def fresh_key(tmp_path, monkeypatch):
    """Each test gets its own key so they don't interfere with each other
    or with the user's real .kilter_secret_key file."""
    monkeypatch.setenv('KILTER_SECRET_KEY', Fernet.generate_key().decode('ascii'))
    sv.reset_for_tests()
    yield
    sv.reset_for_tests()


def test_round_trip_simple():
    plain = "JBSWY3DPEHPK3PXP"     # looks like a TOTP secret
    ct = sv.encrypt(plain)
    assert ct != plain
    assert sv.is_encrypted(ct)
    assert sv.decrypt(ct) == plain


def test_round_trip_unicode():
    plain = "passwørd-müller-中文-🔐"
    ct = sv.encrypt(plain)
    assert sv.decrypt(ct) == plain


def test_empty_passthrough():
    assert sv.encrypt("") == ""
    assert sv.decrypt("") == ""
    assert sv.encrypt(None) is None
    assert sv.decrypt(None) is None


def test_legacy_plaintext_returned_verbatim():
    """A legacy row stored before encryption-at-rest landed must keep
    working until the caller re-writes it. decrypt() detects non-Fernet
    input and returns it untouched."""
    legacy = "JBSWY3DPEHPK3PXP"
    # is_encrypted must say False
    assert not sv.is_encrypted(legacy)
    # decrypt must passthrough
    assert sv.decrypt(legacy) == legacy


def test_wrong_key_rejects_ciphertext(monkeypatch):
    """If KILTER_SECRET_KEY rotates without re-encrypting rows, decrypt
    must raise — never silently return garbage. Operators get a hard
    error and can roll forward / back deliberately."""
    plain = "abc123"
    ct = sv.encrypt(plain)

    # Rotate to a different key.
    monkeypatch.setenv('KILTER_SECRET_KEY', Fernet.generate_key().decode('ascii'))
    sv.reset_for_tests()

    with pytest.raises(InvalidToken):
        sv.decrypt(ct)


def test_is_encrypted_rejects_obvious_non_ciphertext():
    assert not sv.is_encrypted("plain text")
    assert not sv.is_encrypted("000000")             # 6-digit code shape
    assert not sv.is_encrypted("base32abc234567")    # base32-ish


def test_each_encryption_is_unique():
    """Fernet bakes a timestamp + random IV into every ciphertext, so
    encrypting the same input twice produces different outputs. Useful
    invariant for downstream test assertions."""
    a = sv.encrypt("same input")
    b = sv.encrypt("same input")
    assert a != b
    assert sv.decrypt(a) == sv.decrypt(b) == "same input"
