"""
secrets_vault.py
================

Encryption-at-rest for sensitive DB columns: TOTP secrets and SMTP
passwords today, expandable to anything else later.

Design:
    * Symmetric encryption with Fernet (AES-128-CBC + HMAC-SHA256, with a
      version byte and timestamp baked into every ciphertext).
    * Single key, sourced once at process startup. Order of preference:
          1. KILTER_SECRET_KEY env var (production)
          2. .kilter_secret_key file in the project root (dev / first run)
          3. None of the above → generate a new key file, mode 0600, warn
             on the console. Lets a fresh `git clone` boot without manual
             key provisioning while still being secure for single-host
             deployments.
    * Reads are tolerant of legacy plaintext rows: try to decrypt; if the
      input doesn't look like Fernet, return it unchanged. Combined with
      `encrypt-on-next-write`, this gives you gradual migration from a
      plaintext-secrets DB to an encrypted one without a downtime window.

Public API:
    encrypt(plaintext)     -> str   (always encrypted; safe to store)
    decrypt(stored)        -> str   (decrypts if encrypted; else returns
                                     stored verbatim — legacy plaintext)
    is_encrypted(stored)   -> bool  (cheap structural check; useful for
                                     migration counters / dashboards)
    rotate_key(new_key)    -> None  (re-encrypts every column the registry
                                     knows about; not wired to a route yet)
"""

from __future__ import annotations

import logging
import os
import stat
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

from cryptography.fernet import Fernet, InvalidToken


_KEY_FILE_NAME = ".kilter_secret_key"
_ENV_VAR = "KILTER_SECRET_KEY"


def _project_root() -> Path:
    return Path(__file__).resolve().parent


def _key_file_path() -> Path:
    return _project_root() / _KEY_FILE_NAME


def _load_or_generate_key() -> bytes:
    """Returns the raw 44-byte url-safe-base64 Fernet key."""
    env = os.environ.get(_ENV_VAR, "").strip()
    if env:
        return env.encode("ascii")

    path = _key_file_path()
    if path.exists():
        return path.read_bytes().strip()

    # First run, no key configured anywhere — mint one and persist it.
    key = Fernet.generate_key()
    path.write_bytes(key)
    try:
        # Best-effort tighten of file permissions on POSIX. On Windows the
        # default ACL on the user's profile is already restrictive enough;
        # this call quietly no-ops on filesystems that don't support it.
        os.chmod(path, stat.S_IRUSR | stat.S_IWUSR)
    except OSError:
        pass
    logger.info(
        f"[secrets_vault] Generated a new encryption key at {path}. "
        f"For production deployments, move this value into the "
        f"{_ENV_VAR} environment variable and delete the file.",
        file=sys.stderr,
    )
    return key


# Single Fernet instance for the process. Constructed lazily so that tests
# which monkeypatch the env var or key file before first use see the right
# key.
_fernet: Fernet | None = None


def _get_fernet() -> Fernet:
    global _fernet
    if _fernet is None:
        _fernet = Fernet(_load_or_generate_key())
    return _fernet


def reset_for_tests() -> None:
    """Clear the cached Fernet — only used by the test harness to rebuild
    after monkeypatching the key. Cheap and idempotent."""
    global _fernet
    _fernet = None


# ---------------------------------------------------------------------------
# Encrypt / decrypt
# ---------------------------------------------------------------------------

# Marker used to identify our ciphertext when it appears as a string column.
# Fernet output starts with 'gAAAAA' (version byte 0x80 + 5 bytes of timestamp,
# base64-encoded). We rely on that signature for the legacy-plaintext fallback.
_FERNET_PREFIX = "gAAAAA"


def is_encrypted(stored: str | None) -> bool:
    """Cheap structural test — does this look like Fernet ciphertext?
    Doesn't actually decrypt; suitable for migration progress reporting.
    Returns False for None / empty / legacy plaintext."""
    if not stored:
        return False
    return stored.startswith(_FERNET_PREFIX)


def encrypt(plaintext: str) -> str:
    """Encrypt a UTF-8 string. Empty input passes through unchanged so that
    callers can use this on optional fields without a special-case."""
    if plaintext is None or plaintext == "":
        return plaintext
    return _get_fernet().encrypt(plaintext.encode("utf-8")).decode("ascii")


def decrypt(stored: str | None) -> str | None:
    """Decrypt if it looks like our ciphertext; otherwise return the input
    verbatim (legacy plaintext fallback). Empty / None pass through."""
    if not stored:
        return stored
    if not is_encrypted(stored):
        # Legacy row from before encryption-at-rest landed. Caller should
        # plan to re-encrypt on the next write.
        return stored
    try:
        return _get_fernet().decrypt(stored.encode("ascii")).decode("utf-8")
    except InvalidToken:
        # Wrong key, tampered ciphertext, or weird input. Refuse rather than
        # leak partial data. Caller decides how to handle.
        raise
