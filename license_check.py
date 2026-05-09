"""
license_check.py
================
Startup license enforcement for Kilter.

How it works
------------
1.  Kilter ships with this file obfuscated (PyArmor).  The HMAC secret
    embedded here is therefore hidden from clients.

2.  For each deployment Timeless Nypo Tech generates a `kilter.lic` file
    using scripts/gen_license.py and delivers it alongside the software.

3.  On startup, verify_license() is called.  It checks:
      - kilter.lic exists in the deploy root
      - the HMAC signature is valid (prevents hand-editing)
      - the license has not expired
      - the current machine hostname matches the license record

4.  Grace period: if no kilter.lic exists at all, a 14-day grace window
    is granted from first launch (tracked in .kilter_grace).  After that,
    the app refuses to start until a valid license is installed.

5.  Dev bypass: set env var  KILTER_DEV=1  to skip all checks during
    internal development.  Never set this on client deployments.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import socket
import sys
from datetime import date, datetime
from pathlib import Path

# ---------------------------------------------------------------------------
# PRIVATE: embedded signing key.  Change before distributing to a new client
# tier, or after any suspected key compromise.  This value is hidden once the
# file is processed by PyArmor.
# ---------------------------------------------------------------------------
_SIGN_KEY = b"TNT-KILTER-2026-5c9a8f2d1b7e43a0"

_ROOT        = Path(__file__).resolve().parent
_LIC_FILE    = _ROOT / "kilter.lic"
_GRACE_FILE  = _ROOT / ".kilter_grace"
_GRACE_DAYS  = 14


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _canonical(data: dict) -> str:
    """Stable string to sign: pipe-separated sorted key=value pairs."""
    fields = ["product", "licensee", "issued", "expires", "hostname"]
    return "|".join(f"{k}={data[k]}" for k in fields)


def _sign(data: dict) -> str:
    return hmac.new(_SIGN_KEY, _canonical(data).encode(), hashlib.sha256).hexdigest()


def _verify_sig(data: dict, sig: str) -> bool:
    expected = _sign(data)
    return hmac.compare_digest(expected, sig)


def _grace_remaining() -> int | None:
    """
    Return days remaining in grace period, or None if grace already expired.
    Creates .kilter_grace on first call.
    """
    if not _GRACE_FILE.exists():
        _GRACE_FILE.write_text(date.today().isoformat())
    first_run = date.fromisoformat(_GRACE_FILE.read_text().strip())
    elapsed   = (date.today() - first_run).days
    remaining = _GRACE_DAYS - elapsed
    return remaining if remaining > 0 else None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def verify_license() -> None:
    """
    Called at FastAPI startup.  Raises SystemExit(1) with a clear message
    if the license is invalid or missing beyond the grace period.
    """
    if os.environ.get("KILTER_DEV") == "1":
        print("[license] DEV mode — license check bypassed.")
        return

    # ── No license file ────────────────────────────────────────────────────
    if not _LIC_FILE.exists():
        remaining = _grace_remaining()
        if remaining is None:
            _abort(
                "No license file found and the 14-day grace period has expired.\n"
                "  Install kilter.lic in the application directory.\n"
                "  Contact: timelessnypotech@outlook.com"
            )
        print(
            f"[license] WARNING: no kilter.lic found.  "
            f"Grace period: {remaining} day(s) remaining.\n"
            f"           Contact timelessnypotech@outlook.com to obtain a license."
        )
        return

    # ── Parse ───────────────────────────────────────────────────────────────
    try:
        raw  = json.loads(_LIC_FILE.read_text())
        sig  = raw.pop("sig", "")
        data = {k: raw[k] for k in ["product", "licensee", "issued", "expires", "hostname"]}
    except Exception:
        _abort("kilter.lic is malformed or unreadable.")

    # ── Signature ───────────────────────────────────────────────────────────
    if not _verify_sig(data, sig):
        _abort(
            "kilter.lic signature is invalid.  The file may have been altered.\n"
            "  Contact: timelessnypotech@outlook.com"
        )

    # ── Expiry ──────────────────────────────────────────────────────────────
    try:
        expires = date.fromisoformat(data["expires"])
    except ValueError:
        _abort("kilter.lic contains an invalid expiry date.")

    today = date.today()
    if today > expires:
        _abort(
            f"License expired on {data['expires']}.\n"
            f"  Licensee : {data['licensee']}\n"
            f"  Contact  : timelessnypotech@outlook.com to renew."
        )

    days_left = (expires - today).days
    if days_left <= 30:
        print(
            f"[license] WARNING: license expires in {days_left} day(s) "
            f"({data['expires']}).  Contact timelessnypotech@outlook.com to renew."
        )

    # ── Hostname ────────────────────────────────────────────────────────────
    current_host = socket.gethostname()
    if data["hostname"].lower() not in ("*", current_host.lower()):
        _abort(
            f"License hostname mismatch.\n"
            f"  Licensed for : {data['hostname']}\n"
            f"  This machine : {current_host}\n"
            f"  Contact      : timelessnypotech@outlook.com"
        )

    print(
        f"[license] Valid — Licensee: {data['licensee']} | "
        f"Expires: {data['expires']} ({days_left} days)"
    )


def _abort(msg: str) -> None:
    print(f"\n{'='*60}")
    print("  KILTER — LICENSE ERROR")
    print(f"{'='*60}")
    for line in msg.splitlines():
        print(f"  {line}")
    print(f"{'='*60}\n")
    sys.exit(1)
