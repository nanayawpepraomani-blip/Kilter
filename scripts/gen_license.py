"""
gen_license.py
==============
TIMELESS NYPO TECH INTERNAL TOOL — do not distribute to clients.

Generates a signed kilter.lic file for a specific deployment.

Usage
-----
    python scripts/gen_license.py \\
        --licensee "Ecobank Ghana" \\
        --hostname "bank-prod-01" \\
        --expires  "2027-05-09" \\
        --out      ./kilter.lic

Arguments
---------
  --licensee   Client organisation name (printed in startup log)
  --hostname   Server hostname the license binds to  (use * for any host)
  --expires    Expiry date  YYYY-MM-DD
  --out        Output path  (default: ./kilter.lic)

Getting the client hostname
---------------------------
Ask the client to run:  python -c "import socket; print(socket.gethostname())"
on the server that will run Kilter, and send you the output.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import sys
from datetime import date, datetime
from pathlib import Path

# Must match the key in license_check.py
_SIGN_KEY = b"TNT-KILTER-2026-5c9a8f2d1b7e43a0"


def _canonical(data: dict) -> str:
    fields = ["product", "licensee", "issued", "expires", "hostname"]
    return "|".join(f"{k}={data[k]}" for k in fields)


def _sign(data: dict) -> str:
    return hmac.new(_SIGN_KEY, _canonical(data).encode(), hashlib.sha256).hexdigest()


def generate(licensee: str, hostname: str, expires: str, out: Path) -> None:
    try:
        exp_date = date.fromisoformat(expires)
    except ValueError:
        print(f"ERROR: invalid expiry date '{expires}' — use YYYY-MM-DD")
        sys.exit(1)

    if exp_date < date.today():
        print(f"WARNING: expiry date {expires} is already in the past.")

    data = {
        "product":  "Kilter",
        "licensee": licensee,
        "issued":   date.today().isoformat(),
        "expires":  expires,
        "hostname": hostname,
    }
    data["sig"] = _sign(data)

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, indent=2))

    print(f"License written to: {out}")
    print(f"  Licensee : {licensee}")
    print(f"  Hostname : {hostname}")
    print(f"  Issued   : {data['issued']}")
    print(f"  Expires  : {expires}")
    print(f"  Sig      : {data['sig'][:16]}...")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Generate a Kilter deployment license.")
    ap.add_argument("--licensee", required=True, help="Client organisation name")
    ap.add_argument("--hostname", required=True, help="Server hostname (* = any)")
    ap.add_argument("--expires",  required=True, help="Expiry date YYYY-MM-DD")
    ap.add_argument("--out",      default="./kilter.lic", help="Output path")
    args = ap.parse_args()

    generate(
        licensee=args.licensee,
        hostname=args.hostname,
        expires=args.expires,
        out=Path(args.out),
    )
