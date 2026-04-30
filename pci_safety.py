"""
pci_safety.py
=============

Defensive helpers for the cards module. Kilter is designed to stay
**out of PCI-DSS storage scope** — full Primary Account Numbers (PANs)
must NEVER be persisted to disk. This module provides the small
toolkit every cards loader uses to enforce that contract before
records reach the database.

Quick reminder of PCI-DSS storage rules (DSS v4 §3.4):
    * Full PAN may NOT be stored unless under explicit scope-extending
      controls (encryption, key management, restricted access, etc.).
      We deliberately avoid that scope.
    * First 6 (BIN) and last 4 may be stored together without entering
      scope. card_settlement_records carries those two columns only.
    * Sensitive Authentication Data (CVV, full track, PIN block) must
      NEVER be stored in any form post-authorization. We never
      receive these in settlement files anyway, but the helper here
      flags them defensively.

Public surface:
    mask_pan(pan)           -> ('first6', 'last4')   (or raises RefusedPanError)
    is_full_pan(value)      -> bool
    contains_pan(text)      -> bool   (cheap regex scan over a free-text field)
    redact_pan(text)        -> str    (replaces matches with first6***last4)
    refuse_if_pan(record)   -> None   (raises if any value in record looks like a PAN)
"""

from __future__ import annotations

import re
from typing import Iterable


class RefusedPanError(ValueError):
    """Raised when a parser sees a full PAN that we refuse to store.

    The caller should react by either:
      (a) masking the PAN at the parser layer and continuing, or
      (b) rejecting the entire file as out-of-spec.
    Never silently swallow.
    """


# Regex for a candidate PAN. Matches strings of 13–19 digits that pass
# Luhn — covers Visa (13/16/19), Mastercard (16), Amex (15), Discover/
# JCB (16/19), local schemes (often 16 or 19). Conservative: requires
# digit-only contiguous match and Luhn checksum, so a random
# 16-character order ID like "1234567890123456" only matches if it
# happens to satisfy Luhn (rare; ~10% false-positive rate, acceptable
# for a defence-in-depth check).
_PAN_RE = re.compile(r'(?<!\d)(\d{13,19})(?!\d)')


def _luhn_ok(digits: str) -> bool:
    """RFC-style Luhn checksum. Returns True iff the string of digits
    passes; non-digit input returns False."""
    if not digits or not digits.isdigit():
        return False
    total = 0
    parity = (len(digits) - 2) & 1
    for i, ch in enumerate(digits):
        d = ord(ch) - 48
        if (i & 1) == parity:
            d *= 2
            if d > 9:
                d -= 9
        total += d
    return total % 10 == 0


def is_full_pan(value: str | None) -> bool:
    """True iff `value` is a 13–19-digit string passing Luhn."""
    if value is None:
        return False
    s = re.sub(r'[\s-]', '', str(value))
    return bool(_PAN_RE.fullmatch(s) and _luhn_ok(s))


def contains_pan(text: str | None) -> bool:
    """True iff `text` contains an embedded full-PAN-shaped substring
    that also passes Luhn. Useful for scanning narration / merchant
    name / notes fields before persistence."""
    if not text:
        return False
    for m in _PAN_RE.finditer(str(text)):
        if _luhn_ok(m.group(1)):
            return True
    return False


def mask_pan(pan: str | None) -> tuple[str, str]:
    """Reduce a full PAN to (first6, last4). The caller stores ONLY
    these two pieces; the original PAN is discarded.

    Raises RefusedPanError if the input doesn't look like a PAN at all
    — the parser should not call mask_pan() with junk; if a field is
    optional, just don't call this on it.
    """
    if pan is None:
        raise RefusedPanError("mask_pan called with None")
    s = re.sub(r'[\s-]', '', str(pan))
    if not (s.isdigit() and 13 <= len(s) <= 19):
        raise RefusedPanError(
            f"value of length {len(s)} doesn't look like a PAN; refusing to mask")
    if not _luhn_ok(s):
        # Some test data deliberately doesn't satisfy Luhn (known test
        # PANs from scheme spec docs do — this guards against typos).
        raise RefusedPanError("value failed Luhn checksum; refusing to mask")
    return s[:6], s[-4:]


def redact_pan(text: str | None) -> str:
    """Replace every embedded full PAN in `text` with first6***last4.
    No-op when `text` has no PAN-like substrings. Use on free-text
    fields (narration, notes) before persistence; combined with
    `refuse_if_pan(record)` this is belt-and-braces."""
    if not text:
        return text or ''
    out = str(text)
    for m in list(_PAN_RE.finditer(out)):
        s = m.group(1)
        if _luhn_ok(s):
            replacement = f"{s[:6]}***{s[-4:]}"
            out = out.replace(s, replacement, 1)
    return out


# Sensitive-Authentication-Data field names that must NEVER appear in a
# settlement record. Loaders enforce this via refuse_if_sad_present().
_FORBIDDEN_FIELDS = (
    'cvv', 'cvv2', 'cvc', 'cvc2', 'cid',
    'track1', 'track2', 'magnetic_stripe',
    'pin', 'pin_block', 'pin_data',
    'full_pan',                # explicit refusal even if a parser tries to bypass
)


def refuse_if_pan(record: dict, *, fields: Iterable[str] | None = None) -> None:
    """Raise RefusedPanError if any value in `record` looks like a full
    PAN. By default scans every str-typed value; pass an explicit
    `fields` iterable to limit the scan when you know which columns
    are at risk (e.g. merchant_name, narration)."""
    keys = list(fields) if fields is not None else list(record.keys())
    for k in keys:
        v = record.get(k)
        if isinstance(v, str) and contains_pan(v):
            raise RefusedPanError(
                f"field {k!r} contains a value that looks like a full PAN. "
                f"Mask or redact at the parser layer before persistence.")


def refuse_if_sad_present(record: dict) -> None:
    """Raise if a record carries Sensitive Authentication Data fields.
    Defence-in-depth — settlement files shouldn't have these anyway,
    but a malformed file or a misconfigured upstream might leak them.
    """
    lower_keys = {k.lower() for k in record.keys()}
    forbidden = lower_keys & set(_FORBIDDEN_FIELDS)
    if forbidden:
        raise RefusedPanError(
            f"record contains forbidden Sensitive Authentication Data "
            f"field(s): {sorted(forbidden)}. PCI-DSS §3.2 prohibits storing "
            f"these post-authorization. Strip at the parser layer.")
