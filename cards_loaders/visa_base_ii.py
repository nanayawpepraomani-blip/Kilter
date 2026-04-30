"""
visa_base_ii.py — Visa Base II clearing-file parser.

**Status: stub awaiting sample data.**

Visa Base II is a binary, fixed-position record format used for daily
clearing files between Visa and member banks (issuers and acquirers).
Each record is 168 bytes (TC4x types) or 480 bytes (TC1x/TC0x), packed
from offsets the Visa Operating Regulations specify down to the byte.

Building this parser without sample files is high-risk: byte offsets
that look right against the spec frequently differ from real-world
files (regional variants, optional sub-fields, padding conventions).
Visa publishes synthetic test data through the V.I.P. (Visa
Implementation Partner) program — that's the right way to calibrate
this parser.

Until samples are available, `parse()` and `detect()` raise
NotImplementedError so the cards-ingest dispatcher fails cleanly
rather than silently producing wrong records.

To unblock implementation, supply:
    1. A Visa-published synthetic Base II file from V.I.P.
    2. The corresponding TC-record specification document.
    3. The settlement currency and processing-bank ID for the test
       window so we can verify the running totals.

Once those are in hand, replace the NotImplementedError raises with
the actual parser. The output shape is fixed in advance by db.py
(card_settlement_records) and pci_safety.mask_pan — those contracts
do not change.
"""

from . import ParsedFile


SCHEME = 'visa'


def detect(content: bytes) -> bool:
    """Future: identify Base II by the standard file-header TC50 magic
    and the 168/480-byte record stride. Today: never claims a match,
    so the dispatcher routes the file to a manual review path."""
    raise NotImplementedError(
        "Visa Base II detect/parse is not implemented. "
        "Supply a V.I.P. synthetic test file and the TC-record spec to "
        "unblock — see cards_loaders/visa_base_ii.py docstring.")


def parse(content: bytes) -> ParsedFile:
    raise NotImplementedError(
        "Visa Base II parser pending sample data. See docstring.")
