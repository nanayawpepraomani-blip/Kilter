"""
mastercard_ipm.py — Mastercard IPM (Integrated Product Messages) parser.

**Status: stub awaiting sample data.**

Mastercard IPM is the binary clearing format Mastercard members exchange
through the Mastercard File Express (MFE) gateway. Each record is a
TC (Transaction Category) item — bit-mapped fields with PDS (Private
Data Sub-element) extensions for scheme-specific data. Building it
without samples is high-risk: the bit-map encoding is unforgiving and
small offset errors silently corrupt fees and rates.

Mastercard provides synthetic test files through the PUF (Production
Use Files) and Customer Test environments — that's the calibration
source for this parser.

Same contract as visa_base_ii.py: until samples are available we
raise NotImplementedError. The output shape (db.py
card_settlement_records) and PCI rules (pci_safety) do not change.

To unblock:
    1. A Mastercard-published synthetic IPM file (typically .DAT
       extension, ~1.0 KB / record).
    2. The TC-43 / TC-44 / TC-46 sub-element spec for the relevant
       countries.
    3. The processing-currency and ICA for verification.
"""

from . import ParsedFile


SCHEME = 'mastercard'


def detect(content: bytes) -> bool:
    raise NotImplementedError(
        "Mastercard IPM detect/parse is not implemented. "
        "Supply a Mastercard PUF synthetic file and TC-43/44/46 spec to "
        "unblock — see cards_loaders/mastercard_ipm.py docstring.")


def parse(content: bytes) -> ParsedFile:
    raise NotImplementedError(
        "Mastercard IPM parser pending sample data. See docstring.")
