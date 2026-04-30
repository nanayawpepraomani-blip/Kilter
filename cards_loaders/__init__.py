"""
cards_loaders — scheme-specific settlement-file parsers.

Each module in this package implements a single scheme (Visa, Mastercard,
Verve, etc.) and produces records in the canonical shape consumed by the
cards-recon engine. The shape mirrors `card_settlement_records` columns
in db.py (PCI-safe — first6 + last4 only, never full PAN).

Public contract — every loader exposes:

    parse(content: bytes) -> ParsedFile
        Where ParsedFile is a NamedTuple of:
            file_meta:  dict   matching card_settlement_files columns
            records:    list[dict]   each matching card_settlement_records

    detect(content: bytes) -> bool
        Cheap content-sniff used by the cards-ingest dispatcher when
        the operator drops a file into messages/cards/ without
        explicitly choosing a scheme. Should return True iff the
        first record / magic number unambiguously identifies the
        format. Loose matches return False — better to ask than guess.

PCI contract — every loader MUST:
    1. Mask any full PAN it sees via `pci_safety.mask_pan` before
       emitting a record. Full PAN is never written to the returned
       dicts.
    2. Call `pci_safety.refuse_if_sad_present` on each record before
       returning. Sensitive Authentication Data never leaks past the
       parser.
    3. Refuse the entire file (raise) if it contains structural fields
       Kilter doesn't expect (e.g. CVV columns in CSV exports).

Modules in this package as of the foundation pass:
    visa_base_ii.py        — Visa Base II clearing (stub, awaiting samples)
    mastercard_ipm.py      — Mastercard IPM (stub, awaiting samples)
    csv_generic.py         — generic CSV via the existing BYO loader

Note: Visa Base II and Mastercard IPM are binary fixed-position
formats whose byte layouts run to dozens of fields each. We refuse
to implement these speculatively — the byte offsets must be verified
against scheme-published test files (Visa V.I.P. and Mastercard PUF
both publish synthetic test data). Until samples are available the
stubs raise NotImplementedError with a message pointing here.
"""

from typing import NamedTuple


class ParsedFile(NamedTuple):
    """Result of a successful parser run."""
    file_meta: dict
    records: list[dict]
