"""
swift_loader.py
===============

Thin adapter over swift_core for Kilter: read a raw MT940 or MT950 .out
file and return the transaction-dict shape the reconciliation engine
already expects (same keys as reconcile.load_swift produces from a
pre-parsed xlsx).

Lets Kilter consume raw .out files dropped straight into
messages/swift/ — no more external mt940_to_excel / mt950_to_excel step.
"""

from __future__ import annotations

import re
from pathlib import Path

from swift_core import detect_message_type, parse_swift_statement


# Block 2 of MT940/MT950 output:
#     {2:O<mtype:3><input-time:4><input-date:6><sender-bic:8-12>...}
# Example: {2:O9500000991231BAGHGHA2XXXX...}
#                          ^^^^^^ ^^^^^^^^^^^^ input date + sender BIC (12-char)
# BIC is 4-letter bank + 2-letter country + 2-alnum location [+ 3-alnum branch].
# The sender of an output message is the correspondent bank — the counterparty
# we want to identify the nostro with.
BLOCK2_SENDER_BIC = re.compile(
    r'\{2:O\d{3}\d{4}\d{6}(?P<bic>[A-Z]{4}[A-Z]{2}[A-Z0-9]{2}(?:[A-Z0-9]{3,4})?)'
)


def load_swift_raw(path: Path) -> list:
    """Parse MT940/MT950 .out into the txn dict shape used by the engine."""
    parsed = _read_and_parse(path)
    txns: list[dict] = []
    for idx, t in enumerate(parsed['transactions'], start=1):
        if t.get('parse_error'):
            # Skip but preserve the row number so downstream indexing stays
            # intuitive if an ops user checks the source line count.
            continue
        txns.append({
            '_source': 'swift',
            '_row_number': idx,
            '_used': False,
            'value_date': t['value_date'],
            'amount': t['amount'],
            'sign': t['sign'],
            'origin': 'Their',
            'type': 'Other',
            'status': 'Unmatched',
            'book_date': t['entry_date'],
            'our_ref':       t.get('account_ref') or '',
            'their_ref':     t.get('bank_ref') or '',
            'booking_text_1': t.get('supplementary') or '',
            'booking_text_2': t.get('narrative') or '',
        })
    return txns


def extract_swift_meta_raw(path: Path) -> dict:
    """Metadata dict matching account_meta.extract_swift_meta's shape."""
    raw = path.read_text(encoding='latin-1', errors='replace')
    parsed = parse_swift_statement(raw)
    meta: dict = {}
    m = BLOCK2_SENDER_BIC.search(raw)
    if m:
        # SWIFT app header carries a 12-char logical-terminal address: the
        # 8-char BIC + 4-char LT code. The LT code is usually 'XXXX' but can
        # be 'AXXX' or similar for branch terminals — always trim to the BIC.
        bic = m.group('bic')
        meta['bic'] = bic[:8] if len(bic) == 12 else bic
    if parsed.get('account'):
        meta['account'] = parsed['account']
    if parsed.get('transaction_reference'):
        meta['statement_ref'] = parsed['transaction_reference']
    if parsed.get('statement_number'):
        meta['statement_number'] = parsed['statement_number']
    if parsed.get('opening_balance'):
        ob = parsed['opening_balance']
        meta['currency'] = ob['currency']
        meta['opening_balance'] = (
            f"{ob['mark']} {ob['currency']} {ob['amount']:,.2f} on {ob['date']}"
        )
        meta['opening_balance_amount'] = ob['amount']
        meta['opening_balance_sign']   = ob['mark']
        meta['opening_balance_date']   = ob['date']
    if parsed.get('closing_balance'):
        cb = parsed['closing_balance']
        meta['closing_balance'] = (
            f"{cb['mark']} {cb['currency']} {cb['amount']:,.2f} on {cb['date']}"
        )
        meta['closing_balance_amount'] = cb['amount']
        meta['closing_balance_sign']   = cb['mark']
        meta['closing_balance_date']   = cb['date']
    return meta


def _read_and_parse(path: Path) -> dict:
    # SWIFT files are latin-1 by convention; defensive decode in case of stray bytes.
    raw = path.read_text(encoding='latin-1', errors='replace')
    mt = detect_message_type(raw)
    if mt not in ('940', '950'):
        raise ValueError(
            f"'{path.name}' is not MT940 or MT950 "
            f"(detected message type: {mt or 'none'})"
        )
    return parse_swift_statement(raw)
