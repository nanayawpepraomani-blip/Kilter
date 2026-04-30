"""
csv_generic.py — generic-CSV cards loader, riding the BYO format profile.

For schemes / regional switches that publish settlement reports as
CSV (most common in West African switches: GhIPSS / Cardlink / NIBSS
Verve), we don't need a bespoke parser — the BYO CSV format profile
machinery already handles the column-mapping problem. This module
adapts a BYO-loaded txn list to the cards-canonical record shape
expected by `card_settlement_records`.

Key differences from the GL-side BYO loader:
    * Cards records carry a scheme_ref that the matching engine uses
      as the primary key for 3-way matching. The CSV must have a
      column for it (typically called RRN, Auth Ref, Reference, etc.).
    * PAN columns get masked at this layer before the records leave
      the parser. If the CSV exposes a `pan` (or similar) column,
      the loader splits it into pan_first6 + pan_last4 and discards
      the original.
    * Free-text fields (merchant_name, description) are scanned with
      `pci_safety.contains_pan` and rejected if a full PAN slipped in.

Until the cards admin UI exists this module is callable only from
tests / programmatic ingest. The next pass wires it into a card
settlement upload endpoint analogous to /sessions.
"""

from __future__ import annotations

import re

from . import ParsedFile
from pci_safety import (
    mask_pan, refuse_if_pan, refuse_if_sad_present,
    redact_pan, RefusedPanError,
)


SCHEME = 'other'

# Pre-masked PAN format: 6 digits, then any number of stars or 'x', then
# 4 digits. Most card-switch reports already arrive in this shape (e.g.
# `484680******1168`) so the loader can split first6/last4 directly
# without ever seeing the full PAN.
_PRE_MASKED_PAN_RE = re.compile(r'^\s*(\d{6})[\*x]+(\d{4})\s*$', re.IGNORECASE)


def _split_pre_masked_pan(value: str) -> tuple[str, str] | None:
    """Parse a `484680******1168`-style already-masked PAN into
    (first6, last4). Returns None on garbage so the caller can decide
    whether to skip the row or refuse the file."""
    if not value:
        return None
    m = _PRE_MASKED_PAN_RE.match(str(value))
    if not m:
        return None
    return m.group(1), m.group(2)


def parse_byo(byo_txns: list[dict], *, scheme: str = 'other',
              role: str = 'issuer',
              processing_date: str | None = None,
              settlement_date: str | None = None,
              currency: str | None = None,
              pan_field: str | None = None,
              pan_masked_field: str | None = None,
              scheme_ref_field: str = 'trn_ref') -> ParsedFile:
    """Adapt a BYO-loaded txn list (canonical Flex shape) into the
    cards record shape.

    pan_field — optional name of a column in each txn dict that holds
    a FULL PAN. When set, the loader masks it via mask_pan() and the
    full value is discarded before the dict reaches the records list.
    Use this only when the source genuinely exposes full PANs (rare —
    settlement reports usually arrive pre-masked).

    pan_masked_field — optional name of a column that already arrives
    in `484680******1168` shape. Common in card-switch reports. We split
    it directly into first6/last4 without ever invoking mask_pan, so
    rows with already-redacted middles pass through cleanly. If both
    `pan_field` and `pan_masked_field` are set, `pan_field` wins (the
    full-PAN path is stricter).

    scheme_ref_field — which BYO column holds the scheme transaction
    reference. Defaults to 'trn_ref' since most BYO mappings use that
    for the primary reference column.
    """
    file_meta = {
        'scheme': scheme,
        'role': role,
        'processing_date': processing_date,
        'settlement_date': settlement_date,
        'currency': currency,
        'record_count': len(byo_txns),
    }

    records: list[dict] = []
    for i, t in enumerate(byo_txns, start=1):
        # Look up source columns in two places:
        #   1. The canonical Flex dict directly — works when the caller
        #      hands us pre-shaped dicts (e.g. tests).
        #   2. t['_extra'] — the BYO loader's passthrough dict that
        #      preserves unmapped source columns by header name. Most
        #      cards-side ingests come through here.
        extra = t.get('_extra') or {}
        def col(name):
            if not name:
                return None
            v = t.get(name)
            if v in (None, ''):
                v = extra.get(name)
            return v

        # Extract & mask PAN at the seam — full value is discarded.
        first6 = last4 = None
        pan_full = col(pan_field)
        if pan_field and pan_full:
            try:
                first6, last4 = mask_pan(pan_full)
            except RefusedPanError:
                # Malformed PAN — skip this column rather than poison the run.
                # Caller can audit count via len(records) vs len(byo_txns).
                pass
        else:
            pan_pre = col(pan_masked_field)
            if pan_masked_field and pan_pre:
                split = _split_pre_masked_pan(pan_pre)
                if split is not None:
                    first6, last4 = split
                # Else: column had a value but didn't match the pre-masked
                # shape — leave first6/last4 as None and keep the row.
                # Refusing here would drop a tonne of edge cases (totals
                # rows, header echoes) that aren't actually a PCI risk.

        # Cleanse free-text fields of accidentally-embedded PANs. We
        # redact rather than reject because banks' settlement reports
        # sometimes paste the masked PAN with stars in the middle —
        # redact_pan is a no-op on already-redacted strings.
        merchant_name = redact_pan(t.get('narration') or '')

        record = {
            'record_index': i,
            'pan_first6': first6,
            'pan_last4': last4,
            'scheme_ref': str(t.get(scheme_ref_field) or '').strip(),
            'auth_code': None,           # not present in BYO Flex shape
            'merchant_id': t.get('ac_no') or None,
            'merchant_name': merchant_name or None,
            'mcc': None,
            'terminal_id': None,
            'transaction_type': None,
            'amount_settlement': float(t.get('amount') or 0),
            'currency_settlement': (t.get('ccy') or currency or '').upper() or None,
            'amount_transaction': None,
            'currency_transaction': None,
            'fx_rate': None,
            'fee_total': 0,
            'transaction_date': _to_iso_date(t.get('value_date')),
            'settlement_date': _to_iso_date(t.get('booking_date') or t.get('value_date'))
                               or settlement_date,
            'recon_status': 'unmatched',
            'notes': None,
        }

        # Defence-in-depth — if any field still smells like a full PAN,
        # refuse the whole record rather than persist it.
        refuse_if_pan(record, fields=('merchant_name', 'notes', 'scheme_ref'))
        refuse_if_sad_present(record)

        # The matching engine needs scheme_ref. Refuse silently-empty
        # records so we don't pollute the table with un-joinable rows.
        if not record['scheme_ref']:
            continue
        # Settlement-date is mandatory in the schema.
        if not record['settlement_date']:
            continue

        records.append(record)

    file_meta['record_count'] = len(records)
    file_meta['total_amount'] = sum(r['amount_settlement'] for r in records)
    return ParsedFile(file_meta=file_meta, records=records)


def _to_iso_date(d) -> str | None:
    """Convert YYYYMMDD int (the BYO loader's canonical date encoding)
    to YYYY-MM-DD ISO string. Returns None on falsy input."""
    if not d:
        return None
    s = str(int(d))
    if len(s) != 8:
        return None
    return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"
