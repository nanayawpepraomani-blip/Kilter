"""Tests for the generic-CSV cards loader (cards_loaders/csv_generic.py).

What we're pinning:
    - parse_byo turns a list of canonical Flex txns into the cards
      record shape with PCI-safe fields.
    - PAN columns (when present in the source) get masked before the
      record reaches the records list.
    - Records missing a scheme_ref or settlement_date are dropped
      rather than persisted half-formed.
    - Free-text fields with embedded PANs are redacted (not silently
      passed through).
    - file_meta carries record_count and total_amount so the cards
      ingest endpoint can persist the parent file row.
"""

import pytest

from cards_loaders.csv_generic import parse_byo
from pci_safety import RefusedPanError


# Same canonical test PAN as in test_pci_safety.
LUHN_PAN_16 = '4111111111111111'


def _txn(**overrides):
    """Default canonical-Flex txn shape (matches the BYO loader output)."""
    base = {
        '_source': 'flex', '_row_number': 1, '_used': False,
        'trn_ref':       'RRN001',
        'ac_branch':     '',
        'ac_no':         'MERCH001',
        'booking_date':  20260415,
        'value_date':    20260415,
        'type':          'CR',
        'narration':     'Purchase at Acme Co',
        'amount':        125.50,
        'ccy':           'USD',
        'module':        '',
        'external_ref':  'RRN001',
        'user_id':       '',
    }
    base.update(overrides)
    return base


def test_parse_byo_emits_card_record_shape():
    out = parse_byo([_txn()],
                    scheme='visa', role='issuer', currency='USD',
                    settlement_date='2026-04-15')
    assert len(out.records) == 1
    r = out.records[0]
    expected_keys = {
        'record_index', 'pan_first6', 'pan_last4', 'scheme_ref', 'auth_code',
        'merchant_id', 'merchant_name', 'mcc', 'terminal_id', 'transaction_type',
        'amount_settlement', 'currency_settlement', 'amount_transaction',
        'currency_transaction', 'fx_rate', 'fee_total', 'transaction_date',
        'settlement_date', 'recon_status', 'notes',
    }
    assert set(r.keys()) == expected_keys
    assert r['scheme_ref'] == 'RRN001'
    assert r['amount_settlement'] == 125.50
    assert r['currency_settlement'] == 'USD'
    assert r['recon_status'] == 'unmatched'
    assert r['transaction_date'] == '2026-04-15'


def test_parse_byo_file_meta_carries_aggregates():
    out = parse_byo([_txn(amount=100.0), _txn(amount=50.0, _row_number=2)],
                    scheme='mastercard', role='acquirer', currency='GHS',
                    settlement_date='2026-04-20')
    assert out.file_meta['scheme'] == 'mastercard'
    assert out.file_meta['role'] == 'acquirer'
    assert out.file_meta['record_count'] == 2
    assert out.file_meta['total_amount'] == 150.0
    assert out.file_meta['settlement_date'] == '2026-04-20'


def test_parse_byo_masks_pan_when_pan_field_set():
    """A txn carrying a full PAN under a configured column has the PAN
    split into first6/last4 and the original discarded before the
    record leaves the parser."""
    txn = _txn()
    txn['card_number'] = LUHN_PAN_16
    out = parse_byo([txn], pan_field='card_number',
                    settlement_date='2026-04-15')
    r = out.records[0]
    assert r['pan_first6'] == '411111'
    assert r['pan_last4'] == '1111'
    # The original column is not in the record output — only first6/last4.
    assert 'card_number' not in r


def test_parse_byo_redacts_pan_in_narration():
    """A narration that contains a full PAN gets redacted to first6***last4
    rather than carried through verbatim — defence-in-depth against
    cleared fields leaking into merchant_name."""
    txn = _txn(narration=f'Purchase on card {LUHN_PAN_16}')
    out = parse_byo([txn], settlement_date='2026-04-15')
    r = out.records[0]
    assert LUHN_PAN_16 not in (r['merchant_name'] or '')
    assert '411111***1111' in (r['merchant_name'] or '')


def test_parse_byo_drops_record_without_scheme_ref():
    """Cards records need a scheme_ref to participate in 3-way matching.
    Records missing one are dropped silently — no point persisting
    rows the engine can't join."""
    out = parse_byo([_txn(trn_ref='', external_ref='')],
                    settlement_date='2026-04-15')
    assert out.records == []
    assert out.file_meta['record_count'] == 0


def test_parse_byo_drops_record_without_settlement_date():
    """Settlement date is a NOT NULL column in the schema — refuse
    rather than crash on insert."""
    txn = _txn(value_date=0, booking_date=0)   # both empty
    # No file-level settlement_date either
    out = parse_byo([txn])
    assert out.records == []


def test_parse_byo_falls_back_to_file_settlement_date():
    """When a per-row settlement date can't be derived, the file-level
    settlement_date passed by the caller fills in."""
    txn = _txn(value_date=0, booking_date=0)
    out = parse_byo([txn], settlement_date='2026-04-15')
    assert len(out.records) == 1
    assert out.records[0]['settlement_date'] == '2026-04-15'


def test_parse_byo_handles_invalid_pan_gracefully():
    """A pan_field column with garbage value (not a real PAN) shouldn't
    blow up the whole batch — just leave first6/last4 as None and
    keep the record. The point of mask_pan refusing on bad input is
    so we don't accidentally persist a wrongly-masked value."""
    txn = _txn()
    txn['card_number'] = '0000-0000-0000-0001'   # fails Luhn
    out = parse_byo([txn], pan_field='card_number',
                    settlement_date='2026-04-15')
    assert len(out.records) == 1
    assert out.records[0]['pan_first6'] is None
    assert out.records[0]['pan_last4'] is None


def test_parse_byo_currency_falls_back_to_file_level():
    """When a row has no ccy and the loader wasn't given one, the
    currency_settlement ends up None — not '' — so the schema's NOT
    NULL constraint flags the missing data instead of pretending it's
    blank."""
    txn = _txn(ccy='')
    out = parse_byo([txn], currency='', settlement_date='2026-04-15')
    assert out.records[0]['currency_settlement'] is None


# ---------------------------------------------------------------------------
# Pre-masked PAN column — the common case for card-switch reports
# (e.g. `484680******1168`) where the source already redacts the middle.
# ---------------------------------------------------------------------------

def test_parse_byo_splits_pre_masked_pan():
    """A column carrying `484680******1168` style values gets split
    directly into first6/last4, without invoking mask_pan (which
    requires a Luhn-valid full PAN that we don't have)."""
    txn = _txn()
    txn['Pan'] = '484680******1168'
    out = parse_byo([txn], pan_masked_field='Pan',
                    settlement_date='2026-04-15')
    r = out.records[0]
    assert r['pan_first6'] == '484680'
    assert r['pan_last4'] == '1168'


def test_parse_byo_pre_masked_pan_with_x_separator():
    """Some exports use 'x' instead of '*' as the masking char. Both
    must split cleanly so we can wire one profile per format family
    rather than per-issuer."""
    txn = _txn()
    txn['Pan'] = '411111xxxxxx1111'
    out = parse_byo([txn], pan_masked_field='Pan',
                    settlement_date='2026-04-15')
    r = out.records[0]
    assert r['pan_first6'] == '411111'
    assert r['pan_last4'] == '1111'


def test_parse_byo_pre_masked_garbage_keeps_record_with_null_pan():
    """A row whose Pan column doesn't match the pre-masked shape
    (totals rows, header echoes, blank) keeps the record but leaves
    first6/last4 as None — refusing the row would drop a lot of valid
    edge cases for no PCI gain."""
    txn = _txn()
    txn['Pan'] = 'TOTAL'
    out = parse_byo([txn], pan_masked_field='Pan',
                    settlement_date='2026-04-15')
    assert len(out.records) == 1
    assert out.records[0]['pan_first6'] is None
    assert out.records[0]['pan_last4'] is None


def test_parse_byo_pan_field_takes_precedence_over_pan_masked_field():
    """If both are configured (operator confusion), the full-PAN path
    wins — it's stricter (Luhn-validated) so it catches real PANs that
    accidentally bypassed upstream masking."""
    txn = _txn()
    txn['Pan'] = LUHN_PAN_16            # actually a full PAN
    out = parse_byo([txn],
                    pan_field='Pan', pan_masked_field='Pan',
                    settlement_date='2026-04-15')
    r = out.records[0]
    assert r['pan_first6'] == '411111'
    assert r['pan_last4'] == '1111'
