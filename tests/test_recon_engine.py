"""Reconciliation engine tier-classification tests.

Engine contract (from recon_engine.py docstring):
    Tier 1: ref + amount + date, mirrored sign
    Tier 2: ref + mirrored sign, amounts differ (FX/fees)
    Tier 3: no ref, same amount + date, mirrored sign
    Tier 4: no ref, amount + mirrored sign, dates within one day

Mirror-sign rule: SWIFT 'C' (credit at correspondent) <-> Flex 'DR' (debit
to our nostro asset account). These tests pin both classification and the
sign-mirror rule because regressing either silently drops match counts.
"""

import pytest

from recon_engine import (
    propose_candidates, resolve, normalize_ref, MIRROR_SIGN,
)


def _swift(row, ref, amount, sign='C', value_date=20260417, book_date=None):
    return {
        '_source': 'swift',
        '_row_number': row,
        '_used': False,
        'value_date': value_date,
        'amount': amount,
        'sign': sign,
        'origin': 'Their',
        'type': 'Other',
        'status': 'Unmatched',
        'book_date': book_date or value_date,
        'our_ref': ref or '',
        'their_ref': '',
        'booking_text_1': '',
        'booking_text_2': '',
    }


def _flex(row, ref, amount, type_='DR', value_date=20260417, book_date=None,
          narration=''):
    return {
        '_source': 'flex',
        '_row_number': row,
        '_used': False,
        'value_date': value_date,
        'amount': amount,
        'type': type_,
        'trn_ref': ref or '',
        'narration': narration or (ref or ''),
        'ac_no': '10001001',
        'ac_branch': 'MTB',
        'ccy': 'USD',
        'module': 'FT',
        'external_ref': ref or '',
        'user_id': 'TEST',
        'booking_date': book_date or value_date,
    }


def test_mirror_sign_table():
    """Regressing this drops every match. Critical invariant."""
    assert MIRROR_SIGN == {'C': 'DR', 'D': 'CR'}


def test_tier1_strict_match():
    swift = [_swift(1, 'MTB12345678', 5000.00, sign='C')]
    flex  = [_flex(1, 'MTB12345678', 5000.00, type_='DR')]
    cands = propose_candidates(swift, flex)
    assert len(cands) == 1
    assert cands[0].tier == 1


def test_tier2_fee_variant():
    """Same ref, amounts differ by a wire-fee — engine must still propose
    but classify as tier 2."""
    swift = [_swift(1, 'MTB12345678', 5000.00, sign='C')]
    flex  = [_flex(1, 'MTB12345678', 4975.00, type_='DR')]   # $25 fee
    cands = propose_candidates(swift, flex)
    assert len(cands) == 1
    assert cands[0].tier == 2


def test_tier3_no_ref_same_amount_date():
    swift = [_swift(1, '', 5000.00, sign='C')]
    flex  = [_flex(1, '', 5000.00, type_='DR')]
    cands = propose_candidates(swift, flex)
    assert len(cands) == 1
    assert cands[0].tier == 3


def test_tier4_one_day_offset_no_ref():
    """No ref, same amount, value-dates differ by one day."""
    swift = [_swift(1, '', 5000.00, sign='C', value_date=20260417)]
    flex  = [_flex(1, '', 5000.00, type_='DR', value_date=20260418)]
    cands = propose_candidates(swift, flex)
    assert len(cands) == 1
    assert cands[0].tier == 4


def test_no_match_when_signs_same():
    """SWIFT credit + Flex credit must NOT match — that's same-side, not
    mirrored. This was a real bug in the mock-data generator."""
    swift = [_swift(1, 'MTB12345678', 5000.00, sign='C')]
    flex  = [_flex(1, 'MTB12345678', 5000.00, type_='CR')]   # also credit
    cands = propose_candidates(swift, flex)
    assert cands == []


def test_no_match_when_dates_too_far_apart():
    swift = [_swift(1, '', 5000.00, sign='C', value_date=20260417)]
    flex  = [_flex(1, '', 5000.00, type_='DR', value_date=20260420)]   # 3 days
    cands = propose_candidates(swift, flex)
    assert cands == []


def test_short_ref_does_not_count_as_ref_hit():
    """Refs shorter than MIN_REF_LENGTH are ignored — too risky to anchor a
    match on a 3-char string that could collide with anything."""
    swift = [_swift(1, 'X', 5000.00, sign='C')]
    flex  = [_flex(1, 'X', 5000.00, type_='DR', narration='X')]
    cands = propose_candidates(swift, flex)
    # Same amount + date is still a tier 3 hit — but tier 1 (ref+amt+date)
    # must not fire because the ref is too short to be reliable.
    assert all(c.tier >= 3 for c in cands)


def test_resolve_picks_best_tier():
    """When two flex rows both pair with one swift row, resolve() picks the
    stronger tier and demotes the other to a competing candidate."""
    swift = [_swift(1, 'MTB12345678', 5000.00, sign='C')]
    flex  = [
        _flex(1, '',           5000.00, type_='DR'),  # tier 3 (no ref)
        _flex(2, 'MTB12345678', 5000.00, type_='DR'),  # tier 1 (with ref)
    ]
    cands = propose_candidates(swift, flex)
    res = resolve(cands, swift, flex)
    assert len(res.assignments) == 1
    a = res.assignments[0]
    assert a.tier == 1
    assert a.flex_row == 2
    assert len(a.competing) == 1
    assert a.competing[0].flex_row == 1


def test_normalize_ref_strips_decoration():
    """Refs travel through formats that add slashes, dashes, etc. The
    normalizer must collapse cosmetic differences before substring search."""
    assert normalize_ref('MTB-1234/5678') == 'MTB12345678'
    assert normalize_ref('  mtb 1234 5678  ') == 'MTB12345678'
    assert normalize_ref('00000123') == '123'
    assert normalize_ref('') == ''
    assert normalize_ref(None) == ''
