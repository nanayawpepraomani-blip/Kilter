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
    propose_many_to_many, Tolerance,
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


# ---------------------------------------------------------------------------
# Tier 6 — true many-to-many matching (subsets on both sides).
#
# Pinned because false positives here are the single biggest reputational
# risk in the engine. Loosening any guard (date band, sign mirror, pool cap)
# silently lets coincidental sums into the queue and erodes operator trust.
# ---------------------------------------------------------------------------

def test_m2n_basic_2x2_match():
    """Two SWIFT credits that aggregate to two Flex debits on the same day,
    different refs (so tier-5 splits don't fire), summing to identical
    totals — the canonical M:M case."""
    swift = [
        _swift(1, 'AAA', 100.00, value_date=20260420),
        _swift(2, 'BBB', 200.00, value_date=20260420),
    ]
    flex = [
        _flex(10, 'XXX', 120.00, value_date=20260420, narration='unrelated'),
        _flex(11, 'YYY', 180.00, value_date=20260420, narration='unrelated'),
    ]
    out = propose_many_to_many(swift, flex, {1, 2}, {10, 11})
    assert len(out) == 1
    assert out[0].tier == 6
    assert set(out[0].swift_rows) == {1, 2}
    assert set(out[0].flex_rows) == {10, 11}
    # 300.00 swift vs 300.00 flex
    assert abs(out[0].amount_diff) < 0.01


def test_m2n_does_not_match_when_signs_disagree():
    """A subset must mirror its counterpart's sign on EVERY row. Mixing
    a credit and a debit on the same side is NOT a real aggregation."""
    swift = [
        _swift(1, 'AAA', 100.00, sign='C', value_date=20260420),
        _swift(2, 'BBB', 200.00, sign='D', value_date=20260420),  # debit, not credit
    ]
    flex = [
        _flex(10, 'XXX', 120.00, type_='DR', value_date=20260420),
        _flex(11, 'YYY', 180.00, type_='DR', value_date=20260420),
    ]
    out = propose_many_to_many(swift, flex, {1, 2}, {10, 11})
    # 1 swift_C alone can't sum to flex 300; 2 alone can't either.
    # No m=2 subset has uniform sign C, so no candidate.
    assert out == []


def test_m2n_respects_date_band():
    """Two transactions outside the date_tol_days band should not group
    even if their amounts sum correctly. Banks aggregate same-day or
    next-day; not same-week."""
    swift = [
        _swift(1, 'AAA', 100.00, value_date=20260420),
        _swift(2, 'BBB', 200.00, value_date=20260427),  # one week later
    ]
    flex = [
        _flex(10, 'XXX', 150.00, value_date=20260420),
        _flex(11, 'YYY', 150.00, value_date=20260427),
    ]
    out = propose_many_to_many(swift, flex, {1, 2}, {10, 11},
                                tol=Tolerance(date_tol_days=1))
    assert out == []


def test_m2n_skips_sums_outside_tolerance():
    """Aggregation that lands $1 off should NOT match — that's a real
    break, not an aggregation. Tolerance applies to the SUMMED amount."""
    swift = [
        _swift(1, 'AAA', 100.00, value_date=20260420),
        _swift(2, 'BBB', 200.00, value_date=20260420),
    ]
    flex = [
        _flex(10, 'XXX', 120.00, value_date=20260420),
        _flex(11, 'YYY', 181.00, value_date=20260420),  # 301 vs 300
    ]
    out = propose_many_to_many(swift, flex, {1, 2}, {10, 11},
                                tol=Tolerance(amount_tol_abs=0.01))
    assert out == []


def test_m2n_tolerance_pct_allows_close_aggregate():
    """With a 1% tolerance, a 0.5% deviation on the aggregate sum should
    match. The tolerance API is the same as 1:1 — same gate, just on
    the summed amount."""
    swift = [
        _swift(1, 'AAA', 100.00, value_date=20260420),
        _swift(2, 'BBB', 200.00, value_date=20260420),
    ]
    flex = [
        _flex(10, 'XXX', 120.00, value_date=20260420),
        _flex(11, 'YYY', 181.50, value_date=20260420),  # 301.50 vs 300, 0.5%
    ]
    out = propose_many_to_many(swift, flex, {1, 2}, {10, 11},
                                tol=Tolerance(amount_tol_pct=1.0))
    assert len(out) == 1
    assert out[0].tier == 6


def test_m2n_does_not_double_claim_rows():
    """Once a row participates in a candidate, it must not appear in a
    later candidate from the same pass — otherwise the UI would offer
    overlapping decisions and a confirmed first match would silently
    invalidate the second."""
    # Three SWIFT credits, three flex debits — multiple potential subsets.
    # The implementation must pick one and refuse to re-use rows.
    swift = [
        _swift(1, 'A', 100.00, value_date=20260420),
        _swift(2, 'B', 200.00, value_date=20260420),
        _swift(3, 'C', 300.00, value_date=20260420),
    ]
    flex = [
        _flex(10, 'X', 150.00, value_date=20260420),
        _flex(11, 'Y', 150.00, value_date=20260420),
        _flex(12, 'Z', 300.00, value_date=20260420),
    ]
    out = propose_many_to_many(swift, flex, {1, 2, 3}, {10, 11, 12})
    # Whatever subsets land, no row may appear twice across emitted candidates.
    seen_swift = []
    seen_flex = []
    for c in out:
        seen_swift.extend(c.swift_rows)
        seen_flex.extend(c.flex_rows)
    assert len(seen_swift) == len(set(seen_swift))
    assert len(seen_flex) == len(set(seen_flex))


def test_m2n_pool_cap_protects_against_blowup():
    """If unmatched volume in a single date band exceeds the pool cap,
    the function must refuse the bucket rather than try to enumerate
    millions of subset pairs and lock the worker."""
    # Build 25 SWIFT credits and 25 Flex debits all on the same date.
    # POOL_CAP_M2N is 20 — with 25 we should bail out cleanly.
    swift = [_swift(i, f'R{i}', 10.00 * (i + 1), value_date=20260420)
             for i in range(25)]
    flex = [_flex(100 + i, f'X{i}', 10.00 * (i + 1), value_date=20260420)
            for i in range(25)]
    out = propose_many_to_many(swift, flex,
                                {s['_row_number'] for s in swift},
                                {f['_row_number'] for f in flex})
    # Either zero (refused the bucket) or a small handful — never an
    # explosion. We pin the no-explosion property: empty.
    assert out == []


def test_m2n_does_not_confuse_currencies_via_sign():
    """A SWIFT credit and a Flex CREDIT are both incoming-from-our-view; the
    engine never matches same-direction. M:N must respect this."""
    swift = [
        _swift(1, 'A', 100.00, sign='C', value_date=20260420),
        _swift(2, 'B', 200.00, sign='C', value_date=20260420),
    ]
    flex = [
        _flex(10, 'X', 150.00, type_='CR', value_date=20260420),  # NOT mirror
        _flex(11, 'Y', 150.00, type_='CR', value_date=20260420),
    ]
    out = propose_many_to_many(swift, flex, {1, 2}, {10, 11})
    assert out == []


def test_m2n_asymmetric_2x3_shape():
    """2 SWIFT credits aggregate to 3 Flex debits — the asymmetric M:N
    case. Sums must match within tolerance; subset on each side stays
    within the date band."""
    swift = [
        _swift(1, 'A', 100.00, value_date=20260420),
        _swift(2, 'B', 200.00, value_date=20260420),  # total 300
    ]
    flex = [
        _flex(10, 'X', 80.00,  value_date=20260420),
        _flex(11, 'Y', 100.00, value_date=20260420),
        _flex(12, 'Z', 120.00, value_date=20260420),  # total 300
    ]
    out = propose_many_to_many(swift, flex, {1, 2}, {10, 11, 12})
    assert len(out) == 1
    assert len(out[0].swift_rows) == 2
    assert len(out[0].flex_rows) == 3
    assert out[0].tier == 6


# ---------------------------------------------------------------------------
# BTW pattern — symmetric ref matching
# ---------------------------------------------------------------------------
# In the 2026 BTW data, the DR side and CR side don't share `our_ref`/`trn_ref`
# directly: the DR's own_ref is its settlement-side identifier (e.g. H9859…),
# but the wallet ref the CR carries (H26ZEXA…) appears in the DR's narration.
# The original (one-way) ref check missed this entirely. These tests pin the
# symmetric behaviour so the BTW pattern matches at Tier 1.

def test_btw_symmetric_ref_dr_narration_carries_cr_ref():
    """DR's narration embeds the CR's trn_ref → engine should treat
    that as a ref hit and emit a Tier 1 match."""
    swift = [{
        '_source': 'swift', '_row_number': 1, '_used': False,
        'value_date': 20260430, 'amount': 470.0, 'sign': 'D',
        'origin': 'Our', 'type': 'Other', 'status': 'Unmatched',
        'book_date': 20260430,
        'our_ref': 'H9859fe261200002',     # settlement-side ref
        'their_ref': '',
        'booking_text_1': 'Settlement for succ Txn dd 29APR26 Bank2W',
        'booking_text_2': ('Settlement for succ Txn dd 29APR26 Bank2Wallet '
                           '233554447280 H26ZEXA261181CG3 |USERID:EKAKRONG|'),
    }]
    flex = [_flex(2, 'H26ZEXA261181CG3', 470.0, type_='CR',
                   value_date=20260429,
                   narration='Bank2Wallet 233554447280 |USERID:W3S_GH|')]
    cands = propose_candidates(swift, flex)
    res = resolve(cands, swift, flex)
    assert len(res.assignments) == 1
    a = res.assignments[0]
    # Tier 1 — ref hit (CR's trn_ref appears in DR's narration) +
    # exact amount + close-date (within ±1 day band).
    assert a.tier == 1


def test_btw_symmetric_ref_no_phantom_match_without_overlap():
    """Guard against the symmetric ref check inventing matches that
    shouldn't happen. Two unrelated rows whose narrations share only
    English boilerplate (no digit-bearing token overlap) must NOT
    pair as a ref hit."""
    swift = [{
        '_source': 'swift', '_row_number': 1, '_used': False,
        'value_date': 20260430, 'amount': 999.0, 'sign': 'D',
        'origin': 'Our', 'type': 'Other', 'status': 'Unmatched',
        'book_date': 20260430,
        'our_ref': 'X' * 8,
        'their_ref': '',
        'booking_text_1': 'Settlement for succ Txn dd somewhere',
        'booking_text_2': 'Bank2Wallet for some other purpose',
    }]
    flex = [_flex(2, 'COMPLETELYDIFFERENT', 100.0, type_='CR',
                   value_date=20260429,
                   narration='unrelated narration nothing in common')]
    cands = propose_candidates(swift, flex)
    res = resolve(cands, swift, flex)
    # Amounts and refs disagree — no candidate should be emitted at
    # any tier.
    assert len(res.assignments) == 0


def test_btw_pattern_at_scale_indexed_engine_finishes_quickly():
    """Sanity: 2,000 swift × 2,000 flex with BTW-shape narration must
    finish in well under 5 seconds. Guards against accidental
    re-introduction of the O(N×M) inner loop."""
    import time
    swift = []
    flex = []
    for i in range(2000):
        wallet_ref = f'H26ZEXA26119{i:04X}'
        swift.append({
            '_source': 'swift', '_row_number': i + 1, '_used': False,
            'value_date': 20260430, 'amount': 100.0 + i, 'sign': 'D',
            'origin': 'Our', 'type': 'Other', 'status': 'Unmatched',
            'book_date': 20260430,
            'our_ref': f'H9859fe2612{i:05X}',
            'their_ref': '',
            'booking_text_1': 'Settlement for succ Txn',
            'booking_text_2': f'Settlement for succ Txn dd 29APR26 {wallet_ref}',
        })
        flex.append(_flex(10000 + i, wallet_ref, 100.0 + i, type_='CR',
                          value_date=20260429,
                          narration=f'Bank2Wallet 23355{i:07d} |USERID:W3S_GH|'))
    t0 = time.time()
    cands = propose_candidates(swift, flex)
    res = resolve(cands, swift, flex)
    elapsed = time.time() - t0
    assert elapsed < 5.0, f'Engine took {elapsed:.2f}s — indexing regressed'
    # Every pair has matching wallet ref + amount → 2,000 Tier 1 matches.
    assert len(res.assignments) == 2000
    assert all(a.tier == 1 for a in res.assignments)
