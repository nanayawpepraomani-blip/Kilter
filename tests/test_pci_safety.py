"""Tests for the PCI-safety helpers.

What we're pinning:
    - mask_pan returns (first6, last4) for valid PANs and refuses
      anything that fails Luhn or has the wrong length.
    - is_full_pan / contains_pan distinguish real PANs from
      coincidentally-numeric strings.
    - redact_pan replaces full PANs in free text with first6***last4
      and is idempotent on already-redacted strings.
    - refuse_if_pan rejects records with embedded PANs in the named
      fields rather than silently persisting them.
    - refuse_if_sad_present blocks Sensitive Authentication Data
      field names (CVV / track / PIN) per PCI-DSS §3.2.

These are unit tests against pure helpers — no DB, no FastAPI.
"""

import pytest

from pci_safety import (
    mask_pan, is_full_pan, contains_pan, redact_pan,
    refuse_if_pan, refuse_if_sad_present, RefusedPanError,
)


# Canonical Visa test PAN that PASSES Luhn — drawn from public PCI
# educational material. Never use a real card number in tests.
LUHN_PAN_16  = '4111111111111111'
LUHN_PAN_15  = '378282246310005'
NOT_LUHN_PAN = '4111111111111112'


# ---------------------------------------------------------------------------
# is_full_pan / contains_pan
# ---------------------------------------------------------------------------

def test_is_full_pan_accepts_valid_visa_pan():
    assert is_full_pan(LUHN_PAN_16)


def test_is_full_pan_accepts_amex_15_digit():
    assert is_full_pan(LUHN_PAN_15)


def test_is_full_pan_strips_dashes_and_spaces():
    """Banks paste PANs with separators in narration — the detector
    must catch those, not just bare digits."""
    assert is_full_pan('4111-1111-1111-1111')
    assert is_full_pan('4111 1111 1111 1111')


def test_is_full_pan_rejects_failing_luhn():
    """Sequences of digits that look like a PAN but fail Luhn are
    treated as non-PANs. Reduces false positives from reference numbers
    or order ids."""
    assert not is_full_pan(NOT_LUHN_PAN)


def test_is_full_pan_rejects_too_short():
    assert not is_full_pan('411111111111')      # 12 digits, below 13
    assert not is_full_pan('123')


def test_is_full_pan_rejects_too_long():
    """Strings longer than 19 digits aren't PANs, regardless of Luhn."""
    assert not is_full_pan('4' * 20)


def test_is_full_pan_handles_none_and_empty():
    assert not is_full_pan(None)
    assert not is_full_pan('')


def test_contains_pan_finds_embedded_pan():
    text = f"Settlement for txn {LUHN_PAN_16} please refund"
    assert contains_pan(text)


def test_contains_pan_ignores_phone_numbers():
    """A 13-digit phone number that happens to look numeric must NOT
    trigger a PAN match unless it passes Luhn (extremely rare)."""
    assert not contains_pan('Customer phone 233241234567')


def test_contains_pan_ignores_plain_text():
    assert not contains_pan('Refund processed without issue')


# ---------------------------------------------------------------------------
# mask_pan
# ---------------------------------------------------------------------------

def test_mask_pan_returns_first6_and_last4():
    f6, l4 = mask_pan(LUHN_PAN_16)
    assert f6 == '411111'
    assert l4 == '1111'


def test_mask_pan_strips_separators():
    f6, l4 = mask_pan('4111-1111-1111-1111')
    assert f6 == '411111'
    assert l4 == '1111'


def test_mask_pan_refuses_none():
    with pytest.raises(RefusedPanError):
        mask_pan(None)


def test_mask_pan_refuses_too_short():
    with pytest.raises(RefusedPanError):
        mask_pan('411111')


def test_mask_pan_refuses_failing_luhn():
    with pytest.raises(RefusedPanError):
        mask_pan(NOT_LUHN_PAN)


def test_mask_pan_refuses_non_digits():
    with pytest.raises(RefusedPanError):
        mask_pan('not-a-pan-at-all')


# ---------------------------------------------------------------------------
# redact_pan
# ---------------------------------------------------------------------------

def test_redact_pan_replaces_embedded_pan():
    text = f"Refund issued on card {LUHN_PAN_16} thanks"
    out = redact_pan(text)
    assert LUHN_PAN_16 not in out
    assert '411111***1111' in out


def test_redact_pan_is_idempotent():
    """Running redact_pan on already-redacted text must not corrupt it."""
    once = redact_pan(f"see {LUHN_PAN_16}")
    twice = redact_pan(once)
    assert once == twice


def test_redact_pan_preserves_non_matches():
    """Phone numbers and order ids stay verbatim."""
    text = "Customer phone 233241234567 — order #123456"
    assert redact_pan(text) == text


def test_redact_pan_handles_empty_input():
    assert redact_pan(None) == ''
    assert redact_pan('') == ''


# ---------------------------------------------------------------------------
# refuse_if_pan / refuse_if_sad_present
# ---------------------------------------------------------------------------

def test_refuse_if_pan_blocks_record_with_embedded_pan():
    record = {
        'merchant_name': 'Acme Co',
        'narration':     f'Auth {LUHN_PAN_16} on file',
    }
    with pytest.raises(RefusedPanError, match="narration"):
        refuse_if_pan(record)


def test_refuse_if_pan_passes_when_clean():
    record = {
        'merchant_name': 'Acme Co',
        'narration':     'Auth 12345 on file',
    }
    refuse_if_pan(record)   # no exception


def test_refuse_if_pan_only_scans_named_fields_when_given():
    """Operators pass an explicit fields list to limit the scan to
    the columns at risk; other columns are not inspected."""
    record = {
        'safe_field': f'PAN {LUHN_PAN_16}',
        'narration':  'clean text',
    }
    # narration is clean → no exception even though safe_field would trip it
    refuse_if_pan(record, fields=('narration',))


def test_refuse_if_sad_present_blocks_cvv():
    """A record with a CVV column must be rejected — CVV is Sensitive
    Authentication Data and may NEVER persist post-authorization."""
    record = {'pan_last4': '1234', 'cvv': '999'}
    with pytest.raises(RefusedPanError, match="cvv"):
        refuse_if_sad_present(record)


def test_refuse_if_sad_present_blocks_track_data():
    record = {'pan_last4': '1234', 'track2': '%B4111111111111111^...^...'}
    with pytest.raises(RefusedPanError):
        refuse_if_sad_present(record)


def test_refuse_if_sad_present_blocks_pin():
    record = {'pan_last4': '1234', 'pin_block': 'abc123'}
    with pytest.raises(RefusedPanError):
        refuse_if_sad_present(record)


def test_refuse_if_sad_present_passes_when_clean():
    record = {
        'pan_first6': '411111', 'pan_last4': '1111',
        'scheme_ref': 'ABC123', 'amount_settlement': 100.0,
    }
    refuse_if_sad_present(record)   # no exception


def test_refuse_if_sad_present_is_case_insensitive():
    """A column named 'CVV' or 'Cvv' is just as forbidden as 'cvv'."""
    with pytest.raises(RefusedPanError):
        refuse_if_sad_present({'CVV': '999'})
    with pytest.raises(RefusedPanError):
        refuse_if_sad_present({'PIN_Block': 'xyz'})
