"""MT940 / MT950 parser round-trip tests.

The parser is the most regression-prone surface in Kilter — every quirk in
real-world correspondent statements (Alliance Access envelopes, comma
decimal separators, missing optional balance lines, multi-line :86: tags)
costs a pilot bank. These tests pin the behaviour we promise.
"""

import pytest

from swift_core import detect_message_type, parse_swift_statement


# ---------------------------------------------------------------------------
# Minimal but realistic MT940 — covers Block 1/2/3/4/5, opening + closing
# balance, two transactions with :86: narration, comma-decimal amounts.
# ---------------------------------------------------------------------------
MT940_SAMPLE = """\
{1:F01CITIUS33AXXX0000000000}
{2:O9401800260417CITIUS33AXXX10000000260417180000N}
{3:{108:MTB-36014578}}
{4:
:20:36014578260417
:25:36014578
:28C:107/1
:60F:C260417USD1000000,00
:61:2604170417C5000,00NTRFMTB12345678//DZB12345
/Alpha Industries Ltd
:86:NTRF?00customer transfer?10MTB12345678?20Alpha Industries Ltd?30CITIUS33?32Alpha Industries Ltd
:61:2604170417D250,00NCHGMTB99999999//DZB99999
/Wire fee
:86:NCHG?00wire fee?10MTB99999999?30CITIUS33
:62F:C260417USD1004750,00
-}
{5:{CHK:DEADBEEF}}
"""


def test_detect_mt940():
    assert detect_message_type(MT940_SAMPLE) == '940'


def test_detect_unknown_message_type():
    """detect_message_type returns whatever 3-digit MT it finds in Block 2;
    returns None only when no Block 2 application header exists. Callers
    decide whether the type is one Kilter handles (currently 940/950)."""
    assert detect_message_type("not a swift message at all") is None
    assert detect_message_type("") is None
    # Recognises MT999 (free format) — not parseable but detectable.
    assert detect_message_type("{1:F01TESTBANK}{2:O999...}") == '999'


def test_parse_account_and_balances():
    parsed = parse_swift_statement(MT940_SAMPLE)
    assert parsed['account'] == '36014578'
    assert parsed['transaction_reference'] == '36014578260417'

    ob = parsed['opening_balance']
    assert ob['mark'] == 'C'
    assert ob['currency'] == 'USD'
    assert ob['amount'] == 1_000_000.00
    assert ob['date'] == 20260417

    cb = parsed['closing_balance']
    assert cb['amount'] == 1_004_750.00


def test_parse_transactions_count_and_signs():
    parsed = parse_swift_statement(MT940_SAMPLE)
    txns = parsed['transactions']
    assert len(txns) == 2
    assert [t['sign'] for t in txns] == ['C', 'D']
    assert [t['amount'] for t in txns] == [5000.00, 250.00]


def test_parse_transaction_refs():
    parsed = parse_swift_statement(MT940_SAMPLE)
    refs = [t.get('account_ref') for t in parsed['transactions']]
    bank_refs = [t.get('bank_ref') for t in parsed['transactions']]
    assert refs == ['MTB12345678', 'MTB99999999']
    assert bank_refs == ['DZB12345', 'DZB99999']


def test_parse_transaction_dates_yyyymmdd():
    parsed = parse_swift_statement(MT940_SAMPLE)
    for t in parsed['transactions']:
        assert t['value_date'] == 20260417
        assert t['entry_date'] == 20260417


def test_parse_narration_attached_to_preceding_txn():
    parsed = parse_swift_statement(MT940_SAMPLE)
    narrations = [t.get('narrative') for t in parsed['transactions']]
    assert all(n for n in narrations), "Each :61: should have a :86: narration"
    assert 'customer transfer' in narrations[0]
    assert 'wire fee' in narrations[1]


# ---------------------------------------------------------------------------
# MT950 — same as 940 but no :86: narrations.
# ---------------------------------------------------------------------------
MT950_SAMPLE = """\
{1:F01DEUTDEFFAXXX0000000000}
{2:O9501800260417DEUTDEFFAXXX10000000260417180000N}
{4:
:20:9500123456260417
:25:9500123456
:28C:107/1
:60F:C260417EUR500000,00
:61:2604170417C12345,67NTRFMTBABC11122//DZBP12001
:61:2604170417D5000,00NTRFMTBABC22233//DZBP12002
:62F:C260417EUR507345,67
-}
"""


def test_detect_mt950():
    assert detect_message_type(MT950_SAMPLE) == '950'


def test_mt950_no_narrations():
    parsed = parse_swift_statement(MT950_SAMPLE)
    assert len(parsed['transactions']) == 2
    # MT950 statements are ref-only; narrative should be unset/None.
    for t in parsed['transactions']:
        assert not t.get('narrative')
