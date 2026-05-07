"""Phase 1 tests — read_balance_sheet helper.

Pins:
  * Reads the 4-column balances sheet correctly
  * Returns None when the sheet is missing entirely
  * Returns None when the required columns aren't present
  * Returns None when the data row is incomplete
  * Tolerates extra columns
  * Normalises as_of_date to YYYYMMDD int regardless of input form
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from openpyxl import Workbook

from reconcile import read_balance_sheet


def _write_xlsx(path: Path, *, with_balances=True, balances_rows=None,
                acc_entries_only=False) -> None:
    """Build a minimal Flexcube-shape xlsx with optional balances sheet."""
    wb = Workbook()
    ae = wb.active
    ae.title = 'acc_entries'
    ae.append(['TRN_REF_NO', 'BOOKING_DATE', 'TYPE', 'TXN_NARRATIONS',
               'VALUE_DATE', 'LCY_AMOUNT'])
    if not acc_entries_only:
        ae.append(['REF1', datetime(2026, 4, 30), 'CR', 'note',
                   datetime(2026, 4, 30), 100.0])
    if with_balances:
        bs = wb.create_sheet('balances')
        for row in (balances_rows or []):
            bs.append(row)
    wb.save(path)


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------

def test_reads_well_formed_balances_sheet(tmp_path):
    """The canonical layout: header row + one data row with the four
    required columns. as_of_date as ISO string (most common in the
    extract script)."""
    p = tmp_path / 'delta.xlsx'
    _write_xlsx(p, balances_rows=[
        ('as_of_date', 'opening_balance', 'closing_balance', 'currency'),
        ('2026-04-30', -56_704_939.75, -153_320_409.0, 'GHS'),
    ])
    bal = read_balance_sheet(p)
    assert bal is not None
    assert bal['as_of_date'] == 20260430
    assert bal['opening_balance'] == -56_704_939.75
    assert bal['closing_balance'] == -153_320_409.0
    assert bal['currency'] == 'GHS'


def test_normalises_as_of_date_from_python_date(tmp_path):
    """Python date object — extract scripts using oracle_db pull dates as
    datetime/date directly, not strings."""
    p = tmp_path / 'delta.xlsx'
    _write_xlsx(p, balances_rows=[
        ('as_of_date', 'opening_balance', 'closing_balance', 'currency'),
        (date(2026, 4, 30), 0.0, 100.0, 'GHS'),
    ])
    bal = read_balance_sheet(p)
    assert bal is not None
    assert bal['as_of_date'] == 20260430


def test_normalises_as_of_date_from_yyyymmdd_int_string(tmp_path):
    """Some exporters pre-normalise to the YYYYMMDD int shape — accept it."""
    p = tmp_path / 'delta.xlsx'
    _write_xlsx(p, balances_rows=[
        ('as_of_date', 'opening_balance', 'closing_balance', 'currency'),
        ('20260430', 0.0, 100.0, 'GHS'),
    ])
    bal = read_balance_sheet(p)
    assert bal['as_of_date'] == 20260430


def test_currency_uppercased(tmp_path):
    """Defensive: an upstream that emits 'ghs' shouldn't break the
    currency-equality checks downstream."""
    p = tmp_path / 'delta.xlsx'
    _write_xlsx(p, balances_rows=[
        ('as_of_date', 'opening_balance', 'closing_balance', 'currency'),
        ('2026-04-30', 0.0, 100.0, 'ghs'),
    ])
    bal = read_balance_sheet(p)
    assert bal['currency'] == 'GHS'


def test_tolerates_extra_columns(tmp_path):
    """A sheet with the four required columns plus extras (e.g. a
    'notes' column an exporter added) should still parse — only the
    canonical fields are surfaced."""
    p = tmp_path / 'delta.xlsx'
    _write_xlsx(p, balances_rows=[
        ('as_of_date', 'opening_balance', 'closing_balance', 'currency', 'notes'),
        ('2026-04-30', 0.0, 100.0, 'GHS', 'first delta'),
    ])
    bal = read_balance_sheet(p)
    assert bal is not None
    assert bal['opening_balance'] == 0.0
    assert 'notes' not in bal


# ---------------------------------------------------------------------------
# None cases
# ---------------------------------------------------------------------------

def test_returns_none_when_sheet_missing(tmp_path):
    """File has only acc_entries — no balances sheet at all. Caller
    should treat this as 'no embedded balance' and skip the continuity
    check."""
    p = tmp_path / 'delta.xlsx'
    _write_xlsx(p, with_balances=False)
    assert read_balance_sheet(p) is None


def test_returns_none_when_sheet_only_has_header(tmp_path):
    """A balances sheet with header but no data row is malformed —
    return None rather than crash on the missing data row."""
    p = tmp_path / 'delta.xlsx'
    _write_xlsx(p, balances_rows=[
        ('as_of_date', 'opening_balance', 'closing_balance', 'currency'),
    ])
    assert read_balance_sheet(p) is None


def test_returns_none_when_required_column_missing(tmp_path):
    """A sheet that omits closing_balance can't drive the continuity
    check — refuse silently rather than partial-parse."""
    p = tmp_path / 'delta.xlsx'
    _write_xlsx(p, balances_rows=[
        ('as_of_date', 'opening_balance', 'currency'),
        ('2026-04-30', 0.0, 'GHS'),
    ])
    assert read_balance_sheet(p) is None


def test_returns_none_when_amount_is_null(tmp_path):
    """A header that's right but data row whose opening or closing is
    explicitly None — that's not a useful balance, refuse it."""
    p = tmp_path / 'delta.xlsx'
    _write_xlsx(p, balances_rows=[
        ('as_of_date', 'opening_balance', 'closing_balance', 'currency'),
        ('2026-04-30', None, 100.0, 'GHS'),
    ])
    assert read_balance_sheet(p) is None
