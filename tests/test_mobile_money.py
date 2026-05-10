"""Tests for the mobile-money module.

What we're pinning:
    - Schema migration adds account_type/provider/msisdn/short_code to
      accounts.
    - Three CSV format profiles (M-Pesa, MTN MoMo, Airtel Money) are
      seeded on init_db, idempotently.
    - The API accepts mobile_wallet account creation with the right
      fields and rejects mismatched combinations (e.g. cash_nostro with
      a provider, mobile_wallet without one).
    - GET /accounts?account_type=mobile_wallet filters as advertised.
    - The seeded MTN MoMo profile actually parses a representative
      MoMo CSV via byo_csv_loader → canonical Flex shape.
"""

import json
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Fixture — fresh app pointing at a temp DB.
# ---------------------------------------------------------------------------

@pytest.fixture
def fresh_app(tmp_path, monkeypatch):
    db_path = tmp_path / "kilter-mm.db"
    monkeypatch.setenv("KILTER_DB_PATH", str(db_path))
    monkeypatch.setenv("KILTER_SECRET_KEY", "")
    monkeypatch.chdir(tmp_path)

    for mod in list(sys.modules):
        if mod in ('app', 'db', 'auth', 'secrets_vault', 'scanner', 'scheduler',
                   'recon_engine', 'ingest', 'open_items', 'byo_csv_loader'):
            sys.modules.pop(mod, None)

    from fastapi.testclient import TestClient
    import app as app_module
    import db as db_module
    db_module.init_db()

    conn = db_module.get_conn()
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    conn.execute(
        "INSERT INTO users (username, display_name, role, active, created_at, "
        "created_by, totp_secret, totp_enrolled_at) VALUES "
        "('test-admin', 'Test Admin', 'admin', 1, ?, 'system', 'JBSWY3DPEHPK3PXP', ?)",
        (now, now),
    )
    from auth import issue_session
    sess = issue_session(conn, "test-admin")
    conn.commit(); conn.close()

    client = TestClient(app_module.app)
    client.headers.update({"X-Session-Token": sess['token']})
    yield client, db_path


# ---------------------------------------------------------------------------
# Schema migration
# ---------------------------------------------------------------------------

def test_schema_has_mm_columns(fresh_app):
    """init_db must create accounts with account_type/provider/msisdn/
    short_code so legacy installs auto-migrate to the new layout."""
    _, db_path = fresh_app
    conn = sqlite3.connect(db_path)
    cols = {r[1] for r in conn.execute("PRAGMA table_info(accounts)")}
    assert {'account_type', 'provider', 'msisdn', 'short_code'}.issubset(cols)
    conn.close()


def test_default_account_type_is_cash_nostro(fresh_app):
    """A pre-existing account row inserted without account_type must
    inherit the 'cash_nostro' default — no NULL leaks for legacy data."""
    _, db_path = fresh_app
    conn = sqlite3.connect(db_path)
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    conn.execute(
        "INSERT INTO accounts (label, swift_account, flex_ac_no, currency, "
        "active, created_at, created_by) VALUES "
        "('Legacy USD', 'SW999', 'FX999', 'USD', 1, ?, 'system')",
        (now,),
    )
    conn.commit()
    row = conn.execute(
        "SELECT account_type FROM accounts WHERE swift_account='SW999'"
    ).fetchone()
    assert row[0] == 'cash_nostro'
    conn.close()


# ---------------------------------------------------------------------------
# Profile seeding
# ---------------------------------------------------------------------------

def test_seed_creates_mobile_money_profiles(fresh_app):
    _, db_path = fresh_app
    conn = sqlite3.connect(db_path)
    names = {r[0] for r in conn.execute(
        "SELECT name FROM csv_format_profiles WHERE active=1"
    )}
    # Mobile money operator profiles
    assert 'M-Pesa Safaricom statement' in names
    assert 'Telcel Cash organisation statement' in names
    assert 'MTN MoMo agent statement' in names
    assert 'MTN MoMo operator B2W' in names
    assert 'MTN MoMo operator W2B' in names
    assert 'Airtel Money agent statement' in names
    # Card-switch settlement profiles (also flow through csv_format_profiles
    # since they ride the same BYO machinery — feed cards_ingest, not the
    # cash recon engine).
    assert 'Card switch acquirer settlement' in names
    assert 'Card switch issuer settlement' in names
    conn.close()


def test_seed_is_idempotent(fresh_app):
    """Re-running init_db must not duplicate the seeded profiles."""
    _, db_path = fresh_app
    import db as db_module
    db_module.init_db()    # second run
    db_module.init_db()    # third run
    conn = sqlite3.connect(db_path)
    n = conn.execute(
        "SELECT COUNT(*) FROM csv_format_profiles "
        "WHERE name='MTN MoMo agent statement'"
    ).fetchone()[0]
    assert n == 1
    conn.close()


# ---------------------------------------------------------------------------
# Account-create API
# ---------------------------------------------------------------------------

def test_create_cash_nostro_default(fresh_app):
    """The original cash-nostro path must still work without supplying
    account_type — the default takes over."""
    client, _ = fresh_app
    r = client.post("/accounts", json={
        "label": "Citi USD", "swift_account": "SW001",
        "flex_ac_no": "FX001", "currency": "USD",
    })
    assert r.status_code == 200, r.text
    assert r.json()['account_type'] == 'cash_nostro'


def test_create_mobile_wallet_happy_path(fresh_app):
    client, _ = fresh_app
    r = client.post("/accounts", json={
        "label": "MTN MoMo Agent #4012",
        "swift_account": "WALLET-4012",
        "flex_ac_no": "5550010012345",
        "currency": "GHS",
        "account_type": "mobile_wallet",
        "provider": "mtn_momo",
        "msisdn": "+233 24 123 4567",
        "short_code": "4012",
    })
    assert r.status_code == 200, r.text
    body = r.json()
    assert body['account_type'] == 'mobile_wallet'
    # MSISDN normalised to digits-only — the API strips the '+' and spaces
    # so '+233 24 123 4567' and '233241234567' don't create duplicates.
    rows = client.get("/accounts").json()
    created = next(a for a in rows if a['label'] == 'MTN MoMo Agent #4012')
    assert created['msisdn'] == '233241234567'
    assert created['provider'] == 'mtn_momo'


def test_create_mobile_wallet_requires_provider(fresh_app):
    client, _ = fresh_app
    r = client.post("/accounts", json={
        "label": "Wallet without provider",
        "swift_account": "W001", "flex_ac_no": "F001", "currency": "GHS",
        "account_type": "mobile_wallet",
        "msisdn": "233241111111",
    })
    assert r.status_code == 400
    assert "provider" in r.json()['detail'].lower()


def test_create_mobile_wallet_requires_msisdn_or_short_code(fresh_app):
    """A wallet with neither MSISDN nor short code is meaningless — the
    operator can't address the wallet, so reconciliation can't pair."""
    client, _ = fresh_app
    r = client.post("/accounts", json={
        "label": "Headless wallet",
        "swift_account": "W002", "flex_ac_no": "F002", "currency": "GHS",
        "account_type": "mobile_wallet", "provider": "mtn_momo",
    })
    assert r.status_code == 400
    assert "msisdn" in r.json()['detail'].lower() or "short" in r.json()['detail'].lower()


def test_cash_nostro_rejects_mobile_money_fields(fresh_app):
    """A cash_nostro account with provider/msisdn/short_code is a
    mistake — keeps the data model honest."""
    client, _ = fresh_app
    r = client.post("/accounts", json={
        "label": "Confused Citi",
        "swift_account": "W003", "flex_ac_no": "F003", "currency": "USD",
        "account_type": "cash_nostro",
        "provider": "mpesa",
    })
    assert r.status_code == 400


def test_create_unknown_provider_rejected(fresh_app):
    client, _ = fresh_app
    r = client.post("/accounts", json={
        "label": "Made-up provider",
        "swift_account": "W004", "flex_ac_no": "F004", "currency": "GHS",
        "account_type": "mobile_wallet",
        "provider": "btccoin",
        "msisdn": "233000000000",
    })
    assert r.status_code == 400


# ---------------------------------------------------------------------------
# /accounts filter — account_type query param
# ---------------------------------------------------------------------------

def test_accounts_filter_by_type(fresh_app):
    """Create one of each kind, confirm the filter scopes correctly."""
    client, _ = fresh_app
    client.post("/accounts", json={
        "label": "Citi USD", "swift_account": "SW100",
        "flex_ac_no": "FX100", "currency": "USD",
    })
    client.post("/accounts", json={
        "label": "MoMo Agent",
        "swift_account": "WALLET-100", "flex_ac_no": "FX-WALLET-100",
        "currency": "GHS",
        "account_type": "mobile_wallet", "provider": "mtn_momo",
        "msisdn": "233240000000",
    })
    all_acc = client.get("/accounts").json()
    cash    = client.get("/accounts?account_type=cash_nostro").json()
    wallet  = client.get("/accounts?account_type=mobile_wallet").json()
    assert len(all_acc) == 2
    assert len(cash) == 1 and cash[0]['account_type'] == 'cash_nostro'
    assert len(wallet) == 1 and wallet[0]['account_type'] == 'mobile_wallet'


def test_accounts_filter_by_provider(fresh_app):
    client, _ = fresh_app
    client.post("/accounts", json={
        "label": "MoMo agent",
        "swift_account": "W200", "flex_ac_no": "F200", "currency": "GHS",
        "account_type": "mobile_wallet", "provider": "mtn_momo",
        "msisdn": "233240000001",
    })
    client.post("/accounts", json={
        "label": "M-Pesa till",
        "swift_account": "W201", "flex_ac_no": "F201", "currency": "KES",
        "account_type": "mobile_wallet", "provider": "mpesa",
        "short_code": "174379",
    })
    momo = client.get("/accounts?provider=mtn_momo").json()
    mpesa = client.get("/accounts?provider=mpesa").json()
    assert len(momo) == 1 and momo[0]['provider'] == 'mtn_momo'
    assert len(mpesa) == 1 and mpesa[0]['provider'] == 'mpesa'


# ---------------------------------------------------------------------------
# Seeded profile parses representative CSV
# ---------------------------------------------------------------------------

MOMO_SAMPLE = b"""Reference,Description,Type,Amount,Transaction Date,MSISDN
TXN001,Cashout to subscriber,DR,150.00,15/04/2026,233240000001
TXN002,Cashin from agent,CR,500.00,15/04/2026,233240000002
TXN003,Mobile bill payment,DR,72.50,16/04/2026,233240000003
"""


def test_seeded_momo_profile_parses_real_shape(fresh_app):
    """The MTN MoMo seed must produce three canonical Flex txns from a
    minimally-realistic CSV without any admin tweaking."""
    _, db_path = fresh_app
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    row = dict(conn.execute(
        "SELECT * FROM csv_format_profiles WHERE name='MTN MoMo agent statement'"
    ).fetchone())
    conn.close()

    from byo_csv_loader import CsvProfile, load_csv
    profile = CsvProfile.from_db(row)
    # Profile has no currency baked in by default; bind one for the test.
    profile = CsvProfile(
        name=profile.name, delimiter=profile.delimiter,
        header_row=profile.header_row, skip_rows=profile.skip_rows,
        date_format=profile.date_format, currency='GHS',
        column_map=profile.column_map,
        sign_convention=profile.sign_convention,
        sign_column=profile.sign_column,
    )
    result = load_csv(MOMO_SAMPLE, profile)
    assert len(result.txns) == 3
    assert result.errors == []

    # Verify the canonical shape: sign mirrors, amounts are positive,
    # dates parsed as YYYYMMDD ints.
    types = [t['type'] for t in result.txns]
    assert types == ['DR', 'CR', 'DR']
    assert all(t['amount'] > 0 for t in result.txns)
    assert result.txns[0]['value_date'] == 20260415
    assert result.txns[2]['value_date'] == 20260416
    assert result.txns[0]['ccy'] == 'GHS'
