"""Generate mock SWIFT + Flex files for a clean, vendor-neutral demo.

Creates 10 nostro accounts + 10 GL accounts for a fictional bank called
"Meridian Trust Bank" (BIC MRTBGB2L). Generates 10 business days of
SWIFT statements (MT940/MT950/camt.053/camt.054 mix) and matching
Flexcube-shaped xlsx files, then seeds the DB with the 10 banks and
10 cash accounts.

Run AFTER kilter.db has been recreated (stop uvicorn, delete the file,
start uvicorn once to let init_db() build the schema + bootstrap admin,
stop uvicorn again, then run this).

    python _generate_mock_data.py [--copy-to-messages]

Flags:
    --copy-to-messages   After generating, copy files into messages/swift
                         and messages/flexcube so the scanner picks them up.
"""

from __future__ import annotations

import argparse
import random
import shutil
import sqlite3
import sys
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from uuid import uuid4

from openpyxl import Workbook

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.stdout.reconfigure(encoding='utf-8')


# ---------------------------------------------------------------------------
# Reproducible run — same script run twice produces the same data.
# ---------------------------------------------------------------------------
random.seed(20260424)


# ---------------------------------------------------------------------------
# Mock bank identity.
# ---------------------------------------------------------------------------
OWN_BANK_NAME = "Meridian Trust Bank"
OWN_BIC       = "MRTBGB2L"
OWN_BIC_12    = "MRTBGB2LAXXX"      # 12-char logical-terminal version
OWN_BRANCH    = "MTB"


# ---------------------------------------------------------------------------
# 10 nostro / GL account pairs.
# Format mix: 5 MT940, 2 MT950, 2 camt.053, 1 camt.054.
# ---------------------------------------------------------------------------
@dataclass
class Account:
    idx: int
    swift_account: str       # account number at the correspondent
    flex_ac_no: str          # our GL account number
    bic: str                 # correspondent BIC (8-char)
    bic_12: str              # correspondent BIC 12-char LT form for MT headers
    currency: str
    label: str
    shortname: str
    correspondent: str
    format: str              # 'mt940' | 'mt950' | 'camt053' | 'camt054'
    access_area: str
    opening_bal: float


ACCOUNTS: list[Account] = [
    Account(1,  '36014578',        '10001001', 'CITIUS33', 'CITIUS33XXXX', 'USD',
            'Nostro at Citibank NY (USD)',            'CITI NY USD',
            'Citibank N.A. New York',                 'mt940',   'TREASURY',  4_200_000.00),
    Account(2,  '12867451',        '10001002', 'CITIGB2L', 'CITIGB2LXXXX', 'GBP',
            'Nostro at Citibank London (GBP)',        'CITI LDN GBP',
            'Citibank London',                        'mt940',   'TREASURY',  1_850_000.00),
    Account(3,  '9500123456',      '10001003', 'DEUTDEFF', 'DEUTDEFFXXXX', 'EUR',
            'Nostro at Deutsche Bank Frankfurt (EUR)','DEUT FRA EUR',
            'Deutsche Bank AG, Frankfurt',            'mt940',   'NOSTRO',    3_110_000.00),
    Account(4,  '71458932',        '10001004', 'HSBCGB2L', 'HSBCGB2LXXXX', 'GBP',
            'Nostro at HSBC London (GBP)',            'HSBC LDN GBP',
            'HSBC Bank plc, London',                  'mt940',   'TREASURY',    920_500.00),
    Account(5,  '82011675',        '10001005', 'SCBLSGSG', 'SCBLSGSGXXXX', 'SGD',
            'Nostro at StanChart Singapore (SGD)',    'SCB SGP SGD',
            'Standard Chartered Bank, Singapore',     'mt940',   'NOSTRO',    1_250_000.00),
    Account(6,  '30004008500010', '10001006', 'BNPAFRPP', 'BNPAFRPPXXXX', 'EUR',
            'Nostro at BNP Paribas Paris (EUR)',      'BNPP PAR EUR',
            'BNP Paribas, Paris',                     'mt950',   'TREASURY',  2_030_000.00),
    Account(7,  '400408900',       '10001007', 'COBADEFF', 'COBADEFFXXXX', 'EUR',
            'Nostro at Commerzbank Frankfurt (EUR)',  'COBA FRA EUR',
            'Commerzbank AG, Frankfurt',              'mt950',   'NOSTRO',      875_000.00),
    Account(8,  '1180015642',      '10001008', 'NEDSZAJJ', 'NEDSZAJJXXXX', 'ZAR',
            'Nostro at Nedbank Johannesburg (ZAR)',   'NED JNB ZAR',
            'Nedbank Limited, Johannesburg',          'camt053', 'NOSTRO',   18_500_000.00),
    Account(9,  '6550123987',      '10001009', 'CHASUS33', 'CHASUS33XXXX', 'USD',
            'Nostro at JPMorgan Chase NY (USD)',      'JPMC NY USD',
            'JPMorgan Chase Bank NA, New York',       'camt053', 'TREASURY',  5_800_000.00),
    Account(10, '0230123458',      '10001010', 'UBSWCHZH', 'UBSWCHZHXXXX', 'CHF',
            'Nostro at UBS Zurich (CHF)',             'UBS ZRH CHF',
            'UBS AG, Zurich',                         'camt054', 'TREASURY',  1_960_000.00),
]


# ---------------------------------------------------------------------------
# Business days for the mock history (10 days, Mon 2026-04-06 → Fri 2026-04-17).
# ---------------------------------------------------------------------------
BUSINESS_DAYS: list[date] = [
    date(2026, 4, 6),  date(2026, 4, 7),  date(2026, 4, 8),  date(2026, 4, 9),  date(2026, 4, 10),
    date(2026, 4, 13), date(2026, 4, 14), date(2026, 4, 15), date(2026, 4, 16), date(2026, 4, 17),
]


# ---------------------------------------------------------------------------
# Generic anonymous counterparty names. 30 entries for variety.
# ---------------------------------------------------------------------------
COUNTERPARTIES = [
    "Alpha Industries Ltd",          "Harbor Trading Co",            "Summit Logistics Inc",
    "Pioneer Manufacturing Corp",    "Crescent Holdings Ltd",        "Vanguard Commodities SA",
    "Stellar Exports Pte Ltd",       "Meridian Distribution Plc",    "Orion Capital Partners",
    "Atlas Shipping Ltd",            "Blueprint Materials Co",       "Cedar Mountain Foods",
    "Delphi Technologies AG",        "Evergreen Retail Group",       "Fortress Energy Trading",
    "Granite Peak Ventures",         "Horizon Auto Parts GmbH",      "Ironclad Insurance Ltd",
    "Juniper Life Sciences",         "Keystone Construction Ltd",    "Lotus Textile Mills",
    "Mariner Freight Services",      "Nimbus Data Systems",          "Odyssey Travel Group",
    "Phoenix Pharmaceuticals",       "Quantum Research Labs",        "Redwood Timber Holdings",
    "Silverline Media Group",        "Titan Heavy Industries",       "Unity Agritech Ltd",
]


NARRATION_TYPES = [
    ("customer transfer",         "CDT", "NTRF"),
    ("supplier payment",          "DBT", "NTRF"),
    ("trade settlement",          "CDT", "NTRF"),
    ("loan repayment",            "DBT", "NLOR"),
    ("dividend payment",          "CDT", "NDIV"),
    ("fx settlement",             "CDT", "NFEX"),
    ("interest credit",           "CDT", "NINT"),
    ("wire fee",                  "DBT", "NCHG"),
    ("salary payment",            "DBT", "NTRF"),
    ("export proceeds",           "CDT", "NTRF"),
]


# ---------------------------------------------------------------------------
# Transaction generation — returns pairs (swift_txn, flex_txn_or_None) so
# the SWIFT side and Flex side match or mismatch in predictable ways.
# ---------------------------------------------------------------------------
@dataclass
class Txn:
    ref: str
    amount: float
    sign: str            # 'C' or 'D'
    narration_type: str
    narration_code: str  # e.g. NTRF
    counterparty: str
    value_date: date
    book_date: date


def _amount_sample(account: Account) -> float:
    """Generate a plausible amount given currency."""
    if account.currency in ('USD', 'EUR', 'GBP', 'CHF', 'SGD'):
        return round(random.uniform(500, 180_000), 2)
    if account.currency == 'ZAR':
        return round(random.uniform(10_000, 2_500_000), 2)
    return round(random.uniform(1_000, 100_000), 2)


def _mk_ref(prefix: str) -> str:
    return f"{prefix}{random.randint(10000000, 99999999)}"


def _mk_txn(account: Account, day: date) -> Txn:
    narration_type, sign, code = random.choice(NARRATION_TYPES)
    return Txn(
        ref=_mk_ref('MTB'),
        amount=_amount_sample(account),
        sign=sign if sign in ('C', 'D') else 'C',
        narration_type=narration_type,
        narration_code=code,
        counterparty=random.choice(COUNTERPARTIES),
        value_date=day,
        book_date=day,
    )


def generate_day_txns(account: Account, day: date) -> list[tuple[Txn | None, Txn | None]]:
    """Return a list of (swift_side, flex_side) pairs for the day.

    Tier mix (~distribution):
        55% tier 1 — both sides identical
        15% tier 2 — ref matches, Flex amount differs by a fee (SWIFT got the fee deducted)
        10% tier 3 — both sides same amount + date, no ref on Flex side
        10% tier 4 — Flex booked next business day (weekend / cut-off)
        10% break  — one side only (50/50 swift-orphan vs flex-orphan)
    """
    n = random.randint(3, 6)
    pairs: list[tuple[Txn | None, Txn | None]] = []
    for _ in range(n):
        base = _mk_txn(account, day)
        roll = random.random()
        if roll < 0.55:                                   # tier 1 — identical
            pairs.append((base, _copy(base)))
        elif roll < 0.70:                                 # tier 2 — fee deduction
            flex_side = _copy(base)
            fee = round(random.uniform(15, 55), 2)
            flex_side.amount = round(base.amount + fee, 2) if base.sign == 'D' \
                               else round(base.amount - fee, 2)
            pairs.append((base, flex_side))
        elif roll < 0.80:                                 # tier 3 — no-ref match
            flex_side = _copy(base)
            flex_side.ref = ''                            # empty ref
            pairs.append((base, flex_side))
        elif roll < 0.90:                                 # tier 4 — ±1 day booking
            flex_side = _copy(base)
            flex_side.book_date = day + timedelta(days=1)
            pairs.append((base, flex_side))
        else:                                             # break — one-sided
            if random.random() < 0.5:
                pairs.append((base, None))                # SWIFT orphan
            else:
                pairs.append((None, base))                # Flex orphan
    return pairs


def _copy(t: Txn) -> Txn:
    return Txn(ref=t.ref, amount=t.amount, sign=t.sign,
               narration_type=t.narration_type,
               narration_code=t.narration_code,
               counterparty=t.counterparty,
               value_date=t.value_date, book_date=t.book_date)


# ---------------------------------------------------------------------------
# SWIFT amount formatting — comma as decimal separator, no thousands sep.
# ---------------------------------------------------------------------------
def _swift_amount(v: float) -> str:
    s = f"{v:.2f}".replace('.', ',')
    return s


# ---------------------------------------------------------------------------
# MT940 / MT950 generation.
# ---------------------------------------------------------------------------
def gen_mt_text(account: Account, day: date, swift_txns: list[Txn],
                opening: float, closing: float, kind: str = '940') -> str:
    """Produce MT940 or MT950 text for one account-day."""
    assert kind in ('940', '950')
    yymmdd = day.strftime('%y%m%d')
    time_hhmm = '1800'
    stmt_ref = f"{account.swift_account}{yymmdd}"
    seq = f"{day.strftime('%j')}/1"  # statement number / sub-number

    lines: list[str] = []
    lines.append(f"{{1:F01{OWN_BIC_12}0000000000}}")
    lines.append(f"{{2:O{kind}{time_hhmm}{yymmdd}{account.bic_12}{random.randint(10000000, 99999999):08d}{yymmdd}{time_hhmm}N}}")
    lines.append(f"{{3:{{108:MTB-{account.swift_account}}}}}")
    lines.append("{4:")
    lines.append(f":20:{stmt_ref}")
    lines.append(f":25:{account.swift_account}")
    lines.append(f":28C:{seq}")
    lines.append(f":60F:C{yymmdd}{account.currency}{_swift_amount(opening)}")

    for t in swift_txns:
        v_yymmdd = t.value_date.strftime('%y%m%d')
        b_mmdd   = t.book_date.strftime('%m%d')
        # :61:<value_date><entry_date><C/D><amount>NTRF<our_ref>//<their_ref>
        dc = 'C' if t.sign == 'C' else 'D'
        our_ref   = t.ref if t.ref else 'NONREF'
        their_ref = f"DZ{random.randint(10000000, 99999999)}"
        lines.append(f":61:{v_yymmdd}{b_mmdd}{dc}{_swift_amount(t.amount)}{t.narration_code}{our_ref}//{their_ref}")
        # MT950 skips :86:; MT940 includes it
        if kind == '940':
            lines.append(f"/{t.counterparty[:30]}")
            lines.append(f":86:{t.narration_code}?00{t.narration_type}?10{our_ref}"
                         f"?20{t.counterparty[:35]}?30{account.bic}?32{t.counterparty[:35]}")

    lines.append(f":62F:C{yymmdd}{account.currency}{_swift_amount(closing)}")
    lines.append("-}")
    lines.append("{5:{CHK:" + f"{random.randint(0, 0xFFFFFFFF):08X}" + "}}")

    return '\n'.join(lines) + '\n'


# ---------------------------------------------------------------------------
# camt.053 / camt.054 generation.
# ---------------------------------------------------------------------------
def gen_camt053(account: Account, day: date, txns: list[Txn],
                opening: float, closing: float) -> str:
    ymd = day.strftime('%Y-%m-%d')
    stmt_id = f"STMT-{account.bic[:4]}-{account.currency}-{day.strftime('%Y%m%d')}"
    msg_id = f"MSG-{day.strftime('%Y%m%d')}-{random.randint(1000, 9999)}"
    entries: list[str] = []
    for i, t in enumerate(txns, 1):
        dc = 'CRDT' if t.sign == 'C' else 'DBIT'
        ref_id = t.ref or f"INSTR{i:04d}"
        entries.append(f"""      <Ntry>
        <Amt Ccy="{account.currency}">{t.amount:.2f}</Amt>
        <CdtDbtInd>{dc}</CdtDbtInd>
        <Sts>BOOK</Sts>
        <BookgDt><Dt>{t.book_date.strftime('%Y-%m-%d')}</Dt></BookgDt>
        <ValDt><Dt>{t.value_date.strftime('%Y-%m-%d')}</Dt></ValDt>
        <AcctSvcrRef>{ref_id}</AcctSvcrRef>
        <NtryDtls>
          <TxDtls>
            <Refs><EndToEndId>{ref_id}</EndToEndId></Refs>
            <RmtInf><Ustrd>{t.narration_type} - {t.counterparty}</Ustrd></RmtInf>
          </TxDtls>
        </NtryDtls>
        <AddtlNtryInf>{t.narration_type} from/to {t.counterparty}</AddtlNtryInf>
      </Ntry>""")

    return f"""<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:camt.053.001.02">
  <BkToCstmrStmt>
    <GrpHdr>
      <MsgId>{msg_id}</MsgId>
      <CreDtTm>{ymd}T23:30:00Z</CreDtTm>
    </GrpHdr>
    <Stmt>
      <Id>{stmt_id}</Id>
      <StmtPgntn><PgNb>1</PgNb><LastPgInd>true</LastPgInd></StmtPgntn>
      <Acct>
        <Id><Othr><Id>{account.swift_account}</Id></Othr></Id>
        <Ccy>{account.currency}</Ccy>
        <Svcr><FinInstnId><BICFI>{account.bic}</BICFI></FinInstnId></Svcr>
      </Acct>
      <Bal>
        <Tp><CdOrPrtry><Cd>OPBD</Cd></CdOrPrtry></Tp>
        <Amt Ccy="{account.currency}">{opening:.2f}</Amt>
        <CdtDbtInd>CRDT</CdtDbtInd>
        <Dt><Dt>{ymd}</Dt></Dt>
      </Bal>
      <Bal>
        <Tp><CdOrPrtry><Cd>CLBD</Cd></CdOrPrtry></Tp>
        <Amt Ccy="{account.currency}">{closing:.2f}</Amt>
        <CdtDbtInd>CRDT</CdtDbtInd>
        <Dt><Dt>{ymd}</Dt></Dt>
      </Bal>
{chr(10).join(entries)}
    </Stmt>
  </BkToCstmrStmt>
</Document>
"""


def gen_camt054(account: Account, day: date, txns: list[Txn]) -> str:
    """camt.054 is an intraday notification — no balances, just entries."""
    ymd = day.strftime('%Y-%m-%d')
    stmt_id = f"NTFCTN-{account.bic[:4]}-{account.currency}-{day.strftime('%Y%m%d')}"
    msg_id = f"NTF-{day.strftime('%Y%m%d')}-{random.randint(1000, 9999)}"
    entries: list[str] = []
    for i, t in enumerate(txns, 1):
        dc = 'CRDT' if t.sign == 'C' else 'DBIT'
        ref_id = t.ref or f"INSTR{i:04d}"
        entries.append(f"""      <Ntry>
        <Amt Ccy="{account.currency}">{t.amount:.2f}</Amt>
        <CdtDbtInd>{dc}</CdtDbtInd>
        <Sts>BOOK</Sts>
        <BookgDt><Dt>{t.book_date.strftime('%Y-%m-%d')}</Dt></BookgDt>
        <ValDt><Dt>{t.value_date.strftime('%Y-%m-%d')}</Dt></ValDt>
        <AcctSvcrRef>{ref_id}</AcctSvcrRef>
        <NtryDtls>
          <TxDtls>
            <Refs><EndToEndId>{ref_id}</EndToEndId></Refs>
            <RmtInf><Ustrd>{t.narration_type} - {t.counterparty}</Ustrd></RmtInf>
          </TxDtls>
        </NtryDtls>
        <AddtlNtryInf>{t.narration_type} from/to {t.counterparty}</AddtlNtryInf>
      </Ntry>""")
    return f"""<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:camt.054.001.02">
  <BkToCstmrDbtCdtNtfctn>
    <GrpHdr>
      <MsgId>{msg_id}</MsgId>
      <CreDtTm>{ymd}T14:15:00Z</CreDtTm>
    </GrpHdr>
    <Ntfctn>
      <Id>{stmt_id}</Id>
      <CreDtTm>{ymd}T14:15:00Z</CreDtTm>
      <Acct>
        <Id><Othr><Id>{account.swift_account}</Id></Othr></Id>
        <Ccy>{account.currency}</Ccy>
        <Svcr><FinInstnId><BICFI>{account.bic}</BICFI></FinInstnId></Svcr>
      </Acct>
{chr(10).join(entries)}
    </Ntfctn>
  </BkToCstmrDbtCdtNtfctn>
</Document>
"""


# ---------------------------------------------------------------------------
# Flexcube-shaped xlsx.
# Columns: TRN_REF_NO, BOOKING_DATE, TYPE, TXN_NARRATIONS, VALUE_DATE,
#          LCY_AMOUNT, AC_BRANCH, AC_NO, ACCT_CCY, MODULE, EXTERNAL_REF_NO,
#          USER_ID
# Sheet:   acc_entries
# ---------------------------------------------------------------------------
FLEX_HEADERS = [
    'TRN_REF_NO', 'BOOKING_DATE', 'TYPE', 'TXN_NARRATIONS', 'VALUE_DATE',
    'LCY_AMOUNT', 'AC_BRANCH', 'AC_NO', 'ACCT_CCY', 'MODULE',
    'EXTERNAL_REF_NO', 'USER_ID',
]

FLEX_USER_IDS = ['MTREAS01', 'MTREAS02', 'MTRADE01', 'MGRPOPS', 'MCASHMGT']


def gen_flex_xlsx(account: Account, day: date, flex_txns: list[Txn], out_path: Path) -> None:
    wb = Workbook()
    ws = wb.active
    ws.title = 'acc_entries'
    ws.append(FLEX_HEADERS)
    for t in flex_txns:
        # Flex TYPE is the MIRROR of the SWIFT sign. A SWIFT credit (money
        # arrived at the nostro from the correspondent's view) is a DEBIT
        # to our nostro GL (assets increase on the debit side).
        dc = 'DR' if t.sign == 'C' else 'CR'
        module = 'FT' if 'transfer' in t.narration_type or 'payment' in t.narration_type else 'LD'
        user_id = random.choice(FLEX_USER_IDS)
        narration = f"{t.ref or 'NONREF'} /{t.counterparty} {t.narration_type} |USERID:{user_id}|"
        ws.append([
            t.ref or f"MTB{random.randint(10000000, 99999999)}",
            datetime(t.book_date.year, t.book_date.month, t.book_date.day),
            dc,
            narration,
            datetime(t.value_date.year, t.value_date.month, t.value_date.day),
            t.amount,
            OWN_BRANCH,
            account.flex_ac_no,
            account.currency,
            module,
            t.ref or f"MTB{random.randint(10000000, 99999999)}",
            user_id,
        ])
    out_path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(out_path)


# ---------------------------------------------------------------------------
# DB seeding.
# ---------------------------------------------------------------------------
def seed_banks_and_accounts(db_path: Path) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    conn = sqlite3.connect(db_path)
    try:
        # Insert own bank + 10 correspondents.
        bank_rows = [
            (OWN_BIC, OWN_BANK_NAME, 'MTB', 'our', 'bank', None, 'MTB', 1, now, 'system'),
        ]
        for a in ACCOUNTS:
            nick = a.correspondent.split(',')[0].split(' ')[0][:10].upper()
            bank_rows.append(
                (a.bic, a.correspondent, nick, 'their', 'bank', a.access_area, None, 1, now, 'system')
            )
        conn.executemany(
            "INSERT OR IGNORE INTO banks "
            "(bic, name, nickname, origin, type, access_area, user_code, active, created_at, created_by) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            bank_rows,
        )

        # Insert 10 cash accounts.
        account_rows = []
        for a in ACCOUNTS:
            account_rows.append((
                a.label, a.shortname, a.access_area, a.bic,
                a.swift_account, a.flex_ac_no, a.currency,
                None, 1, now, 'system',
            ))
        conn.executemany(
            "INSERT OR IGNORE INTO accounts "
            "(label, shortname, access_area, bic, swift_account, flex_ac_no, currency, "
            " notes, active, created_at, created_by) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            account_rows,
        )
        conn.commit()
        print(f"Seeded {len(bank_rows)} banks and {len(account_rows)} accounts.")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Main.
# ---------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--copy-to-messages', action='store_true',
                    help='After generating, copy files into messages/swift and messages/flexcube.')
    args = ap.parse_args()

    root = Path(__file__).resolve().parent.parent
    db_path = root / 'kilter.db'
    out_swift = root / 'demo_data' / 'swift'
    out_flex  = root / 'demo_data' / 'flexcube'
    out_swift.mkdir(parents=True, exist_ok=True)
    out_flex.mkdir(parents=True, exist_ok=True)

    if not db_path.exists():
        print(f"!! kilter.db not found at {db_path}.")
        print("   Start uvicorn once so init_db() creates the schema, then re-run this.")
        sys.exit(1)

    # 1) Seed banks + accounts
    seed_banks_and_accounts(db_path)

    # 2) Generate files per account per day
    stats = {'mt940': 0, 'mt950': 0, 'camt053': 0, 'camt054': 0, 'flex': 0,
             'swift_txns': 0, 'flex_txns': 0}
    for account in ACCOUNTS:
        running_balance = account.opening_bal
        for day in BUSINESS_DAYS:
            pairs = generate_day_txns(account, day)
            swift_txns = [p[0] for p in pairs if p[0] is not None]
            flex_txns  = [p[1] for p in pairs if p[1] is not None]

            # Compute closing balance = opening +/- signed amounts
            opening = running_balance
            net = sum((t.amount if t.sign == 'C' else -t.amount) for t in swift_txns)
            closing = opening + net
            running_balance = closing

            # Write SWIFT
            fmt = account.format
            if fmt in ('mt940', 'mt950'):
                kind = '940' if fmt == 'mt940' else '950'
                text = gen_mt_text(account, day, swift_txns, opening, closing, kind=kind)
                ext = '.out'
                (out_swift / f"N{account.idx:02d}_{day.isoformat()}_{fmt}{ext}").write_text(
                    text, encoding='latin-1')
                stats[fmt] += 1
            elif fmt == 'camt053':
                xml = gen_camt053(account, day, swift_txns, opening, closing)
                (out_swift / f"N{account.idx:02d}_{day.isoformat()}_camt053.xml").write_text(
                    xml, encoding='utf-8')
                stats['camt053'] += 1
            elif fmt == 'camt054':
                xml = gen_camt054(account, day, swift_txns)
                (out_swift / f"N{account.idx:02d}_{day.isoformat()}_camt054.xml").write_text(
                    xml, encoding='utf-8')
                stats['camt054'] += 1

            # Write Flex xlsx — named after flex_ac_no + day (Kilter's scanner
            # pairs on account key + currency).
            flex_name = f"{account.flex_ac_no}_{day.isoformat()}.xlsx"
            gen_flex_xlsx(account, day, flex_txns, out_flex / flex_name)
            stats['flex'] += 1

            stats['swift_txns'] += len(swift_txns)
            stats['flex_txns']  += len(flex_txns)

    print(f"\nGenerated in {out_swift.parent}:")
    print(f"  MT940:     {stats['mt940']} files")
    print(f"  MT950:     {stats['mt950']} files")
    print(f"  camt.053:  {stats['camt053']} files")
    print(f"  camt.054:  {stats['camt054']} files")
    print(f"  Flex xlsx: {stats['flex']} files")
    print(f"  SWIFT transactions: {stats['swift_txns']}")
    print(f"  Flex transactions:  {stats['flex_txns']}")

    # 3) Optionally copy to messages/ for the scanner to pick up
    if args.copy_to_messages:
        msg_swift = root / 'messages' / 'swift'
        msg_flex  = root / 'messages' / 'flexcube'
        msg_swift.mkdir(parents=True, exist_ok=True)
        msg_flex.mkdir(parents=True, exist_ok=True)
        for f in out_swift.iterdir():
            shutil.copy(f, msg_swift / f.name)
        for f in out_flex.iterdir():
            shutil.copy(f, msg_flex / f.name)
        print(f"\nCopied files into {msg_swift} and {msg_flex} for scanner pickup.")


if __name__ == '__main__':
    main()
