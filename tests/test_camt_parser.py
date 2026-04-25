"""ISO 20022 camt.053 / camt.054 parser tests.

Covers:
    - Plain Document-rooted messages
    - SWIFT Alliance Access (Saa:DataPDU) envelopes
    - Transactions with EndToEndId, RmtInf/Ustrd, AcctSvcrRef
    - Balance synonyms (OPBD vs PRCD)
    - camt.054 (no balances expected)
"""

import textwrap
from pathlib import Path

import pytest

from iso20022_loader import (
    detect_camt_type, load_camt_raw, extract_camt_meta_raw,
)


def _write(tmp_path: Path, name: str, xml: str) -> Path:
    p = tmp_path / name
    p.write_text(xml, encoding='utf-8')
    return p


CAMT053_PLAIN = """<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:camt.053.001.02">
  <BkToCstmrStmt>
    <GrpHdr><MsgId>MSG-1</MsgId><CreDtTm>2026-04-17T23:30:00Z</CreDtTm></GrpHdr>
    <Stmt>
      <Id>STMT-USD-20260417</Id>
      <Acct>
        <Id><IBAN>GB00CITI00001234567890</IBAN></Id>
        <Ccy>USD</Ccy>
        <Svcr><FinInstnId><BICFI>CITIUS33</BICFI></FinInstnId></Svcr>
      </Acct>
      <Bal>
        <Tp><CdOrPrtry><Cd>OPBD</Cd></CdOrPrtry></Tp>
        <Amt Ccy="USD">10000.00</Amt>
        <CdtDbtInd>CRDT</CdtDbtInd>
        <Dt><Dt>2026-04-17</Dt></Dt>
      </Bal>
      <Bal>
        <Tp><CdOrPrtry><Cd>CLBD</Cd></CdOrPrtry></Tp>
        <Amt Ccy="USD">14500.00</Amt>
        <CdtDbtInd>CRDT</CdtDbtInd>
        <Dt><Dt>2026-04-17</Dt></Dt>
      </Bal>
      <Ntry>
        <Amt Ccy="USD">5000.00</Amt>
        <CdtDbtInd>CRDT</CdtDbtInd>
        <BookgDt><Dt>2026-04-17</Dt></BookgDt>
        <ValDt><Dt>2026-04-17</Dt></ValDt>
        <AcctSvcrRef>BANKREF12</AcctSvcrRef>
        <NtryDtls><TxDtls>
          <Refs><EndToEndId>MTBE2E001</EndToEndId></Refs>
          <RmtInf><Ustrd>Trade settlement</Ustrd></RmtInf>
        </TxDtls></NtryDtls>
      </Ntry>
      <Ntry>
        <Amt Ccy="USD">500.00</Amt>
        <CdtDbtInd>DBIT</CdtDbtInd>
        <BookgDt><Dt>2026-04-17</Dt></BookgDt>
        <ValDt><Dt>2026-04-17</Dt></ValDt>
        <AcctSvcrRef>BANKREF13</AcctSvcrRef>
        <NtryDtls><TxDtls>
          <Refs><EndToEndId>MTBE2E002</EndToEndId></Refs>
          <RmtInf><Ustrd>Wire fee</Ustrd></RmtInf>
        </TxDtls></NtryDtls>
      </Ntry>
    </Stmt>
  </BkToCstmrStmt>
</Document>"""


def test_detect_camt053(tmp_path):
    p = _write(tmp_path, 'plain.xml', CAMT053_PLAIN)
    assert detect_camt_type(p) == 'camt053'


def test_camt053_load_returns_engine_shape(tmp_path):
    p = _write(tmp_path, 'plain.xml', CAMT053_PLAIN)
    txns = load_camt_raw(p)
    assert len(txns) == 2
    # Engine expects '_row_number', amount, sign, value_date, our_ref keys.
    assert all('_row_number' in t for t in txns)
    assert [t['sign'] for t in txns] == ['C', 'D']
    assert [t['amount'] for t in txns] == [5000.00, 500.00]
    assert [t['our_ref'] for t in txns] == ['MTBE2E001', 'MTBE2E002']


def test_camt053_balances_and_account(tmp_path):
    p = _write(tmp_path, 'plain.xml', CAMT053_PLAIN)
    meta = extract_camt_meta_raw(p)
    assert meta['account'] == 'GB00CITI00001234567890'
    assert meta['currency'] == 'USD'
    assert meta['bic'] == 'CITIUS33'
    assert meta['opening_balance_amount'] == 10000.00
    assert meta['closing_balance_amount'] == 14500.00


def test_camt053_prcd_treated_as_opening(tmp_path):
    """BoG RTGS continuously-carried accounts emit PRCD instead of OPBD —
    Kilter must treat them as synonyms so the session has a balance panel."""
    xml = CAMT053_PLAIN.replace('<Cd>OPBD</Cd>', '<Cd>PRCD</Cd>')
    p = _write(tmp_path, 'prcd.xml', xml)
    meta = extract_camt_meta_raw(p)
    assert meta['opening_balance_amount'] == 10000.00


CAMT053_SAA = """<?xml version="1.0" encoding="UTF-8"?>
<Saa:DataPDU xmlns:Saa="urn:swift:saa:xsd:saa.2.0">
  <Saa:Body>
    <AppHdr xmlns="urn:iso:std:iso:20022:tech:xsd:head.001.001.02">
      <Fr><FIId><FinInstnId><BICFI>CITIUS33</BICFI></FinInstnId></FIId></Fr>
    </AppHdr>
    """ + CAMT053_PLAIN.split('?>')[1] + """
  </Saa:Body>
</Saa:DataPDU>"""


def test_camt053_inside_saa_envelope(tmp_path):
    """Real-world banks ship camt.053 wrapped in a SWIFT Alliance Access
    envelope. The loader must walk past Saa:DataPDU/Saa:Body to find the
    Document inside."""
    p = _write(tmp_path, 'saa.xml', CAMT053_SAA)
    assert detect_camt_type(p) == 'camt053'
    txns = load_camt_raw(p)
    assert len(txns) == 2


CAMT054_PLAIN = """<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:camt.054.001.02">
  <BkToCstmrDbtCdtNtfctn>
    <GrpHdr><MsgId>NTF-1</MsgId><CreDtTm>2026-04-17T14:15:00Z</CreDtTm></GrpHdr>
    <Ntfctn>
      <Id>NTF-USD-1</Id>
      <CreDtTm>2026-04-17T14:15:00Z</CreDtTm>
      <Acct>
        <Id><Othr><Id>987654321</Id></Othr></Id>
        <Ccy>USD</Ccy>
        <Svcr><FinInstnId><BICFI>CHASUS33</BICFI></FinInstnId></Svcr>
      </Acct>
      <Ntry>
        <Amt Ccy="USD">7777.00</Amt>
        <CdtDbtInd>CRDT</CdtDbtInd>
        <BookgDt><Dt>2026-04-17</Dt></BookgDt>
        <ValDt><Dt>2026-04-17</Dt></ValDt>
        <AcctSvcrRef>NTFREF1</AcctSvcrRef>
        <NtryDtls><TxDtls>
          <Refs><EndToEndId>NTFE2E1</EndToEndId></Refs>
        </TxDtls></NtryDtls>
      </Ntry>
    </Ntfctn>
  </BkToCstmrDbtCdtNtfctn>
</Document>"""


def test_detect_camt054(tmp_path):
    p = _write(tmp_path, 'ntf.xml', CAMT054_PLAIN)
    assert detect_camt_type(p) == 'camt054'


def test_camt054_no_balances(tmp_path):
    """camt.054 is intraday — typically no OPBD/CLBD. Meta should still
    return account + bic + currency without exploding."""
    p = _write(tmp_path, 'ntf.xml', CAMT054_PLAIN)
    meta = extract_camt_meta_raw(p)
    assert meta['account'] == '987654321'
    assert 'opening_balance_amount' not in meta
    assert 'closing_balance_amount' not in meta
