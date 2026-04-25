"""
_smoke_camt.py
==============

Parser smoke-test for iso20022_loader. Generates two synthetic fixtures
(camt.053 + camt.054), parses them back, and asserts the pipeline-facing
dict shape matches what the MT loader produces.

Run:
    python _smoke_camt.py
"""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from iso20022_loader import (
    detect_camt_type,
    extract_camt_meta_raw,
    load_camt_raw,
)


CAMT_053_FIXTURE = """<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:camt.053.001.02">
  <BkToCstmrStmt>
    <GrpHdr>
      <MsgId>MSG-20260424-001</MsgId>
      <CreDtTm>2026-04-24T23:30:00Z</CreDtTm>
    </GrpHdr>
    <Stmt>
      <Id>STMT-ECO-USD-20260424</Id>
      <StmtPgntn>
        <PgNb>1</PgNb>
        <LastPgInd>true</LastPgInd>
      </StmtPgntn>
      <Acct>
        <Id>
          <IBAN>GH12ECOB00001234567890</IBAN>
        </Id>
        <Ccy>USD</Ccy>
        <Svcr>
          <FinInstnId>
            <BICFI>CITIUS33XXX</BICFI>
          </FinInstnId>
        </Svcr>
      </Acct>
      <Bal>
        <Tp><CdOrPrtry><Cd>OPBD</Cd></CdOrPrtry></Tp>
        <Amt Ccy="USD">1000000.00</Amt>
        <CdtDbtInd>CRDT</CdtDbtInd>
        <Dt><Dt>2026-04-24</Dt></Dt>
      </Bal>
      <Bal>
        <Tp><CdOrPrtry><Cd>CLBD</Cd></CdOrPrtry></Tp>
        <Amt Ccy="USD">1125000.50</Amt>
        <CdtDbtInd>CRDT</CdtDbtInd>
        <Dt><Dt>2026-04-24</Dt></Dt>
      </Bal>
      <Ntry>
        <NtryRef>NTRY-001</NtryRef>
        <Amt Ccy="USD">50000.00</Amt>
        <CdtDbtInd>CRDT</CdtDbtInd>
        <Sts>BOOK</Sts>
        <BookgDt><Dt>2026-04-24</Dt></BookgDt>
        <ValDt><Dt>2026-04-24</Dt></ValDt>
        <AcctSvcrRef>CITI-REF-98765</AcctSvcrRef>
        <BkTxCd><Domn><Cd>PMNT</Cd></Domn></BkTxCd>
        <NtryDtls>
          <TxDtls>
            <Refs>
              <EndToEndId>PAY-E2E-00123</EndToEndId>
              <InstrId>INSTR-555</InstrId>
              <TxId>TX-ABC-999</TxId>
            </Refs>
            <RmtInf>
              <Ustrd>Inward remittance from ACME Corp</Ustrd>
              <Ustrd>Invoice 2026-04-15</Ustrd>
            </RmtInf>
          </TxDtls>
        </NtryDtls>
        <AddtlNtryInf>Credit via correspondent Citi NY</AddtlNtryInf>
      </Ntry>
      <Ntry>
        <NtryRef>NTRY-002</NtryRef>
        <Amt Ccy="USD">75000.00</Amt>
        <CdtDbtInd>DBIT</CdtDbtInd>
        <Sts>BOOK</Sts>
        <BookgDt><Dt>2026-04-24</Dt></BookgDt>
        <ValDt><Dt>2026-04-24</Dt></ValDt>
        <NtryDtls>
          <TxDtls>
            <Refs>
              <EndToEndId>PAY-E2E-00456</EndToEndId>
            </Refs>
            <RmtInf>
              <Ustrd>Outward transfer</Ustrd>
            </RmtInf>
          </TxDtls>
        </NtryDtls>
      </Ntry>
    </Stmt>
  </BkToCstmrStmt>
</Document>
"""


CAMT_054_FIXTURE = """<?xml version="1.0" encoding="UTF-8"?>
<Document xmlns="urn:iso:std:iso:20022:tech:xsd:camt.054.001.02">
  <BkToCstmrDbtCdtNtfctn>
    <GrpHdr>
      <MsgId>NTF-20260424-ABCD</MsgId>
      <CreDtTm>2026-04-24T10:15:00Z</CreDtTm>
    </GrpHdr>
    <Ntfctn>
      <Id>NTF-ECO-EUR-INTRA-042</Id>
      <CreDtTm>2026-04-24T10:15:00Z</CreDtTm>
      <Acct>
        <Id>
          <Othr>
            <Id>ECO-NOSTRO-EUR-001</Id>
          </Othr>
        </Id>
        <Ccy>EUR</Ccy>
        <Svcr>
          <FinInstnId>
            <BIC>DEUTDEFF</BIC>
          </FinInstnId>
        </Svcr>
      </Acct>
      <Ntry>
        <Amt Ccy="EUR">12500.75</Amt>
        <CdtDbtInd>CRDT</CdtDbtInd>
        <Sts>BOOK</Sts>
        <BookgDt><DtTm>2026-04-24T10:14:30Z</DtTm></BookgDt>
        <ValDt><Dt>2026-04-24</Dt></ValDt>
        <AcctSvcrRef>DB-INTRA-7788</AcctSvcrRef>
        <NtryDtls>
          <TxDtls>
            <Refs>
              <EndToEndId>E2E-INTRA-321</EndToEndId>
            </Refs>
            <RmtInf>
              <Ustrd>Intraday credit notification</Ustrd>
            </RmtInf>
          </TxDtls>
        </NtryDtls>
      </Ntry>
    </Ntfctn>
  </BkToCstmrDbtCdtNtfctn>
</Document>
"""


def _check(label: str, actual, expected):
    ok = actual == expected
    print(f"  [{'OK' if ok else 'FAIL'}] {label}: got={actual!r} expected={expected!r}")
    return ok


def _smoke_camt053(tmpdir: Path) -> bool:
    path = tmpdir / 'sample.camt053.xml'
    path.write_text(CAMT_053_FIXTURE, encoding='utf-8')
    print(f"\n== camt.053 fixture ({path.name}) ==")

    passed = True
    passed &= _check("detect_camt_type", detect_camt_type(path), 'camt053')

    meta = extract_camt_meta_raw(path)
    passed &= _check("meta.account",  meta.get('account'),  'GH12ECOB00001234567890')
    passed &= _check("meta.currency", meta.get('currency'), 'USD')
    passed &= _check("meta.bic",      meta.get('bic'),      'CITIUS33')
    passed &= _check("meta.statement_ref",    meta.get('statement_ref'),   'STMT-ECO-USD-20260424')
    passed &= _check("meta.statement_number", meta.get('statement_number'), '1')
    passed &= _check("meta.opening_balance_amount", meta.get('opening_balance_amount'), 1000000.00)
    passed &= _check("meta.opening_balance_sign",   meta.get('opening_balance_sign'),   'C')
    passed &= _check("meta.opening_balance_date",   meta.get('opening_balance_date'),   20260424)
    passed &= _check("meta.closing_balance_amount", meta.get('closing_balance_amount'), 1125000.50)
    passed &= _check("meta.closing_balance_sign",   meta.get('closing_balance_sign'),   'C')

    txns = load_camt_raw(path)
    passed &= _check("txn count", len(txns), 2)

    t0 = txns[0]
    passed &= _check("t0.amount",         t0['amount'],         50000.00)
    passed &= _check("t0.sign",           t0['sign'],           'C')
    passed &= _check("t0.value_date",     t0['value_date'],     20260424)
    passed &= _check("t0.book_date",      t0['book_date'],      20260424)
    passed &= _check("t0.our_ref",        t0['our_ref'],        'PAY-E2E-00123')
    passed &= _check("t0.their_ref",      t0['their_ref'],      'CITI-REF-98765')
    passed &= _check("t0.origin",         t0['origin'],         'Their')
    passed &= _check("t0.status",         t0['status'],         'Unmatched')
    passed &= _check("t0.booking_text_1", t0['booking_text_1'], 'Inward remittance from ACME Corp Invoice 2026-04-15')
    passed &= _check("t0.booking_text_2", t0['booking_text_2'], 'Credit via correspondent Citi NY')

    t1 = txns[1]
    passed &= _check("t1.amount", t1['amount'], 75000.00)
    passed &= _check("t1.sign",   t1['sign'],   'D')
    passed &= _check("t1.our_ref", t1['our_ref'], 'PAY-E2E-00456')
    return passed


def _smoke_camt054(tmpdir: Path) -> bool:
    path = tmpdir / 'sample.camt054.xml'
    path.write_text(CAMT_054_FIXTURE, encoding='utf-8')
    print(f"\n== camt.054 fixture ({path.name}) ==")

    passed = True
    passed &= _check("detect_camt_type", detect_camt_type(path), 'camt054')

    meta = extract_camt_meta_raw(path)
    passed &= _check("meta.account",      meta.get('account'),      'ECO-NOSTRO-EUR-001')
    passed &= _check("meta.currency",     meta.get('currency'),     'EUR')
    passed &= _check("meta.bic",          meta.get('bic'),          'DEUTDEFF')
    passed &= _check("meta.statement_ref", meta.get('statement_ref'), 'NTF-ECO-EUR-INTRA-042')
    # camt.054 has no OPBD/CLBD — those fields must stay missing, not crash.
    passed &= _check("meta no opening_balance", 'opening_balance' in meta, False)
    passed &= _check("meta no closing_balance", 'closing_balance' in meta, False)

    txns = load_camt_raw(path)
    passed &= _check("txn count", len(txns), 1)
    t0 = txns[0]
    passed &= _check("t0.amount",    t0['amount'],    12500.75)
    passed &= _check("t0.sign",      t0['sign'],      'C')
    passed &= _check("t0.book_date", t0['book_date'], 20260424)
    passed &= _check("t0.our_ref",   t0['our_ref'],   'E2E-INTRA-321')
    passed &= _check("t0.their_ref", t0['their_ref'], 'DB-INTRA-7788')
    return passed


def main():
    with tempfile.TemporaryDirectory() as td:
        tmpdir = Path(td)
        ok053 = _smoke_camt053(tmpdir)
        ok054 = _smoke_camt054(tmpdir)

    print("\n" + "=" * 60)
    if ok053 and ok054:
        print("All ISO 20022 smoke tests PASSED")
        return 0
    print("Some ISO 20022 smoke tests FAILED — see above")
    return 1


if __name__ == '__main__':
    sys.exit(main())
