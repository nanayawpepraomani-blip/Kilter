"""
iso20022_loader.py
==================

Parse ISO 20022 camt.053 (end-of-day statement) and camt.054 (intraday
debit/credit notification) XML messages into the same dict shape the
MT940/MT950 pipeline produces. Downstream the reconciliation engine is
format-agnostic — same normalize_ref, same propose_candidates.

SWIFT are retiring MT940/MT950 on the FIN network; most correspondents
already deliver camt.053 as an alternative. This module lets Kilter
accept either format by simply dropping files into messages/swift/ —
the scanner routes by file content.

Public API (mirrors swift_loader.py):
    detect_camt_type(path)        -> 'camt053' | 'camt054' | None
    load_camt_raw(path)           -> list[dict]   (same keys as load_swift_raw)
    extract_camt_meta_raw(path)   -> dict         (same keys as extract_swift_meta_raw)
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path


# ---------------------------------------------------------------------------
# Namespace-agnostic walkers
# ---------------------------------------------------------------------------

def _local(tag: str) -> str:
    """'{urn:iso:...}Stmt' -> 'Stmt'."""
    return tag.split('}', 1)[1] if '}' in tag else tag


def _child(el, name):
    if el is None:
        return None
    for c in el:
        if _local(c.tag) == name:
            return c
    return None


def _children(el, name):
    if el is None:
        return []
    return [c for c in el if _local(c.tag) == name]


def _descendant(el, *path):
    cur = el
    for name in path:
        cur = _child(cur, name)
        if cur is None:
            return None
    return cur


def _text(el, *path):
    node = _descendant(el, *path) if path else el
    if node is None:
        return None
    t = (node.text or '').strip()
    return t or None


def _amount_from(el):
    """<Amt Ccy='USD'>1234.56</Amt> -> (1234.56, 'USD'). Missing -> (None, None)."""
    if el is None:
        return None, None
    try:
        amt = float((el.text or '').strip())
    except (TypeError, ValueError):
        return None, el.get('Ccy')
    return amt, el.get('Ccy')


def _date_yyyymmdd(s):
    """'2026-04-24' or '2026-04-24T08:15:00Z' -> 20260424. None on bad input."""
    if not s:
        return None
    s = s.strip()[:10]
    try:
        d = datetime.strptime(s, '%Y-%m-%d')
    except ValueError:
        return None
    return int(d.strftime('%Y%m%d'))


# ---------------------------------------------------------------------------
# Top-level dispatch
# ---------------------------------------------------------------------------

def detect_camt_type(path: Path):
    """Return 'camt053', 'camt054', or None. Tolerates namespace stripping
    and SWIFT Alliance Access envelopes (Saa:DataPDU wraps <Document>)."""
    try:
        tree = ET.parse(path)
    except ET.ParseError:
        return None
    root = tree.getroot()
    # Namespace URI test first — cheapest and version-agnostic. Walk the whole
    # tree because real BoG RTGS messages wrap the ISO 20022 Document inside a
    # Saa:DataPDU envelope, so the camt URI isn't on the root element.
    for el in root.iter():
        tag = el.tag
        if 'camt.053' in tag:
            return 'camt053'
        if 'camt.054' in tag:
            return 'camt054'
        # Payload-tag fallback for namespace-stripped messages.
        local = _local(tag)
        if local == 'BkToCstmrStmt':
            return 'camt053'
        if local == 'BkToCstmrDbtCdtNtfctn':
            return 'camt054'
    return None


def load_camt_raw(path: Path) -> list:
    """Parse camt.053/054 into the txn dict shape the engine expects."""
    container, kind, _ = _locate_container(path)
    return list(_iter_txns(container))


def extract_camt_meta_raw(path: Path) -> dict:
    """Metadata dict matching extract_swift_meta_raw's shape."""
    container, kind, root = _locate_container(path)
    return _build_meta(container, kind, root)


# ---------------------------------------------------------------------------
# Shared parsing
# ---------------------------------------------------------------------------

def _locate_container(path: Path):
    """Return (Stmt-or-Ntfctn, kind, root). Validates single-account-per-file.

    Searches any depth for the wrapper element so files wrapped in a SWIFT
    Alliance Access envelope (Saa:DataPDU > Saa:Body > Document > ...) parse
    identically to files whose root element is <Document>. The root element
    comes back too so _build_meta can pull AppHdr-level fields (sender BIC)."""
    kind = detect_camt_type(path)
    if kind not in ('camt053', 'camt054'):
        raise ValueError(
            f"'{path.name}' is not a recognised camt.053 or camt.054 message.")
    tree = ET.parse(path)
    root = tree.getroot()
    wrapper_name = 'BkToCstmrStmt' if kind == 'camt053' else 'BkToCstmrDbtCdtNtfctn'
    stmt_name    = 'Stmt'          if kind == 'camt053' else 'Ntfctn'
    wrapper = None
    for el in root.iter():
        if _local(el.tag) == wrapper_name:
            wrapper = el
            break
    if wrapper is None:
        raise ValueError(f"'{path.name}' is missing <{wrapper_name}>.")
    stmts = _children(wrapper, stmt_name)
    if not stmts:
        raise ValueError(f"'{path.name}' contains no <{stmt_name}>.")
    if len(stmts) > 1:
        # Ops contract: one account per file (same rule as the Flexcube side).
        raise ValueError(
            f"'{path.name}' contains {len(stmts)} <{stmt_name}> elements; "
            "Kilter expects one account per file. Split the message and re-load.")
    return stmts[0], kind, root


def _iter_txns(container):
    """Yield txn dicts from <Ntry> children in document order.

    When an <Ntry> omits BookgDt/ValDt — common in camt.054 intraday
    notifications — fall back to the container's CreDtTm so the engine
    still has a date for day-window matching."""
    fallback_date = _date_yyyymmdd(
        _text(container, 'CreDtTm')
        or _text(container, 'FrToDt', 'ToDtTm')
        or _text(container, 'FrToDt', 'FrDtTm'))
    row = 0
    for ntry in _children(container, 'Ntry'):
        row += 1
        amount, _ = _amount_from(_child(ntry, 'Amt'))
        if amount is None:
            continue
        sign = 'C' if (_text(ntry, 'CdtDbtInd') or '').upper() == 'CRDT' else 'D'
        book_date = _date_yyyymmdd(
            _text(ntry, 'BookgDt', 'Dt') or _text(ntry, 'BookgDt', 'DtTm'))
        value_date = _date_yyyymmdd(
            _text(ntry, 'ValDt', 'Dt') or _text(ntry, 'ValDt', 'DtTm'))
        our_ref, their_ref, rmt, addl = _refs_and_narration(ntry)
        yield {
            '_source': 'swift',
            '_row_number': row,
            '_used': False,
            'value_date': value_date or book_date or fallback_date,
            'amount': amount,
            'sign': sign,
            'origin': 'Their',
            'type': 'Other',
            'status': 'Unmatched',
            'book_date': book_date or value_date or fallback_date,
            'our_ref': our_ref or '',
            'their_ref': their_ref or '',
            'booking_text_1': rmt or '',
            'booking_text_2': addl or '',
        }


def _find_apphdr_sender_bic(root):
    """Walk any-depth for AppHdr/Fr/FIId/FinInstnId/BICFI (or BIC)."""
    for el in root.iter():
        if _local(el.tag) != 'AppHdr':
            continue
        bic = (_text(el, 'Fr', 'FIId', 'FinInstnId', 'BICFI')
               or _text(el, 'Fr', 'FIId', 'FinInstnId', 'BIC'))
        if bic:
            return bic
    return None


def _refs_and_narration(ntry):
    """Mine references and narration from an Ntry element.

    EndToEndId is usually the payment-initiation reference ops teams match
    on most often, so it wins our_ref. AcctSvcrRef is the bank's own
    statement reference — goes to their_ref. NtryRef, UETR, and TxId are
    fallbacks. UETR (unique end-to-end transaction reference) is the
    GPI tracker id SWIFT mandates on ISO 20022 payments.
    """
    end_to_end = None
    instr_id = None
    tx_id = None
    uetr = None
    rmt_parts = []
    for dtls in _children(ntry, 'NtryDtls'):
        for tx in _children(dtls, 'TxDtls'):
            refs = _child(tx, 'Refs')
            if refs is not None:
                end_to_end = end_to_end or _text(refs, 'EndToEndId')
                instr_id   = instr_id   or _text(refs, 'InstrId')
                tx_id      = tx_id      or _text(refs, 'TxId')
                uetr       = uetr       or _text(refs, 'UETR')
            for rmt in _children(tx, 'RmtInf'):
                for ustrd in _children(rmt, 'Ustrd'):
                    if ustrd.text and ustrd.text.strip():
                        rmt_parts.append(ustrd.text.strip())
    addl = _text(ntry, 'AddtlNtryInf') or ''
    our_ref = end_to_end or instr_id or _text(ntry, 'NtryRef') or uetr or ''
    their_ref = _text(ntry, 'AcctSvcrRef') or tx_id or ''
    return our_ref, their_ref, ' '.join(rmt_parts), addl


def _build_meta(container, kind, root=None):
    meta: dict = {}
    acct = _child(container, 'Acct')
    if acct is not None:
        # Account id: IBAN wins; fall back to proprietary <Othr><Id>.
        account = _text(acct, 'Id', 'IBAN') or _text(acct, 'Id', 'Othr', 'Id')
        if account:
            meta['account'] = account
        currency = _text(acct, 'Ccy')
        if currency:
            meta['currency'] = currency
        bic = (_text(acct, 'Svcr', 'FinInstnId', 'BICFI')
               or _text(acct, 'Svcr', 'FinInstnId', 'BIC'))
        if bic:
            # Match MT path: keep the 8-char institution BIC, not the 12-char LT.
            meta['bic'] = bic[:8] if len(bic) >= 8 else bic
    # SWIFT Alliance Access envelopes carry the sender BIC on the AppHdr
    # rather than the Acct/Svcr element — pull it from there as a fallback.
    if 'bic' not in meta and root is not None:
        app_bic = _find_apphdr_sender_bic(root)
        if app_bic:
            meta['bic'] = app_bic[:8] if len(app_bic) >= 8 else app_bic

    stmt_id = _text(container, 'Id')
    if stmt_id:
        meta['statement_ref'] = stmt_id
    pg_num = _text(container, 'StmtPgntn', 'PgNb')
    if pg_num:
        meta['statement_number'] = pg_num

    # Balances are a camt.053 concept (end-of-day statement). camt.054 is an
    # intraday notification and typically has no OPBD/CLBD — if those fields
    # stay missing the session simply has no balance panel, which is fine.
    #
    # BoG's RTGS camt.053 emits PRCD (previously-closed-booked) as the
    # day's opening balance instead of OPBD, because the account is
    # continuously carried. Treat the two as synonyms.
    for bal in _children(container, 'Bal'):
        code = _text(bal, 'Tp', 'CdOrPrtry', 'Cd')
        if code in ('OPBD', 'PRCD'):
            label = 'opening_balance'
        elif code == 'CLBD':
            label = 'closing_balance'
        else:
            continue
        amount, ccy = _amount_from(_child(bal, 'Amt'))
        if amount is None:
            continue
        # Don't let PRCD clobber an OPBD if both are present.
        if f'{label}_amount' in meta:
            continue
        sign = 'C' if (_text(bal, 'CdtDbtInd') or '').upper() == 'CRDT' else 'D'
        date = _date_yyyymmdd(_text(bal, 'Dt', 'Dt')
                              or _text(bal, 'Dt', 'DtTm'))
        ccy_out = ccy or meta.get('currency') or ''
        meta[label] = f"{sign} {ccy_out} {amount:,.2f} on {date}"
        meta[f'{label}_amount'] = amount
        meta[f'{label}_sign'] = sign
        meta[f'{label}_date'] = date
    return meta
