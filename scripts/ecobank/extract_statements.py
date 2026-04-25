#!/usr/bin/env python3
"""
Flexcube CORONA statement extractor.

Pulls statements for one or more accounts from Flexcube (Oracle) and writes
one CORONA-format file per account into OUTPUT_DIR, plus a consolidated
balance report.

Usage:
    python extract_statements.py -f accounts.txt
    python extract_statements.py -f accounts.txt -s 01-OCT-2024 -e 01-OCT-2024
    python extract_statements.py -c <account_no>

Environment overrides:
    FCUBS_USER, FCUBS_PASSWD, FCUBS_WORKERS, FCUBS_OUTPUT_DIR
"""

import os
import re
import sys
import string
import logging
import datetime
import multiprocessing
from optparse import OptionParser

import oracledb
import recordlib


# ---------- configuration ---------------------------------------------------

USER = os.environ.get("FCUBS_USER", "EGH_REPORT_GENERATOR")
PASSWD = os.environ.get("FCUBS_PASSWD", "se68tepTQzNKaz7H")

AWA_LIVE_DSN = """
(DESCRIPTION =
    (CONNECT_TIMEOUT= 90)(RETRY_COUNT=20)(RETRY_DELAY=3)(TRANSPORT_CONNECT_TIMEOUT=3)
    (ADDRESS_LIST = (LOAD_BALANCE=on) (ADDRESS = (PROTOCOL = TCP)(HOST = ADC-awafc-SCAN)(PORT = 1521)))
    (ADDRESS_LIST = (LOAD_BALANCE=on) (ADDRESS = (PROTOCOL = TCP)(HOST = LDC-awafc-SCAN)(PORT = 1521)))
    (CONNECT_DATA = (SERVICE_NAME = SRVFCUBSAWA))
)
"""

DEFAULT_WORKERS = int(os.environ.get("FCUBS_WORKERS", "24"))
OUTPUT_DIR = os.environ.get("FCUBS_OUTPUT_DIR", "flex_statements")
BALANCE_REPORT = "corona_report_balances.txt"

# Special sentinel account with no extractable txns/balances — emits zeros.
INTERBRANCH_SENTINEL = "GHS000238400023"

log = logging.getLogger("extract_statements")


# ---------- narration parsers ----------------------------------------------
#
# Each parser takes (narration, db_row) and returns (text1, text2, our_ref)
# where our_ref is None unless the parser wants to override "Our Reference".
# Relevant row indices: [0]=trn_ref_no  [3]=narration  [9]=user_ref_no
#                       [12]=module

_PRINTABLE = set(string.printable)


def _clean(narr):
    return (narr or "").replace("\t", "").replace("\n", "").replace("\r", "").strip()


def _printable_only(s):
    return "".join(c if c in _PRINTABLE else " " for c in s)


def parse_generic(narr, row):
    """Default: narration split at column 41."""
    clean = _clean(narr)
    return clean[:41], clean[41:400], None


def parse_zexa(narr, row):
    """ZEXA transfers: first digit-run is recon ref. If narration carries a
    ZEXA token that isn't already the transaction ref, promote it to
    Our Reference and use the raw narration as Text1."""
    clean = _printable_only(_clean(narr))
    recon_ref = (re.findall(r"(\d+)", clean) or [""])[0]
    if "ZEXA" not in row[0]:
        m = re.search(r"(\w+ZEXA\w+)", narr or "")
        if m:
            return (narr or ""), clean, m.group(1)
    return recon_ref, clean, None


def parse_rapid_transfer(narr, row):
    """Rapid Transfer: extract RT followed by 10 digits."""
    clean = _clean(narr)
    m = re.search(r"(RT\d{10})", clean)
    return (m.group(1) if m else ""), clean[:400], None


def parse_prime(narr, row):
    """PRIME card settlement: pull RRN from narration (several patterns).
    For manual DE entries use RRN in Text1; otherwise prefer user_ref_number."""
    clean = _clean(narr)
    rrn = ""
    for pattern in (
        r"RRN (\d{12})",
        r"(000\d{9})",
        r"(\d{12})\d{6}\*{6}\d{4}",
    ):
        m = re.search(pattern, clean)
        if m:
            rrn = m.group(1)
            break
    if row[12] == "DE":
        text1 = "{0:<200}".format(rrn)
    else:
        text1 = row[9] or ""
    return text1, clean[:400], None


def parse_omni(narr, row):
    """OMNI channel: first ` word:` token is the channel reference."""
    clean = _clean(narr)
    m = re.search(r" (\w+):", clean)
    ref = m.group(1) if m else ""
    return "{0:<200}".format(ref), clean[:400], None


def parse_atm(narr, row):
    """Bank Collect / ATM: strip ATM withdrawal boilerplate."""
    clean = _clean(narr)
    out = clean
    for prefix in (
        "ATM CASH WITHDRAWAL (REMOTE-ON-US)-",
        "ATM CASH WITHDRAWAL (REMOTE-ON-US)",
        "ATM WITHDRAWAL-",
        "ATM WITHDRAWAL",
    ):
        out = out.replace(prefix, "")
    return out, clean[:400], None


def parse_mtn_momo(narr, row):
    """MTN Mobile Money: pull REF:xxxx value up to next space."""
    clean = _clean(narr)
    idx = clean.find("REF:")
    if idx < 0:
        return "", clean[:400], None
    after = clean[idx + 4:]
    end = after.find(" ")
    ref = after if end < 0 else after[:end]
    return ref, clean[:400], None


# Account → parser. Unlisted accounts fall through to parse_generic.
NARRATION_PARSERS = {}
for _acc in ("GHSH98238210005", "1441000601589"):
    NARRATION_PARSERS[_acc] = parse_zexa
for _acc in ("9989194420939701", "9989194420939702", "9989194420939703"):
    NARRATION_PARSERS[_acc] = parse_rapid_transfer
for _acc in (
    "GHS998238400115", "GHS998238400119", "GHS998238400120", "GHS998238400121",
    "USD998238400115", "USD998238400119", "USD998238400120", "USD998238400121",
):
    NARRATION_PARSERS[_acc] = parse_prime
for _acc in (
    "GHS998238300036", "GHS998238300037", "GHS998238300038",
    "GHS998238300044", "GHS998238300046", "GHS998238300047",
    "GHS998238300048", "GHS998238300102",
    "USD998238300036", "USD998238300037", "USD998238300044", "USD998238300046",
    "GBP998238300036", "GBP998238300037", "GBP998238300046",
    "EUR998238300036", "EUR998238300037", "EUR998238300046",
    "XOF998238300037", "NGN998238300037", "AUD998238300037",
):
    NARRATION_PARSERS[_acc] = parse_omni
for _acc in ("1441001658780", "1441001658781"):
    NARRATION_PARSERS[_acc] = parse_atm
for _acc in ("1441002508082",):
    NARRATION_PARSERS[_acc] = parse_mtn_momo


# ---------- Oracle access ---------------------------------------------------

def connect():
    return oracledb.connect(user=USER, password=PASSWD, dsn=AWA_LIVE_DSN)


def get_default_dates():
    with connect() as orcl, orcl.cursor() as cursor:
        cursor.execute(
            "SELECT PREV_WORKING_DAY FROM fcubsawa.STTM_DATES WHERE BRANCH_CODE = 'EGH'"
        )
        d = cursor.fetchone()[0]
    return d.strftime("%d-%b-%Y")


def _is_gl(account):
    return len(account) >= 2 and not account[0].isdigit() and not account[1].isdigit()


def get_acc_ccy(cursor, account):
    if account == INTERBRANCH_SENTINEL:
        return "GHS"
    if _is_gl(account):
        return account[0:3]
    cursor.execute(
        "select ac_gl_ccy from fcubsawa.sttb_account where ac_gl_no = :account",
        {"account": account},
    )
    row = cursor.fetchone()
    if row is None:
        raise ValueError(f"No currency found for account {account}")
    return row[0]


def get_balances(cursor, account, bkg_date):
    """Sum of signed entries strictly before bkg_date. Returns (balance, ccy)."""
    if account == INTERBRANCH_SENTINEL:
        return 0, "GHS"

    if _is_gl(account):
        ccy, branch, ac_no = account[0:3], account[3:6], account[6:]
        amount_col = "lcy_amount" if ccy == "GHS" else "fcy_amount"
        sql = f"""
            select sum(decode(drcr_ind, 'C', {amount_col}, -{amount_col}))
            from fcubsawa.acvw_all_ac_entries_new a
            where ac_no = :ac_no
              and trn_dt < :bkg_date
              and ac_ccy = :ccy
              and ac_branch = :branch
              and a.ac_branch in (
                  select branch_code from fcubsawa.sttm_branch
                  where regional_office = 'EGH')
        """
        cursor.execute(sql, {
            "ac_no": ac_no, "bkg_date": bkg_date, "ccy": ccy, "branch": branch,
        })
    else:
        ccy = get_acc_ccy(cursor, account)
        amount_col = "lcy_amount" if ccy == "GHS" else "fcy_amount"
        sql = f"""
            select sum(decode(drcr_ind, 'C', {amount_col}, -{amount_col}))
            from fcubsawa.acvw_all_ac_entries_new a
            where ac_no = :account
              and trn_dt < :bkg_date
              and ac_ccy = :ccy
              and a.ac_branch in (
                  select branch_code from fcubsawa.sttm_branch
                  where regional_office = 'EGH')
        """
        cursor.execute(sql, {"account": account, "bkg_date": bkg_date, "ccy": ccy})

    row = cursor.fetchone()
    return (row[0] or 0), ccy


_TXN_SQL_CUSTOMER = """
select
    decode(module, 'DE',
        decode(ac_no,
            '0010014430913501', substr(a.trn_ref_no, 1, 12)||'0000',
            '9989104497167602', substr(a.trn_ref_no, 1, 12)||'0000',
            '9989104497167603', substr(a.trn_ref_no, 1, 12)||'0000',
            a.trn_ref_no),
        a.trn_ref_no),
    a.trn_dt,
    decode(a.drcr_ind, 'D', 'DR', 'CR'),
    nvl(boawa.brg_addl_text(A.TRN_REF_NO, A.EVENT_SR_NO, A.MODULE, A.AC_ENTRY_SR_NO), '')
        ||' |USERID:'||user_id||'|',
    nvl(a.instrument_code, ''),
    a.value_dt,
    a.AC_CCY,
    decode(a.drcr_ind, 'D', decode(ac_ccy, 'GHS', lcy_amount, fcy_amount), 0) paid_out,
    decode(a.drcr_ind, 'C', decode(ac_ccy, 'GHS', lcy_amount, fcy_amount), 0) paid_in,
    user_ref_number,
    a.event_sr_no,
    a.ac_entry_sr_no,
    a.module
from fcubsawa.acvw_all_ac_entries_new a
    left outer join fcubsawa.sttm_cust_account b
        on a.ac_no = b.cust_ac_no and a.ac_ccy = b.ccy
    left outer join fcubsawa.fttb_contract_master c
        on c.contract_ref_no = a.trn_ref_no and a.event_sr_no = c.event_seq_no
where a.module <> 'RE'
    and a.ac_branch in (
        select branch_code from fcubsawa.sttm_branch where regional_office = 'EGH')
    and a.ac_no = :account
    and a.auth_stat = 'A'
    and a.trn_dt between :st_bkg_date and :end_bkg_date
order by a.AC_ENTRY_SR_NO, a.trn_dt
"""

_TXN_SQL_GL = """
select
    decode(module, 'DE',
        decode(ac_no,
            '0010014430913501', substr(a.trn_ref_no, 1, 12)||'0000',
            '9989104497167602', substr(a.trn_ref_no, 1, 12)||'0000',
            '9989104497167603', substr(a.trn_ref_no, 1, 12)||'0000',
            a.trn_ref_no),
        a.trn_ref_no),
    a.trn_dt,
    decode(a.drcr_ind, 'D', 'DR', 'CR'),
    decode(trn_ref_no,
        'H98ZEXA2009802RY', external_ref_no,
        'H98ZEXA2009800ZQ', external_ref_no,
        boawa.get_stmt_acct_ecobank(a.trn_ref_no, a.event_sr_no, a.module, a.ac_entry_sr_no))
        ||' |USERID:'||user_id||'|',
    nvl(a.instrument_code, ''),
    a.value_dt,
    a.AC_CCY,
    decode(a.drcr_ind, 'D', decode(ac_ccy, 'GHS', lcy_amount, fcy_amount), 0) paid_out,
    decode(a.drcr_ind, 'C', decode(ac_ccy, 'GHS', lcy_amount, fcy_amount), 0) paid_in,
    user_ref_number,
    a.event_sr_no,
    a.ac_entry_sr_no,
    a.module
from fcubsawa.acvw_all_ac_entries_new a
    left outer join fcubsawa.sttm_cust_account b
        on a.ac_no = b.cust_ac_no and a.ac_ccy = b.ccy
    left outer join fcubsawa.fttb_contract_master c
        on c.contract_ref_no = a.trn_ref_no and a.event_sr_no = c.event_seq_no
where a.module <> 'RE'
    and a.ac_branch in (
        select branch_code from fcubsawa.sttm_branch where regional_office = 'EGH')
    and a.ac_no = :account
    and a.ac_branch = :brn
    and a.ac_ccy = :ccy
    and a.auth_stat = 'A'
    and a.trn_dt between :st_bkg_date and :end_bkg_date
order by a.AC_ENTRY_SR_NO, a.trn_dt
"""


def get_txns(cursor, account, st_bkg_date, end_bkg_date):
    if account == INTERBRANCH_SENTINEL:
        return []

    if _is_gl(account):
        cursor.execute(_TXN_SQL_GL, {
            "account": account[6:], "brn": account[3:6], "ccy": account[0:3],
            "st_bkg_date": st_bkg_date, "end_bkg_date": end_bkg_date,
        })
    else:
        cursor.execute(_TXN_SQL_CUSTOMER, {
            "account": account,
            "st_bkg_date": st_bkg_date, "end_bkg_date": end_bkg_date,
        })

    txns = []
    for row in cursor:
        row = list(row)
        # Flip DR/CR when the amount came back negative.
        if row[2] == "DR" and row[7] is not None and row[7] < 0:
            row[8] = abs(row[7]); row[7] = 0; row[2] = "CR"
        elif row[2] == "CR" and row[8] is not None and row[8] < 0:
            row[7] = abs(row[8]); row[8] = 0; row[2] = "DR"
        row[3] = (row[3] or "").replace("\r\n", " ").replace("\n", " ")
        txns.append(row)
    return txns


# ---------- statement generation -------------------------------------------

def _fmt_amount(amount):
    return ("%019.4f" % abs(amount)).replace(",", "").replace(".", "")


def _value_date_str(value_date):
    """Value dates occasionally come back with a 2-digit year; assume 20xx."""
    try:
        return value_date.strftime("%Y%m%d")
    except Exception:
        return datetime.date(2000 + value_date.year,
                             value_date.month,
                             value_date.day).strftime("%Y%m%d")


def build_corona_records(account, start_date, end_date, open_bal, close_bal, ccy, txns):
    stmt_date = datetime.datetime.strptime(start_date, "%d-%b-%Y")
    end_dt = datetime.datetime.strptime(end_date, "%d-%b-%Y")

    records = [recordlib.CORONA_HDR()]

    opening = recordlib.CORONA_OPENING_BAL()
    opening.set_field("Account ID", account)
    opening.set_field("Currency ID", ccy)
    opening.set_field("Statement Date", stmt_date.strftime("%Y%m%d"))
    opening.set_field("Statement No", "00001")
    opening.set_field("Statement Page No", "00001")
    opening.set_field("Statement Counter", "0000")
    opening.set_field("Opening Balance Date", stmt_date.strftime("%Y%m%d"))
    opening.set_field("Opening Balance Amount", _fmt_amount(open_bal))
    opening.set_field("Opening Balance Sign", "D" if open_bal < 0 else "C")
    opening.set_field("Record ID", "1")
    opening.set_field("Record ID2", "1")
    records.append(opening)

    parser = NARRATION_PARSERS.get(account, parse_generic)

    for entry in txns:
        sign = entry[2]
        amount = (entry[8] if sign == "CR" else entry[7]) or 0
        if amount == 0:
            continue

        text1, text2, our_ref = parser(entry[3] or "", entry)

        stmt = recordlib.CORONA_STMT_ENTRY()
        stmt.set_field("Account ID", account)
        stmt.set_field("Currency ID", ccy)
        stmt.set_field("Statement Date", stmt_date.strftime("%Y%m%d"))
        stmt.set_field("Statement No", "00001")
        stmt.set_field("Statement Page No", "00002")
        stmt.set_field("Statement Counter", "0000")
        stmt.set_field("Booking Date", entry[1].strftime("%Y%m%d"))
        stmt.set_field("Value Date", _value_date_str(entry[5]))
        stmt.set_field("Our Reference", our_ref if our_ref else entry[0])
        stmt.set_field("Their Reference", entry[9] or "")
        stmt.set_field("Booking Text1", text1)
        stmt.set_field("Booking Text2", text2)
        stmt.set_field("Record ID", "2")
        stmt.set_field("Record ID2", "2")
        stmt.set_field("Amount", _fmt_amount(amount))
        stmt.set_field("Sign", "C" if sign == "CR" else "D")

        # Scrub non-ASCII from the formatted record.
        for i in range(len(stmt)):
            if ord(stmt[i]) > 127:
                stmt[i] = " "

        if str(stmt).strip():
            records.append(stmt)

    closing = recordlib.CORONA_CLOSING_BAL()
    closing.set_field("Account ID", account)
    closing.set_field("Currency ID", ccy)
    closing.set_field("Statement Date", stmt_date.strftime("%Y%m%d"))
    closing.set_field("Statement No", "00001")
    closing.set_field("Statement Page No", "00001")
    closing.set_field("Statement Counter", "0000")
    closing.set_field("Closing Balance Date", end_dt.strftime("%Y%m%d"))
    closing.set_field("Closing Balance Amount", _fmt_amount(close_bal))
    closing.set_field("Closing Balance Sign", "D" if close_bal < 0 else "C")
    closing.set_field("Record ID", "3")
    closing.set_field("Record ID2", "3")
    records.append(closing)

    return records, stmt_date


def gen_corona_stmt(account, start_date, end_date):
    end_dt = datetime.datetime.strptime(end_date, "%d-%b-%Y")
    next_day = end_dt + datetime.timedelta(days=1)

    with connect() as orcl, orcl.cursor() as cursor:
        open_bal, ccy = get_balances(cursor, account, start_date)
        close_bal, _ = get_balances(cursor, account, next_day)
        txns = get_txns(cursor, account, start_date, end_date)

    records, stmt_date = build_corona_records(
        account, start_date, end_date, open_bal, close_bal, ccy, txns)

    out_path = os.path.join(OUTPUT_DIR, f"{account}.txt")
    with open(out_path, "w", encoding="utf-8") as fh:
        for r in records:
            fh.write(str(r) + "\n")

    return {
        "account": account,
        "open_bal": open_bal,
        "close_bal": close_bal,
        "ccy": ccy,
        "stmt_date": stmt_date.strftime("%d-%b-%Y"),
        "txn_count": len(records) - 3,
    }


# ---------- worker + main ---------------------------------------------------

def process_account(job):
    account, start_date, end_date = job
    try:
        result = gen_corona_stmt(account, start_date, end_date)
        log.info("OK    %s  %s  open=%s close=%s txns=%d",
                 result["account"], result["ccy"],
                 result["open_bal"], result["close_bal"], result["txn_count"])
        return result
    except Exception as e:
        log.error("FAIL  %s: %s", account, e)
        return {"account": account, "error": str(e)}


def _read_accounts_file(path):
    accounts, seen = [], set()
    with open(path) as fh:
        for line in fh:
            line = line.strip()
            if not line or line.startswith("#") or line in seen:
                continue
            seen.add(line)
            accounts.append(line)
    return accounts


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        stream=sys.stderr,
    )

    default_date = get_default_dates()

    parser = OptionParser()
    parser.add_option("-s", "--start_date", dest="start_date", default=default_date,
                      help="Starting date, eg 01-OCT-2024 (default: previous working day)")
    parser.add_option("-e", "--end_date", dest="end_date", default=default_date,
                      help="End date, eg 01-OCT-2024 (default: previous working day)")
    parser.add_option("-f", "--input_file", dest="input_file",
                      help="File with one account per line. Lines starting with # are skipped.")
    parser.add_option("-c", "--company", dest="company",
                      help="Run a single account (shortcut, no input file needed).")
    parser.add_option("-w", "--workers", dest="workers", type="int", default=DEFAULT_WORKERS,
                      help=f"Parallel workers (default: {DEFAULT_WORKERS})")
    (options, _) = parser.parse_args()

    if not options.input_file and not options.company:
        parser.error("one of -f or -c is required")

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if options.company:
        accounts = [options.company.strip()]
    else:
        accounts = _read_accounts_file(options.input_file)

    if not accounts:
        log.error("No accounts to process.")
        sys.exit(1)

    log.info("Extracting %d account(s) for %s..%s",
             len(accounts), options.start_date, options.end_date)

    jobs = [(a, options.start_date, options.end_date) for a in accounts]

    if options.workers <= 1 or len(jobs) == 1:
        results = [process_account(j) for j in jobs]
    else:
        with multiprocessing.Pool(processes=min(options.workers, len(jobs))) as pool:
            results = pool.map(process_account, jobs)

    successes = [r for r in results if "error" not in r]
    failures = [r for r in results if "error" in r]

    with open(BALANCE_REPORT, "w", encoding="utf-8") as rep:
        rep.write("Date\tAccount\tCurrency\tOpening Balance\tClosing Balance\n")
        for r in successes:
            rep.write("{stmt_date}\t{account}\t{ccy}\t{open_bal}\t{close_bal}\n".format(**r))

    log.info("Done. %d ok, %d failed. Balance report: %s",
             len(successes), len(failures), BALANCE_REPORT)
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
