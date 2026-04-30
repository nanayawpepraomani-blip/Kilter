#!/usr/bin/env python3
"""
Flexcube xlsx statement extractor (Kilter-ready).

Pulls one-day-or-range statements for one or more accounts from Flexcube
(Oracle) and writes one acc_entries-style .xlsx per account. Output goes
straight into messages/flexcube/ so Kilter's scanner ingests it on the
next sweep — no CORONA step in the middle.

Usage:
    python extract_flex_xlsx.py -f accounts.txt
    python extract_flex_xlsx.py -f accounts.txt -s 01-OCT-2024 -e 01-OCT-2024
    python extract_flex_xlsx.py -c <account_no>

Environment overrides:
    FCUBS_USER, FCUBS_PASSWD, FCUBS_WORKERS, FCUBS_OUTPUT_DIR

Column schema (matches reconcile.load_flexcube's expectations):
    TRN_REF_NO, AC_BRANCH, AC_NO, BOOKING_DATE, VALUE_DATE, TYPE,
    TXN_NARRATIONS, LCY_AMOUNT, ACCT_CCY, MODULE, EXTERNAL_REF_NO, USER_ID
"""

from __future__ import annotations

import datetime
import logging
import multiprocessing
import os
import re
import sys
from optparse import OptionParser

import oracledb
from openpyxl import Workbook


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
BALANCE_REPORT = "flex_balances_report.tsv"

# Special sentinel account with no extractable txns/balances — emits an
# empty xlsx with zero balances.
INTERBRANCH_SENTINEL = "GHS000238400023"

log = logging.getLogger("extract_flex_xlsx")


# ---------- Oracle access ---------------------------------------------------

def connect():
    return oracledb.connect(user=USER, password=PASSWD, dsn=AWA_LIVE_DSN)


def get_default_dates():
    """Previous working day according to FCUBS's branch calendar."""
    with connect() as orcl, orcl.cursor() as cursor:
        cursor.execute(
            "SELECT PREV_WORKING_DAY FROM fcubsawa.STTM_DATES WHERE BRANCH_CODE = 'EGH'"
        )
        d = cursor.fetchone()[0]
    return d.strftime("%d-%b-%Y")


def _is_gl(account: str) -> bool:
    """GL account numbers start with a 3-letter currency prefix (e.g.
    'GHSH98...'). Customer accounts start with digits."""
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


# Two SQLs: one for customer accounts (keyed by AC_NO), one for GL accounts
# (keyed by branch+ccy+ac_no). They return the same 13-column tuple shape.
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
    a.module,
    a.ac_branch,
    a.user_id
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
    a.module,
    a.ac_branch,
    a.user_id
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
        # Flip DR/CR when the amount came back negative (reversals booked to
        # the original side).
        if row[2] == "DR" and row[7] is not None and row[7] < 0:
            row[8] = abs(row[7]); row[7] = 0; row[2] = "CR"
        elif row[2] == "CR" and row[8] is not None and row[8] < 0:
            row[7] = abs(row[8]); row[8] = 0; row[2] = "DR"
        row[3] = (row[3] or "").replace("\r\n", " ").replace("\n", " ")
        txns.append(row)
    return txns


# ---------- xlsx writing ----------------------------------------------------

# Column order matches reconcile.load_flexcube's expected header set. Required
# columns first, then optional ones.
XLSX_HEADERS = [
    "TRN_REF_NO",        # row[0]
    "BOOKING_DATE",      # row[1]
    "TYPE",              # row[2]  'DR'|'CR'
    "TXN_NARRATIONS",    # row[3]  includes |USERID:xxx|
    "VALUE_DATE",        # row[5]
    "LCY_AMOUNT",        # row[7] or row[8]  (positive value, sign is in TYPE)
    "AC_BRANCH",         # row[13]
    "AC_NO",             # passed in; for GL accounts we keep the full prefixed id
    "ACCT_CCY",          # row[6]
    "MODULE",            # row[12]
    "EXTERNAL_REF_NO",   # row[9]  user_ref_number
    "USER_ID",           # row[14]  separate USER_ID column
]


def _txn_to_row(account: str, txn: list) -> list:
    """Convert one Oracle tuple to an xlsx row matching XLSX_HEADERS."""
    amount = txn[7] if txn[2] == "DR" else txn[8]
    # Keep the full account identifier (e.g. 'GHSH98238400190') in AC_NO so
    # the scanner can match it against the registered flex_ac_no as-is.
    ac_no = account
    ac_branch = txn[13] if len(txn) > 13 else ""
    user_id = txn[14] if len(txn) > 14 else ""
    return [
        txn[0],                           # TRN_REF_NO
        txn[1],                           # BOOKING_DATE (datetime)
        txn[2],                           # TYPE
        txn[3],                           # TXN_NARRATIONS
        txn[5],                           # VALUE_DATE (datetime)
        float(amount) if amount is not None else 0.0,
        ac_branch,
        ac_no,
        txn[6],                           # ACCT_CCY
        txn[12],                          # MODULE
        txn[9] or "",                     # EXTERNAL_REF_NO
        user_id,
    ]


def write_xlsx(out_path: str, account: str, txns: list) -> int:
    """Write one acc_entries-style xlsx. Returns rows written (excluding header)."""
    wb = Workbook()
    ws = wb.active
    ws.title = "acc_entries"
    ws.append(XLSX_HEADERS)
    for t in txns:
        ws.append(_txn_to_row(account, t))
    wb.save(out_path)
    return len(txns)


def gen_xlsx_stmt(account: str, start_date: str, end_date: str) -> dict:
    """Extract one account for [start_date, end_date] inclusive and write the
    xlsx. Returns summary for the balance-report row."""
    end_dt = datetime.datetime.strptime(end_date, "%d-%b-%Y")
    next_day = end_dt + datetime.timedelta(days=1)

    with connect() as orcl, orcl.cursor() as cursor:
        open_bal, ccy = get_balances(cursor, account, start_date)
        close_bal, _ = get_balances(cursor, account, next_day)
        txns = get_txns(cursor, account, start_date, end_date)

    out_path = os.path.join(OUTPUT_DIR, f"{account}.xlsx")
    rows_written = write_xlsx(out_path, account, txns)

    return {
        "account": account,
        "open_bal": open_bal,
        "close_bal": close_bal,
        "ccy": ccy,
        "stmt_date": start_date,
        "txn_count": rows_written,
        "out_path": out_path,
    }


# ---------- worker + main ---------------------------------------------------

def process_account(job):
    account, start_date, end_date = job
    try:
        result = gen_xlsx_stmt(account, start_date, end_date)
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
    global OUTPUT_DIR
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
    parser.add_option("-o", "--output_dir", dest="output_dir", default=None,
                      help=f"Output directory for xlsx files (default: {OUTPUT_DIR}; "
                           f"set FCUBS_OUTPUT_DIR to override)")
    (options, _) = parser.parse_args()

    if not options.input_file and not options.company:
        parser.error("one of -f or -c is required")

    if options.output_dir:
        OUTPUT_DIR = options.output_dir
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if options.company:
        accounts = [options.company.strip()]
    else:
        accounts = _read_accounts_file(options.input_file)

    if not accounts:
        log.error("No accounts to process.")
        sys.exit(1)

    log.info("Extracting %d account(s) for %s..%s into %s",
             len(accounts), options.start_date, options.end_date, OUTPUT_DIR)

    jobs = [(a, options.start_date, options.end_date) for a in accounts]

    if options.workers <= 1 or len(jobs) == 1:
        results = [process_account(j) for j in jobs]
    else:
        with multiprocessing.Pool(processes=min(options.workers, len(jobs))) as pool:
            results = pool.map(process_account, jobs)

    successes = [r for r in results if "error" not in r]
    failures = [r for r in results if "error" in r]

    balance_report = os.path.join(OUTPUT_DIR, BALANCE_REPORT)
    with open(balance_report, "w", encoding="utf-8") as rep:
        rep.write("Date\tAccount\tCurrency\tOpening Balance\tClosing Balance\tTxn Count\n")
        for r in successes:
            rep.write("{stmt_date}\t{account}\t{ccy}\t{open_bal}\t{close_bal}\t{txn_count}\n".format(**r))

    log.info("Done. %d ok, %d failed. Balance report: %s",
             len(successes), len(failures), balance_report)
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
