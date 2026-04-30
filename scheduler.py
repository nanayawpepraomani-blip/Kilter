"""
scheduler.py
============

In-process job runner — the "runs itself" backbone of Kilter. A single
daemon thread polls scheduled_jobs every 30s and invokes anything due.
No APScheduler / cron dependency — the whole thing is stdlib threading +
SQLite, so IT has one less moving part to audit.

Design decisions:
  * One worker thread. Jobs are I/O-bound (file scan, SMTP, HTTP webhooks)
    and take seconds-not-minutes; running them serially keeps ordering
    predictable and avoids the "two scan runs racing on the same folder"
    class of bug.
  * Each job run is recorded in job_runs + audit_log. The latter matters
    for the pitch: auditors can see that yes, the scan actually ran at
    06:15, and yes, it ingested 17 new files.
  * The daemon thread catches every exception per job; one job blowing up
    never takes the whole scheduler down.
  * Jobs are registered in a module-level dict at import time. Adding a
    new job type = one function + one entry in JOBS.

Public:
    start()    — called from FastAPI startup event
    stop()     — called from FastAPI shutdown event
    run_now(conn, job_id, actor) — one-shot invocation for the UI
    compute_next_run(job) — used by the admin page for "next run at"
"""

from __future__ import annotations

import json
import threading
import time
import traceback
from datetime import datetime, timedelta

from db import get_conn


POLL_SECONDS = 30                 # how often the daemon wakes up
RUN_HISTORY_KEEP = 20             # per-job; older runs are pruned

_thread: threading.Thread | None = None
_stop_event = threading.Event()


# ---------------------------------------------------------------------------
# Job implementations. Each takes (conn, params_dict) and returns a short
# human-readable summary string. Exceptions propagate — the daemon wraps
# them and stores the traceback on the run record.
# ---------------------------------------------------------------------------

def _job_scan(conn, params: dict) -> str:
    # scanner.scan() manages its own DB connections; it takes a user label,
    # not a conn. Passing the conn here was the cause of the initial "Error
    # binding parameter 2" crash when the daemon first fired.
    from scanner import scan
    report = scan(user='system_scheduler')
    created = len(report.sessions_created)
    return (f"Created {created} session(s); "
            f"processed={report.counts.get('ingested', 0)}, "
            f"duplicate={report.counts.get('duplicate', 0)}, "
            f"unregistered={report.counts.get('unregistered', 0)}, "
            f"no_partner={report.counts.get('no_partner', 0)}, "
            f"mismatch={report.counts.get('mismatch', 0)}, "
            f"error={report.counts.get('error', 0)}")


def _job_daily_close(conn, params: dict) -> str:
    """Close sessions older than min_age_hours that still have pending rows.
    Mirrors what daily_close.py does from the CLI, but inline so it lives
    in the scheduler's run history."""
    from open_items import close_session
    min_age_hours = int((params or {}).get('min_age_hours', 12))
    cutoff = (datetime.utcnow() - timedelta(hours=min_age_hours)).isoformat()
    rows = conn.execute(
        "SELECT id FROM sessions WHERE status='open' "
        "AND account_id IS NOT NULL "
        "AND created_at <= ?",
        (cutoff,),
    ).fetchall()
    closed = 0
    errors = 0
    for r in rows:
        try:
            close_session(conn, r['id'], 'system_scheduler')
            closed += 1
        except Exception:
            errors += 1
    conn.commit()
    return f"Closed {closed} session(s) older than {min_age_hours}h; errors={errors}"


def _job_sla_check(conn, params: dict) -> str:
    from sla import run_check
    results = run_check(conn)
    total_items = sum(r.get('items', 0) for r in results.values())
    total_sent  = sum(r.get('sent', 0)  for r in results.values())
    errors      = sum(1 for r in results.values() if r.get('error'))
    return (f"Channels evaluated: {len(results)}; "
            f"aged items seen: {total_items}; "
            f"alerts dispatched: {total_sent}; errors: {errors}")


def _job_daily_breaks_report(conn, params: dict) -> str:
    """Record a daily marker that the report is ready for download. The
    actual xlsx is generated on-demand by GET /reports/daily-breaks — this
    job exists to put the daily rhythm into the scheduler history (ops can
    point at the audit log and say 'the report was ready at 07:00')."""
    from datetime import date as _date
    as_of = (params or {}).get('as_of') or _date.today().isoformat()
    count = conn.execute(
        "SELECT COUNT(*) FROM open_items WHERE status='open' "
        "AND DATE(opened_at) <= ?", (as_of,)
    ).fetchone()[0]
    return (f"Report for {as_of} is ready: {count} open item(s). "
            f"Download at /reports/daily-breaks?as_of={as_of}&format=xlsx")


def _job_flex_extract(conn, params: dict) -> str:
    """Pull daily Flexcube statements for every registered account via the
    Oracle-side extractor, dropping xlsx files directly into the scanner's
    intake folder. The next scan job then ingests them.

    Degrades gracefully when oracledb isn't installed or credentials aren't
    set — in that state it emits a clear diagnostic instead of crashing, so
    the pitch demo still works on a machine with no Oracle client.

    Params (all optional):
        {"start_date": "01-OCT-2026", "end_date": "01-OCT-2026",
         "workers": 4, "limit": null}
    If start/end are omitted the extractor uses FCUBS's branch calendar
    (previous working day)."""
    import os
    from pathlib import Path

    # Soft-check for the driver; give ops actionable feedback.
    try:
        import oracledb  # noqa: F401
    except ImportError:
        return ("oracledb driver not installed — run `pip install oracledb` "
                "on this host, then enable this job. The scheduler will "
                "pick it up on the next tick.")

    # Env-var creds are the cleanest boundary with IT-managed secrets.
    if not os.environ.get('FCUBS_USER') or not os.environ.get('FCUBS_PASSWD'):
        return ("FCUBS_USER / FCUBS_PASSWD env vars not set on this host. "
                "Set them (or point them at a vault-mounted path) and retry.")

    # Route output directly into scanner intake so the next scan consumes it.
    intake = Path(__file__).resolve().parent / 'messages' / 'flexcube'
    intake.mkdir(parents=True, exist_ok=True)
    os.environ.setdefault('FCUBS_OUTPUT_DIR', str(intake))

    # Grab the registered flex account numbers.
    rows = conn.execute(
        "SELECT flex_ac_no FROM accounts WHERE active=1 "
        "AND flex_ac_no IS NOT NULL AND flex_ac_no != '' "
        "ORDER BY flex_ac_no"
    ).fetchall()
    accounts = [r[0] for r in rows]
    if not accounts:
        return 'no registered accounts to extract — register at least one cash account first'
    limit = (params or {}).get('limit')
    if limit:
        accounts = accounts[:int(limit)]

    p = params or {}
    start_date = p.get('start_date')
    end_date   = p.get('end_date') or start_date
    workers    = int(p.get('workers') or os.environ.get('FCUBS_WORKERS', '4'))

    try:
        from extract_flex_xlsx import get_default_dates, process_account
    except ImportError as exc:
        return f"extract_flex_xlsx import failed: {exc}"

    if not start_date:
        try:
            start_date = end_date = get_default_dates()
        except Exception as exc:
            return f"could not resolve default date via FCUBS calendar: {exc}"

    # Sequential in-process execution — the extractor itself supports
    # multiprocessing but the scheduler is single-threaded by design. For
    # volumes larger than "all registered accounts" (~30 today), switch
    # to the multiprocessing path by invoking the CLI.
    ok = 0; errors = 0
    err_msgs = []
    for acc in accounts:
        # process_account takes a (account, start_date, end_date) tuple.
        result = process_account((acc, start_date, end_date))
        if isinstance(result, dict) and result.get('error'):
            errors += 1
            if len(err_msgs) < 3:
                err_msgs.append(f"{acc}: {result['error']}")
        else:
            ok += 1
    msg = (f"Extracted {ok}/{len(accounts)} account(s) for {start_date}"
           f" → intake {intake}. Errors: {errors}")
    if err_msgs:
        msg += " — " + "; ".join(err_msgs)
    return msg


# Lookup table used by dispatch(). Add new job types here.
JOBS = {
    'scan':                 _job_scan,
    'daily_close':          _job_daily_close,
    'sla_check':            _job_sla_check,
    'daily_breaks_report':  _job_daily_breaks_report,
    'flex_extract':         _job_flex_extract,
}


# ---------------------------------------------------------------------------
# Scheduling math — "should this job fire right now?"
# ---------------------------------------------------------------------------

def compute_next_run(job: dict, after: datetime | None = None) -> datetime | None:
    """Given a job row, compute when it should next run after `after`
    (default: now). Returns None when the job is disabled."""
    if not job['enabled']:
        return None
    base = after or datetime.utcnow()
    if job['schedule_kind'] == 'interval':
        mins = int(job['interval_minutes'] or 0)
        if mins <= 0:
            return None
        if job['last_run_at']:
            try:
                last = datetime.fromisoformat(job['last_run_at'])
                candidate = last + timedelta(minutes=mins)
                return max(candidate, base)
            except Exception:
                return base
        return base
    if job['schedule_kind'] == 'daily_at':
        hhmm = (job['daily_at_utc'] or '').strip()
        try:
            hh, mm = hhmm.split(':')
            hh = int(hh); mm = int(mm)
        except Exception:
            return None
        today = base.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if today > base:
            return today
        return today + timedelta(days=1)
    return None


def _is_due(job: dict, now: datetime) -> bool:
    nxt = compute_next_run(job)
    if nxt is None:
        return False
    # Interval jobs: ready if the last_run + interval <= now.
    if job['schedule_kind'] == 'interval':
        if not job['last_run_at']:
            return True
        try:
            last = datetime.fromisoformat(job['last_run_at'])
            return now >= last + timedelta(minutes=int(job['interval_minutes'] or 0))
        except Exception:
            return True
    # Daily: ready if today's target time has passed AND it hasn't already
    # run today. Use the ISO date of last_run to check.
    if job['schedule_kind'] == 'daily_at':
        hhmm = (job['daily_at_utc'] or '').strip()
        try:
            hh, mm = hhmm.split(':'); hh = int(hh); mm = int(mm)
        except Exception:
            return False
        today_target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
        if now < today_target:
            return False
        if job['last_run_at']:
            try:
                last = datetime.fromisoformat(job['last_run_at'])
                if last.date() == now.date():
                    return False
            except Exception:
                pass
        return True
    return False


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------

def _execute(conn, job: dict, actor: str = 'system_scheduler') -> dict:
    """Run one job, record the result in job_runs + update last_run_* on
    scheduled_jobs + write an audit_log entry. Never raises — exceptions
    are captured into the output field."""
    params = {}
    if job.get('params_json'):
        try:
            params = json.loads(job['params_json'])
        except Exception:
            params = {}

    handler = JOBS.get(job['job_type'])
    started_at = datetime.utcnow().isoformat()
    run_cur = conn.execute(
        "INSERT INTO job_runs (job_id, started_at) VALUES (?, ?)",
        (job['id'], started_at),
    )
    run_id = run_cur.lastrowid
    conn.commit()

    status, output = 'ok', ''
    t0 = time.monotonic()
    try:
        if handler is None:
            status, output = 'error', f"unknown job_type '{job['job_type']}'"
        else:
            output = handler(conn, params) or 'ok'
    except Exception as exc:
        status = 'error'
        tb = traceback.format_exc()
        output = f"{type(exc).__name__}: {exc}\n{tb[-1500:]}"  # tail-truncate
    duration_ms = int((time.monotonic() - t0) * 1000)
    ended_at = datetime.utcnow().isoformat()

    conn.execute(
        "UPDATE job_runs SET ended_at=?, status=?, output=?, duration_ms=? WHERE id=?",
        (ended_at, status, output, duration_ms, run_id),
    )
    next_run = compute_next_run({**job, 'last_run_at': ended_at})
    conn.execute(
        "UPDATE scheduled_jobs SET last_run_at=?, last_run_status=?, "
        "last_run_output=?, last_run_ms=?, next_run_at=? WHERE id=?",
        (ended_at, status, output[:2000], duration_ms,
         next_run.isoformat() if next_run else None, job['id']),
    )
    conn.execute(
        "INSERT INTO audit_log (session_id, action, actor, timestamp, details) "
        "VALUES (NULL, 'scheduler_run', ?, ?, ?)",
        (actor, ended_at, json.dumps({
            'job_id': job['id'], 'job_name': job['name'],
            'job_type': job['job_type'], 'status': status,
            'duration_ms': duration_ms,
            'output': output[:500]})),
    )
    _prune_history(conn, job['id'])
    conn.commit()
    return {'status': status, 'output': output, 'duration_ms': duration_ms,
            'run_id': run_id}


def _prune_history(conn, job_id: int, keep: int = RUN_HISTORY_KEEP) -> None:
    conn.execute(
        "DELETE FROM job_runs WHERE job_id=? AND id NOT IN "
        "(SELECT id FROM job_runs WHERE job_id=? ORDER BY id DESC LIMIT ?)",
        (job_id, job_id, keep),
    )


def run_now(conn, job_id: int, actor: str) -> dict:
    """One-shot manual invocation from the admin UI. Runs on the caller's
    thread (the web request) to give immediate feedback."""
    row = conn.execute(
        "SELECT * FROM scheduled_jobs WHERE id=?", (job_id,)
    ).fetchone()
    if row is None:
        raise ValueError(f"job {job_id} not found")
    return _execute(conn, dict(row), actor=actor)


# ---------------------------------------------------------------------------
# Daemon thread
# ---------------------------------------------------------------------------

def _loop() -> None:
    """Main scheduler loop. Wakes every POLL_SECONDS; on each tick:
       * opens a fresh SQLite connection (thread affinity)
       * scans scheduled_jobs for enabled + due rows
       * runs them serially
    One failing job never kills the loop."""
    while not _stop_event.is_set():
        try:
            conn = get_conn()
            try:
                rows = conn.execute(
                    "SELECT * FROM scheduled_jobs WHERE enabled=1"
                ).fetchall()
                now = datetime.utcnow()
                for r in rows:
                    if _stop_event.is_set():
                        break
                    job = dict(r)
                    if _is_due(job, now):
                        _execute(conn, job)
            finally:
                conn.close()
        except Exception:
            # Log and keep going — scheduler dying is worse than one bad tick.
            try:
                conn = get_conn()
                conn.execute(
                    "INSERT INTO audit_log (session_id, action, actor, timestamp, details) "
                    "VALUES (NULL, 'scheduler_tick_error', 'system', ?, ?)",
                    (datetime.utcnow().isoformat(),
                     json.dumps({'traceback': traceback.format_exc()[-1500:]})),
                )
                conn.commit()
                conn.close()
            except Exception:
                pass
        # Sleep in small slices so stop() is responsive.
        for _ in range(POLL_SECONDS * 2):
            if _stop_event.is_set():
                return
            time.sleep(0.5)


def start() -> None:
    """Kick off the daemon thread. Idempotent — a second call during
    uvicorn --reload is a no-op."""
    global _thread
    if _thread is not None and _thread.is_alive():
        return
    _stop_event.clear()
    _thread = threading.Thread(target=_loop, name='kilter-scheduler',
                                daemon=True)
    _thread.start()


def stop() -> None:
    _stop_event.set()


def is_running() -> bool:
    return _thread is not None and _thread.is_alive()
