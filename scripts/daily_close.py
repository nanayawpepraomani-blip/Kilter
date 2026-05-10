"""
daily_close.py
==============

Nightly cron: auto-close any session that is still `status='open'` after a
minimum age. Seeds unmatched rows into open_items and stamps closed_at/by.

Run once a day via Task Scheduler / cron. Idempotent — re-running after
everything is already closed is a no-op.

Usage:
    python daily_close.py                # close sessions >= 12h old
    python daily_close.py --min-age-hours 6
    python daily_close.py --dry-run
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone

from db import get_conn, init_db
from open_items import close_session


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--min-age-hours', type=int, default=12,
                    help='Close only sessions older than this many hours.')
    ap.add_argument('--actor', default='cron',
                    help='Actor name recorded in audit_log.')
    ap.add_argument('--dry-run', action='store_true',
                    help='List what would close, but do not change anything.')
    args = ap.parse_args()

    init_db()  # make sure migrations are current before we touch rows

    cutoff_iso = (datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(hours=args.min_age_hours)).isoformat()
    conn = get_conn()
    try:
        candidates = conn.execute(
            "SELECT id, account_label, created_at FROM sessions "
            "WHERE status='open' AND created_at <= ? AND account_id IS NOT NULL "
            "ORDER BY created_at",
            (cutoff_iso,),
        ).fetchall()

        if not candidates:
            print(f"[{datetime.now(timezone.utc).replace(tzinfo=None).isoformat()}] No open sessions older than "
                  f"{args.min_age_hours}h. Nothing to do.")
            return 0

        print(f"[{datetime.now(timezone.utc).replace(tzinfo=None).isoformat()}] {len(candidates)} session(s) "
              f"to close (min-age-hours={args.min_age_hours}):")
        total_seeded = 0
        for row in candidates:
            if args.dry_run:
                print(f"  [DRY-RUN] would close #{row['id']} {row['account_label']} "
                      f"(created {row['created_at']})")
                continue
            try:
                result = close_session(conn, row['id'], args.actor)
                conn.commit()
                total_seeded += result['seeded']
                print(f"  closed #{row['id']} {row['account_label']} "
                      f"- seeded {result['seeded']} open_items")
            except Exception as exc:
                print(f"  ERROR closing #{row['id']}: {exc}", file=sys.stderr)
                conn.rollback()

        if not args.dry_run:
            print(f"Total: closed {len(candidates)} session(s), seeded "
                  f"{total_seeded} open_items.")
    finally:
        conn.close()

    return 0


if __name__ == '__main__':
    sys.exit(main())
