"""
open_items.py
=============

The rolling reconciliation ledger. Flex and SWIFT entries that finish a
session unmatched become open_items keyed by *account* (not session) so the
next session can clear them when their counterpart arrives.

This module owns three things:

    seed_open_items_for_session(conn, session_id) — called at the tail of
        ingest_pair(). Walks unmatched rows on both sides, creates an
        open_items row for each, auto-categorises via
        auto_categorization_rules, and records an audit entry.

    carry_forward_match(conn, session_id) — also called at ingest. Before
        seeding, it tries to clear *prior* open items against the current
        session's rows using the same tolerance-aware logic the engine
        uses. Clearing an open_item produces a new assignment with
        source='auto_carry' and open_item_id set, so the UI renders
        these like any other match but they're traceable.

    clear_open_item_manually(conn, open_item_id, session_id, row_number,
                              user, note) — what the force-match UI calls
        when an analyst hand-picks the counterpart from a new session.

A Note on semantics: open_items.sign is stored exactly as it came in —
SWIFT 'C'/'D' for swift-side items, Flex 'CR'/'DR' for flex-side. The
match logic uses MIRROR_SIGN from recon_engine so the same rule applies.
"""

from __future__ import annotations

import json
from datetime import datetime

from recon_engine import MIRROR_SIGN, Tolerance, normalize_ref


def load_tolerance(conn, account_id: int | None) -> Tolerance:
    """Per-account override, or defaults when there's no row (or no account)."""
    if account_id is None:
        return Tolerance()
    row = conn.execute(
        "SELECT amount_tol_abs, amount_tol_pct, date_tol_days, min_ref_len "
        "FROM tolerance_rules WHERE account_id=?",
        (account_id,),
    ).fetchone()
    if row is None:
        return Tolerance()
    return Tolerance(
        amount_tol_abs=row['amount_tol_abs'],
        amount_tol_pct=row['amount_tol_pct'],
        date_tol_days=row['date_tol_days'],
        min_ref_len=row['min_ref_len'],
    )


# --- carry-forward matching ------------------------------------------------

def carry_forward_match(conn, session_id: int, tol: Tolerance) -> dict:
    """Try to clear prior open_items of this session's account against the
    current session's still-unassigned SWIFT/Flex rows.

    Runs once, immediately after the engine has produced assignments for
    the current session but before seed_open_items_for_session. Any match
    here becomes an assignment with source='auto_carry' and open_item_id
    set — the UI treats it like any other confirmed match, but the trail
    shows where the counterpart came from.

    Returns counts so the ingest summary can surface them."""
    sess = conn.execute(
        "SELECT id, account_id FROM sessions WHERE id=?", (session_id,),
    ).fetchone()
    if sess is None or sess['account_id'] is None:
        return {'cleared_against_swift': 0, 'cleared_against_flex': 0}

    account_id = sess['account_id']
    open_rows = conn.execute(
        "SELECT * FROM open_items WHERE account_id=? AND status='open'",
        (account_id,),
    ).fetchall()
    if not open_rows:
        return {'cleared_against_swift': 0, 'cleared_against_flex': 0}

    # Current session's unassigned rows.
    assigned_swift = {r[0] for r in conn.execute(
        "SELECT swift_row FROM assignments WHERE session_id=?", (session_id,),
    ).fetchall()}
    assigned_flex = {r[0] for r in conn.execute(
        "SELECT flex_row FROM assignments WHERE session_id=?", (session_id,),
    ).fetchall()}
    swift_free = [dict(r) for r in conn.execute(
        "SELECT * FROM swift_txns WHERE session_id=?", (session_id,)
    ).fetchall() if r['row_number'] not in assigned_swift]
    flex_free = [dict(r) for r in conn.execute(
        "SELECT * FROM flex_txns WHERE session_id=?", (session_id,)
    ).fetchall() if r['row_number'] not in assigned_flex]

    cleared_swift = 0
    cleared_flex = 0
    now = datetime.utcnow().isoformat()

    for oi in open_rows:
        oi_dict = dict(oi)
        if oi_dict['source_side'] == 'swift':
            # Prior SWIFT entry — look for a Flex counterpart in this session.
            target_type = MIRROR_SIGN.get(oi_dict['sign'])
            if target_type is None:
                continue
            picked = _pick_flex_match(oi_dict, flex_free, target_type, tol)
            if picked is None:
                continue
            flex_free.remove(picked)
            _record_carry_assignment(
                conn, session_id,
                swift_row=None, flex_row=picked['row_number'],
                open_item_id=oi_dict['id'],
                oi_side='swift', oi_src_session=oi_dict['src_session_id'],
                oi_row=oi_dict['src_row_number'],
                now=now,
            )
            cleared_swift += 1
        else:
            # Prior Flex entry — look for a SWIFT counterpart in this session.
            target_sign = _flex_to_swift_sign(oi_dict['sign'])
            if target_sign is None:
                continue
            picked = _pick_swift_match(oi_dict, swift_free, target_sign, tol)
            if picked is None:
                continue
            swift_free.remove(picked)
            _record_carry_assignment(
                conn, session_id,
                swift_row=picked['row_number'], flex_row=None,
                open_item_id=oi_dict['id'],
                oi_side='flex', oi_src_session=oi_dict['src_session_id'],
                oi_row=oi_dict['src_row_number'],
                now=now,
            )
            cleared_flex += 1

    if cleared_swift or cleared_flex:
        conn.execute(
            "UPDATE sessions SET open_items_cleared = open_items_cleared + ? WHERE id=?",
            (cleared_swift + cleared_flex, session_id),
        )

    return {
        'cleared_against_swift': cleared_swift,
        'cleared_against_flex': cleared_flex,
    }


def _pick_flex_match(oi: dict, flex_candidates: list[dict], target_type: str,
                     tol: Tolerance) -> dict | None:
    """A prior SWIFT open item is looking for a current-session Flex row.
    Same ranking logic as the engine: ref-in-narration first, then date-
    tight amount match, then ±N-day. First passable hit wins."""
    raw_ref = (oi.get('ref') or '').strip().upper()
    oi_ref_norm = normalize_ref(oi.get('ref'))
    oi_has_ref = len(raw_ref) >= tol.min_ref_len
    for f in flex_candidates:
        if f['type'] != target_type:
            continue
        if not tol.amount_ok(oi['amount'], f['amount']):
            continue
        if oi_has_ref:
            narr = (f.get('narration') or '')
            ext  = (f.get('external_ref') or '')
            trn  = (f.get('trn_ref') or '')
            hay_raw = (narr + ' ' + ext + ' ' + trn).upper()
            if raw_ref in hay_raw:
                return f
            if len(oi_ref_norm) >= 3:
                hay_norm = normalize_ref(narr) + ' ' + normalize_ref(ext) + ' ' + normalize_ref(trn)
                if oi_ref_norm in hay_norm:
                    return f
    # Second pass without the ref gate.
    for f in flex_candidates:
        if f['type'] != target_type:
            continue
        if not tol.amount_ok(oi['amount'], f['amount']):
            continue
        if _date_close(oi.get('value_date'), f.get('value_date'), tol.date_tol_days):
            return f
    return None


def _pick_swift_match(oi: dict, swift_candidates: list[dict], target_sign: str,
                      tol: Tolerance) -> dict | None:
    """Mirror of _pick_flex_match for prior Flex open items."""
    raw_ref = (oi.get('ref') or '').strip().upper()
    oi_ref_norm = normalize_ref(oi.get('ref'))
    for s in swift_candidates:
        if s['sign'] != target_sign:
            continue
        if not tol.amount_ok(oi['amount'], s['amount']):
            continue
        our = (s.get('our_ref') or '')
        their = (s.get('their_ref') or '')
        s_ref_raw = (our + ' ' + their).upper()
        if raw_ref and raw_ref in s_ref_raw:
            return s
        if len(oi_ref_norm) >= 3:
            s_ref_norm = normalize_ref(our) + ' ' + normalize_ref(their)
            if oi_ref_norm in s_ref_norm:
                return s
    for s in swift_candidates:
        if s['sign'] != target_sign:
            continue
        if not tol.amount_ok(oi['amount'], s['amount']):
            continue
        if _date_close(oi.get('value_date'), s.get('value_date'), tol.date_tol_days):
            return s
    return None


def _record_carry_assignment(conn, session_id: int, *, swift_row: int | None,
                             flex_row: int | None, open_item_id: int,
                             oi_side: str, oi_src_session: int, oi_row: int,
                             now: str) -> None:
    """When carry-forward clears an open_item, we still need an assignments
    row so the current session's UI can show the match. We point swift_row
    and flex_row to actual rows in this session and the *other* side to the
    prior session's row — using row_number=-oi_row as a marker would be
    confusing, so we store the prior row in assignments.reason + flag the
    half by leaving one side NULL via synthetic -1 is dangerous because of
    the FK-less PRIMARY KEY on (session_id, row_number).

    Pragmatic compromise: the half that came from the prior session is
    flagged in reason; the half in this session is the real row_number.
    For display, the queue/register queries already JOIN by (session_id,
    row_number), so we put the prior-side row as ''-'' by using row_number
    of 0 (which never exists in swift_txns/flex_txns). The UI treats
    source='auto_carry' specially and fetches the prior row via open_item_id."""
    if swift_row is None:
        # Prior item was SWIFT; we matched Flex row `flex_row` this session.
        swift_row = 0
    if flex_row is None:
        flex_row = 0
    reason = (f"auto-carry: cleared prior {oi_side}-side open item "
              f"(session {oi_src_session}, row {oi_row})")
    cur = conn.execute(
        "INSERT INTO assignments (session_id, swift_row, flex_row, tier, reason, "
        "amount_diff, status, decided_by, decided_at, source, open_item_id) "
        "VALUES (?,?,?,?,?, 0.0, 'confirmed', 'system_carry', ?, 'auto_carry', ?)",
        (session_id, swift_row, flex_row, 0, reason, now, open_item_id),
    )
    conn.execute(
        "UPDATE open_items SET status='cleared', cleared_at=?, cleared_by=?, "
        "cleared_via='auto_carry', cleared_session_id=?, cleared_assignment_id=? "
        "WHERE id=?",
        (now, 'system_carry', session_id, cur.lastrowid, open_item_id),
    )
    conn.execute(
        "INSERT INTO audit_log (session_id, action, actor, timestamp, details) "
        "VALUES (?, 'open_item_auto_cleared', 'system', ?, ?)",
        (session_id, now, json.dumps({
            'open_item_id': open_item_id,
            'source_side': oi_side,
            'src_session_id': oi_src_session,
            'assignment_id': cur.lastrowid,
        })),
    )


def _flex_to_swift_sign(flex_sign: str) -> str | None:
    if flex_sign == 'CR':
        return 'D'
    if flex_sign == 'DR':
        return 'C'
    return None


def _date_close(d1, d2, tol_days: int) -> bool:
    try:
        a = int(d1 or 0); b = int(d2 or 0)
        if not a or not b:
            return False
        if a == b:
            return True
        from datetime import datetime as _dt
        da = _dt.strptime(str(a), '%Y%m%d')
        db = _dt.strptime(str(b), '%Y%m%d')
        return abs((da - db).days) <= tol_days
    except (ValueError, TypeError):
        return False


# --- seeding ---------------------------------------------------------------

def seed_open_items_for_session(conn, session_id: int) -> int:
    """After carry-forward + engine assignments, any SWIFT/Flex row still
    without an assignment becomes an open_item. Returns the count created.

    Requires the session to have an account_id — unregistered sessions
    can't feed the rolling ledger (there's no account to key against).
    For those, we skip silently."""
    sess = conn.execute(
        "SELECT id, account_id FROM sessions WHERE id=?", (session_id,),
    ).fetchone()
    if sess is None or sess['account_id'] is None:
        return 0
    account_id = sess['account_id']

    assigned_swift = {r[0] for r in conn.execute(
        "SELECT swift_row FROM assignments WHERE session_id=? AND swift_row > 0",
        (session_id,),
    ).fetchall()}
    assigned_flex = {r[0] for r in conn.execute(
        "SELECT flex_row FROM assignments WHERE session_id=? AND flex_row > 0",
        (session_id,),
    ).fetchall()}

    swift_rows = [dict(r) for r in conn.execute(
        "SELECT * FROM swift_txns WHERE session_id=?", (session_id,)
    ).fetchall() if r['row_number'] not in assigned_swift]
    flex_rows = [dict(r) for r in conn.execute(
        "SELECT * FROM flex_txns WHERE session_id=?", (session_id,)
    ).fetchall() if r['row_number'] not in assigned_flex]

    rules = _load_active_rules(conn)
    grouping_rules = _load_active_grouping_rules(conn)
    now = datetime.utcnow().isoformat()
    seeded = 0

    for s in swift_rows:
        narr = ((s.get('booking_text_1') or '') + ' ' + (s.get('booking_text_2') or '')).strip()
        ref = s.get('our_ref') or ''
        cat, rule_id = _apply_auto_category('swift', narr, s.get('sign'),
                                             s.get('amount'), rules)
        grp, grp_rule_id = _apply_auto_grouping('swift', narr, ref, s.get('sign'),
                                                 s.get('amount'), grouping_rules)
        try:
            conn.execute(
                "INSERT INTO open_items (account_id, source_side, src_session_id, "
                "src_row_number, value_date, amount, sign, ref, narration, category, "
                "category_source, category_rule_id, functional_group, grouping_source, "
                "grouping_rule_id, status, opened_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, 'open', ?)",
                (account_id, 'swift', session_id, s['row_number'],
                 s.get('value_date'), s.get('amount'), s.get('sign'),
                 s.get('our_ref'), narr,
                 cat, ('auto_rule' if rule_id else None), rule_id,
                 grp, ('auto_rule' if grp_rule_id else None), grp_rule_id,
                 now),
            )
            seeded += 1
        except Exception:
            # UNIQUE constraint — already seeded by a prior retry. Skip.
            continue

    for f in flex_rows:
        narr = f.get('narration') or ''
        ref = f.get('trn_ref') or ''
        cat, rule_id = _apply_auto_category('flex', narr, f.get('type'),
                                             f.get('amount'), rules)
        grp, grp_rule_id = _apply_auto_grouping('flex', narr, ref, f.get('type'),
                                                 f.get('amount'), grouping_rules)
        try:
            conn.execute(
                "INSERT INTO open_items (account_id, source_side, src_session_id, "
                "src_row_number, value_date, amount, sign, ref, narration, category, "
                "category_source, category_rule_id, functional_group, grouping_source, "
                "grouping_rule_id, status, opened_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, 'open', ?)",
                (account_id, 'flex', session_id, f['row_number'],
                 f.get('value_date'), f.get('amount'), f.get('type'),
                 f.get('trn_ref'), narr,
                 cat, ('auto_rule' if rule_id else None), rule_id,
                 grp, ('auto_rule' if grp_rule_id else None), grp_rule_id,
                 now),
            )
            seeded += 1
        except Exception:
            continue

    if seeded:
        conn.execute(
            "UPDATE sessions SET open_items_seeded = open_items_seeded + ? WHERE id=?",
            (seeded, session_id),
        )
        conn.execute(
            "INSERT INTO audit_log (session_id, action, actor, timestamp, details) "
            "VALUES (?, 'open_items_seeded', 'system', ?, ?)",
            (session_id, now, json.dumps({'seeded': seeded, 'account_id': account_id})),
        )

    return seeded


def _load_active_rules(conn) -> list[dict]:
    return [dict(r) for r in conn.execute(
        "SELECT * FROM auto_categorization_rules WHERE active=1 "
        "ORDER BY priority, id"
    ).fetchall()]


def _apply_auto_category(side: str, narration: str, type_or_sign: str | None,
                         amount: float | None, rules: list[dict]) -> tuple[str, int | None]:
    """First matching rule wins. Returns (category, rule_id_or_None). When
    no rule matches, defaults to 'uncategorized' with rule_id=None."""
    nar = (narration or '').upper()
    for r in rules:
        if r['side'] and r['side'] != side:
            continue
        if r['narration_contains']:
            if r['narration_contains'].upper() not in nar:
                continue
        if r['type_equals'] and (type_or_sign or '').upper() != r['type_equals'].upper():
            continue
        if r['amount_min'] is not None and (amount or 0) < r['amount_min']:
            continue
        if r['amount_max'] is not None and (amount or 0) > r['amount_max']:
            continue
        return r['category'], r['id']
    return 'uncategorized', None


def _load_active_grouping_rules(conn) -> list[dict]:
    return [dict(r) for r in conn.execute(
        "SELECT * FROM auto_grouping_rules WHERE active=1 "
        "ORDER BY priority, id"
    ).fetchall()]


def _apply_auto_grouping(side: str, narration: str, ref: str,
                          type_or_sign: str | None, amount: float | None,
                          rules: list[dict]) -> tuple[str, int | None]:
    """First matching rule wins. Returns (functional_group, rule_id_or_None).
    Matches narration_contains against narration+booking texts, and
    ref_contains against our_ref/trn_ref. A rule with neither narration nor
    ref filter acts as a catch-all at its priority level — use it for the
    default bucket."""
    from db import FUNCTIONAL_GROUP_DEFAULT
    nar = (narration or '').upper()
    rf = (ref or '').upper()
    for r in rules:
        if r['side'] and r['side'] != side:
            continue
        if r['narration_contains']:
            if r['narration_contains'].upper() not in nar:
                continue
        if r['ref_contains']:
            if r['ref_contains'].upper() not in rf:
                continue
        if r['type_equals'] and (type_or_sign or '').upper() != r['type_equals'].upper():
            continue
        if r['amount_min'] is not None and (amount or 0) < r['amount_min']:
            continue
        if r['amount_max'] is not None and (amount or 0) > r['amount_max']:
            continue
        return r['functional_group'], r['id']
    return FUNCTIONAL_GROUP_DEFAULT, None


def close_session(conn, session_id: int, actor: str) -> dict:
    """Mark a session closed, seeding any still-unmatched rows into open_items.
    Idempotent — re-closing a closed session is a no-op. Returns a summary
    dict for the caller / audit trail."""
    sess = conn.execute(
        "SELECT id, status, closed_at, account_id FROM sessions WHERE id=?",
        (session_id,),
    ).fetchone()
    if sess is None:
        raise ValueError(f"session {session_id} not found")
    if sess['status'] == 'closed':
        return {'session_id': session_id, 'already_closed': True, 'seeded': 0}
    if sess['account_id'] is None:
        raise ValueError(f"session {session_id} has no registered account — "
                         "cannot seed open items")

    seeded = seed_open_items_for_session(conn, session_id)
    now = datetime.utcnow().isoformat()
    conn.execute(
        "UPDATE sessions SET status='closed', closed_at=?, closed_by=?, "
        "functional_groups_applied=1 WHERE id=?",
        (now, actor, session_id),
    )
    conn.execute(
        "INSERT INTO audit_log (session_id, action, actor, timestamp, details) "
        "VALUES (?, 'session_closed', ?, ?, ?)",
        (session_id, actor, now, json.dumps({'seeded': seeded})),
    )
    return {'session_id': session_id, 'already_closed': False, 'seeded': seeded}


def apply_grouping_to_existing(conn, account_id: int | None = None) -> int:
    """Re-score functional_group for open_items that don't have one yet. Used
    after rolling out the grouping feature to backfill existing rows, and
    any time new rules are added. Returns the count updated."""
    rules = _load_active_grouping_rules(conn)
    where = "WHERE functional_group IS NULL"
    params: list = []
    if account_id is not None:
        where += " AND account_id=?"
        params.append(account_id)
    rows = conn.execute(
        f"SELECT id, source_side, narration, ref, sign, amount FROM open_items {where}",
        params,
    ).fetchall()
    updated = 0
    for r in rows:
        grp, rule_id = _apply_auto_grouping(
            r['source_side'], r['narration'] or '', r['ref'] or '',
            r['sign'], r['amount'], rules,
        )
        conn.execute(
            "UPDATE open_items SET functional_group=?, grouping_source=?, "
            "grouping_rule_id=? WHERE id=?",
            (grp, 'auto_rule' if rule_id else 'auto_default', rule_id, r['id']),
        )
        updated += 1
    return updated


# --- manual clearing -------------------------------------------------------

def clear_open_item_manually(conn, open_item_id: int, session_id: int,
                             counterpart_row: int, counterpart_side: str,
                             user: str, note: str | None) -> int:
    """Analyst hand-links an open_item to a row in a current session.
    Creates a confirmed assignment with source='manual' + open_item_id,
    marks the open_item as cleared, returns the new assignment id.

    counterpart_side is 'swift' or 'flex' — it's the side of the row in
    the current session being paired with the (opposite-side) open_item."""
    now = datetime.utcnow().isoformat()
    oi = conn.execute("SELECT * FROM open_items WHERE id=?", (open_item_id,)).fetchone()
    if oi is None:
        raise ValueError("open_item not found")
    if oi['status'] != 'open':
        raise ValueError(f"open_item already {oi['status']}")

    # Sanity: oi side and counterpart side must differ.
    if oi['source_side'] == counterpart_side:
        raise ValueError("counterpart must be the opposite side to the open item")

    if counterpart_side == 'swift':
        swift_row = counterpart_row; flex_row = 0
    else:
        swift_row = 0; flex_row = counterpart_row

    reason = (f"manual carry-match: open item #{open_item_id} "
              f"(orig session {oi['src_session_id']}, row {oi['src_row_number']})")
    if note:
        reason += f" — {note}"

    cur = conn.execute(
        "INSERT INTO assignments (session_id, swift_row, flex_row, tier, reason, "
        "amount_diff, status, decided_by, decided_at, source, manual_reason, open_item_id) "
        "VALUES (?,?,?,?,?, 0.0, 'confirmed', ?, ?, 'manual', ?, ?)",
        (session_id, swift_row, flex_row, 0, reason, user, now, note, open_item_id),
    )
    conn.execute(
        "UPDATE open_items SET status='cleared', cleared_at=?, cleared_by=?, "
        "cleared_via='manual_match', cleared_session_id=?, cleared_assignment_id=? "
        "WHERE id=?",
        (now, user, session_id, cur.lastrowid, open_item_id),
    )
    conn.execute(
        "INSERT INTO audit_log (session_id, action, actor, timestamp, details) "
        "VALUES (?, 'open_item_manual_cleared', ?, ?, ?)",
        (session_id, user, now, json.dumps({
            'open_item_id': open_item_id, 'assignment_id': cur.lastrowid,
            'counterpart_side': counterpart_side, 'counterpart_row': counterpart_row,
        })),
    )
    return cur.lastrowid


def write_off_open_item(conn, open_item_id: int, user: str, reason: str) -> None:
    """Terminal state — ageing-out item the ops team has given up on.
    Logged so auditors can see who wrote off what and why."""
    now = datetime.utcnow().isoformat()
    oi = conn.execute("SELECT id, status FROM open_items WHERE id=?",
                      (open_item_id,)).fetchone()
    if oi is None:
        raise ValueError("open_item not found")
    if oi['status'] != 'open':
        raise ValueError(f"open_item already {oi['status']}")
    conn.execute(
        "UPDATE open_items SET status='written_off', cleared_at=?, cleared_by=?, "
        "cleared_via='write_off', write_off_reason=? WHERE id=?",
        (now, user, reason, open_item_id),
    )
    conn.execute(
        "INSERT INTO audit_log (session_id, action, actor, timestamp, details) "
        "VALUES (NULL, 'open_item_written_off', ?, ?, ?)",
        (user, now, json.dumps({'open_item_id': open_item_id, 'reason': reason})),
    )
