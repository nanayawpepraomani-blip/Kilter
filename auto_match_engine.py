"""
auto_match_engine.py
====================

Evaluates auto-match rules against pending assignments in a session and
auto-confirms those that satisfy all conditions. Called at the tail of
ingest_pair() and also callable manually via POST /sessions/{id}/auto-match.

Rules are evaluated in priority order (lower number = higher priority).
For each pending assignment, the first matching rule wins. The assignment
is confirmed with decided_by='system_auto:<rule_name>' and audit-logged.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone


def apply_auto_rules(conn, session_id: int, actor: str = 'system_auto') -> dict:
    """
    Returns:
        {
            'auto_confirmed': int,
            'rules_fired': {rule_id: count},
            'skipped': int,
        }
    """
    rules = conn.execute(
        "SELECT * FROM auto_match_rules WHERE active=1 ORDER BY priority ASC, id ASC"
    ).fetchall()
    if not rules:
        return {'auto_confirmed': 0, 'rules_fired': {}, 'skipped': 0}

    pending = conn.execute(
        "SELECT a.*, "
        "  s.our_ref AS swift_our_ref, s.their_ref AS swift_their_ref, "
        "  s.value_date AS swift_value_date, "
        "  f.trn_ref AS flex_trn_ref, f.value_date AS flex_value_date "
        "FROM assignments a "
        "LEFT JOIN swift_txns s ON s.session_id=a.session_id AND s.row_number=a.swift_row "
        "LEFT JOIN flex_txns f ON f.session_id=a.session_id AND f.row_number=a.flex_row "
        "WHERE a.session_id=? AND a.status='pending'",
        (session_id,)
    ).fetchall()

    auto_confirmed = 0
    rules_fired: dict[int, int] = {}
    skipped = 0
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()

    for assignment in pending:
        matched_rule = None
        for rule in rules:
            if _rule_matches(rule, assignment):
                matched_rule = rule
                break

        if matched_rule is None:
            skipped += 1
            continue

        decided_by = f"{actor}:{matched_rule['name']}"
        conn.execute(
            "UPDATE assignments SET status='confirmed', decided_by=?, decided_at=? WHERE id=?",
            (decided_by, now, assignment['id'])
        )
        conn.execute(
            "INSERT INTO audit_log (session_id, action, actor, timestamp, details) "
            "VALUES (?, 'auto_confirmed', ?, ?, ?)",
            (session_id, decided_by, now, json.dumps({
                'assignment_id': assignment['id'],
                'rule_id': matched_rule['id'],
                'rule_name': matched_rule['name'],
                'swift_row': assignment['swift_row'],
                'flex_row': assignment['flex_row'],
                'tier': assignment['tier'],
            }))
        )
        auto_confirmed += 1
        rules_fired[matched_rule['id']] = rules_fired.get(matched_rule['id'], 0) + 1

    return {'auto_confirmed': auto_confirmed, 'rules_fired': rules_fired, 'skipped': skipped}


def _rule_matches(rule, assignment) -> bool:
    """Return True if all non-null conditions on the rule are satisfied."""
    # Tier requirement
    if rule['require_tier'] is not None:
        if str(assignment['tier']) != str(rule['require_tier']):
            return False

    # Amount exact (diff must be 0.00)
    if rule['require_amount_exact']:
        diff = assignment['amount_diff'] if assignment['amount_diff'] is not None else 0
        if abs(diff) > 0.005:
            return False

    # Max amount diff
    if rule['max_amount_diff'] is not None:
        diff = abs(assignment['amount_diff'] if assignment['amount_diff'] is not None else 0)
        if diff > rule['max_amount_diff']:
            return False

    # Reference match: at least one of swift our_ref or their_ref overlaps with flex trn_ref
    if rule['require_ref_match']:
        swift_our = (assignment['swift_our_ref'] or '').strip().upper()
        swift_their = (assignment['swift_their_ref'] or '').strip().upper()
        flex_ref = (assignment['flex_trn_ref'] or '').strip().upper()
        if not flex_ref:
            return False
        if flex_ref not in swift_our and flex_ref not in swift_their:
            # Also check the reverse
            if swift_our not in flex_ref and swift_their not in flex_ref:
                return False

    # Same value date
    if rule['require_same_date']:
        if (assignment['swift_value_date'] or '') != (assignment['flex_value_date'] or ''):
            return False

    return True
