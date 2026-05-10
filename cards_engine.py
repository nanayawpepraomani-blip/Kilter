"""
cards_engine.py — settlement reconciliation across cards files.

Approach (v1, scheme_ref join):
    For every distinct `scheme_ref` (the scheme-assigned txn reference —
    Visa TRR/ARN, Mastercard Banknet, switch RRN), gather all records
    that carry it. Classify the group by how many files contribute and
    whether the settlement amounts agree:

        'matched'    — ≥2 distinct file_ids, all amounts within
                       tolerance. The classic case is auth → clearing →
                       settlement (3 stages) but works for any subset
                       (e.g. acquirer + issuer 2-way).
        'mismatched' — ≥2 file_ids but amounts diverge beyond
                       tolerance (likely a fee or FX gap to investigate).
        'unmatched'  — exactly one file contributes. The other stage(s)
                       haven't been ingested yet, or this is a solo
                       reversal/chargeback.

A `recompute_matches` pass writes the resulting status back into
`card_settlement_records.recon_status` so the Cards UI and exports
reflect the latest match state. Idempotent — re-running over the same
inputs produces the same statuses, and previously-set 'disputed' /
'written_off' overrides are preserved (operator wins).

Out of scope for v1:
    - Auth/clearing/settlement stage tagging. We treat distinct file
      contributions as the proxy. Once Visa Base II / Mastercard IPM
      parsers ship, we'll add a stage tag on each file and tighten the
      'matched' condition to require all three stages.
    - Per-record amount tolerance overrides (per-MCC, per-region).
      One global tolerance for now.
    - Fee reconciliation against expected interchange + scheme-fee
      schedules. That's the v2 'fee recon' workstream noted in
      docs/CARDS_DESIGN.md.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable

# Default amount-equivalence tolerance, in the settlement currency's
# minor units. 0.01 catches genuine cents-of-rounding differences but
# flags real fee gaps (which typically land in 10s of minor units at
# minimum). Operators can tune this once we have a tolerance-policy
# UI for cards (post-v1).
DEFAULT_TOLERANCE = 0.01

# Recon-status values written to card_settlement_records.recon_status.
# 'disputed' and 'written_off' are operator-driven and never overwritten
# by the engine — they win over any computed status.
COMPUTED_STATUSES   = ('matched', 'mismatched', 'unmatched', 'incomplete')
PROTECTED_STATUSES  = ('disputed', 'written_off')

# Required stages for a group to be considered fully matched. Operators
# can override via environment — e.g. set to 'auth,settlement' to skip
# clearing for schemes that don't emit clearing files.
_raw_required = os.environ.get('KILTER_CARDS_REQUIRED_STAGES', 'auth,clearing,settlement')
REQUIRED_STAGES = {s for s in _raw_required.split(',') if s}


@dataclass
class MatchGroup:
    """One scheme_ref's reconciliation summary."""
    scheme_ref: str
    record_count: int
    file_count: int
    file_ids: list[int]
    record_ids: list[int]
    amount_min: float
    amount_max: float
    amount_total: float
    amount_spread: float          # max − min, the headline mismatch number
    currencies: list[str]
    pan_last4_set: list[str]
    schemes: list[str]
    stages: list[str]             # distinct stages present across files in this group
    status: str                   # one of COMPUTED_STATUSES

    @property
    def is_within_tolerance(self) -> bool:
        return self.amount_spread <= DEFAULT_TOLERANCE


def compute_match_groups(conn, *, scheme: str | None = None,
                          settlement_date_from: str | None = None,
                          settlement_date_to: str | None = None,
                          tolerance: float = DEFAULT_TOLERANCE,
                          ) -> list[MatchGroup]:
    """Compute match groups across `card_settlement_records`. Pure read —
    does not write status back. Use `apply_match_status` for that.

    Filters narrow the population the engine sees:
        scheme              limit to one card scheme
        settlement_date_*   ISO YYYY-MM-DD, inclusive

    Aggregation runs as a single SQL `GROUP BY scheme_ref` so memory
    cost is O(groups), not O(records) — important once a real issuer
    pilot ingests 100K+ settlement rows per day. The previous
    implementation pulled every record into Python and grouped in a
    dict; that worked but spiked RAM at scale.

    The `GROUP_CONCAT(DISTINCT …)` pattern is safe here because every
    aggregated column is a comma-free atom (file_id / id integers,
    3-letter ISO currencies, 4-digit pan_last4, scheme keys). If we
    ever start aggregating free-text columns we'd switch to JSON_GROUP_ARRAY.
    """
    where = ["r.scheme_ref IS NOT NULL", "r.scheme_ref != ''"]
    params: list = []
    if scheme:
        where.append("f.scheme = ?")
        params.append(scheme)
    if settlement_date_from:
        where.append("r.settlement_date >= ?")
        params.append(settlement_date_from)
    if settlement_date_to:
        where.append("r.settlement_date <= ?")
        params.append(settlement_date_to)
    where_sql = " AND ".join(where)

    sql = f"""
        SELECT r.scheme_ref                                 AS scheme_ref,
               COUNT(*)                                     AS record_count,
               COUNT(DISTINCT r.file_id)                    AS file_count,
               MIN(r.amount_settlement)                     AS amount_min,
               MAX(r.amount_settlement)                     AS amount_max,
               SUM(r.amount_settlement)                     AS amount_total,
               GROUP_CONCAT(DISTINCT r.file_id)             AS file_ids_csv,
               GROUP_CONCAT(DISTINCT r.id)                  AS record_ids_csv,
               GROUP_CONCAT(DISTINCT r.currency_settlement) AS currencies_csv,
               GROUP_CONCAT(DISTINCT r.pan_last4)           AS pan4_csv,
               GROUP_CONCAT(DISTINCT f.scheme)              AS schemes_csv,
               GROUP_CONCAT(DISTINCT f.stage)               AS stages_concat
        FROM card_settlement_records r
        JOIN card_settlement_files f ON f.id = r.file_id
        WHERE {where_sql}
        GROUP BY r.scheme_ref
    """
    rows = conn.execute(sql, params).fetchall()

    def _parse_csv(s: str | None, conv=str) -> list:
        """SQLite GROUP_CONCAT returns NULL for empty groups, comma-
        joined string otherwise. Skip empty-string fragments so a
        DISTINCT over a column with NULLs doesn't yield a stray ''."""
        if not s:
            return []
        return sorted({conv(p) for p in s.split(',') if p != ''})

    groups: list[MatchGroup] = []
    for r in rows:
        amount_min = float(r['amount_min'] or 0)
        amount_max = float(r['amount_max'] or 0)
        spread = amount_max - amount_min
        file_count = int(r['file_count'] or 0)

        # Float-safe tolerance check: a 0.01 spread computed from
        # `100.00 + 0.01` lands on 0.01000000000000023 due to IEEE-754
        # rounding, which would falsely fail a strict `<=` against 0.01.
        # Add a fixed epsilon to absorb that.
        stages = [s for s in (r['stages_concat'] or '').split(',') if s]

        if file_count <= 1:
            status = 'unmatched'
        elif spread > tolerance + 1e-9:
            status = 'mismatched'
        elif (REQUIRED_STAGES and file_count >= 2
              and not REQUIRED_STAGES.issubset(set(stages))):
            # All amounts agree but not all required stages are present.
            # 'incomplete' takes priority over 'matched'.
            status = 'incomplete'
        else:
            status = 'matched'

        groups.append(MatchGroup(
            scheme_ref=r['scheme_ref'],
            record_count=int(r['record_count'] or 0),
            file_count=file_count,
            file_ids=_parse_csv(r['file_ids_csv'], int),
            record_ids=_parse_csv(r['record_ids_csv'], int),
            amount_min=amount_min,
            amount_max=amount_max,
            amount_total=float(r['amount_total'] or 0),
            amount_spread=spread,
            currencies=[c.upper() for c in _parse_csv(r['currencies_csv'])],
            pan_last4_set=_parse_csv(r['pan4_csv']),
            schemes=_parse_csv(r['schemes_csv']),
            stages=stages,
            status=status,
        ))

    # Stable ordering: most-mismatched first, then alpha by ref.
    status_rank = {'mismatched': 0, 'incomplete': 1, 'matched': 2, 'unmatched': 3}
    groups.sort(key=lambda g: (status_rank.get(g.status, 9),
                                -g.amount_spread, g.scheme_ref))
    return groups


@dataclass
class RecomputeResult:
    groups_total: int
    matched: int
    mismatched: int
    unmatched: int
    incomplete: int
    records_updated: int
    records_protected: int   # operator status preserved (disputed / written_off)


def apply_match_status(conn, groups: Iterable[MatchGroup], *,
                        actor: str = 'system') -> RecomputeResult:
    """Write each group's status into card_settlement_records.recon_status
    for every record id in the group. Skips records already in a
    PROTECTED_STATUS so operator-set state is never trampled.

    Wraps writes in `with conn:` so the run is atomic — partial failures
    leave the table consistent."""
    counts = {'matched': 0, 'mismatched': 0, 'unmatched': 0, 'incomplete': 0}
    updated = 0
    protected = 0
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()

    with conn:
        for g in groups:
            counts[g.status] = counts.get(g.status, 0) + 1
            if not g.record_ids:
                continue
            placeholders = ",".join("?" * len(g.record_ids))
            # Skip rows in operator-controlled status. SQLite doesn't
            # support array-valued IN binding so we splice param markers.
            # For 'matched' and 'incomplete', record matched_at/matched_by
            # only when the status is 'matched'.
            cur = conn.execute(
                f"UPDATE card_settlement_records "
                f"SET recon_status=?, matched_at=?, matched_by=? "
                f"WHERE id IN ({placeholders}) "
                f"AND recon_status NOT IN ('disputed','written_off')",
                [g.status, now if g.status == 'matched' else None,
                 actor if g.status == 'matched' else None,
                 *g.record_ids],
            )
            updated += cur.rowcount
            # Count rows in this group that were protected.
            prot = conn.execute(
                f"SELECT COUNT(*) FROM card_settlement_records "
                f"WHERE id IN ({placeholders}) "
                f"AND recon_status IN ('disputed','written_off')",
                g.record_ids,
            ).fetchone()[0]
            protected += int(prot)

    return RecomputeResult(
        groups_total=sum(counts.values()),
        matched=counts.get('matched', 0),
        mismatched=counts.get('mismatched', 0),
        unmatched=counts.get('unmatched', 0),
        incomplete=counts.get('incomplete', 0),
        records_updated=updated,
        records_protected=protected,
    )
