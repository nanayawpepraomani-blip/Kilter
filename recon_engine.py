"""
recon_engine.py
===============

Proposer-style reconciliation engine. Companion to reconcile.py.

Unlike reconcile.match(), this module does NOT mutate input rows or commit
decisions. It enumerates every plausible (SWIFT, Flexcube) pair, tags each
with the strongest tier it qualifies at, and hands back a ranked list. A
separate resolve() step auto-picks the best pair per row and records the
losers as "competing candidates" on the winner — so the review UI can
offer a swap without re-running the engine.

Public surface:
    propose_candidates(swift_txns, flex_txns) -> list[Candidate]
    resolve(candidates, swift_txns, flex_txns) -> Resolution

Tier rules (same as reconcile.py, kept in one place for easy tuning):
    1. Reference in narration + exact amount + mirrored sign (C<->DR).
    2. Reference in narration + mirrored sign, amounts differ (FX/partial).
    3. No ref link, same value date, exact amount, mirrored sign.
    4. No ref link, amount + mirrored sign, value dates within one day.

Both loaders in reconcile.py already produce the dict shape this engine
expects: each row carries a stable '_row_number' that we use as its id.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


AMOUNT_TOLERANCE = 0.01
MIN_REF_LENGTH = 6
MIRROR_SIGN = {'C': 'DR', 'D': 'CR'}


# Characters stripped during reference normalisation. Counterparty refs often
# travel through a chain of formats that introduce decorations ("OUR REF:
# ABC/12-3" vs "ABC123" vs "ABC 123") — the engine should treat all of those
# as the same ref when scanning for a hit in the Flex narration.
_REF_STRIP = set(' \t/\\-_.,;:|()[]{}"\'')


def normalize_ref(ref: str | None) -> str:
    """Collapse cosmetic differences so a SWIFT :20: ref can be found inside
    a Flex narration regardless of formatting. Strips whitespace, slashes,
    dashes, dots, and leading zeros; upper-cases the result. Conservative
    enough that we don't accidentally collide unrelated short strings — we
    still gate matches on MIN_REF_LENGTH afterwards."""
    if not ref:
        return ''
    out_chars = [c for c in ref if c not in _REF_STRIP]
    s = ''.join(out_chars).upper()
    # Strip leading zeros but keep at least one character so "000000" -> "0"
    # rather than empty (which would disable the ref gate entirely).
    if s and s[0] == '0':
        s = s.lstrip('0') or s[:1]
    return s


@dataclass(frozen=True)
class Tolerance:
    """Per-account overrides for engine thresholds. Defaults reproduce the
    original (pre-2026-04-22) engine behaviour so callers that don't pass a
    Tolerance get identical results to the previous version.

    fx_tol_bps (basis points, 100 bps = 1%) is the FX cushion: when enabled
    and a swift<->flex FX rate is supplied, the engine will accept amounts
    whose delta (after FX conversion) falls within this spread. Stays zero
    for same-currency accounts — they should never trigger the FX branch."""
    amount_tol_abs: float = AMOUNT_TOLERANCE
    amount_tol_pct: float = 0.0
    date_tol_days: int = 1
    min_ref_len: int = MIN_REF_LENGTH
    fx_tol_bps: float = 0.0

    def amount_ok(self, swift_amt: float, flex_amt: float,
                   fx_rate: float | None = None) -> bool:
        diff = abs(flex_amt - swift_amt)
        if diff <= self.amount_tol_abs:
            return True
        if self.amount_tol_pct > 0:
            pivot = max(abs(swift_amt), abs(flex_amt), 1.0)
            if (diff / pivot) * 100.0 <= self.amount_tol_pct:
                return True
        # FX branch: compare flex against the swift amount converted at the
        # supplied rate. bps = 10_000ths of a point (100 bps = 1%).
        if self.fx_tol_bps > 0 and fx_rate and fx_rate > 0:
            converted = swift_amt * fx_rate
            fx_diff = abs(flex_amt - converted)
            pivot = max(abs(converted), abs(flex_amt), 1.0)
            if (fx_diff / pivot) * 10_000.0 <= self.fx_tol_bps:
                return True
        return False


@dataclass(frozen=True)
class Candidate:
    swift_row: int
    flex_row: int
    tier: int
    reason: str
    amount_diff: float


@dataclass
class Assignment:
    swift_row: int
    flex_row: int
    tier: int
    reason: str
    amount_diff: float
    competing: list[Candidate] = field(default_factory=list)


@dataclass
class Resolution:
    assignments: list[Assignment]
    unmatched_swift: list[int]
    unmatched_flex: list[int]
    orphan_candidates: list[Candidate]


def propose_candidates(swift_txns: list[dict], flex_txns: list[dict],
                       tol: Tolerance | None = None) -> list[Candidate]:
    """Every plausible pair, tagged with the strongest tier it qualifies at.

    tol supplies per-account overrides. When None, built-in defaults apply —
    preserving historic behaviour for callers that predate tolerance rules."""
    t = tol or Tolerance()
    candidates: list[Candidate] = []
    for s in swift_txns:
        target_type = MIRROR_SIGN.get(s['sign'])
        if target_type is None:
            continue
        s_ref = (s['our_ref'] or '').strip()
        s_has_ref = len(s_ref) >= t.min_ref_len

        for f in flex_txns:
            if f['type'] != target_type:
                continue

            amount_match = t.amount_ok(s['amount'], f['amount'])
            ref_hit = s_has_ref and _ref_in_narration(s_ref, f)
            # Guard against both dates being 0 (unparseable) — don't fake a match.
            same_date = bool(s['value_date']) and f['value_date'] == s['value_date']
            close_date = _days_between(f['value_date'], s['value_date']) <= t.date_tol_days

            tier = _classify(amount_match, ref_hit, same_date, close_date)
            if tier is None:
                continue

            candidates.append(Candidate(
                swift_row=s['_row_number'],
                flex_row=f['_row_number'],
                tier=tier,
                reason=_reason_for(tier, s, f, s_ref, t),
                amount_diff=f['amount'] - s['amount'],
            ))
    return candidates


def resolve(candidates: list[Candidate],
            swift_txns: list[dict],
            flex_txns: list[dict]) -> Resolution:
    """Greedy assign: strongest tier first, tightest amount diff as tiebreak.
    Losers attach to the winner that claimed their SWIFT or Flex row."""
    ordered = sorted(candidates, key=lambda c: (c.tier, abs(c.amount_diff)))

    swift_winner: dict[int, Assignment] = {}
    flex_winner: dict[int, Assignment] = {}
    orphans: list[Candidate] = []

    for cand in ordered:
        blocker = swift_winner.get(cand.swift_row) or flex_winner.get(cand.flex_row)
        if blocker is not None:
            blocker.competing.append(cand)
            continue
        if cand.swift_row in swift_winner or cand.flex_row in flex_winner:
            # Shouldn't happen — blocker lookup above would catch it — but belt-and-braces.
            orphans.append(cand)
            continue
        a = Assignment(
            swift_row=cand.swift_row,
            flex_row=cand.flex_row,
            tier=cand.tier,
            reason=cand.reason,
            amount_diff=cand.amount_diff,
        )
        swift_winner[cand.swift_row] = a
        flex_winner[cand.flex_row] = a

    all_swift = {s['_row_number'] for s in swift_txns}
    all_flex = {f['_row_number'] for f in flex_txns}
    return Resolution(
        assignments=list(swift_winner.values()),
        unmatched_swift=sorted(all_swift - swift_winner.keys()),
        unmatched_flex=sorted(all_flex - flex_winner.keys()),
        orphan_candidates=orphans,
    )


def _classify(amount_match: bool, ref_hit: bool, same_date: bool, close_date: bool) -> int | None:
    if ref_hit and amount_match:
        return 1
    if ref_hit:
        return 2
    if amount_match and same_date:
        return 3
    if amount_match and close_date:
        return 4
    return None


def _reason_for(tier: int, s: dict, f: dict, s_ref: str, t: Tolerance) -> str:
    if tier == 1:
        return f"ref {s_ref} in narration; amount={s['amount']:.2f}; sign mirror"
    if tier == 2:
        return (f"ref {s_ref} in narration; sign mirror; "
                f"amounts differ ({s['amount']:.2f} vs {f['amount']:.2f})")
    if tier == 3:
        return (f"same value date {s['value_date']}; amount={s['amount']:.2f}; "
                f"sign mirror; no ref link")
    return (f"amount={s['amount']:.2f}; sign mirror; "
            f"value dates {s['value_date']} vs {f['value_date']} "
            f"(within {t.date_tol_days} day{'s' if t.date_tol_days != 1 else ''})")


def _ref_in_narration(ref: str, flex_row: dict) -> bool:
    needle = normalize_ref(ref)
    if len(needle) < 3:
        # After stripping decorations the ref got too short to match safely.
        # Fall back to the raw-uppercase comparison for the legacy path.
        needle = (ref or '').strip().upper()
        if not needle:
            return False
        for field_name in ('narration', 'external_ref', 'trn_ref'):
            hay = (flex_row.get(field_name) or '').upper()
            if needle in hay:
                return True
        return False
    for field_name in ('narration', 'external_ref', 'trn_ref'):
        hay_raw = flex_row.get(field_name) or ''
        if not hay_raw:
            continue
        # Compare both the raw (uppercased) hay and a normalised hay so a
        # padded ref in the narration still hits even when our needle was
        # aggressively cleaned.
        if needle in hay_raw.upper():
            return True
        if needle in normalize_ref(hay_raw):
            return True
    return False


def _days_between(d1: int, d2: int) -> int:
    try:
        a = datetime.strptime(str(d1), '%Y%m%d')
        b = datetime.strptime(str(d2), '%Y%m%d')
        return abs((a - b).days)
    except ValueError:
        return 999


# ---------------------------------------------------------------------------
# Split matching (tier 5) — 1:N and N:1 aggregates.
#
# Runs as a dedicated post-pass on rows the main proposer couldn't pair 1:1.
# A typical case: SWIFT shows one aggregate debit (OUR REF: ABC123, $10,000)
# while Flex posts three entries (same ref, $3k + $3k + $4k). The 1:1
# proposer finds none of them (no amount match). The split proposer groups
# by normalised ref, sums, and matches against the SWIFT row.
#
# Constraints kept strict to avoid false positives:
#   * Ref gate mandatory — no amount-only split matches (too many false
#     positives on common amounts).
#   * Subset size capped at 4 to bound the combinatorial search.
#   * All subset rows must share the same sign (no netting across CR/DR).
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class SplitCandidate:
    """A 1:N or N:1 aggregate match. `swift_rows` and `flex_rows` hold the
    row_numbers involved; one side has length 1, the other length >= 2."""
    swift_rows: tuple[int, ...]
    flex_rows: tuple[int, ...]
    tier: int
    reason: str
    amount_diff: float


def propose_splits(swift_txns: list[dict], flex_txns: list[dict],
                   unmatched_swift: set[int], unmatched_flex: set[int],
                   tol: Tolerance | None = None,
                   max_group_size: int = 4) -> list[SplitCandidate]:
    """Search for aggregate matches among the rows the 1:1 proposer left
    unmatched. Returns a list of SplitCandidate; the caller decides whether
    to promote them to confirmed matches or queue them for review.

    Algorithm: group Flex rows by (normalised ref, type). For each SWIFT
    row with a matching normalised ref and mirrored sign, test every
    subset of size 2..max_group_size for a summed-amount match. Then do
    the mirror (group SWIFT, test against Flex)."""
    from itertools import combinations
    t = tol or Tolerance()
    out: list[SplitCandidate] = []

    # Index by row_number for quick lookup.
    swift_by_row = {s['_row_number']: s for s in swift_txns}
    flex_by_row  = {f['_row_number']: f for f in flex_txns}

    # --- 1 SWIFT -> N Flex ----------------------------------------------
    flex_by_ref: dict[tuple[str, str], list[dict]] = {}
    for row in unmatched_flex:
        f = flex_by_row.get(row)
        if f is None:
            continue
        key = (normalize_ref(f.get('trn_ref') or f.get('external_ref')), f.get('type') or '')
        if not key[0] or len(key[0]) < 4:
            continue
        flex_by_ref.setdefault(key, []).append(f)

    for row in unmatched_swift:
        s = swift_by_row.get(row)
        if s is None:
            continue
        target_type = MIRROR_SIGN.get(s.get('sign'))
        if target_type is None:
            continue
        s_ref_norm = normalize_ref(s.get('our_ref'))
        if len(s_ref_norm) < t.min_ref_len:
            continue

        # Collect flex candidates where ref fires on SWIFT's our_ref either
        # by exact key match or by inclusion in the narration.
        pool: list[dict] = []
        for (fx_ref, fx_type), rows in flex_by_ref.items():
            if fx_type != target_type:
                continue
            if s_ref_norm == fx_ref or s_ref_norm in fx_ref or fx_ref in s_ref_norm:
                pool.extend(rows)
        # Deduplicate + stable sort by row number.
        seen = set(); unique_pool = []
        for f in pool:
            if f['_row_number'] in seen:
                continue
            seen.add(f['_row_number'])
            unique_pool.append(f)
        if len(unique_pool) < 2:
            continue

        hit = _find_split_subset(s['amount'], unique_pool, t, max_group_size)
        if hit is None:
            continue
        flex_rows = tuple(f['_row_number'] for f in hit)
        total = sum(f['amount'] for f in hit)
        out.append(SplitCandidate(
            swift_rows=(s['_row_number'],),
            flex_rows=flex_rows,
            tier=5,
            reason=(f"1:N split on ref {s_ref_norm!r}; SWIFT {s['amount']:.2f} <-> "
                    f"{len(flex_rows)} Flex rows summing {total:.2f}"),
            amount_diff=total - s['amount'],
        ))

    # --- N SWIFT -> 1 Flex ----------------------------------------------
    swift_by_ref: dict[tuple[str, str], list[dict]] = {}
    for row in unmatched_swift:
        s = swift_by_row.get(row)
        if s is None:
            continue
        key = (normalize_ref(s.get('our_ref')), s.get('sign') or '')
        if not key[0] or len(key[0]) < 4:
            continue
        swift_by_ref.setdefault(key, []).append(s)

    for row in unmatched_flex:
        f = flex_by_row.get(row)
        if f is None:
            continue
        # reverse of MIRROR_SIGN
        target_sign = 'D' if f.get('type') == 'CR' else ('C' if f.get('type') == 'DR' else None)
        if target_sign is None:
            continue
        f_ref_norm = normalize_ref(f.get('trn_ref') or f.get('external_ref'))
        if len(f_ref_norm) < t.min_ref_len:
            continue

        pool: list[dict] = []
        for (sw_ref, sw_sign), rows in swift_by_ref.items():
            if sw_sign != target_sign:
                continue
            if f_ref_norm == sw_ref or f_ref_norm in sw_ref or sw_ref in f_ref_norm:
                pool.extend(rows)
        seen = set(); unique_pool = []
        for s in pool:
            if s['_row_number'] in seen:
                continue
            seen.add(s['_row_number'])
            unique_pool.append(s)
        if len(unique_pool) < 2:
            continue

        hit = _find_split_subset(f['amount'], unique_pool, t, max_group_size)
        if hit is None:
            continue
        swift_rows = tuple(s['_row_number'] for s in hit)
        # Skip if we already emitted a 1:N candidate covering this same
        # Flex row to avoid double-matching in the reverse direction.
        if any(f['_row_number'] in c.flex_rows for c in out):
            continue
        total = sum(s['amount'] for s in hit)
        out.append(SplitCandidate(
            swift_rows=swift_rows,
            flex_rows=(f['_row_number'],),
            tier=5,
            reason=(f"N:1 split on ref {f_ref_norm!r}; "
                    f"{len(swift_rows)} SWIFT rows summing {total:.2f} <-> "
                    f"Flex {f['amount']:.2f}"),
            amount_diff=f['amount'] - total,
        ))

    return out


def _find_split_subset(target: float, pool: list[dict], t: Tolerance,
                       max_size: int) -> list[dict] | None:
    """Return the smallest subset of `pool` whose summed amount matches
    `target` within tolerance, or None. Prefers 2-row splits, then 3, then 4."""
    from itertools import combinations
    for size in range(2, max_size + 1):
        for combo in combinations(pool, size):
            total = sum(r['amount'] for r in combo)
            if t.amount_ok(total, target):
                return list(combo)
    return None


# ---------------------------------------------------------------------------
# True many-to-many matching (tier 6) — subsets on BOTH sides.
#
# Use case: a bank aggregates 2-3 outgoing payments into one internal GL
# entry while the counterparty bundles 2-3 of their own debits into a
# single SWIFT credit. Neither tier 1-4 (1:1) nor tier 5 (1:N or N:1)
# catches this; the engine has to test subsets on both sides.
#
# The combinatorics are unforgiving — C(20,3) * C(20,3) is 1.3M pairs.
# So we gate aggressively before searching:
#
#   * Date band: every row in the candidate subset must fall within
#     `tol.date_tol_days` of the subset's median value date. Banks
#     aggregate same-day-or-next-day; we never match across a wide
#     window.
#   * Sign mirror: every SWIFT row must mirror every Flex row. No
#     netting CR against DR within a subset.
#   * Pool cap: if either side has more than POOL_CAP unmatched rows
#     in a date band, refuse rather than blow up. The caller can run
#     manual review on those.
#   * Subset sizes 2..3 each side; (1,1) is tier 1-4, (1,N)/(N,1) is
#     tier 5.
#
# Rationale for no ref gate (unlike tier 5): real M:M aggregations
# usually have heterogeneous refs across the bundled rows, so a ref
# gate is too strict. The date-band + sign-mirror + amount-sum gate is
# stricter than it looks at first glance — it produces few false
# positives in pilot data.
# ---------------------------------------------------------------------------

POOL_CAP_M2N = 20         # per side, per date band — guards against blowup
MAX_SUBSET_M2N = 3        # max rows on either side


def propose_many_to_many(swift_txns: list[dict], flex_txns: list[dict],
                         unmatched_swift: set[int], unmatched_flex: set[int],
                         tol: Tolerance | None = None) -> list[SplitCandidate]:
    """True M:N matching. Returns SplitCandidate rows where BOTH
    swift_rows and flex_rows have length >= 2. Caller should run after
    propose_splits() so we don't overlap the 1:N / N:1 cases.

    Algorithm:
        1. Bucket unmatched txns by value-date band of width
           `tol.date_tol_days`. (Bucketing on the integer YYYYMMDD value
           date with overlap of `date_tol_days` either side.)
        2. Within each bucket, separate by sign (mirror to find pairs).
        3. For sizes (m, n) in {(2,2), (2,3), (3,2), (3,3)}:
              For every m-subset of SWIFT rows in the bucket:
                  target = sum(swift amounts)
                  For every n-subset of Flex rows with mirror sign:
                      if amount_ok(target, sum(flex amounts)):
                          emit candidate
        4. Skip candidates that would conflict with prior emissions
           (same row claimed twice).
    """
    from itertools import combinations

    t = tol or Tolerance()
    out: list[SplitCandidate] = []

    swift_by_row = {s['_row_number']: s for s in swift_txns}
    flex_by_row  = {f['_row_number']: f for f in flex_txns}

    swift_pool = [swift_by_row[r] for r in unmatched_swift if r in swift_by_row]
    flex_pool  = [flex_by_row[r]  for r in unmatched_flex  if r in flex_by_row]

    if not swift_pool or not flex_pool:
        return out

    # --- 1. Bucket by value-date band ---------------------------------
    # We use the SWIFT-side date as the bucket anchor and pull every
    # Flex row whose date falls within ±date_tol_days of any SWIFT row
    # in the bucket. Buckets overlap deliberately — better duplicates
    # than missed matches; conflict-resolution at emission time culls.
    band = max(t.date_tol_days, 1)

    # Index swift rows by their integer date.
    by_date_swift: dict[int, list[dict]] = {}
    for s in swift_pool:
        d = int(s.get('value_date') or 0)
        if not d:
            continue
        by_date_swift.setdefault(d, []).append(s)

    # Track which (swift_row, flex_row) pairs we've already claimed so
    # the same physical row can't appear in two emitted candidates.
    claimed_swift: set[int] = set()
    claimed_flex: set[int] = set()

    # Walk distinct anchor dates — each defines a bucket.
    for anchor_date in sorted(by_date_swift):
        # Pull all swift rows within band of the anchor.
        bucket_swift = [
            s for s in swift_pool
            if int(s.get('value_date') or 0) and
               _days_between(int(s['value_date']), anchor_date) <= band
        ]
        bucket_flex = [
            f for f in flex_pool
            if int(f.get('value_date') or 0) and
               _days_between(int(f['value_date']), anchor_date) <= band
        ]
        if len(bucket_swift) < 2 or len(bucket_flex) < 2:
            continue
        # Pool cap — too many means we can't safely enumerate.
        if len(bucket_swift) > POOL_CAP_M2N or len(bucket_flex) > POOL_CAP_M2N:
            continue

        # 2. Split bucket by sign so we only consider mirror-sign pairs.
        for sign in ('C', 'D'):
            target_type = MIRROR_SIGN[sign]
            s_side = [s for s in bucket_swift if s.get('sign') == sign
                      and s['_row_number'] not in claimed_swift]
            f_side = [f for f in bucket_flex if f.get('type') == target_type
                      and f['_row_number'] not in claimed_flex]
            if len(s_side) < 2 or len(f_side) < 2:
                continue

            # 3. Subset sweep — outer loop over swift sizes, inner over flex.
            for m in range(2, MAX_SUBSET_M2N + 1):
                if len(s_side) < m:
                    break
                for s_combo in combinations(s_side, m):
                    if any(s['_row_number'] in claimed_swift for s in s_combo):
                        continue
                    target = sum(s['amount'] for s in s_combo)
                    for n in range(2, MAX_SUBSET_M2N + 1):
                        if len(f_side) < n:
                            break
                        for f_combo in combinations(f_side, n):
                            if any(f['_row_number'] in claimed_flex for f in f_combo):
                                continue
                            total = sum(f['amount'] for f in f_combo)
                            if not t.amount_ok(total, target):
                                continue
                            s_rows = tuple(s['_row_number'] for s in s_combo)
                            f_rows = tuple(f['_row_number'] for f in f_combo)
                            out.append(SplitCandidate(
                                swift_rows=s_rows,
                                flex_rows=f_rows,
                                tier=6,
                                reason=(f"{m}:{n} aggregate match in date band "
                                        f"around {anchor_date}; "
                                        f"SWIFT {len(s_rows)} rows summing "
                                        f"{target:.2f} <-> Flex {len(f_rows)} rows "
                                        f"summing {total:.2f}; sign mirror"),
                                amount_diff=total - target,
                            ))
                            claimed_swift.update(s_rows)
                            claimed_flex.update(f_rows)
                            # Move on once one m,n hit lands so we don't
                            # over-emit on the same swift subset.
                            break
                        else:
                            continue
                        break

    return out
