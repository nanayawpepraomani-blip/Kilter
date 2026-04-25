"""
validate_engine.py
==================

Runs the original reconcile.match() and the new recon_engine.resolve()
against the same SWIFT + Flexcube files, then diffs the assignments.
Run this before the UI work so we know the new proposer engine doesn't
regress on real data.

Usage:
    python validate_engine.py <swift.xlsx> <flex.xlsx>
    python validate_engine.py     # auto-discover from input_swift/ and input_flexcube/

Output:
    - Per-engine summary (total matches + tier breakdown)
    - Pairs OLD found but NEW missed (regressions)
    - Pairs NEW found but OLD missed (additions)
    - Pairs both engines agree on but classify at different tiers
    - Verdict line at the end

Requires reconcile.py and recon_engine.py to sit in the same folder.
"""

import sys
import copy
from collections import Counter
from pathlib import Path

from reconcile import load_swift, load_flexcube, match
from recon_engine import propose_candidates, resolve


def main():
    script_dir = Path(__file__).resolve().parent

    if len(sys.argv) == 3:
        swift_path = Path(sys.argv[1])
        flex_path = Path(sys.argv[2])
    elif len(sys.argv) == 1:
        swift_path, flex_path = _auto_discover(script_dir)
        if swift_path is None:
            print("No args and no .xlsx files found in input_swift/ or input_flexcube/.")
            print("Usage: python validate_engine.py [<swift.xlsx> <flex.xlsx>]")
            return 2
    else:
        print("Usage: python validate_engine.py [<swift.xlsx> <flex.xlsx>]")
        return 2

    print(f"SWIFT:    {swift_path.name}")
    print(f"Flexcube: {flex_path.name}")
    print()

    # Load once — deep-copy for each engine so the old engine's mutation of
    # '_used' flags doesn't bleed into the new engine's inputs.
    swift_base = load_swift(swift_path)
    flex_base = load_flexcube(flex_path)
    print(f"Loaded {len(swift_base)} SWIFT rows, {len(flex_base)} Flexcube rows")
    print()

    swift_old = copy.deepcopy(swift_base)
    flex_old = copy.deepcopy(flex_base)
    old_matches = match(swift_old, flex_old)
    old_pairs = {(m['swift']['_row_number'], m['flex']['_row_number']): m['tier']
                 for m in old_matches}

    swift_new = copy.deepcopy(swift_base)
    flex_new = copy.deepcopy(flex_base)
    candidates = propose_candidates(swift_new, flex_new)
    resolution = resolve(candidates, swift_new, flex_new)
    new_pairs = {(a.swift_row, a.flex_row): a.tier for a in resolution.assignments}

    _print_summary("OLD (reconcile.match)", old_pairs)
    _print_summary("NEW (recon_engine.resolve)", new_pairs)

    old_only = set(old_pairs) - set(new_pairs)
    new_only = set(new_pairs) - set(old_pairs)
    in_both = set(old_pairs) & set(new_pairs)
    tier_changes = {p: (old_pairs[p], new_pairs[p])
                    for p in in_both if old_pairs[p] != new_pairs[p]}

    print("DIFF")
    print("-" * 60)
    print(f"Agree (same pair, same tier): {len(in_both) - len(tier_changes)}")
    print(f"Tier disagreements:           {len(tier_changes)}")
    print(f"OLD only (missing from NEW):  {len(old_only)}")
    print(f"NEW only (missing from OLD):  {len(new_only)}")
    print()

    if tier_changes:
        print("Tier disagreements (first 20):")
        for (s_row, f_row), (o_tier, n_tier) in list(tier_changes.items())[:20]:
            print(f"  SWIFT row {s_row}  <->  Flex row {f_row}:  OLD T{o_tier}  vs  NEW T{n_tier}")
        print()

    if old_only:
        print("Pairs OLD found but NEW missed (first 20):")
        for s_row, f_row in list(old_only)[:20]:
            print(f"  SWIFT row {s_row}  <->  Flex row {f_row}  (T{old_pairs[(s_row, f_row)]})")
        print()

    if new_only:
        print("Pairs NEW found but OLD missed (first 20):")
        for s_row, f_row in list(new_only)[:20]:
            print(f"  SWIFT row {s_row}  <->  Flex row {f_row}  (T{new_pairs[(s_row, f_row)]})")
        print()

    identical = not (old_only or new_only or tier_changes)
    print("=" * 60)
    if identical:
        print("VERDICT: PASS  (engines produce identical assignments)")
    elif not old_only and not tier_changes:
        print("VERDICT: NEW-STRICTLY-BETTER  (NEW covers OLD plus extras)")
    elif not old_only and not new_only:
        print("VERDICT: TIER DRIFT  (same pairs, different tier classifications)")
    else:
        print("VERDICT: DIFFER  (review details above)")
    print("=" * 60)

    return 0


def _auto_discover(script_dir: Path):
    swift_dir = script_dir / 'input_swift'
    flex_dir = script_dir / 'input_flexcube'
    if not swift_dir.exists() or not flex_dir.exists():
        return None, None
    swift_files = sorted(p for p in swift_dir.glob('*.xlsx') if not p.name.startswith('~$'))
    flex_files = sorted(p for p in flex_dir.glob('*.xlsx') if not p.name.startswith('~$'))
    if not swift_files or not flex_files:
        return None, None
    return swift_files[0], flex_files[0]


def _print_summary(label: str, pairs: dict) -> None:
    print(label)
    print("-" * 60)
    print(f"  Total matches: {len(pairs)}")
    tier_counts = Counter(pairs.values())
    for t in (1, 2, 3, 4):
        print(f"  Tier {t}: {tier_counts.get(t, 0)}")
    print()


if __name__ == '__main__':
    sys.exit(main())
