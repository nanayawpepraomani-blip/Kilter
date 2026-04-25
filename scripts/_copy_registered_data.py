"""Pick out and copy only the registered-account SWIFT + Flexcube data into
Kilter's messages/ intake folders. Skips anything for unregistered accounts
or in the wrong format."""

import shutil
import sqlite3
import sys
from pathlib import Path
from collections import defaultdict, Counter

_HERE = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_HERE))
sys.stdout.reconfigure(encoding='utf-8')

from swift_loader import extract_swift_meta_raw
from swift_core import detect_message_type

# External data-source paths — update these to wherever you keep the raw
# SWIFT spool and Flex extract. The script auto-renamed the folder names
# during the Kilter rebrand; restore your actual on-disk paths if needed.
SRC_SWIFT = Path('C:/Users/NYPO/Desktop/Swift_04232026/Swift_04232026')
SRC_FLEX_DIR = Path('C:/Users/NYPO/Desktop/Kilter/Kilter')

DST_SWIFT = _HERE / 'messages' / 'swift'
DST_FLEX  = _HERE / 'messages' / 'flexcube'


def load_registered(db: Path) -> tuple[set[str], set[tuple[str, str]]]:
    """Return (flex_ac_nos, {(swift_account, currency)} pairs) for active
    registered accounts."""
    c = sqlite3.connect(db)
    flex_acs = set()
    swift_pairs = set()
    for r in c.execute(
        "SELECT flex_ac_no, swift_account, currency FROM accounts WHERE active=1"
    ).fetchall():
        flex_acs.add(r[0])
        if r[1] and r[1].strip():
            swift_pairs.add((r[1].strip(), r[2].strip().upper()))
    c.close()
    return flex_acs, swift_pairs


def copy_flex(flex_acs: set[str]) -> tuple[int, int, list[str]]:
    """Copy .xlsx files from the source folder whose filename stem matches a
    registered flex_ac_no. Returns (copied, skipped, not_found)."""
    copied, skipped = 0, 0
    not_found = set(flex_acs)

    DST_FLEX.mkdir(parents=True, exist_ok=True)

    for src in SRC_FLEX_DIR.glob('*.xlsx'):
        stem = src.stem
        if stem not in flex_acs:
            skipped += 1
            continue
        shutil.copy2(src, DST_FLEX / src.name)
        copied += 1
        not_found.discard(stem)
    return copied, skipped, sorted(not_found)


def copy_swift(swift_pairs: set[tuple[str, str]]) -> tuple[int, int, dict]:
    """Scan SWIFT folder, copy only MT940/950 files whose :25: account+ccy
    matches a registered (swift_account, currency) pair.
    Returns (copied, skipped, per_pair_counts)."""
    DST_SWIFT.mkdir(parents=True, exist_ok=True)
    copied, skipped = 0, 0
    per_pair = defaultdict(int)
    skipped_reasons = Counter()

    for f in sorted(SRC_SWIFT.glob('*.out')):
        raw = f.read_text(encoding='latin-1', errors='replace')
        mt = detect_message_type(raw)
        if mt not in ('940', '950'):
            skipped += 1; skipped_reasons[f'not MT940/950 (is MT{mt})'] += 1
            continue
        try:
            meta = extract_swift_meta_raw(f)
        except Exception as e:
            skipped += 1; skipped_reasons[f'parse error'] += 1
            continue
        acc = (meta.get('account') or '').strip()
        ccy = (meta.get('currency') or '').strip().upper()
        if not acc or not ccy:
            skipped += 1; skipped_reasons['no account/currency'] += 1
            continue
        if (acc, ccy) not in swift_pairs:
            skipped += 1; skipped_reasons['not in registered pairs'] += 1
            continue

        # Matched: copy to intake
        shutil.copy2(f, DST_SWIFT / f.name)
        copied += 1
        per_pair[(acc, ccy)] += 1

    return copied, skipped, dict(per_pair), dict(skipped_reasons)


def main():
    from db import DB_PATH
    flex_acs, swift_pairs = load_registered(DB_PATH)
    print(f"Registered flex accounts:  {len(flex_acs)}")
    print(f"Registered (swift, ccy) pairs: {len(swift_pairs)}")

    print("\n--- Flexcube ---")
    fc_copied, fc_skipped, fc_missing = copy_flex(flex_acs)
    print(f"  Copied to messages/flexcube/:      {fc_copied}")
    print(f"  Skipped (unregistered in zip):     {fc_skipped}")
    if fc_missing:
        print(f"  Registered but not in zip:         {len(fc_missing)}")
        for a in fc_missing:
            print(f"    {a}")

    print("\n--- SWIFT ---")
    sw_copied, sw_skipped, sw_per_pair, sw_reasons = copy_swift(swift_pairs)
    print(f"  Copied to messages/swift/:         {sw_copied}")
    print(f"  Skipped (not registered / format): {sw_skipped}")
    print(f"  Skipped breakdown: {sw_reasons}")

    if sw_per_pair:
        print("\n  Files-per-registered-pair (top 15):")
        for (acc, ccy), n in sorted(sw_per_pair.items(), key=lambda x: -x[1])[:15]:
            print(f"    {n:3d}  {acc}  {ccy}")

    # Which registered SWIFT pairs got ZERO files today?
    no_file_pairs = [p for p in swift_pairs if p not in sw_per_pair]
    if no_file_pairs:
        print(f"\n  Registered SWIFT pairs with NO file in today's spool: "
              f"{len(no_file_pairs)}")
        for acc, ccy in sorted(no_file_pairs):
            print(f"    {acc}  {ccy}")

    print("\n--- Intake folder state after copy ---")
    n_flex = len(list(DST_FLEX.iterdir()))
    n_swift = len(list(DST_SWIFT.iterdir()))
    print(f"  messages/flexcube/: {n_flex} files")
    print(f"  messages/swift/:    {n_swift} files")


if __name__ == '__main__':
    main()
