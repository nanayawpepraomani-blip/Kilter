"""Scan a SWIFT spool folder and compare against yesterday's."""

import sys
from pathlib import Path
from collections import defaultdict, Counter

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.stdout.reconfigure(encoding='utf-8')

from swift_loader import extract_swift_meta_raw


def scan(folder: Path):
    by_account = defaultdict(lambda: {'count': 0, 'currencies': Counter(),
                                       'bics': Counter(), 'sample_file': None,
                                       'min_date': None, 'max_date': None})
    errors = []
    for f in sorted(folder.glob('*.out')):
        try:
            meta = extract_swift_meta_raw(f)
        except Exception as e:
            errors.append((f.name, str(e)[:80])); continue
        acc, ccy, bic = meta.get('account'), meta.get('currency'), meta.get('bic')
        if not acc:
            errors.append((f.name, "no :25:")); continue
        d = by_account[acc]; d['count'] += 1
        if ccy: d['currencies'][ccy] += 1
        if bic: d['bics'][bic] += 1
        if d['sample_file'] is None: d['sample_file'] = f.name
        ob = meta.get('opening_balance_date')
        if ob:
            if d['min_date'] is None or ob < d['min_date']: d['min_date'] = ob
            if d['max_date'] is None or ob > d['max_date']: d['max_date'] = ob
    return by_account, errors


d21 = Path('C:/Users/NYPO/Desktop/21042026/21042026')
d22 = Path('C:/Users/NYPO/Desktop/22042026/22042026')

b21, e21 = scan(d21)
b22, e22 = scan(d22)

files21 = sum(d['count'] for d in b21.values())
files22 = sum(d['count'] for d in b22.values())

print("=" * 70)
print(f"21042026 folder: {files21 + len(e21)} files, "
      f"{len(b21)} distinct accounts, {len(e21)} parse errors")
print(f"22042026 folder: {files22 + len(e22)} files, "
      f"{len(b22)} distinct accounts, {len(e22)} parse errors")
print()

# accounts seen in one vs both
pairs21 = set((a, c) for a, d in b21.items() for c in d['currencies'])
pairs22 = set((a, c) for a, d in b22.items() for c in d['currencies'])

print(f"Account-currency pairs in 21042026 only: {len(pairs21 - pairs22)}")
print(f"Account-currency pairs in 22042026 only: {len(pairs22 - pairs21)}")
print(f"Account-currency pairs in both: {len(pairs21 & pairs22)}")
print(f"Total UNION pairs across both days: {len(pairs21 | pairs22)}")

# BIC universe
bics21 = Counter()
for d in b21.values():
    for b, n in d['bics'].items():
        bics21[b] += n
bics22 = Counter()
for d in b22.values():
    for b, n in d['bics'].items():
        bics22[b] += n

print(f"\nDistinct BICs in 21042026: {len(bics21)}")
print(f"Distinct BICs in 22042026: {len(bics22)}")
print(f"BICs in 21042026 but NOT in 22042026 (new today vs yesterday):")
for b, n in sorted(set(bics21) - set(bics22)):
    # error - need to format properly
    pass
new21_only = [(b, bics21[b]) for b in bics21 if b not in bics22]
new22_only = [(b, bics22[b]) for b in bics22 if b not in bics21]
print(f"  in 21042026 only: {[(b,n) for b,n in sorted(new21_only, key=lambda x:-x[1])]}")
print(f"  in 22042026 only: {[(b,n) for b,n in sorted(new22_only, key=lambda x:-x[1])]}")

# Check for Bank of Ghana in either
print(f"\nSearch for BoG-related BICs (anything with GHA but NOT ECOC/BAGH):")
for b in sorted(set(bics21) | set(bics22)):
    if 'GHA' in b and not b.startswith('ECOC') and not b.startswith('BAGH'):
        print(f"  {b}  21day={bics21[b]}  22day={bics22[b]}")

# Top BICs combined
print(f"\nTop BICs combined across both days:")
combined = Counter()
for b, n in bics21.items(): combined[b] += n
for b, n in bics22.items(): combined[b] += n
for b, n in combined.most_common(20):
    print(f"  {b}  total={n}  (21day={bics21[b]}  22day={bics22[b]})")

# Accounts new in 21042026 that weren't in 22042026 — these extend our match universe
print(f"\nSample of (account,ccy) pairs seen 21042026 but not 22042026:")
new_pairs = sorted(pairs21 - pairs22)
for a, c in new_pairs[:20]:
    d = b21[a]
    bic = ','.join(d['bics'].keys())
    print(f"  {a}  {c}  {bic}  ({d['count']} files)")
print(f"  ... and {max(0, len(new_pairs)-20)} more")
