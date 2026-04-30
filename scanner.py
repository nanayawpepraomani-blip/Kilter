"""
scanner.py
==========

Scans messages/swift/ and messages/flexcube/ for new files, pairs them
by registered account, and ingests each pair. One call = one sweep.

Folder layout under D:\\Kilter\\messages\\:

    swift\\                      Drop parsed SWIFT xlsx here.
    flexcube\\                   Drop Flexcube acc_entries xlsx here.
    processed\\swift\\           Successfully ingested SWIFT files land here.
    processed\\flexcube\\        Successfully ingested Flexcube files land here.
    unloaded\\duplicate\\        Content hash already ingested.
    unloaded\\no_partner\\       File has no matching partner in this sweep.
    unloaded\\unregistered\\     Account pair not in the accounts registry.
    unloaded\\mismatch\\         Currency mismatch or other validation failure.

Pairing logic:
    1. For each file on both sides, compute SHA-256 and peek at metadata.
       Dupes → straight to unloaded\\duplicate\\ with a note.
    2. Look up the account registry by swift_account (SWIFT side) or
       flex_ac_no (Flex side). Files whose account is not registered go
       to unloaded\\unregistered\\ — we can't safely pair an unknown
       pairing via folder drop.
    3. For each registered account with files on both sides, pair them
       1-to-1 by sorted filename (same strategy reconcile.py main uses).
    4. Unpaired leftovers (one side has more files than the other) go
       to unloaded\\no_partner\\.

Idempotent: running the scan twice in a row does nothing the second time
because every ingested file's hash is already in the DB, so dupes are
caught and moved before any new session is created.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path

from account_meta import extract_flex_meta, extract_swift_meta
from db import get_conn
from ingest import IngestError, IngestResult, DuplicateFileError, ingest_pair, sha256_of
from iso20022_loader import extract_camt_meta_raw
from reconcile import load_flexcube
from swift_loader import extract_swift_meta_raw


SWIFT_SUFFIXES = ('.out', '.xlsx', '.xlsm', '.xml', '.txt')
# .csv routed through BYO format profiles (csv_format_profiles.filename_pattern).
# .txt accepted alongside .csv because some banks ship CSV-shaped data with
# a .txt extension; the profile decides delimiter regardless of extension.
FLEX_SUFFIXES = ('.xlsx', '.xlsm', '.csv', '.txt')


MESSAGES_DIR = Path(__file__).resolve().parent / 'messages'
SWIFT_IN = MESSAGES_DIR / 'swift'
FLEX_IN = MESSAGES_DIR / 'flexcube'
PROCESSED_SWIFT = MESSAGES_DIR / 'processed' / 'swift'
PROCESSED_FLEX = MESSAGES_DIR / 'processed' / 'flexcube'
UNLOADED_DUPLICATE = MESSAGES_DIR / 'unloaded' / 'duplicate'
UNLOADED_NO_PARTNER = MESSAGES_DIR / 'unloaded' / 'no_partner'
UNLOADED_UNREGISTERED = MESSAGES_DIR / 'unloaded' / 'unregistered'
UNLOADED_MISMATCH = MESSAGES_DIR / 'unloaded' / 'mismatch'


@dataclass
class ScanOutcome:
    file: str
    kind: str
    status: str       # 'ingested' | 'duplicate' | 'unregistered' | 'no_partner' | 'mismatch' | 'error'
    reason: str
    session_id: int | None = None
    moved_to: str | None = None


@dataclass
class ScanReport:
    sessions_created: list[int] = field(default_factory=list)
    outcomes: list[ScanOutcome] = field(default_factory=list)

    @property
    def counts(self) -> dict:
        c: dict[str, int] = {}
        for o in self.outcomes:
            c[o.status] = c.get(o.status, 0) + 1
        return c


def ensure_dirs() -> None:
    for d in (SWIFT_IN, FLEX_IN, PROCESSED_SWIFT, PROCESSED_FLEX,
              UNLOADED_DUPLICATE, UNLOADED_NO_PARTNER,
              UNLOADED_UNREGISTERED, UNLOADED_MISMATCH):
        d.mkdir(parents=True, exist_ok=True)


def scan(user: str = 'auto-scan') -> ScanReport:
    ensure_dirs()
    report = ScanReport()

    swift_files = _list_files(SWIFT_IN, SWIFT_SUFFIXES)
    flex_files = _list_files(FLEX_IN, FLEX_SUFFIXES)

    # Phase 1: triage by hash (dupes out) and by account registry match.
    registry = _load_registry()
    swift_by_account: dict[tuple, list[Path]] = {}
    flex_by_account: dict[tuple, list[Path]] = {}
    # Per-file BYO profile lookup. We populate this in _triage_flex when
    # the file is a CSV matched against an active csv_format_profiles row;
    # _ingest_one reads it back to pass `flex_profile_id` to ingest_pair.
    # Keyed by str(Path) so we don't fight Path equality semantics across
    # platforms.
    flex_profile_for: dict[str, int | None] = {}
    profiles = _load_active_csv_profiles()

    for p in swift_files:
        _triage_swift(p, registry, swift_by_account, report)
    for p in flex_files:
        _triage_flex(p, registry, flex_by_account, report, profiles, flex_profile_for)

    # Phase 2: pair and ingest.
    all_keys = set(swift_by_account) | set(flex_by_account)
    for key in all_keys:
        s_list = sorted(swift_by_account.get(key, []))
        f_list = sorted(flex_by_account.get(key, []))
        pair_n = min(len(s_list), len(f_list))
        for s_path, f_path in zip(s_list[:pair_n], f_list[:pair_n]):
            _ingest_one(s_path, f_path, user, report,
                        flex_profile_id=flex_profile_for.get(str(f_path)))
        # Leftovers with no partner.
        for p in s_list[pair_n:]:
            _move(p, UNLOADED_NO_PARTNER, report, 'swift', 'no_partner',
                  reason=f"No matching Flexcube file for account {key[0]} {key[2]}")
        for p in f_list[pair_n:]:
            _move(p, UNLOADED_NO_PARTNER, report, 'flexcube', 'no_partner',
                  reason=f"No matching SWIFT file for account {key[1]} {key[2]}")

    return report


# ---------------------------------------------------------------------------
# Triage — move obvious rejects out before we try to pair.
# ---------------------------------------------------------------------------

def _triage_swift(path: Path, registry: dict, buckets: dict, report: ScanReport) -> None:
    sha = sha256_of(path)
    if _hash_already_ingested(sha):
        _move(path, UNLOADED_DUPLICATE, report, 'swift', 'duplicate',
              reason=f"SHA-256 already ingested: {sha[:12]}...")
        return
    try:
        suffix = path.suffix.lower()
        if suffix == '.out':
            meta = extract_swift_meta_raw(path)
        elif suffix in ('.xml', '.txt'):
            # ISO 20022 camt.053 (EOD statement) or camt.054 (intraday
            # notification). BoG's RTGS feed writes these as .txt on disk,
            # some correspondents as .xml — either way it's XML content.
            meta = extract_camt_meta_raw(path)
        else:
            meta = extract_swift_meta(path)
    except Exception as exc:
        _move(path, UNLOADED_MISMATCH, report, 'swift', 'error',
              reason=f"Couldn't read metadata: {exc}")
        return
    acc = meta.get('account')
    ccy = meta.get('currency')
    if not (acc and ccy):
        _move(path, UNLOADED_MISMATCH, report, 'swift', 'error',
              reason="SWIFT metadata missing account or currency")
        return
    key = _registry_key_from_swift(registry, acc, ccy)
    if key is None:
        _record_discovery('swift', acc, ccy, path.name, bic=meta.get('bic'))
        _move(path, UNLOADED_UNREGISTERED, report, 'swift', 'unregistered',
              reason=f"SWIFT account {acc} ({ccy}) not in accounts registry")
        return
    buckets.setdefault(key, []).append(path)


def _triage_flex(path: Path, registry: dict, buckets: dict, report: ScanReport,
                  profiles: list[dict] | None = None,
                  profile_for: dict[str, int | None] | None = None) -> None:
    sha = sha256_of(path)
    if _hash_already_ingested(sha):
        _move(path, UNLOADED_DUPLICATE, report, 'flexcube', 'duplicate',
              reason=f"SHA-256 already ingested: {sha[:12]}...")
        return

    suffix = path.suffix.lower()
    profile = None
    if suffix in ('.csv', '.txt'):
        # CSV intake — must match an active profile by filename pattern.
        profile = _match_csv_profile(path.name, profiles or [])
        if profile is None:
            _move(path, UNLOADED_UNREGISTERED, report, 'flexcube',
                  'unregistered',
                  reason=f"No CSV format profile matches filename {path.name!r}. "
                         f"Add a profile with a matching filename_pattern, or "
                         f"upload manually with a profile selected.")
            return
        try:
            txns = _load_byo_for_triage(path, profile)
        except Exception as exc:
            _move(path, UNLOADED_MISMATCH, report, 'flexcube', 'error',
                  reason=f"BYO profile {profile['name']!r} could not parse: {exc}")
            return
        meta = extract_flex_meta(txns) if txns else {}
        # Apply same fallbacks as ingest._load_flex_via_profile so the
        # registry-key lookup below sees a complete (ac_no, ccy) pair.
        meta = _apply_profile_fallbacks_to_meta(meta, profile)
    else:
        try:
            txns = load_flexcube(path)
            meta = extract_flex_meta(txns)
        except Exception as exc:
            _move(path, UNLOADED_MISMATCH, report, 'flexcube', 'error',
                  reason=f"Couldn't load: {exc}")
            return

    if meta.get('multi_account'):
        _move(path, UNLOADED_MISMATCH, report, 'flexcube', 'mismatch',
              reason=f"Multiple AC_NOs in file: {meta['all_accounts']}")
        return
    ac = meta.get('ac_no')
    ccy = meta.get('currency')
    if not (ac and ccy):
        _move(path, UNLOADED_MISMATCH, report, 'flexcube', 'error',
              reason="Flexcube file missing AC_NO or ACCT_CCY (and profile "
                     "had no fallback binding to fill them in)")
        return
    key = _registry_key_from_flex(registry, ac, ccy)
    if key is None:
        _record_discovery('flexcube', ac, ccy, path.name)
        _move(path, UNLOADED_UNREGISTERED, report, 'flexcube', 'unregistered',
              reason=f"Flexcube GL {ac} ({ccy}) not in accounts registry")
        return
    buckets.setdefault(key, []).append(path)
    if profile is not None and profile_for is not None:
        profile_for[str(path)] = int(profile['id'])


def _ingest_one(swift_path: Path, flex_path: Path, user: str, report: ScanReport,
                 flex_profile_id: int | None = None) -> None:
    try:
        result: IngestResult = ingest_pair(swift_path, flex_path, user,
                                            flex_profile_id=flex_profile_id)
    except DuplicateFileError as exc:
        # Defensive — shouldn't fire because we pre-hashed, but handle anyway.
        losing_path = swift_path if exc.which == 'SWIFT' else flex_path
        _move(losing_path, UNLOADED_DUPLICATE, report,
              exc.which.lower(), 'duplicate', reason=str(exc))
        # The other file might still be valid — return it to its source folder
        # so the next scan can try to pair it with a different partner.
        return
    except IngestError as exc:
        _move(swift_path, UNLOADED_MISMATCH, report, 'swift', 'mismatch', reason=str(exc))
        _move(flex_path, UNLOADED_MISMATCH, report, 'flexcube', 'mismatch', reason=str(exc))
        return

    # Success — move both files to processed/
    s_dest = _move(swift_path, PROCESSED_SWIFT, report, 'swift', 'ingested',
                   reason=f"Session {result.session_id}", session_id=result.session_id)
    f_dest = _move(flex_path, PROCESSED_FLEX, report, 'flexcube', 'ingested',
                   reason=f"Session {result.session_id}", session_id=result.session_id)
    report.sessions_created.append(result.session_id)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _list_files(d: Path, suffixes: tuple) -> list[Path]:
    if not d.exists():
        return []
    return [p for p in d.iterdir()
            if p.is_file() and p.suffix.lower() in suffixes
            and not p.name.startswith('~$') and not p.name.startswith('.')]


def _record_discovery(kind: str, identifier: str, currency: str,
                      filename: str, bic: str | None = None) -> None:
    """Upsert a discovered-but-unregistered account identifier. Increments
    seen_count on each re-scan so admins can see which accounts keep showing
    up. Already-registered or admin-ignored rows are left alone.

    bic is populated from SWIFT Block 2 for kind='swift'; it stays null for
    kind='flexcube' because Flex files don't carry a correspondent BIC."""
    now = datetime.utcnow().isoformat()
    conn = get_conn()
    try:
        row = conn.execute(
            "SELECT id, status FROM discovered_accounts "
            "WHERE kind=? AND identifier=? AND currency=?",
            (kind, identifier, currency),
        ).fetchone()
        if row is None:
            conn.execute(
                "INSERT INTO discovered_accounts (kind, identifier, currency, "
                "first_seen_at, last_seen_at, seen_count, sample_file, bic, status) "
                "VALUES (?,?,?,?,?,1,?,?, 'pending')",
                (kind, identifier, currency, now, now, filename, bic),
            )
        elif row['status'] == 'pending':
            conn.execute(
                "UPDATE discovered_accounts SET last_seen_at=?, seen_count=seen_count+1, "
                "sample_file=COALESCE(sample_file, ?), bic=COALESCE(bic, ?) WHERE id=?",
                (now, filename, bic, row['id']),
            )
        conn.commit()
    finally:
        conn.close()


def _hash_already_ingested(sha: str) -> bool:
    conn = get_conn()
    try:
        row = conn.execute("SELECT 1 FROM ingested_files WHERE sha256=?", (sha,)).fetchone()
        return row is not None
    finally:
        conn.close()


def _load_registry() -> list[dict]:
    conn = get_conn()
    try:
        return [dict(r) for r in conn.execute(
            "SELECT id, swift_account, flex_ac_no, currency, label FROM accounts "
            "WHERE active=1"
        ).fetchall()]
    finally:
        conn.close()


def _load_active_csv_profiles() -> list[dict]:
    """Pull only profiles that have a filename_pattern set — those are the
    ones intended for scan auto-routing. Profiles without a pattern are
    manual-upload-only by design."""
    conn = get_conn()
    try:
        rows = conn.execute(
            "SELECT p.*, a.flex_ac_no AS bound_ac_no, "
            "       a.currency AS bound_currency "
            "FROM csv_format_profiles p "
            "LEFT JOIN accounts a ON a.id = p.account_id "
            "WHERE p.active=1 AND p.filename_pattern IS NOT NULL "
            "  AND p.filename_pattern != '' "
            "ORDER BY p.name"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _match_csv_profile(filename: str, profiles: list[dict]) -> dict | None:
    """Return the single profile whose filename_pattern matches `filename`,
    or None if zero or more-than-one match. Patterns use fnmatch glob
    rules (case-insensitive on Windows; we lower-case both sides so the
    same profile matches across platforms).

    Returning None on multi-match is deliberate — the operator picked
    overlapping patterns and we'd rather error out than guess wrong."""
    import fnmatch
    name_lc = filename.lower()
    hits = [p for p in profiles
            if fnmatch.fnmatch(name_lc, (p.get('filename_pattern') or '').lower())]
    if len(hits) == 1:
        return hits[0]
    return None


def _load_byo_for_triage(path: Path, profile_row: dict) -> list[dict]:
    """Run the BYO loader against `path` to produce canonical Flex txns
    suitable for extract_flex_meta. Same parsing the real ingest does;
    we run it twice (once here, once in ingest_pair) and accept the cost
    because the alternative is sharing mutable state through the pipeline.
    Files this size parse in milliseconds."""
    from byo_csv_loader import CsvProfile, load_csv as load_byo_csv
    profile = CsvProfile.from_db(profile_row)
    content = path.read_bytes()
    result = load_byo_csv(content, profile)
    if not result.txns and result.errors:
        first = result.errors[0]
        raise RuntimeError(f"row {first[0]}: {first[1]}")
    return result.txns


def _apply_profile_fallbacks_to_meta(meta: dict, profile_row: dict) -> dict:
    """Fill in ac_no / currency from the profile's bound account when
    the data didn't carry them. Same logic as ingest._load_flex_via_profile,
    re-applied here so the registry-key lookup in _triage_flex sees a
    complete pair."""
    out = dict(meta)
    if not out.get('ac_no') and profile_row.get('bound_ac_no'):
        out['ac_no'] = profile_row['bound_ac_no']
    if not out.get('currency'):
        out['currency'] = (profile_row.get('currency')
                           or profile_row.get('bound_currency'))
    return out


def _registry_key_from_swift(registry: list[dict], swift_account: str, ccy: str):
    for r in registry:
        if r['swift_account'] == swift_account and r['currency'] == ccy:
            return (r['swift_account'], r['flex_ac_no'], r['currency'])
    return None


def _registry_key_from_flex(registry: list[dict], flex_ac_no: str, ccy: str):
    for r in registry:
        if r['flex_ac_no'] == flex_ac_no and r['currency'] == ccy:
            return (r['swift_account'], r['flex_ac_no'], r['currency'])
    return None


def _move(src: Path, dest_dir: Path, report: ScanReport,
          kind: str, status: str, *, reason: str,
          session_id: int | None = None) -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    target = dest_dir / src.name
    # Don't clobber — append a counter if a file with this name already moved here.
    i = 1
    while target.exists():
        target = dest_dir / f"{src.stem}__{i}{src.suffix}"
        i += 1
    shutil.move(str(src), str(target))
    report.outcomes.append(ScanOutcome(
        file=src.name, kind=kind, status=status, reason=reason,
        session_id=session_id, moved_to=str(target.relative_to(MESSAGES_DIR)),
    ))
    return target
