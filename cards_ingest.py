"""
cards_ingest.py — settlement-file ingest for the cards module.

Mirrors `ingest.py` for the cash side, but handles the cards-specific
flow: one settlement file per ingest (no pair), routed through the
PCI-safe parsers in `cards_loaders/`.

Scope of this pass:
    * CSV settlement files via the existing BYO format profile machinery,
      adapted by `cards_loaders.csv_generic.parse_byo`. This covers most
      West African switches (GhIPSS, NIBSS Verve, Cardlink) and any
      issuer that exports settlement as CSV.
    * Visa Base II and Mastercard IPM stay stubbed — see CARDS_DESIGN.md
      for why we don't build those speculatively.

Flow:
    1. SHA-256 the file, dedup against `card_settlement_files.sha256`.
    2. Route to the matching parser. For now: CSV via profile.
    3. Persist one `card_settlement_files` row + N `card_settlement_records`
       rows in a single transaction.

PCI posture: full PAN never reaches this module. Parsers mask before
returning records; the schema has no column for full PAN.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from byo_csv_loader import CsvProfile, load_csv as load_byo_csv
from cards_loaders.csv_generic import parse_byo as cards_parse_byo
from pci_safety import RefusedPanError


VALID_SCHEMES = ('visa', 'mastercard', 'verve', 'gh_cardlink', 'other')
VALID_ROLES   = ('issuer', 'acquirer', 'switch')
VALID_STAGES  = ('auth', 'clearing', 'settlement')


class CardsIngestError(Exception):
    """Something is wrong with the file. Message is user-facing."""


class DuplicateCardFileError(CardsIngestError):
    def __init__(self, sha: str, prior_file_id: int):
        self.sha = sha
        self.prior_file_id = prior_file_id
        super().__init__(
            f"Settlement file already ingested as card_settlement_files row "
            f"{prior_file_id} (sha256={sha[:12]}…)")


@dataclass
class CardsIngestResult:
    file_id: int
    scheme: str
    role: str
    stage: str | None
    record_count: int
    total_amount: float
    currency: str | None
    settlement_date: str | None
    skipped_records: int   # parser returned fewer rows than the source had


def ingest_card_settlement(
    *, file_path: Path,
    scheme: str,
    role: str,
    settlement_date: str | None,
    currency: str | None,
    ingested_by: str,
    original_filename: str | None = None,
    profile_id: int | None = None,
    pan_field: str | None = None,
    pan_masked_field: str | None = None,
    notes: str | None = None,
    stage: str | None = None,
) -> CardsIngestResult:
    """Ingest one cards settlement file. Currently CSV-only via profile.

    Visa Base II / Mastercard IPM ingest will dispatch on `scheme` once
    those parsers leave stub status — this is the entry point that
    will route to them.
    """
    if scheme not in VALID_SCHEMES:
        raise CardsIngestError(
            f"scheme must be one of {VALID_SCHEMES}, got {scheme!r}")
    if role not in VALID_ROLES:
        raise CardsIngestError(
            f"role must be one of {VALID_ROLES}, got {role!r}")
    if stage is not None and stage not in VALID_STAGES:
        raise CardsIngestError(
            f"stage must be one of {VALID_STAGES}, got {stage!r}")
    if profile_id is None:
        # Without a profile we'd need a Visa/Mastercard binary parser,
        # which is still stubbed. Tell the operator clearly.
        raise CardsIngestError(
            "CSV settlement files require a BYO profile_id today. "
            "Visa Base II and Mastercard IPM parsers are stubs awaiting "
            "scheme-published synthetic test data — see "
            "docs/CARDS_DESIGN.md.")

    content = file_path.read_bytes()
    sha = hashlib.sha256(content).hexdigest()

    # Lazy import to keep cycles manageable.
    from db import get_conn
    conn = get_conn()
    try:
        dup = conn.execute(
            "SELECT id FROM card_settlement_files WHERE sha256=?", (sha,),
        ).fetchone()
        if dup:
            raise DuplicateCardFileError(sha, dup['id'])

        # Load the CSV via the BYO profile. Same machinery as the cash
        # side — operators reuse the column-mapping wizard they already
        # know.
        prof_row = conn.execute(
            "SELECT * FROM csv_format_profiles WHERE id=? AND active=1",
            (profile_id,),
        ).fetchone()
        if prof_row is None:
            raise CardsIngestError(
                f"CSV profile {profile_id} not found or inactive.")
        profile = CsvProfile.from_db(dict(prof_row))

        try:
            csv_result = load_byo_csv(content, profile)
        except ValueError as exc:
            raise CardsIngestError(
                f"BYO CSV profile rejected the file: {exc}") from exc

        if not csv_result.txns and csv_result.errors:
            first = csv_result.errors[0]
            raise CardsIngestError(
                f"CSV produced no rows. First error on row {first[0]}: "
                f"{first[1]}")

        try:
            parsed = cards_parse_byo(
                csv_result.txns,
                scheme=scheme, role=role,
                settlement_date=settlement_date,
                currency=currency,
                pan_field=pan_field,
                pan_masked_field=pan_masked_field,
            )
        except RefusedPanError as exc:
            # A row carried a full PAN in a free-text field that survived
            # the redaction pass — refuse the whole file rather than
            # half-persist. The operator must fix the export.
            raise CardsIngestError(
                f"File refused: a record contains an embedded PAN. {exc}")

        skipped = len(csv_result.txns) - parsed.file_meta['record_count']

        # Persist file row + records in one transaction. SQLite default
        # is autocommit per connection; using a `with conn:` block (or
        # explicit BEGIN) wraps the inserts atomically.
        now = datetime.utcnow().isoformat()
        with conn:
            cur = conn.execute(
                "INSERT INTO card_settlement_files "
                "(sha256, scheme, role, stage, file_id, processing_date, "
                "settlement_date, record_count, total_amount, currency, "
                "original_filename, ingested_at, ingested_by, notes) "
                "VALUES (?, ?, ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    sha, scheme, role, stage,
                    settlement_date or now[:10],
                    settlement_date,
                    parsed.file_meta['record_count'],
                    parsed.file_meta.get('total_amount') or 0.0,
                    (currency or '').upper() or None,
                    original_filename,
                    now,
                    ingested_by,
                    notes,
                ),
            )
            file_id = cur.lastrowid

            for r in parsed.records:
                conn.execute(
                    "INSERT INTO card_settlement_records "
                    "(file_id, record_index, pan_first6, pan_last4, "
                    "scheme_ref, auth_code, merchant_id, merchant_name, "
                    "mcc, terminal_id, transaction_type, "
                    "amount_settlement, currency_settlement, "
                    "amount_transaction, currency_transaction, fx_rate, "
                    "fee_total, transaction_date, settlement_date, "
                    "recon_status, notes) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                    "?, ?, ?, ?, ?, ?)",
                    (
                        file_id, r['record_index'],
                        r['pan_first6'], r['pan_last4'],
                        r['scheme_ref'], r['auth_code'],
                        r['merchant_id'], r['merchant_name'],
                        r['mcc'], r['terminal_id'],
                        r['transaction_type'],
                        r['amount_settlement'], r['currency_settlement'],
                        r['amount_transaction'], r['currency_transaction'],
                        r['fx_rate'], r['fee_total'],
                        r['transaction_date'], r['settlement_date'],
                        r['recon_status'], r['notes'],
                    ),
                )

        return CardsIngestResult(
            file_id=file_id,
            scheme=scheme, role=role,
            stage=stage,
            record_count=parsed.file_meta['record_count'],
            total_amount=parsed.file_meta.get('total_amount') or 0.0,
            currency=(currency or '').upper() or None,
            settlement_date=settlement_date,
            skipped_records=max(0, skipped),
        )
    finally:
        conn.close()
