"""
db.py
=====

SQLite schema and connection helper for Kilter.

The DB lives in kilter.db next to this file. init_db() is idempotent —
safe to call on every startup. Tables:

    sessions       One row per upload. Holds filenames and status.
    swift_txns     Full SWIFT rows, keyed by (session_id, row_number).
    flex_txns      Full Flexcube rows, same key shape.
    candidates     Every plausible pair the engine proposed — kept so the
                   UI can offer 'swap' options without re-running.
    assignments    The winning pair per SWIFT row. status = pending /
                   confirmed / rejected. Audit fields for who/when decided.
    audit_log      Append-only trail of actions (session creation,
                   decisions, exports). Details field is a JSON blob.
"""

import os
import sqlite3
from pathlib import Path

# DB location is env-overridable so containerised deployments can mount the
# database file on a persistent volume (e.g. /data/kilter.db). Defaults to
# kilter.db next to this file for local / dev runs.
DB_PATH = Path(os.environ.get('KILTER_DB_PATH') or Path(__file__).resolve().parent / 'kilter.db')

SCHEMA = """
CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TEXT NOT NULL,
    created_by TEXT NOT NULL,
    swift_filename TEXT NOT NULL,
    flex_filename TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'open',
    swift_account TEXT,
    swift_currency TEXT,
    swift_statement_ref TEXT,
    flex_ac_no TEXT,
    flex_ac_branch TEXT,
    flex_currency TEXT,
    account_id INTEGER,
    account_label TEXT,
    -- The CSV format profile used to load the Flex side, or NULL when
    -- the default Flexcube xlsx loader handled it. Lets the recent-
    -- sessions UI surface the source per session and filter by profile.
    flex_profile_id INTEGER REFERENCES csv_format_profiles(id),
    FOREIGN KEY (account_id) REFERENCES accounts(id)
);

CREATE TABLE IF NOT EXISTS accounts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    label TEXT NOT NULL,
    shortname TEXT,
    access_area TEXT,
    bic TEXT,
    swift_account TEXT NOT NULL,
    flex_ac_no TEXT NOT NULL,
    currency TEXT NOT NULL,
    notes TEXT,
    active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    created_by TEXT NOT NULL,
    -- Domain selector. 'cash_nostro' = traditional correspondent-banking
    -- account (the original Kilter use-case). 'mobile_wallet' = mobile-money
    -- operator wallet (M-Pesa / MTN MoMo / Airtel Money etc.). The recon
    -- engine itself doesn't care; the field drives UI grouping, default
    -- BYO profile selection, and dashboard filters.
    account_type TEXT NOT NULL DEFAULT 'cash_nostro',
    -- For mobile_wallet rows: which network. 'mpesa', 'mtn_momo',
    -- 'airtel_money'. NULL for cash_nostro. Open vocabulary so a new
    -- wallet operator doesn't require a code change to onboard.
    provider TEXT,
    -- The wallet's primary identifiers. msisdn = subscriber phone
    -- (E.164 stored without leading +). short_code = paybill/till number
    -- some operators use as the merchant identifier. Either or both may
    -- be set for a wallet; both NULL for cash_nostro.
    msisdn TEXT,
    short_code TEXT,
    UNIQUE (swift_account, flex_ac_no, currency)
);

-- swift_txns: holds the "left-hand side" of every reconciliation pair.
--
-- For TWO-SIDED sessions (session_kind='recon'): real SWIFT statement
--   rows (MT940/MT950/CAMT). Matched against flex_txns Flexcube rows.
--
-- For ONE-SIDED sessions (session_kind='seed' or 'flex_delta', e.g. the
--   BTW Bank-to-Wallet GL): there is NO SWIFT involved. Ingest's
--   _split_flex_for_self_match() in ingest.py reshapes the Flexcube
--   file's DR rows into swift-shape and parks them here so the engine
--   can match them against flex_txns CR rows as a self-match.
--
--   Operator-facing surfaces (UI labels, xlsx exports, error messages,
--   chat replies) MUST translate "swift_txns row" → "Flexcube DR leg"
--   on one-sided sessions. Calling it "SWIFT" outside the engine is
--   wrong. See ingest.py::_split_flex_for_self_match and
--   recon_engine.py module docstring for the same warning.
CREATE TABLE IF NOT EXISTS swift_txns (
    session_id INTEGER NOT NULL,
    row_number INTEGER NOT NULL,
    value_date INTEGER,
    amount REAL,
    sign TEXT,
    origin TEXT,
    type TEXT,
    status TEXT,
    book_date INTEGER,
    our_ref TEXT,
    their_ref TEXT,
    booking_text_1 TEXT,
    booking_text_2 TEXT,
    PRIMARY KEY (session_id, row_number),
    FOREIGN KEY (session_id) REFERENCES sessions(id)
);

CREATE TABLE IF NOT EXISTS flex_txns (
    session_id INTEGER NOT NULL,
    row_number INTEGER NOT NULL,
    trn_ref TEXT,
    ac_branch TEXT,
    ac_no TEXT,
    booking_date INTEGER,
    value_date INTEGER,
    type TEXT,
    narration TEXT,
    amount REAL,
    ccy TEXT,
    module TEXT,
    external_ref TEXT,
    user_id TEXT,
    PRIMARY KEY (session_id, row_number),
    FOREIGN KEY (session_id) REFERENCES sessions(id)
);

CREATE TABLE IF NOT EXISTS candidates (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL,
    swift_row INTEGER NOT NULL,
    flex_row INTEGER NOT NULL,
    tier INTEGER NOT NULL,
    reason TEXT NOT NULL,
    amount_diff REAL NOT NULL,
    FOREIGN KEY (session_id) REFERENCES sessions(id)
);

CREATE INDEX IF NOT EXISTS idx_candidates_session ON candidates(session_id);
CREATE INDEX IF NOT EXISTS idx_candidates_lookup ON candidates(session_id, swift_row, flex_row);

CREATE TABLE IF NOT EXISTS assignments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL,
    swift_row INTEGER NOT NULL,
    flex_row INTEGER NOT NULL,
    tier INTEGER NOT NULL,
    reason TEXT NOT NULL,
    amount_diff REAL NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    decided_by TEXT,
    decided_at TEXT,
    -- Case management. assignee = the operator who has claimed the case
    -- (NULL = unclaimed, in the general queue). due_date = ISO date the
    -- case is expected to be resolved by (NULL = no SLA). priority is a
    -- low/normal/high/urgent string for filtering.
    assignee TEXT,
    due_date TEXT,
    priority TEXT NOT NULL DEFAULT 'normal',
    FOREIGN KEY (session_id) REFERENCES sessions(id)
);

CREATE INDEX IF NOT EXISTS idx_assignments_session_status ON assignments(session_id, status);
-- The (assignee, status) index lives in the migration block — it references
-- a column added by _ensure_columns, and putting CREATE INDEX here would
-- explode on legacy DBs where the column hasn't been migrated in yet
-- (executescript runs before _ensure_columns, so the column doesn't exist
-- when this script runs against a pre-case-management DB). See bottom of
-- init_db() for the post-migration index creation.

-- Bring-your-own-format CSV profiles. Each row is a saved column-mapping
-- + parsing-config that turns a non-standard GL extract into the same
-- canonical txn shape the engine expects from a Flex xlsx. Created via
-- the /byo-formats wizard; selected at ingest time when the file is a
-- CSV not matching the built-in parsers.
CREATE TABLE IF NOT EXISTS csv_format_profiles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL UNIQUE,
    -- Only 'flex' supported initially. The SWIFT side is handled by
    -- native MT/camt parsers; banks rarely send their nostro statements
    -- as CSV, so we draw the line here.
    side TEXT NOT NULL DEFAULT 'flex',
    delimiter TEXT NOT NULL DEFAULT ',',         -- ',' ';' '\t' '|'
    header_row INTEGER NOT NULL DEFAULT 1,       -- 1-based; row that holds column names
    skip_rows INTEGER NOT NULL DEFAULT 0,        -- preamble rows above the header
    date_format TEXT NOT NULL DEFAULT '%Y-%m-%d',
    currency TEXT,                               -- ISO; null if pulled from a column or inherited from bound account
    -- JSON object: {amount, value_date, ref, narration, type, currency, ac_no}
    -- Each value is the source column name (e.g. "Posting Amount") or null
    -- when the field isn't present in this format. The loader handles
    -- missing optional fields by emitting empty strings.
    column_map TEXT NOT NULL,
    -- 'positive_credit' = positive amount means CR, negative DR
    -- 'separate_column' = read sign_column for 'CR'/'DR' / '+' / '-'
    -- 'cr_dr_column'    = the type column already contains 'CR' or 'DR'
    sign_convention TEXT NOT NULL DEFAULT 'positive_credit',
    sign_column TEXT,                            -- only used when sign_convention != positive_credit
    -- Optional binding to a specific account. When set, files using this
    -- profile are routed to this account regardless of whether the file
    -- contains an account-number column. Currency falls back to the
    -- account's currency when neither profile.currency nor a currency
    -- column is set. Most banks send one CSV format per account, so this
    -- removes the requirement to repeat that information in every file.
    account_id INTEGER REFERENCES accounts(id),
    -- Glob pattern for scanner intake. When the daily scan finds a .csv
    -- in messages/flexcube/ matching this pattern, it ingests using
    -- this profile. Examples: 'acme_gl_*.csv', '*.tsv'. Null = profile
    -- is manual-upload-only.
    filename_pattern TEXT,
    sample_filename TEXT,
    created_by TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT,
    active INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX IF NOT EXISTS idx_csv_profiles_active ON csv_format_profiles(active, name);

-- ---------------------------------------------------------------------------
-- Cards module — PCI-safe schema.
--
-- Storage rule: full PAN is NEVER persisted to either of these tables.
-- PCI-DSS permits storing first-6 (BIN) and last-4 separately or together
-- without putting the storage in scope. Loaders MUST mask full PANs at
-- the parser layer before any INSERT — see pci_safety.mask_pan().
--
-- Schemes covered (or planned):
--   * visa         — Visa Base II clearing
--   * mastercard   — Mastercard IPM (TC items via CMF)
--   * verve        — Interswitch Verve (Nigeria)
--   * gh_cardlink  — Ghana Cardlink switch
--   * other        — generic CSV via BYO profile
--
-- Each settlement file is one row in card_settlement_files; its
-- transactions live in card_settlement_records joined by file_id.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS card_settlement_files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sha256 TEXT NOT NULL UNIQUE,        -- de-dup guard, same idea as ingested_files
    scheme TEXT NOT NULL,               -- visa | mastercard | verve | gh_cardlink | other
    role TEXT NOT NULL,                 -- 'issuer' | 'acquirer' | 'switch'
    file_id TEXT,                       -- scheme-assigned file identifier (Visa file ID, Mastercard file ID)
    processing_date TEXT NOT NULL,      -- YYYY-MM-DD when scheme generated the file
    settlement_date TEXT,               -- YYYY-MM-DD when funds move
    record_count INTEGER NOT NULL DEFAULT 0,
    total_amount REAL,                  -- sum of record amounts in settlement currency
    currency TEXT,                      -- ISO 4217
    original_filename TEXT,
    ingested_at TEXT NOT NULL,
    ingested_by TEXT NOT NULL,
    notes TEXT
);

CREATE INDEX IF NOT EXISTS idx_card_files_scheme_date
    ON card_settlement_files(scheme, settlement_date DESC);

CREATE TABLE IF NOT EXISTS card_settlement_records (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    file_id INTEGER NOT NULL REFERENCES card_settlement_files(id),
    record_index INTEGER NOT NULL,      -- 1-based row number within the file
    -- ---- PCI-safe card identifiers ----
    -- BIN (first 6 digits) — out of PCI-DSS storage scope.
    pan_first6 TEXT,
    -- Last 4 — out of PCI-DSS storage scope.
    pan_last4 TEXT,
    -- ---- Transaction identifiers ----
    -- Scheme-assigned transaction reference — Visa: Transaction
    -- Reference Number (TRR/ARN); Mastercard: Banknet Reference Number.
    -- This is the primary key for 3-way matching (auth → clearing →
    -- settlement) so it MUST be present and unique within a settlement
    -- batch.
    scheme_ref TEXT NOT NULL,
    auth_code TEXT,                     -- 6-digit issuer auth code (PCI-out-of-scope)
    -- ---- Merchant context ----
    merchant_id TEXT,
    merchant_name TEXT,
    mcc TEXT,                           -- merchant category code (4 digits)
    terminal_id TEXT,
    -- ---- Transaction context ----
    transaction_type TEXT,              -- purchase | refund | cash_advance | chargeback | fee
    -- ---- Amounts ----
    amount_settlement REAL NOT NULL,    -- in settlement currency
    currency_settlement TEXT NOT NULL,  -- ISO 4217
    amount_transaction REAL,            -- in original transaction currency (for cross-border)
    currency_transaction TEXT,
    fx_rate REAL,                       -- transaction → settlement
    fee_total REAL NOT NULL DEFAULT 0,  -- interchange + scheme fees combined
    -- ---- Dates ----
    transaction_date TEXT,              -- YYYY-MM-DD when card was presented
    settlement_date TEXT NOT NULL,      -- YYYY-MM-DD when funds settle
    -- ---- Recon state ----
    -- 'matched' = paired with a GL entry; 'unmatched' = needs review;
    -- 'disputed' = chargeback in flight; 'written_off' = approved manually.
    recon_status TEXT NOT NULL DEFAULT 'unmatched',
    matched_at TEXT,
    matched_by TEXT,
    notes TEXT,
    UNIQUE (file_id, record_index)
);

CREATE INDEX IF NOT EXISTS idx_card_records_scheme_ref
    ON card_settlement_records(scheme_ref);
CREATE INDEX IF NOT EXISTS idx_card_records_settlement_date
    ON card_settlement_records(settlement_date);
CREATE INDEX IF NOT EXISTS idx_card_records_pan_last4
    ON card_settlement_records(pan_last4);
CREATE INDEX IF NOT EXISTS idx_card_records_status
    ON card_settlement_records(recon_status, settlement_date);

CREATE TABLE IF NOT EXISTS audit_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER,
    action TEXT NOT NULL,
    actor TEXT NOT NULL,
    timestamp TEXT NOT NULL,
    details TEXT
);

CREATE INDEX IF NOT EXISTS idx_audit_session ON audit_log(session_id);

CREATE TABLE IF NOT EXISTS ingested_files (
    sha256 TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    original_filename TEXT NOT NULL,
    session_id INTEGER,
    ingested_at TEXT NOT NULL,
    ingested_by TEXT NOT NULL,
    FOREIGN KEY (session_id) REFERENCES sessions(id)
);

CREATE TABLE IF NOT EXISTS users (
    username          TEXT PRIMARY KEY,
    display_name      TEXT,
    role              TEXT NOT NULL,   -- admin, ops, audit, internal_control
    active            INTEGER NOT NULL DEFAULT 1,
    created_at        TEXT NOT NULL,
    created_by        TEXT,
    last_seen_at      TEXT,
    totp_secret       TEXT,             -- null until user completes enrollment
    totp_enrolled_at  TEXT,
    enrollment_token  TEXT,             -- one-time; cleared after enrollment
    -- Password layer source. 'local' = TOTP-only login (current default,
    -- preserved for the bootstrap admin). 'ldap' = require AD/LDAP bind
    -- before TOTP. See ldap_auth.py for the bind logic.
    auth_source       TEXT NOT NULL DEFAULT 'local',
    -- DN we successfully bound as on the most recent LDAP login. Recorded
    -- for the audit log; not used for authentication decisions.
    ldap_dn           TEXT
);

CREATE TABLE IF NOT EXISTS user_sessions (
    token         TEXT PRIMARY KEY,
    username      TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    expires_at    TEXT NOT NULL,
    user_agent    TEXT,
    revoked_at    TEXT,
    last_used_at  TEXT,                  -- sliding idle-timeout marker
    FOREIGN KEY (username) REFERENCES users(username)
);

CREATE INDEX IF NOT EXISTS idx_user_sessions_username ON user_sessions(username);

CREATE TABLE IF NOT EXISTS discovered_accounts (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    kind                   TEXT NOT NULL,        -- 'swift' or 'flexcube'
    identifier             TEXT NOT NULL,        -- swift account number or flex AC_NO
    currency               TEXT NOT NULL,
    first_seen_at          TEXT NOT NULL,
    last_seen_at           TEXT NOT NULL,
    seen_count             INTEGER NOT NULL DEFAULT 1,
    sample_file            TEXT,
    status                 TEXT NOT NULL DEFAULT 'pending',  -- pending|registered|ignored
    registered_account_id  INTEGER,
    resolved_at            TEXT,
    resolved_by            TEXT,
    UNIQUE (kind, identifier, currency),
    FOREIGN KEY (registered_account_id) REFERENCES accounts(id)
);

CREATE INDEX IF NOT EXISTS idx_discovered_status ON discovered_accounts(status);

CREATE TABLE IF NOT EXISTS access_areas (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL UNIQUE,
    parent      TEXT,
    active      INTEGER NOT NULL DEFAULT 1,
    created_at  TEXT NOT NULL,
    created_by  TEXT
);

CREATE INDEX IF NOT EXISTS idx_access_areas_active ON access_areas(active);

-- ---------------------------------------------------------------------------
-- Reconciliation-ledger tables (rolling open items, tolerance rules, notes).
-- Added 2026-04-22. Safe to call on every startup — IF NOT EXISTS guards.
-- ---------------------------------------------------------------------------

-- open_items carries single-sided entries across sessions. A row is opened
-- when a SWIFT or Flex txn ends a session unmatched and is closed when a
-- later session produces its counterpart, a user force-matches it, or an
-- admin writes it off. Keyed by account (not session) so the ledger
-- survives file boundaries — this is the heart of the reconciliation flow.
CREATE TABLE IF NOT EXISTS open_items (
    id                     INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id             INTEGER NOT NULL,
    source_side            TEXT NOT NULL,        -- 'swift' or 'flex'
    src_session_id         INTEGER NOT NULL,
    src_row_number         INTEGER NOT NULL,
    value_date             INTEGER,
    amount                 REAL NOT NULL,
    sign                   TEXT NOT NULL,        -- SWIFT 'C'/'D' or Flex 'CR'/'DR'
    ref                    TEXT,
    narration              TEXT,
    category               TEXT,                 -- auto- or user-classified
    category_source        TEXT,                 -- 'auto_rule' | 'manual' | null
    category_rule_id       INTEGER,              -- set when auto-categorized
    status                 TEXT NOT NULL DEFAULT 'open',   -- open|cleared|written_off
    opened_at              TEXT NOT NULL,
    cleared_at             TEXT,
    cleared_by             TEXT,
    cleared_via            TEXT,                 -- 'auto_carry' | 'manual_match' | 'write_off'
    cleared_session_id     INTEGER,              -- session where counterpart arrived
    cleared_assignment_id  INTEGER,
    write_off_reason       TEXT,
    FOREIGN KEY (account_id) REFERENCES accounts(id),
    FOREIGN KEY (src_session_id) REFERENCES sessions(id),
    UNIQUE (account_id, source_side, src_session_id, src_row_number)
);

CREATE INDEX IF NOT EXISTS idx_open_items_account_status ON open_items(account_id, status);
CREATE INDEX IF NOT EXISTS idx_open_items_status_age ON open_items(status, opened_at);

-- break_comments — free-text notes against an assignment or an open_item.
-- Analysts use this to leave "awaiting bank ref XYZ, expected T+2" trails.
CREATE TABLE IF NOT EXISTS break_comments (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    target_type   TEXT NOT NULL,          -- 'assignment' | 'open_item'
    target_id     INTEGER NOT NULL,
    session_id    INTEGER,                -- context for display; NULL for open_item cross-session
    author        TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    body          TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_break_comments_target ON break_comments(target_type, target_id);

-- tolerance_rules — per-account overrides for matching thresholds. Null row
-- means "use defaults". Edited via admin; every change audit-logged.
CREATE TABLE IF NOT EXISTS tolerance_rules (
    account_id     INTEGER PRIMARY KEY,
    amount_tol_abs REAL NOT NULL DEFAULT 0.01,
    amount_tol_pct REAL NOT NULL DEFAULT 0.0,      -- 0.5 = 0.5%
    date_tol_days  INTEGER NOT NULL DEFAULT 1,
    min_ref_len    INTEGER NOT NULL DEFAULT 6,
    updated_at     TEXT,
    updated_by     TEXT,
    FOREIGN KEY (account_id) REFERENCES accounts(id)
);

-- match_tiers — user-editable matching tier definitions. Replaces the
-- hardcoded T1-T4 logic that used to live in recon_engine._classify().
-- Each tier is an ordered list of conditions (all AND'd); the engine
-- walks tiers by priority and returns the first whose conditions all
-- pass for a given (DR, CR) pair.
--
-- Scope:
--   account_id IS NULL  → default tier set for `recon_type` (one_sided,
--                         two_sided, mobile_money, cards). Applied to
--                         any account that doesn't have its own row.
--   account_id IS set   → per-account override; replaces the default
--                         set entirely (not additive).
--
-- conditions_json is a JSON array of objects:
--   [{"field": "value_date", "op": "equal"},
--    {"field": "amount", "op": "equal_within_tol"},
--    {"field": "ref", "op": "symmetric_in_narration"}, ...]
-- See recon_engine.CONDITION_OPS for the supported (field, op, params)
-- combinations.
--
-- legacy_tier preserves the old T1-T4 numbering for the seeded defaults
-- so historical assignments + UI references keep working without a
-- separate translation table.
CREATE TABLE IF NOT EXISTS match_tiers (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id      INTEGER,                      -- NULL = default for recon_type
    recon_type      TEXT NOT NULL,                -- 'one_sided' | 'two_sided' | 'mobile_money' | 'cards'
    name            TEXT NOT NULL,
    priority        INTEGER NOT NULL,             -- lower runs first
    conditions_json TEXT NOT NULL,                -- JSON array of conditions
    enabled         INTEGER NOT NULL DEFAULT 1,
    auto_confirm    INTEGER NOT NULL DEFAULT 0,
    legacy_tier     INTEGER,                      -- 1-4 for seeded defaults; NULL for custom
    created_at      TEXT NOT NULL,
    updated_at      TEXT,
    created_by      TEXT,
    FOREIGN KEY (account_id) REFERENCES accounts(id)
);
CREATE INDEX IF NOT EXISTS idx_match_tiers_lookup
    ON match_tiers(account_id, recon_type, enabled, priority);

-- auto_categorization_rules — simple keyword-based auto-tagging for
-- one-sided items. Evaluated in priority order; first match wins. The
-- audit trail (open_items.category_rule_id + category_source) ensures
-- analysts can tell whether a category came from a rule or a human.
CREATE TABLE IF NOT EXISTS auto_categorization_rules (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    name                 TEXT NOT NULL,
    priority             INTEGER NOT NULL DEFAULT 100,
    side                 TEXT,                -- 'swift' | 'flex' | null = both
    narration_contains   TEXT,                -- case-insensitive substring
    type_equals          TEXT,                -- e.g. 'DR', 'CR', 'C', 'D'
    amount_min           REAL,
    amount_max           REAL,
    category             TEXT NOT NULL,       -- one of BREAK_CATEGORIES below
    active               INTEGER NOT NULL DEFAULT 1,
    created_at           TEXT NOT NULL,
    created_by           TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_auto_cat_active_priority ON auto_categorization_rules(active, priority);

-- auto_grouping_rules — tags each open_item with a functional_group
-- (TREASURY, PSC TROPS, TRADE SWITCHES, GRA, CASH MGT, TPU, ...) to drive
-- the daily ops report's per-team tab layout. Independent of auto_categorization
-- because "what team owns this" is a different axis from "what kind of break".
CREATE TABLE IF NOT EXISTS auto_grouping_rules (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    name                 TEXT NOT NULL,
    priority             INTEGER NOT NULL DEFAULT 100,
    side                 TEXT,                -- 'swift' | 'flex' | null = both
    narration_contains   TEXT,                -- case-insensitive substring; matched against narration + booking texts
    ref_contains         TEXT,                -- case-insensitive substring on our_ref / trn_ref
    type_equals          TEXT,
    amount_min           REAL,
    amount_max           REAL,
    functional_group     TEXT NOT NULL,       -- free-text, drives report tab name
    active               INTEGER NOT NULL DEFAULT 1,
    created_at           TEXT NOT NULL,
    created_by           TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_auto_grp_active_priority ON auto_grouping_rules(active, priority);

-- Managed currency list. Mirrors the ISO-code dropdown in Corona so ops has
-- one canonical picker when assigning a currency to a cash account. Free-text
-- currency on accounts is still allowed for backward-compat; the UI just
-- prefers values from this table.
CREATE TABLE IF NOT EXISTS currencies (
    iso_code       TEXT PRIMARY KEY,     -- 3-letter ISO 4217, e.g. 'GHS'
    name           TEXT NOT NULL,         -- 'GHANIAN CEDIS'
    decimals       INTEGER NOT NULL DEFAULT 2,
    euro_currency  INTEGER NOT NULL DEFAULT 0,   -- 1 if code is the euro itself
    active         INTEGER NOT NULL DEFAULT 1,
    created_at     TEXT NOT NULL,
    created_by     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_currencies_active ON currencies(active);

-- Registered counterparty banks. Mirrors Corona 7.9's Banks registry so ops
-- has one canonical BIC list for the correspondent field on cash accounts.
-- Strict: cash_accounts.bic is only allowed to reference rows in here (UI-
-- enforced; a foreign key is intentionally not enforced so pre-registry
-- historic rows don't break).
CREATE TABLE IF NOT EXISTS banks (
    bic          TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    nickname     TEXT,
    origin       TEXT NOT NULL DEFAULT 'their',   -- 'their' | 'our'
    type         TEXT NOT NULL DEFAULT 'bank',    -- 'bank' | 'broker'
    access_area  TEXT,
    user_code    TEXT,
    active       INTEGER NOT NULL DEFAULT 1,
    created_at   TEXT NOT NULL,
    created_by   TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_banks_active ON banks(active);

-- FX rate registry. Feeds the cross-currency tolerance rule in the match
-- engine AND the Corona-style exchange-rates screen. Pairs are stored both
-- directions (GHS↔USD and USD↔GHS) so the engine can look up either way
-- without arithmetic inversion. Historic rows stay active=0 so audit trails
-- can reference them; the picker only shows active=1.
CREATE TABLE IF NOT EXISTS fx_rates (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    from_ccy     TEXT NOT NULL,
    to_ccy       TEXT NOT NULL,
    rate         REAL NOT NULL,           -- amount in from_ccy * rate = amount in to_ccy
    valid_from   TEXT NOT NULL,            -- ISO date; also used as a version key
    source       TEXT,                     -- 'manual' | 'bog' | 'reuters' | …
    active       INTEGER NOT NULL DEFAULT 1,
    created_at   TEXT NOT NULL,
    created_by   TEXT NOT NULL,
    UNIQUE (from_ccy, to_ccy, valid_from)
);

CREATE INDEX IF NOT EXISTS idx_fx_rates_lookup ON fx_rates(from_ccy, to_ccy, active);

-- Reconciliation certificates — month-end sign-off artefact generated per
-- account+period. The record of generation and sign-off lives here; the
-- actual xlsx is regenerated on demand from the ledger, so it always reflects
-- the current state. Signed certificates become immutable and carry a
-- snapshot (JSON) of the figures at signing time so later ledger changes
-- don't silently rewrite history.
CREATE TABLE IF NOT EXISTS reconciliation_certificates (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id      INTEGER NOT NULL,
    period_start    TEXT NOT NULL,            -- ISO date (first of month)
    period_end      TEXT NOT NULL,            -- ISO date (last of month)
    generated_at    TEXT NOT NULL,
    generated_by    TEXT NOT NULL,
    prepared_by     TEXT,                     -- analyst who prepared (maker)
    prepared_at     TEXT,
    reviewed_by     TEXT,                     -- reviewer (checker)
    reviewed_at     TEXT,
    signed_by       TEXT,                     -- final sign-off (approver)
    signed_at       TEXT,
    status          TEXT NOT NULL DEFAULT 'draft',  -- draft|prepared|reviewed|signed|superseded
    snapshot_json   TEXT,                     -- frozen figures at signing
    FOREIGN KEY (account_id) REFERENCES accounts(id)
);

CREATE INDEX IF NOT EXISTS idx_certs_account_period ON reconciliation_certificates(account_id, period_start, period_end);

-- Notification channels — e-mail / Teams webhook endpoints the SLA checker
-- posts to when open items breach the ageing threshold. Kept deliberately
-- generic so ops can add multiple channels (different teams, different
-- urgency thresholds) without code changes. config_json holds channel-type
-- specific payload (webhook URL for Teams, SMTP details for email).
CREATE TABLE IF NOT EXISTS notification_channels (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    name              TEXT NOT NULL,
    kind              TEXT NOT NULL,            -- 'teams' | 'email' | 'log'
    config_json       TEXT NOT NULL,            -- webhook URL, SMTP conf, etc.
    threshold_days    INTEGER NOT NULL DEFAULT 30,   -- alert when open items > N days
    access_area_filter TEXT,                    -- JSON list, null = all
    active            INTEGER NOT NULL DEFAULT 1,
    last_run_at       TEXT,
    last_result       TEXT,
    created_at        TEXT NOT NULL,
    created_by        TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_channels_active ON notification_channels(active);

-- Scheduled jobs — the "runs itself" backbone. An in-process daemon thread
-- polls this table every 30s; jobs whose next-run time has passed get
-- executed. No external cron dependency — everything lives in the app so
-- ops sees a single source of truth for what auto-ran and when.
CREATE TABLE IF NOT EXISTS scheduled_jobs (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    name           TEXT NOT NULL UNIQUE,
    job_type       TEXT NOT NULL,                 -- 'scan'|'daily_close'|'sla_check'|'flex_extract'|'daily_breaks_report'|'db_backup'|'session_cleanup'
    schedule_kind  TEXT NOT NULL,                 -- 'interval' | 'daily_at'
    interval_minutes INTEGER,                     -- for 'interval': run every N minutes
    daily_at_utc   TEXT,                          -- for 'daily_at': 'HH:MM' in UTC
    params_json    TEXT,                          -- optional per-job args as JSON
    enabled        INTEGER NOT NULL DEFAULT 1,
    last_run_at    TEXT,
    last_run_status TEXT,                         -- 'ok' | 'error' | 'skipped'
    last_run_output TEXT,
    last_run_ms    INTEGER,
    next_run_at    TEXT,
    created_at     TEXT NOT NULL,
    created_by     TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_scheduled_jobs_enabled ON scheduled_jobs(enabled);

-- Job run history — keep the last N runs per job so the admin page can
-- show a trend. Older rows are pruned by the scheduler after each run.
CREATE TABLE IF NOT EXISTS job_runs (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id       INTEGER NOT NULL,
    started_at   TEXT NOT NULL,
    ended_at     TEXT,
    status       TEXT,          -- 'ok' | 'error'
    output       TEXT,          -- human-readable summary
    duration_ms  INTEGER,
    FOREIGN KEY (job_id) REFERENCES scheduled_jobs(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_job_runs_job ON job_runs(job_id, started_at DESC);

-- MFA recovery codes — single-use backup codes issued at enrollment.
-- code_hash is SHA-256(code). The plaintext is shown once and never stored.
CREATE TABLE IF NOT EXISTS user_recovery_codes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    username    TEXT NOT NULL REFERENCES users(username) ON DELETE CASCADE,
    code_hash   TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    used_at     TEXT             -- NULL until consumed; once set, code is dead
);
CREATE INDEX IF NOT EXISTS idx_recovery_codes_username ON user_recovery_codes(username);

-- Approval requests — two-person gate for match decisions.
-- Only created when KILTER_REQUIRE_APPROVAL=true. An ops user confirms
-- (sets assignment to pending_approval), then an admin/internal_control
-- approves or rejects, moving the assignment to confirmed/rejected.
CREATE TABLE IF NOT EXISTS approval_requests (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    assignment_id   INTEGER NOT NULL REFERENCES assignments(id),
    requested_by    TEXT NOT NULL,
    requested_at    TEXT NOT NULL,
    reviewed_by     TEXT,
    reviewed_at     TEXT,
    action          TEXT,   -- 'approved' | 'rejected'
    note            TEXT
);
CREATE INDEX IF NOT EXISTS idx_approval_assignment ON approval_requests(assignment_id);

-- Auto-match rules — operator-defined rules for auto-confirming proposals.
-- Evaluated in priority order (lower number = higher priority) at ingest tail.
-- All non-null conditions must match for the rule to fire.
CREATE TABLE IF NOT EXISTS auto_match_rules (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    name                 TEXT NOT NULL,
    description          TEXT,
    priority             INTEGER NOT NULL DEFAULT 0,
    active               INTEGER NOT NULL DEFAULT 1,
    require_tier         TEXT,             -- '1'|'2'|'3'|'4' or NULL (any tier)
    require_amount_exact INTEGER,          -- 1 = amount_diff must be 0.00
    require_ref_match    INTEGER,          -- 1 = refs must overlap
    max_amount_diff      REAL,             -- NULL = use session tolerance
    require_same_date    INTEGER,          -- 1 = value_date must match exactly
    action               TEXT NOT NULL DEFAULT 'confirm',
    created_by           TEXT NOT NULL,
    created_at           TEXT NOT NULL,
    updated_at           TEXT
);

-- Immutability triggers — no DELETE or UPDATE on the audit trail.
-- These fire even for the admin user at the SQL layer, so the only way to
-- tamper with audit_log is direct filesystem access to kilter.db, which is
-- already a "shell access" risk accepted in the threat model.
CREATE TRIGGER IF NOT EXISTS audit_log_block_delete
BEFORE DELETE ON audit_log
BEGIN SELECT RAISE(ABORT, 'audit_log rows are immutable'); END;

CREATE TRIGGER IF NOT EXISTS audit_log_block_update
BEFORE UPDATE ON audit_log
BEGIN SELECT RAISE(ABORT, 'audit_log rows are immutable'); END;
"""

# The fixed taxonomy for break_category. Kept out of the DB so the UI and
# validators can import it without a DB round-trip; extend carefully because
# historic open_items rows may reference values that get retired here.
BREAK_CATEGORIES = (
    'timing_diff',          # expected to clear in a future session
    'fx_mismatch',          # amount differs due to FX conversion
    'internal_journal',     # Flex-only entry: accrual, GL reclass, etc.
    'reversal_pending',     # awaiting the reversal leg
    'bank_error',           # correspondent bank booked incorrectly
    'genuine_break',        # unexplained, needs investigation
    'written_off',          # terminal state for ageing-out items
    'uncategorized',        # default when no rule matched
)

# Known functional groups. Not enforced (free text on open_items) — listed here
# so the report builder has a stable tab order. Extend by editing this tuple
# and auto_grouping_rules.
FUNCTIONAL_GROUPS = (
    'TREASURY',
    'TRADE SWITCHES',
    'PSC TROPS',
    'CSD FEES',
    'GRA',
    'CASH MGT',
    'TPU',
)
FUNCTIONAL_GROUP_DEFAULT = 'PSC TROPS'

ROLES = ('admin', 'ops', 'audit', 'internal_control')


def get_conn():
    """Return a database connection. Uses MySQL if DATABASE_URL is set, else SQLite."""
    import os as _os_local
    db_url = _os_local.environ.get('DATABASE_URL', '')
    if db_url.startswith('mysql'):
        from db_mysql import get_mysql_conn
        return get_mysql_conn()
    # SQLite default
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    # WAL = better concurrent read/write at the cost of one extra file.
    # secure_delete = overwrite freelist pages with zeros so deleted rows
    # (TOTP secrets, audit-log purges, SMTP passwords) cannot be recovered
    # from the SQLite freelist by anyone with filesystem access. Both are
    # idempotent — set on every open is fine; SQLite caches the value.
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA secure_delete = ON")
    return conn


def init_db() -> None:
    conn = get_conn()
    try:
        conn.executescript(SCHEMA)
        _migrate_add_session_account_columns(conn)
        _migrate_encrypt_secrets_at_rest(conn)
        _seed_bootstrap_admin(conn)
        _seed_access_areas(conn)
        _seed_grouping_rules(conn)
        _seed_currencies(conn)
        _seed_banks(conn)
        _seed_fx_identity(conn)
        _seed_scheduled_jobs(conn)
        _seed_mobile_money_profiles(conn)
        _seed_match_tiers(conn)
        conn.commit()
    finally:
        conn.close()


def _seed_match_tiers(conn) -> None:
    """Seed the default tier set per recon_type if match_tiers is empty.
    These rows reproduce the legacy hardcoded T1-T4 behavior; they ARE
    deletable + editable by admins via the visual rule builder.

    For one-sided / mobile_money / cards: T3 + T4 ship DISABLED per
    Ecobank Ghana ops policy (amount+date alone is unsafe on busy GLs
    where many transactions share common amounts). Operator can enable
    per account via the rule builder once they confirm safety.

    Idempotent — keyed on (account_id IS NULL, recon_type, legacy_tier).
    Admin edits to seeded rows survive re-init; only missing rows are
    created."""
    import json
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()

    # All seeded defaults ship DISABLED. Operator must explicitly
    # enable via the Matching tiers page (or build their own custom
    # tiers from scratch via the visual rule builder). This keeps
    # loading + matching fully under operator control — no engine
    # behavior happens until the operator wires it up.
    DEFAULT_TIERS = {
        # priority, name, conditions, auto_confirm
        1: ('Strong match',
            [{'field': 'sign',       'op': 'mirror'},
             {'field': 'amount',     'op': 'equal_within_tol'},
             {'field': 'ref',        'op': 'symmetric_in_narration'}],
            0),  # auto_confirm OFF — operator opts in per tier
        2: ('Reference matches, amounts differ',
            [{'field': 'sign',       'op': 'mirror'},
             {'field': 'ref',        'op': 'symmetric_in_narration'}],
            0),
        3: ('Same amount, same day, no ref',
            [{'field': 'sign',       'op': 'mirror'},
             {'field': 'amount',     'op': 'equal_within_tol'},
             {'field': 'value_date', 'op': 'equal'}],
            0),
        4: ('Same amount, off by one day, no ref',
            [{'field': 'sign',       'op': 'mirror'},
             {'field': 'amount',     'op': 'equal_within_tol'},
             {'field': 'value_date', 'op': 'within_days', 'params': {'n': 1}}],
            0),
    }

    for recon_type in ('one_sided', 'two_sided', 'mobile_money', 'cards'):
        for legacy_tier, (name, conds, auto) in DEFAULT_TIERS.items():
            existing = conn.execute(
                "SELECT id FROM match_tiers "
                "WHERE account_id IS NULL AND recon_type=? AND legacy_tier=?",
                (recon_type, legacy_tier),
            ).fetchone()
            if existing is not None:
                continue
            conn.execute(
                "INSERT INTO match_tiers (account_id, recon_type, name, priority, "
                "  conditions_json, enabled, auto_confirm, legacy_tier, created_at, created_by) "
                "VALUES (NULL, ?, ?, ?, ?, 0, ?, ?, ?, 'system_seed')",
                (recon_type, name, legacy_tier, json.dumps(conds),
                 auto, legacy_tier, now),
            )


# Generic first-run access-area seed. Gives admins a starting taxonomy
# so the UI isn't empty on day one; extend or replace via the admin UI
# to match the bank's own business-line / branch structure. Seeding is
# idempotent — already-present names are never overwritten.
_DEFAULT_ACCESS_AREAS = [
    'TREASURY',
    'TRADE',
    'CASH MGT',
    'NOSTRO',
    'SUBSIDIARIES',
    'AFFILIATES',
    'HEAD OFFICE',
    'SUSPENSE',
]


def _seed_access_areas(conn) -> None:
    """First-run seed of a generic access-area taxonomy. Idempotent — only
    inserts names not already present, so admins can delete or rename
    seeded rows without them being restored on next startup."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    existing = {r[0] for r in conn.execute("SELECT name FROM access_areas").fetchall()}
    to_insert = [(name, 'SYSTEM', 1, now, 'system')
                 for name in _DEFAULT_ACCESS_AREAS if name not in existing]
    if to_insert:
        conn.executemany(
            "INSERT INTO access_areas (name, parent, active, created_at, created_by) "
            "VALUES (?,?,?,?,?)",
            to_insert,
        )


# Initial functional-group classification rules for BoG-style daily reports.
# Order matters: first match wins. Priority 10-19 = named individuals (most
# reliable); 20-29 = ref prefixes (deterministic for specific codes); 30-39 =
# narration-keyword fallback (catches system userIDs like OMNIFLOW_GH doing
# treasury work). Default bucket is PSC TROPS if nothing matches.
#
# Tuple shape:
#   (name, priority, narration_contains, ref_contains, functional_group)
_INITIAL_GROUPING_RULES = [
    # --- individual users (unambiguous) -----------------------------------
    ('TREASURY user KGARRUBA',     10, 'USERID:KGARRUBA',   None, 'TREASURY'),
    ('TREASURY user JNNARTEY',     10, 'USERID:JNNARTEY',   None, 'TREASURY'),
    ('TREASURY user CALYPSO',      10, 'USERID:CALYPSO',    None, 'TREASURY'),
    ('TREASURY user SAVAGA',       10, 'USERID:SAVAGA',     None, 'TREASURY'),
    ('TREASURY user PDZOBOKU',     10, 'USERID:PDZOBOKU',   None, 'TREASURY'),
    ('TRADE user MAAAMOH',         10, 'USERID:MAAAMOH',    None, 'TRADE SWITCHES'),
    ('TRADE user MABOATENG',       10, 'USERID:MABOATENG',  None, 'TRADE SWITCHES'),
    ('TRADE user VOSRAH',          10, 'USERID:VOSRAH',     None, 'TRADE SWITCHES'),
    # --- ref-prefix rules -------------------------------------------------
    ('GRA ref prefix GG0',         20,  None,               'GG0',  'GRA'),
    ('CASH MGT ref prefix PN1',    20,  None,               'PN1',  'CASH MGT'),
    ('CASH MGT ref prefix SH1',    20,  None,               'SH1',  'CASH MGT'),
    ('CASH MGT ref prefix SU1',    20,  None,               'SU1',  'CASH MGT'),
    ('TPU ref contains FEEC',      20,  None,               'FEEC', 'TPU'),
    # --- narration keywords (catches OMNIFLOW_GH and other generic users) -
    ('TREASURY bill auction',      30, 'DAY BILL AUC',      None, 'TREASURY'),
    ('TREASURY switches',          30, 'SWITCHES',          None, 'TREASURY'),
    ('TREASURY repo',              30, 'REPO',              None, 'TREASURY'),
    ('TREASURY CSD primary',       30, 'CENTRAL SECURITIES DEPOSITORY', None, 'TREASURY'),
    ('TREASURY EGH_FI system',     30, 'EGH_FI_',           None, 'TREASURY'),
    ('TRADE custody settlement',   30, 'CUSTODY TRADE STL', None, 'TRADE SWITCHES'),
    ('TRADE custody equity',       30, 'EQUITY TRADE STL',  None, 'TRADE SWITCHES'),
    # --- default fallback ------------------------------------------------
    ('Default PSC TROPS',          99,  None,               None, 'PSC TROPS'),
]


def _seed_grouping_rules(conn) -> None:
    """First-run seed of auto_grouping_rules. Idempotent — only inserts rule
    names not already present. Admins can delete, edit, or deactivate seeded
    rules via the admin UI and they stay gone."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    existing = {r[0] for r in conn.execute("SELECT name FROM auto_grouping_rules").fetchall()}
    to_insert = [
        (name, priority, narr, ref, grp, 1, now, 'system')
        for (name, priority, narr, ref, grp) in _INITIAL_GROUPING_RULES
        if name not in existing
    ]
    if to_insert:
        conn.executemany(
            "INSERT INTO auto_grouping_rules "
            "(name, priority, narration_contains, ref_contains, functional_group, "
            " active, created_at, created_by) VALUES (?,?,?,?,?,?,?,?)",
            to_insert,
        )


# Initial currency list mirroring the Corona 7.9 picker — 24 ISO codes the
# ops team already recognises. Legacy euro-area predecessors (BEF/DEM/FRF/ITL/
# NLG) are kept inactive-friendly so the list matches existing tooling;
# admins can deactivate or extend via the Currencies Admin UI.
#
# Tuple: (iso_code, name, decimals, euro_currency)
_INITIAL_CURRENCIES = [
    ('AED', 'Arab Emirates Dirham',    2, 0),
    ('AUD', 'AUSTRALIAN DOLLAR',       2, 0),
    ('BEF', 'BELGIAN FRANC',           2, 0),
    ('CAD', 'CANADIAN DOLLARS',        2, 0),
    ('CHF', 'SWISS FRANC',             2, 0),
    ('CNY', 'YUAN RENMINBI',           2, 0),
    ('DEM', 'DEUTSCHE MARK',           2, 0),
    ('DKK', 'DKK DANISH KRONE',        2, 0),
    ('DZD', 'ALGERIAN DINAR',          2, 0),
    ('EUR', 'EUROPEAN MONETARY UNION', 2, 1),
    ('FRF', 'FRENCH FRANC',            2, 0),
    ('GBP', 'POUND STERLING',          2, 0),
    ('GHS', 'GHANIAN CEDIS',           2, 0),
    ('GMD', 'DALASI',                  2, 0),
    ('ITL', 'ITALIAN LIRA',            2, 0),
    ('JPY', 'JAPANESE YEN',            2, 0),
    ('NGN', 'NIGERIAN NAIRA',          2, 0),
    ('NLG', 'NETHERLANDS GUILDER',     2, 0),
    ('SLL', 'SIERRA LEONE',            2, 0),
    ('USD', 'US DOLLARS',              2, 0),
    ('USN', 'US DOLLARS',              2, 0),
    ('XAF', 'Central African CFA',     2, 0),
    ('XOF', 'CFA FRANC',               2, 0),
    ('ZAR', 'SOUTH AFRICAN RAND',      2, 0),
]


def _seed_currencies(conn) -> None:
    """First-run seed of the 24 Corona-compatible currencies. Idempotent —
    only inserts codes not already present, so admin-managed edits persist."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    existing = {r[0] for r in conn.execute("SELECT iso_code FROM currencies").fetchall()}
    to_insert = [
        (code, name, dec, euro, 1, now, 'system')
        for (code, name, dec, euro) in _INITIAL_CURRENCIES
        if code not in existing
    ]
    if to_insert:
        conn.executemany(
            "INSERT INTO currencies (iso_code, name, decimals, euro_currency, "
            "active, created_at, created_by) VALUES (?,?,?,?,?,?,?)",
            to_insert,
        )


def _seed_scheduled_jobs(conn) -> None:
    """First-run seed of the default automation schedule. Admins can
    tweak times / disable jobs via the scheduler admin UI. Re-seeding
    is a no-op — only inserts names that aren't already present."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    # Shape: (name, job_type, schedule_kind, interval_minutes, daily_at_utc, params_json)
    # Tuple shape: (name, job_type, schedule_kind, interval_minutes,
    #               daily_at_utc, params_json, enabled_default)
    defaults = [
        # Poll the messages/ intake every 15 minutes during the business day
        # for new files that were dropped outside a scheduled pull.
        ('Intake scan (every 15 min)', 'scan', 'interval', 15, None, None, 1),
        # Auto-close sessions that have been open >= 12 hours. Captures the
        # BoG ops pattern of "open a session, review, leave overnight for
        # late items, close in the morning".
        ('Auto-close stale sessions', 'daily_close', 'daily_at', None, '23:00',
         '{"min_age_hours": 12}', 1),
        # SLA alerts fire once a day in the morning so the nightshift team
        # walks in to a fresh list.
        ('SLA alerts (morning fan-out)', 'sla_check', 'daily_at', None, '07:30', None, 1),
        # Daily breaks report — xlsx generation for the ops workbook. When
        # email is configured this job can be extended to auto-send it.
        ('Daily breaks report', 'daily_breaks_report', 'daily_at', None, '07:00', None, 1),
        # Flex extract — disabled by default; enable once FCUBS_USER /
        # FCUBS_PASSWD env vars are set and oracledb is installed. Runs
        # before the morning scan so the breaks report has fresh data.
        ('Flexcube pull (pre-scan)', 'flex_extract', 'daily_at', None, '05:45', None, 0),
        # DB backup — SQLite online backup via sqlite3.backup() (safe
        # while the DB is live under WAL mode). Writes to ./backups/ by
        # default; set KILTER_BACKUP_DIR env var to redirect to a mounted
        # volume. Keeps 7 daily snapshots; configure keep_days via params.
        ('DB backup (nightly)', 'db_backup', 'daily_at', None, '02:00',
         '{"keep_days": 7}', 1),
        # Session cleanup — prunes revoked/expired user_sessions rows.
        # Prevents unbounded table growth at ~1 row per login.
        # Runs weekly (every 7 days × 24 h × 60 min = 10 080 min).
        ('Session cleanup (weekly)', 'session_cleanup', 'interval', 10080, None,
         '{"revoked_keep_days": 90, "expired_keep_days": 7}', 1),
    ]
    existing = {r[0] for r in conn.execute(
        "SELECT name FROM scheduled_jobs").fetchall()}
    to_insert = [(name, jt, sk, im, da, pj, en, now, 'system')
                 for (name, jt, sk, im, da, pj, en) in defaults
                 if name not in existing]
    if to_insert:
        conn.executemany(
            "INSERT INTO scheduled_jobs (name, job_type, schedule_kind, "
            "interval_minutes, daily_at_utc, params_json, enabled, "
            "created_at, created_by) VALUES (?,?,?,?,?,?,?,?,?)",
            [(r[0], r[1], r[2], r[3], r[4], r[5], r[6], r[7], r[8])
             for r in to_insert],
        )


def _seed_mobile_money_profiles(conn) -> None:
    """Pre-seed three CSV format profiles for the major African mobile-
    money operators so admins don't have to figure out the column maps
    from scratch. Profiles are unbound (no account_id) and have no
    filename pattern by default — admins bind them to specific wallet
    accounts via the BYO formats UI once they've been imported.

    Idempotent — keyed on the profile name. Re-running init_db never
    overwrites a profile an admin has customised after the first seed.
    """
    import json
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()

    # Each operator publishes its statement export in a slightly
    # different shape. The mappings below match the official downloads
    # available from the operator portal as of late 2025; admins can
    # tune them per-bank via the BYO wizard. Keys not present in a
    # given operator's export stay None — the loader treats those as
    # absent.
    seeds = [
        {
            'name': 'M-Pesa Safaricom statement',
            # Safaricom Web Self-Service "Daily Statement" CSV. Two-column
            # amount shape (Paid In / Withdrawn) — handled natively via
            # the paid_in_withdrawn sign convention.
            'delimiter': ',',
            'date_format': '%Y-%m-%d %H:%M:%S',
            'column_map': {
                'amount':       'Paid In',
                'value_date':   'Completion Time',
                'ref':          'Receipt No.',
                'narration':    'Details',
                'type':         None,
                'currency':     'Currency',
                'ac_no':        None,
                'ac_branch':    None,
                'booking_date': 'Initiation Time',
            },
            'sign_convention': 'paid_in_withdrawn',
            'sign_column': 'Withdrawn',
        },
        {
            'name': 'Telcel Cash organisation statement',
            # Telcel Cash (formerly Vodafone Cash) Business Self-Service
            # statement. Same Paid In / Withdrawn shape as M-Pesa — the
            # bulk-payments product publishes an Excel statement that
            # operators export to CSV before upload (BYO loader is
            # CSV-only today).
            'delimiter': ',',
            'date_format': '%d/%m/%Y %H:%M:%S',
            'column_map': {
                'amount':       'Paid In',
                'value_date':   'Completion Time',
                'ref':          'Receipt No.',
                'narration':    'Details',
                'type':         None,
                'currency':     'Currency',
                'ac_no':        'Opposite Party',
                'ac_branch':    None,
                'booking_date': 'Initiation Time',
            },
            'sign_convention': 'paid_in_withdrawn',
            'sign_column': 'Withdrawn',
        },
        {
            'name': 'MTN MoMo agent statement',
            # MTN MoMo Agent Portal — Ghana, Uganda, Cote d'Ivoire all
            # ship the same shape (modulo regional date format).
            'delimiter': ',',
            'date_format': '%d/%m/%Y',
            'column_map': {
                'amount':       'Amount',
                'value_date':   'Transaction Date',
                'ref':          'Reference',
                'narration':    'Description',
                'type':         'Type',
                'currency':     None,
                'ac_no':        'MSISDN',
                'ac_branch':    None,
                'booking_date': None,
            },
            'sign_convention': 'cr_dr_column',
            'sign_column': 'Type',
        },
        {
            'name': 'MTN MoMo operator B2W',
            # MTN MoMo operator-side Bank-to-Wallet (pull) feed. Signed
            # amount: negative for outflows from the operator's bank
            # account (= bank-to-wallet transfer). Columns mirror the
            # MTN B2W CSV export. The first 'Currency' column wins on
            # lookup (Python csv keeps the first match for duplicate
            # header names).
            'delimiter': ',',
            'date_format': '%m/%d/%Y %H:%M',
            'column_map': {
                'amount':       'Amount',
                'value_date':   'Date',
                'ref':          'External Transaction Id',
                'narration':    'To Message',
                'type':         None,
                'currency':     'Currency',
                'ac_no':        'To',
                'ac_branch':    None,
                'booking_date': None,
            },
            'sign_convention': 'positive_credit',
            'sign_column': None,
        },
        {
            'name': 'MTN MoMo operator W2B',
            # MTN MoMo operator-side Wallet-to-Bank (push) feed. Same
            # 26-column core shape as B2W. Some MTN file generations
            # also carry External Amount / External FX Rate / External
            # Service Provider — when present they land in _extra
            # automatically and are available to downstream FX-recon.
            # Positive Amount = funds IN to the operator's bank account.
            'delimiter': ',',
            'date_format': '%m/%d/%Y %H:%M',
            'column_map': {
                'amount':       'Amount',
                'value_date':   'Date',
                'ref':          'External Transaction Id',
                'narration':    'To Message',
                'type':         None,
                'currency':     'Currency',
                'ac_no':        'From',
                'ac_branch':    None,
                'booking_date': None,
            },
            'sign_convention': 'positive_credit',
            'sign_column': None,
        },
        {
            'name': 'Airtel Money agent statement',
            # Airtel Money Africa — common across Kenya, Tanzania,
            # Uganda, Zambia, DRC etc. Ships an explicit Credit/Debit
            # column.
            'delimiter': ',',
            'date_format': '%Y-%m-%d',
            'column_map': {
                'amount':       'Amount',
                'value_date':   'Date',
                'ref':          'Transaction ID',
                'narration':    'Description',
                'type':         'CR/DR',
                'currency':     'Currency',
                'ac_no':        'Customer MSISDN',
                'ac_branch':    None,
                'booking_date': None,
            },
            'sign_convention': 'cr_dr_column',
            'sign_column': 'CR/DR',
        },
        {
            'name': 'Card switch acquirer settlement',
            # Daily acquirer transaction report from a payment switch.
            # Tab-separated. PAN arrives pre-masked (484680******1168);
            # cards_ingest reads it via pan_masked_field. Currency is
            # the ISO numeric code (936=GHS, 840=USD) — admins should
            # bind this profile to a single-currency wallet account or
            # clone it per currency.
            'delimiter': '\t',
            'date_format': '%m/%d/%Y %H:%M:%S',
            'column_map': {
                'amount':       'Settle Amount Impact',
                'value_date':   'Datetime Req',
                'ref':          'Retrieval Reference Nr',
                'narration':    'Card Acceptor Name Loc',
                'type':         None,
                'currency':     None,
                'ac_no':        'Acquiring Inst Id Code',
                'ac_branch':    None,
                'booking_date': None,
            },
            'sign_convention': 'positive_credit',
            'sign_column': None,
        },
        {
            'name': 'Card switch issuer settlement',
            # Daily issuer transaction report from a payment switch.
            # Same TSV shape as acquirer but the settlement amount lives
            # in 'Settle Amount Rsp' rather than 'Settle Amount Impact'.
            'delimiter': '\t',
            'date_format': '%m/%d/%Y %H:%M:%S',
            'column_map': {
                'amount':       'Settle Amount Rsp',
                'value_date':   'Datetime Req',
                'ref':          'Retrieval Reference Nr',
                'narration':    'Card Acceptor Name Loc',
                'type':         None,
                'currency':     None,
                'ac_no':        'Acquiring Inst Id Code',
                'ac_branch':    None,
                'booking_date': None,
            },
            'sign_convention': 'positive_credit',
            'sign_column': None,
        },
    ]

    existing = {r[0] for r in conn.execute(
        "SELECT name FROM csv_format_profiles").fetchall()}

    for s in seeds:
        if s['name'] in existing:
            continue
        conn.execute(
            "INSERT INTO csv_format_profiles "
            "(name, side, delimiter, header_row, skip_rows, date_format, "
            " currency, column_map, sign_convention, sign_column, "
            " account_id, filename_pattern, created_by, created_at, active) "
            "VALUES (?, 'flex', ?, 1, 0, ?, NULL, ?, ?, ?, NULL, NULL, "
            "        'system-seed', ?, 1)",
            (s['name'], s['delimiter'], s['date_format'],
             json.dumps(s['column_map']),
             s['sign_convention'], s['sign_column'], now),
        )


def _seed_fx_identity(conn) -> None:
    """Guarantee same-currency identity rows exist so the FX-tolerance lookup
    always finds a row when the two sides share a currency. Idempotent."""
    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).replace(tzinfo=None).date().isoformat()
    codes = [r[0] for r in conn.execute(
        "SELECT iso_code FROM currencies WHERE active=1").fetchall()]
    existing = {(r[0], r[1], r[2]) for r in conn.execute(
        "SELECT from_ccy, to_ccy, valid_from FROM fx_rates").fetchall()}
    to_insert = []
    for c in codes:
        key = (c, c, today)
        if key not in existing:
            to_insert.append((c, c, 1.0, today, 'identity', 1,
                              datetime.now(timezone.utc).replace(tzinfo=None).isoformat(), 'system'))
    if to_insert:
        conn.executemany(
            "INSERT INTO fx_rates (from_ccy, to_ccy, rate, valid_from, source, "
            "active, created_at, created_by) VALUES (?,?,?,?,?,?,?,?)",
            to_insert,
        )


def _seed_banks(conn) -> None:
    """First-run seed of the banks registry from whatever BICs already exist
    on cash_accounts, so strict-BIC enforcement doesn't orphan historic rows.
    Placeholder name = the BIC itself; ops is expected to rename via the
    admin page. Idempotent — only inserts BICs not already present."""
    from datetime import datetime, timezone
    now = datetime.now(timezone.utc).replace(tzinfo=None).isoformat()
    existing = {r[0] for r in conn.execute("SELECT bic FROM banks").fetchall()}
    rows = conn.execute(
        "SELECT DISTINCT bic FROM accounts "
        "WHERE bic IS NOT NULL AND bic != ''"
    ).fetchall()
    to_insert = [
        (bic, bic, None, 'their', 'bank', None, None, 1, now, 'system')
        for (bic,) in rows if bic not in existing
    ]
    if to_insert:
        conn.executemany(
            "INSERT INTO banks (bic, name, nickname, origin, type, access_area, "
            "user_code, active, created_at, created_by) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            to_insert,
        )


def _seed_bootstrap_admin(conn) -> None:
    """If the users table is empty, seed one admin with an enrollment token
    so someone can bootstrap MFA and log in. The token is printed to the
    uvicorn console AND written to first_login.txt in the project root so
    it isn't lost if the terminal scrolls past it."""
    from datetime import datetime, timezone
    import secrets
    from pathlib import Path

    count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    if count == 0:
        token = secrets.token_urlsafe(16)
        conn.execute(
            "INSERT INTO users (username, display_name, role, active, created_at, "
            "created_by, enrollment_token) VALUES (?, ?, 'admin', 1, ?, 'system', ?)",
            ('admin', 'Administrator', datetime.now(timezone.utc).replace(tzinfo=None).isoformat(), token),
        )
        enroll_url = f"http://localhost:8000/enroll?user=admin&token={token}"
        print("")
        print("=" * 66)
        print("  FIRST-RUN ADMIN ENROLLMENT")
        print("")
        print(f"  {enroll_url}")
        print("")
        print("  Step 1: Open the URL above in your browser.")
        print("  Step 2: Scan the QR code with any authenticator app")
        print("          (Microsoft Authenticator, Google Authenticator, Authy).")
        print("  Step 3: Go to http://localhost:8000/login")
        print("          Username: admin   Code: 6-digit code from the app")
        print("=" * 66)
        print("")
        # Also write to first_login.txt so the URL isn't lost on scroll.
        try:
            txt_path = Path(__file__).resolve().parent / "first_login.txt"
            txt_path.write_text(
                "KILTER — FIRST LOGIN\n"
                "====================\n\n"
                "Step 1: Open this URL in your browser:\n\n"
                f"  {enroll_url}\n\n"
                "Step 2: Scan the QR code with any authenticator app\n"
                "        (Microsoft Authenticator, Google Authenticator, Authy, etc.)\n\n"
                "Step 3: Go to http://localhost:8000/login\n"
                "        Username: admin\n"
                "        Code: the 6-digit code from your authenticator app\n\n"
                "NOTE: This file is deleted automatically after you enroll.\n"
                "      Keep it private until then — anyone with the link can enroll.\n",
                encoding="utf-8",
            )
            print(f"  [Saved enrollment link to: {txt_path}]")
            print("")
        except OSError:
            pass


def _migrate_encrypt_secrets_at_rest(conn) -> None:
    """One-shot migration: encrypt any legacy plaintext TOTP secrets and
    SMTP passwords. Idempotent — already-encrypted rows are left alone, so
    rerunning on every startup is cheap and safe.

    Runs before the schema seeders so a fresh install (no rows) does
    nothing; runs on every existing install to upgrade rows in place.
    Failures here don't block startup — log + continue. The runtime
    decrypt path remains tolerant of plaintext for the same reason."""
    try:
        from secrets_vault import encrypt, is_encrypted
    except Exception as exc:
        print(f"[migrate] secrets_vault unavailable, skipping encryption migration: {exc}")
        return

    # Users.totp_secret
    rows = conn.execute(
        "SELECT username, totp_secret FROM users WHERE totp_secret IS NOT NULL AND totp_secret != ''"
    ).fetchall()
    n_users = 0
    for r in rows:
        if not is_encrypted(r['totp_secret']):
            try:
                conn.execute(
                    "UPDATE users SET totp_secret=? WHERE username=?",
                    (encrypt(r['totp_secret']), r['username']),
                )
                n_users += 1
            except Exception as exc:
                print(f"[migrate] failed to encrypt totp_secret for {r['username']}: {exc}")

    # notification_channels.config_json — encrypt the smtp_password sub-field
    import json as _json
    n_channels = 0
    rows = conn.execute(
        "SELECT id, config_json FROM notification_channels WHERE config_json IS NOT NULL"
    ).fetchall()
    for r in rows:
        try:
            cfg = _json.loads(r['config_json'])
        except (TypeError, ValueError):
            continue
        if not isinstance(cfg, dict):
            continue
        pwd = cfg.get('smtp_password')
        if pwd and not is_encrypted(pwd):
            try:
                cfg['smtp_password'] = encrypt(pwd)
                conn.execute(
                    "UPDATE notification_channels SET config_json=? WHERE id=?",
                    (_json.dumps(cfg), r['id']),
                )
                n_channels += 1
            except Exception as exc:
                print(f"[migrate] failed to encrypt smtp_password for channel {r['id']}: {exc}")

    if n_users or n_channels:
        conn.commit()
        print(f"[migrate] encrypted {n_users} TOTP secret(s) and {n_channels} SMTP password(s) at rest.")


def _migrate_add_session_account_columns(conn) -> None:
    """Additive migrations — SQLite ALTER TABLE is column-at-a-time and
    CREATE TABLE IF NOT EXISTS won't add new columns to existing tables."""
    _ensure_columns(conn, 'sessions', [
        ('swift_account',        'TEXT'),
        ('swift_currency',       'TEXT'),
        ('swift_statement_ref',  'TEXT'),
        ('flex_ac_no',           'TEXT'),
        ('flex_ac_branch',       'TEXT'),
        ('flex_currency',        'TEXT'),
        ('account_id',           'INTEGER'),
        ('account_label',        'TEXT'),
    ])
    _ensure_columns(conn, 'accounts', [
        ('shortname',    'TEXT'),
        ('access_area',  'TEXT'),
        # Mobile-money expansion. Default 'cash_nostro' preserves every
        # existing account row's behaviour; mobile_wallet rows opt-in
        # via the new account-create form.
        ('account_type', "TEXT NOT NULL DEFAULT 'cash_nostro'"),
        ('provider',     'TEXT'),
        ('msisdn',       'TEXT'),
        ('short_code',   'TEXT'),
    ])
    _ensure_columns(conn, 'users', [
        ('totp_secret',      'TEXT'),
        ('totp_enrolled_at', 'TEXT'),
        ('enrollment_token', 'TEXT'),
        # LDAP integration. 'local' (TOTP-only) is the legacy default so
        # existing rows behave exactly as they did pre-migration.
        ('auth_source',      "TEXT NOT NULL DEFAULT 'local'"),
        ('ldap_dn',          'TEXT'),
    ])
    _ensure_columns(conn, 'discovered_accounts', [
        ('resolved_at', 'TEXT'),
        ('resolved_by', 'TEXT'),
        ('bic',         'TEXT'),   # sender BIC from SWIFT Block 2; null for Flex discoveries
    ])
    # JSON array of access_area names the user is currently scoped to. NULL =
    # "all areas" (no filter). Per-session so two browsers for the same user
    # can have different active scopes.
    _ensure_columns(conn, 'user_sessions', [
        ('active_access_areas', 'TEXT'),
        # Sliding idle-timeout window. NULL on legacy rows is treated as
        # "freshly used" by resolve_session so deploying this column
        # doesn't force a wave of logouts.
        ('last_used_at',        'TEXT'),
    ])
    _ensure_columns(conn, 'sessions', [
        ('opening_balance',        'TEXT'),
        ('opening_balance_amount', 'REAL'),
        ('opening_balance_sign',   'TEXT'),
        ('opening_balance_date',   'INTEGER'),
        ('closing_balance',        'TEXT'),
        ('closing_balance_amount', 'REAL'),
        ('closing_balance_sign',   'TEXT'),
        ('closing_balance_date',   'INTEGER'),
        # 'open' (accepting decisions) | 'closed' (unmatched have been
        # spun off into open_items and the session is frozen).
        ('closed_at',              'TEXT'),
        ('closed_by',              'TEXT'),
        ('open_items_seeded',      'INTEGER NOT NULL DEFAULT 0'),
        ('open_items_cleared',     'INTEGER NOT NULL DEFAULT 0'),
    ])
    # Assignments get a provenance and manual-note field so engine vs. human
    # matches are distinguishable in the audit trail.
    _ensure_columns(conn, 'assignments', [
        ('source',          "TEXT NOT NULL DEFAULT 'engine'"),  # 'engine' | 'manual' | 'auto_carry' | 'split'
        ('manual_reason',   'TEXT'),
        ('open_item_id',    'INTEGER'),  # set when this match cleared a carry-forward open_item
        ('split_group_id',  'TEXT'),     # shared UUID when a 1:N or N:1 aggregate split is confirmed; null for plain 1:1 matches
        # Case-management columns. NULL on legacy rows is fine — code
        # paths fall back to "unassigned, no SLA, normal priority".
        ('assignee',        'TEXT'),
        ('due_date',        'TEXT'),
        ('priority',        "TEXT NOT NULL DEFAULT 'normal'"),
    ])
    # Optional secondary index on assignee — only useful if the column was
    # added via migration, since the CREATE TABLE above already sets one
    # when the table is fresh. CREATE INDEX IF NOT EXISTS is idempotent.
    conn.execute("CREATE INDEX IF NOT EXISTS idx_assignments_assignee "
                 "ON assignments(assignee, status)")
    # Functional group (TREASURY / PSC TROPS / etc.) for ops' daily tab-per-team
    # report. Set by auto_grouping_rules at seed time; also editable via the
    # manual-reclassify endpoint.
    _ensure_columns(conn, 'open_items', [
        ('functional_group',       'TEXT'),
        ('grouping_source',        'TEXT'),   # 'auto_rule' | 'manual' | null
        ('grouping_rule_id',       'INTEGER'),
    ])
    _ensure_columns(conn, 'sessions', [
        # Indicates whether closure has been run — decoupled from seeded count
        # because a session can legitimately have zero unmatched rows to seed.
        ('functional_groups_applied', 'INTEGER NOT NULL DEFAULT 0'),
        # CSV profile that loaded the Flex side. NULL on legacy rows
        # ingested via the default xlsx loader.
        ('flex_profile_id',           'INTEGER'),
    ])
    # FX tolerance — max acceptable spread (in basis points) between the
    # SWIFT amount converted at the prevailing FX rate and the Flex amount.
    # Zero (default) keeps the engine currency-strict; set to 25 for a
    # quarter-percent FX cushion.
    _ensure_columns(conn, 'tolerance_rules', [
        ('fx_tol_bps', 'REAL NOT NULL DEFAULT 0'),
    ])
    # CSV-profile bindings — added in the post-pilot UX pass after the
    # initial release. Both columns are nullable so existing manual-only
    # profiles keep working; setting them just unlocks scan routing and
    # currency inheritance.
    _ensure_columns(conn, 'csv_format_profiles', [
        ('account_id',       'INTEGER'),
        ('filename_pattern', 'TEXT'),
    ])
    # Session lock — set when a reconciliation certificate is signed.
    # Locked sessions reject all decision mutations (confirm/reject/manual).
    _ensure_columns(conn, 'sessions', [
        ('locked_at', 'TEXT'),
        ('locked_by', 'TEXT'),
    ])
    # Balance validation — computed at ingest time from the SWIFT :60F:/:62F: fields.
    # balance_valid=1 means matched txns explain the opening→closing delta exactly.
    # balance_delta is (expected_closing - actual_closing); 0.0 on perfect match.
    _ensure_columns(conn, 'sessions', [
        ('balance_valid', 'INTEGER'),
        ('balance_delta', 'REAL'),
    ])
    # SLA snooze/acknowledge columns on open_items.
    _ensure_columns(conn, 'open_items', [
        ('snoozed_until',   'TEXT'),
        ('acknowledged_by', 'TEXT'),
        ('acknowledged_at', 'TEXT'),
        ('escalated_at',    'TEXT'),
        ('escalated_to',    'TEXT'),
    ])
    # Cards engine stage tagging — auth | clearing | settlement | NULL.
    _ensure_columns(conn, 'card_settlement_files', [
        ('stage', 'TEXT'),
    ])
    # Assignment approval gate — pending_approval is the intermediate state
    # when KILTER_REQUIRE_APPROVAL=true and an ops user confirms a match.
    _ensure_columns(conn, 'assignments', [
        ('approval_required', 'INTEGER NOT NULL DEFAULT 0'),
    ])
    # Balance-chain columns. The anchor on accounts advances every time a
    # delta successfully ingests; the next delta's stated opening must
    # match this within tolerance or the file is rejected. NULL anchor =
    # account hasn't been seeded yet (first-load behaviour: take the
    # file's opening as the implicit start, no continuity check).
    _ensure_columns(conn, 'accounts', [
        ('last_closing_balance', 'REAL'),
        ('last_closing_date',    'INTEGER'),
        ('last_session_id',      'INTEGER'),
        # 'two_sided' = paired SWIFT + Flex (the original Kilter use-case).
        # 'one_sided' = Flex-only GL with no SWIFT counterpart (typical
        # for internal suspense / wallet-settlement GLs). Drives scanner
        # routing (proof seed + Flex-only delta vs the standard pair flow)
        # and the review-page UI shape.
        ('account_recon_type',   "TEXT NOT NULL DEFAULT 'two_sided'"),
    ])
    # Per-account continuity tolerance: how much delta-opening vs anchor
    # mismatch the chain accepts before raising ContinuityBreakError.
    # 0.01 (one cent / pesewa) is the strict default; bumps go on the
    # tolerance_rules row for accounts whose feed is known-noisy.
    _ensure_columns(conn, 'tolerance_rules', [
        ('continuity_tol_abs', 'REAL NOT NULL DEFAULT 0.01'),
    ])
    # session_kind disambiguates the three flows that share the sessions
    # table: 'recon' (legacy two-sided pair), 'seed' (Day-0 proof anchor),
    # 'flex_delta' (one-sided daily). Default keeps every existing row
    # behaving as a recon session.
    _ensure_columns(conn, 'sessions', [
        ('session_kind',           "TEXT NOT NULL DEFAULT 'recon'"),
        ('flex_opening_balance',   'REAL'),
        ('flex_closing_balance',   'REAL'),
        ('flex_balance_as_of',     'INTEGER'),
        ('flex_balance_currency',  'TEXT'),
    ])
    # Matching-engine progress tracking. run_matching() stamps
    # started_at when the engine begins and finished_at when it
    # commits. The UI uses these to reconnect to in-flight runs after
    # navigation/refresh — without them, the client's elapsed-timer
    # resets every time the user moves away and the run-matching
    # button reappears even though the engine is mid-run on the
    # server. Both columns are nullable: NULL → never run; started
    # set + finished NULL → in-flight; both set → completed.
    _ensure_columns(conn, 'sessions', [
        ('matching_started_at',  'TEXT'),
        ('matching_finished_at', 'TEXT'),
    ])


def _ensure_columns(conn, table: str, columns: list) -> None:
    existing = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for col, ddl in columns:
        if col not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")
