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

import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parent / 'kilter.db'

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
    UNIQUE (swift_account, flex_ac_no, currency)
);

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
    FOREIGN KEY (session_id) REFERENCES sessions(id)
);

CREATE INDEX IF NOT EXISTS idx_assignments_session_status ON assignments(session_id, status);

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
    enrollment_token  TEXT              -- one-time; cleared after enrollment
);

CREATE TABLE IF NOT EXISTS user_sessions (
    token       TEXT PRIMARY KEY,
    username    TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    expires_at  TEXT NOT NULL,
    user_agent  TEXT,
    revoked_at  TEXT,
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
    job_type       TEXT NOT NULL,                 -- 'scan'|'daily_close'|'sla_check'|'flex_extract'|'daily_breaks_report'
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


def get_conn() -> sqlite3.Connection:
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
        _seed_bootstrap_admin(conn)
        _seed_access_areas(conn)
        _seed_grouping_rules(conn)
        _seed_currencies(conn)
        _seed_banks(conn)
        _seed_fx_identity(conn)
        _seed_scheduled_jobs(conn)
        conn.commit()
    finally:
        conn.close()


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
    from datetime import datetime
    now = datetime.utcnow().isoformat()
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
    from datetime import datetime
    now = datetime.utcnow().isoformat()
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
    from datetime import datetime
    now = datetime.utcnow().isoformat()
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
    from datetime import datetime
    now = datetime.utcnow().isoformat()
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


def _seed_fx_identity(conn) -> None:
    """Guarantee same-currency identity rows exist so the FX-tolerance lookup
    always finds a row when the two sides share a currency. Idempotent."""
    from datetime import datetime
    today = datetime.utcnow().date().isoformat()
    codes = [r[0] for r in conn.execute(
        "SELECT iso_code FROM currencies WHERE active=1").fetchall()]
    existing = {(r[0], r[1], r[2]) for r in conn.execute(
        "SELECT from_ccy, to_ccy, valid_from FROM fx_rates").fetchall()}
    to_insert = []
    for c in codes:
        key = (c, c, today)
        if key not in existing:
            to_insert.append((c, c, 1.0, today, 'identity', 1,
                              datetime.utcnow().isoformat(), 'system'))
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
    from datetime import datetime
    now = datetime.utcnow().isoformat()
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
    uvicorn console — out-of-band delivery is the correct pattern for a
    first-run secret."""
    from datetime import datetime
    import secrets

    count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    if count == 0:
        token = secrets.token_urlsafe(16)
        conn.execute(
            "INSERT INTO users (username, display_name, role, active, created_at, "
            "created_by, enrollment_token) VALUES (?, ?, 'admin', 1, ?, 'system', ?)",
            ('admin', 'Administrator', datetime.utcnow().isoformat(), token),
        )
        print("=" * 66)
        print("  FIRST-RUN ADMIN ENROLLMENT")
        print(f"  Open: http://localhost:8000/enroll?user=admin&token={token}")
        print("  Scan the QR with Microsoft Authenticator, then log in.")
        print("=" * 66)


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
        ('shortname',   'TEXT'),
        ('access_area', 'TEXT'),
    ])
    _ensure_columns(conn, 'users', [
        ('totp_secret',      'TEXT'),
        ('totp_enrolled_at', 'TEXT'),
        ('enrollment_token', 'TEXT'),
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
    ])
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
    ])
    # FX tolerance — max acceptable spread (in basis points) between the
    # SWIFT amount converted at the prevailing FX rate and the Flex amount.
    # Zero (default) keeps the engine currency-strict; set to 25 for a
    # quarter-percent FX cushion.
    _ensure_columns(conn, 'tolerance_rules', [
        ('fx_tol_bps', 'REAL NOT NULL DEFAULT 0'),
    ])


def _ensure_columns(conn, table: str, columns: list) -> None:
    existing = {r[1] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for col, ddl in columns:
        if col not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {col} {ddl}")
