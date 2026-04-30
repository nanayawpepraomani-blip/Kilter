"""
ldap_auth.py
============

Active Directory / LDAP authentication for the password layer of Kilter
login. TOTP remains the second factor — this module never replaces it,
only sits in front of it.

Per-user opt-in. A row in `users` with `auth_source = 'ldap'` requires
LDAP bind to succeed before TOTP is checked. Rows with the default
`auth_source = 'local'` are unaffected (TOTP-only login, current
behaviour) — so the bootstrap admin keeps working even after LDAP is
turned on.

Two bind modes, picked by which env vars are set:

    Direct-bind (default, simplest)
        KILTER_LDAP_URL                = ldaps://ad.bank.local:636
        KILTER_LDAP_BIND_DN_TEMPLATE   = {username}@bank.local
        # or: cn={username},ou=Users,dc=bank,dc=com

    Search-then-bind (use when the DN can't be templated)
        KILTER_LDAP_URL                  = ldaps://ad.bank.local:636
        KILTER_LDAP_USER_SEARCH_BASE     = dc=bank,dc=com
        KILTER_LDAP_USER_SEARCH_FILTER   = (sAMAccountName={username})
        KILTER_LDAP_SERVICE_BIND_DN      = cn=kilter-svc,ou=Service,dc=bank,dc=com
        KILTER_LDAP_SERVICE_BIND_PASSWORD = ********

    Optional for both modes
        KILTER_LDAP_USE_SSL       = true   # forced true if URL is ldaps://
        KILTER_LDAP_CA_CERTS_FILE = /etc/ssl/certs/bank-ca.pem

`authenticate()` is the single entry point. It returns a dataclass that
the caller (app.py /login) can pattern-match on. It never raises for
ordinary bind failure — only for misconfiguration.

Security notes:
    * Filter input is escaped with ldap3's escape_filter_chars to prevent
      LDAP injection (e.g. `*)(uid=*` in a username field).
    * Empty passwords are rejected before hitting the server. RFC 4513
      treats empty-password binds as anonymous and they will succeed.
    * SSL/TLS is mandatory for production; we do not turn off cert
      verification, ever. Use KILTER_LDAP_CA_CERTS_FILE for self-signed
      bank CAs.
"""

from __future__ import annotations

import os
import ssl
from dataclasses import dataclass
from typing import Optional

import ldap3
from ldap3.core.exceptions import LDAPException
from ldap3.utils.conv import escape_filter_chars


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class LdapResult:
    """Outcome of an LDAP authentication attempt.

    success — True iff bind succeeded with the supplied credentials.
    user_dn — the DN we successfully bound as (audit log + persistence).
    reason  — short machine-readable string for logs / metrics. Never
              surface this to the end user; the /login handler returns a
              generic 'invalid credentials' for every failure mode.
    """
    success: bool
    user_dn: Optional[str] = None
    reason: str = ""


# Sentinel reasons. Tests assert on these.
REASON_OK              = "ok"
REASON_DISABLED        = "ldap_not_configured"
REASON_EMPTY_PASSWORD  = "empty_password"
REASON_USER_NOT_FOUND  = "user_not_found"
REASON_BIND_FAILED     = "bind_failed"
REASON_SERVICE_BIND    = "service_bind_failed"
REASON_LDAP_ERROR      = "ldap_error"
REASON_MISCONFIGURED   = "misconfigured"


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class LdapConfig:
    url: str
    bind_dn_template: Optional[str]
    search_base: Optional[str]
    search_filter: Optional[str]
    service_bind_dn: Optional[str]
    service_bind_password: Optional[str]
    use_ssl: bool
    ca_certs_file: Optional[str]

    @property
    def mode(self) -> str:
        if self.search_filter and self.search_base:
            return "search_then_bind"
        if self.bind_dn_template:
            return "direct_bind"
        return "invalid"


def _env(name: str) -> Optional[str]:
    v = os.environ.get(name, "").strip()
    return v or None


def load_config() -> Optional[LdapConfig]:
    """Read env vars. Returns None when LDAP is not configured (i.e.
    KILTER_LDAP_URL is unset). Returns a config dataclass otherwise."""
    url = _env("KILTER_LDAP_URL")
    if not url:
        return None
    use_ssl_env = _env("KILTER_LDAP_USE_SSL")
    use_ssl = (
        url.lower().startswith("ldaps://")
        or (use_ssl_env is not None and use_ssl_env.lower() in ("1", "true", "yes"))
    )
    return LdapConfig(
        url=url,
        bind_dn_template=_env("KILTER_LDAP_BIND_DN_TEMPLATE"),
        search_base=_env("KILTER_LDAP_USER_SEARCH_BASE"),
        search_filter=_env("KILTER_LDAP_USER_SEARCH_FILTER"),
        service_bind_dn=_env("KILTER_LDAP_SERVICE_BIND_DN"),
        service_bind_password=_env("KILTER_LDAP_SERVICE_BIND_PASSWORD"),
        use_ssl=use_ssl,
        ca_certs_file=_env("KILTER_LDAP_CA_CERTS_FILE"),
    )


def is_enabled() -> bool:
    """True iff LDAP is configured. Cheap — re-read every call so test
    monkeypatches and live config changes both work without restart."""
    return load_config() is not None


# ---------------------------------------------------------------------------
# Server / connection construction (factored out so tests can monkeypatch)
# ---------------------------------------------------------------------------

def _build_server(cfg: LdapConfig) -> ldap3.Server:
    tls = None
    if cfg.use_ssl:
        tls = ldap3.Tls(
            ca_certs_file=cfg.ca_certs_file,
            validate=ssl.CERT_REQUIRED,
        )
    return ldap3.Server(
        cfg.url,
        use_ssl=cfg.use_ssl,
        tls=tls,
        get_info=ldap3.NONE,        # don't fetch schema; we don't need it
        connect_timeout=5,
    )


def _connect(server: ldap3.Server, user: str, password: str) -> ldap3.Connection:
    """Construct a Connection. We do NOT auto-bind here — the caller
    invokes .bind() so that bind failures don't raise."""
    return ldap3.Connection(
        server,
        user=user,
        password=password,
        authentication=ldap3.SIMPLE,
        client_strategy=ldap3.SYNC,
        raise_exceptions=False,
        read_only=True,
    )


# ---------------------------------------------------------------------------
# The two bind strategies
# ---------------------------------------------------------------------------

def _direct_bind(cfg: LdapConfig, username: str, password: str) -> LdapResult:
    user_dn = cfg.bind_dn_template.format(username=username)  # type: ignore[union-attr]
    server = _build_server(cfg)
    conn = _connect(server, user_dn, password)
    try:
        if conn.bind():
            return LdapResult(success=True, user_dn=user_dn, reason=REASON_OK)
        return LdapResult(success=False, reason=REASON_BIND_FAILED)
    finally:
        try:
            conn.unbind()
        except Exception:
            pass


def _search_then_bind(cfg: LdapConfig, username: str, password: str) -> LdapResult:
    server = _build_server(cfg)

    # 1) bind as the service account so we can search for the user's DN
    svc_conn = _connect(server, cfg.service_bind_dn or "", cfg.service_bind_password or "")
    try:
        if not svc_conn.bind():
            return LdapResult(success=False, reason=REASON_SERVICE_BIND)

        safe_username = escape_filter_chars(username)
        flt = cfg.search_filter.format(username=safe_username)  # type: ignore[union-attr]
        ok = svc_conn.search(
            search_base=cfg.search_base,
            search_filter=flt,
            search_scope=ldap3.SUBTREE,
            attributes=[],
            size_limit=2,           # ambiguity guard
        )
        if not ok or not svc_conn.entries:
            return LdapResult(success=False, reason=REASON_USER_NOT_FOUND)
        if len(svc_conn.entries) > 1:
            # Filter matched multiple users — refuse rather than guess.
            return LdapResult(success=False, reason=REASON_USER_NOT_FOUND)

        user_dn = svc_conn.entries[0].entry_dn
    finally:
        try:
            svc_conn.unbind()
        except Exception:
            pass

    # 2) bind as the user with their password
    user_conn = _connect(server, user_dn, password)
    try:
        if user_conn.bind():
            return LdapResult(success=True, user_dn=user_dn, reason=REASON_OK)
        return LdapResult(success=False, reason=REASON_BIND_FAILED)
    finally:
        try:
            user_conn.unbind()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def authenticate(username: str, password: str) -> LdapResult:
    """Verify (username, password) against the configured LDAP server.

    Never raises for an ordinary bind failure (wrong password, unknown
    user, server down) — those return LdapResult(success=False, ...).
    Misconfiguration (no LDAP set, conflicting modes) returns the
    REASON_MISCONFIGURED / REASON_DISABLED variants so the caller can
    log loudly.

    Empty password is rejected before any network I/O — RFC 4513 binds
    with an empty password as 'anonymous' and would succeed against a
    permissive directory."""
    if not username or not password:
        # RFC 4513 anonymous-bind guard.
        return LdapResult(success=False, reason=REASON_EMPTY_PASSWORD)

    cfg = load_config()
    if cfg is None:
        return LdapResult(success=False, reason=REASON_DISABLED)

    try:
        if cfg.mode == "search_then_bind":
            return _search_then_bind(cfg, username, password)
        if cfg.mode == "direct_bind":
            return _direct_bind(cfg, username, password)
        return LdapResult(success=False, reason=REASON_MISCONFIGURED)
    except LDAPException:
        # Network errors, TLS handshake failures, server unreachable, etc.
        # Treat as bind failure — the user can't authenticate right now.
        # The /login handler logs the reason for ops, the user just sees
        # "invalid credentials".
        return LdapResult(success=False, reason=REASON_LDAP_ERROR)
