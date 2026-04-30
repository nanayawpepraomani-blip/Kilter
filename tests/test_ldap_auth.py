"""Tests for the LDAP authentication module.

What we're pinning:
    - load_config() returns None when KILTER_LDAP_URL is unset, and a
      fully-populated dataclass otherwise.
    - mode resolution: search_then_bind beats direct_bind when both sets
      of vars are present; direct_bind otherwise; invalid when neither.
    - authenticate() rejects empty username / password before any I/O
      (RFC 4513 anonymous-bind guard).
    - direct_bind: returns success on bind() == True, reason=bind_failed
      on bind() == False, reason=ldap_error on raised LDAPException.
    - search_then_bind: rejects when service bind fails, when search
      returns 0 entries, when search returns 2+ entries, succeeds when
      both binds succeed.
    - LDAP injection: username with special filter chars is escaped
      before being interpolated into the search filter.
"""

import os
from unittest.mock import MagicMock, patch

import pytest
from ldap3.core.exceptions import LDAPException

import ldap_auth as la


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def clean_env(monkeypatch):
    """Strip every LDAP env var so tests start from a clean state."""
    for v in (
        "KILTER_LDAP_URL", "KILTER_LDAP_BIND_DN_TEMPLATE",
        "KILTER_LDAP_USER_SEARCH_BASE", "KILTER_LDAP_USER_SEARCH_FILTER",
        "KILTER_LDAP_SERVICE_BIND_DN", "KILTER_LDAP_SERVICE_BIND_PASSWORD",
        "KILTER_LDAP_USE_SSL", "KILTER_LDAP_CA_CERTS_FILE",
    ):
        monkeypatch.delenv(v, raising=False)


def _set_direct_bind(monkeypatch):
    monkeypatch.setenv("KILTER_LDAP_URL", "ldaps://ad.example.com:636")
    monkeypatch.setenv("KILTER_LDAP_BIND_DN_TEMPLATE", "{username}@example.com")


def _set_search_then_bind(monkeypatch):
    monkeypatch.setenv("KILTER_LDAP_URL", "ldaps://ad.example.com:636")
    monkeypatch.setenv("KILTER_LDAP_USER_SEARCH_BASE", "dc=example,dc=com")
    monkeypatch.setenv("KILTER_LDAP_USER_SEARCH_FILTER", "(sAMAccountName={username})")
    monkeypatch.setenv("KILTER_LDAP_SERVICE_BIND_DN", "cn=svc,dc=example,dc=com")
    monkeypatch.setenv("KILTER_LDAP_SERVICE_BIND_PASSWORD", "service-password")


# ---------------------------------------------------------------------------
# load_config / is_enabled
# ---------------------------------------------------------------------------

def test_disabled_when_url_missing(clean_env):
    assert la.load_config() is None
    assert la.is_enabled() is False


def test_enabled_with_direct_bind_config(clean_env, monkeypatch):
    _set_direct_bind(monkeypatch)
    cfg = la.load_config()
    assert cfg is not None
    assert cfg.url == "ldaps://ad.example.com:636"
    assert cfg.mode == "direct_bind"
    assert cfg.use_ssl is True   # forced by ldaps://
    assert la.is_enabled() is True


def test_search_then_bind_takes_priority(clean_env, monkeypatch):
    """If both a bind template AND search vars are set, the more flexible
    search-then-bind path wins. Operators set both during migration."""
    _set_search_then_bind(monkeypatch)
    monkeypatch.setenv("KILTER_LDAP_BIND_DN_TEMPLATE", "{username}@example.com")
    cfg = la.load_config()
    assert cfg.mode == "search_then_bind"


def test_misconfigured_when_neither_template_nor_search(clean_env, monkeypatch):
    monkeypatch.setenv("KILTER_LDAP_URL", "ldaps://ad.example.com:636")
    cfg = la.load_config()
    assert cfg.mode == "invalid"
    result = la.authenticate("alice", "pw")
    assert result.success is False
    assert result.reason == la.REASON_MISCONFIGURED


def test_use_ssl_explicit_yes(clean_env, monkeypatch):
    monkeypatch.setenv("KILTER_LDAP_URL", "ldap://ad.example.com:389")
    monkeypatch.setenv("KILTER_LDAP_BIND_DN_TEMPLATE", "{username}@example.com")
    monkeypatch.setenv("KILTER_LDAP_USE_SSL", "true")
    cfg = la.load_config()
    assert cfg.use_ssl is True


# ---------------------------------------------------------------------------
# authenticate() — input validation
# ---------------------------------------------------------------------------

def test_empty_password_rejected_without_io(clean_env, monkeypatch):
    """Empty password must fail before any LDAP call — RFC 4513 treats
    empty-password binds as anonymous, which would succeed."""
    _set_direct_bind(monkeypatch)
    with patch.object(la, "_build_server") as build:
        result = la.authenticate("alice", "")
    assert result.success is False
    assert result.reason == la.REASON_EMPTY_PASSWORD
    build.assert_not_called()


def test_empty_username_rejected(clean_env, monkeypatch):
    _set_direct_bind(monkeypatch)
    result = la.authenticate("", "password")
    assert result.success is False
    assert result.reason == la.REASON_EMPTY_PASSWORD


def test_disabled_returns_disabled(clean_env):
    result = la.authenticate("alice", "pw")
    assert result.success is False
    assert result.reason == la.REASON_DISABLED


# ---------------------------------------------------------------------------
# Direct bind
# ---------------------------------------------------------------------------

def _stub_connect(bind_returns, *, raise_on_bind=False):
    """Build a fake _connect that returns a Connection-like object whose
    .bind() returns the configured value (or raises)."""
    conn = MagicMock()
    if raise_on_bind:
        conn.bind.side_effect = LDAPException("connection refused")
    else:
        conn.bind.return_value = bind_returns
    return MagicMock(return_value=conn), conn


def test_direct_bind_success(clean_env, monkeypatch):
    _set_direct_bind(monkeypatch)
    fake_connect, conn = _stub_connect(True)
    with patch.object(la, "_build_server"), patch.object(la, "_connect", fake_connect):
        result = la.authenticate("alice", "correct-password")
    assert result.success is True
    assert result.user_dn == "alice@example.com"
    assert result.reason == la.REASON_OK
    # Bound with the templated DN, not the bare username.
    args, kwargs = fake_connect.call_args
    assert args[1] == "alice@example.com"
    assert args[2] == "correct-password"


def test_direct_bind_wrong_password(clean_env, monkeypatch):
    _set_direct_bind(monkeypatch)
    fake_connect, _ = _stub_connect(False)
    with patch.object(la, "_build_server"), patch.object(la, "_connect", fake_connect):
        result = la.authenticate("alice", "wrong-password")
    assert result.success is False
    assert result.reason == la.REASON_BIND_FAILED


def test_direct_bind_server_unreachable(clean_env, monkeypatch):
    _set_direct_bind(monkeypatch)
    fake_connect, _ = _stub_connect(None, raise_on_bind=True)
    with patch.object(la, "_build_server"), patch.object(la, "_connect", fake_connect):
        result = la.authenticate("alice", "password")
    assert result.success is False
    assert result.reason == la.REASON_LDAP_ERROR


# ---------------------------------------------------------------------------
# Search-then-bind
# ---------------------------------------------------------------------------

def _make_entries(*dns):
    return [MagicMock(entry_dn=dn) for dn in dns]


def test_search_then_bind_success(clean_env, monkeypatch):
    _set_search_then_bind(monkeypatch)

    svc = MagicMock()
    svc.bind.return_value = True
    svc.search.return_value = True
    svc.entries = _make_entries("cn=Alice,ou=Users,dc=example,dc=com")

    user = MagicMock()
    user.bind.return_value = True

    # _connect is called twice: first for the service bind, then for the user.
    fake_connect = MagicMock(side_effect=[svc, user])

    with patch.object(la, "_build_server"), patch.object(la, "_connect", fake_connect):
        result = la.authenticate("alice", "user-password")

    assert result.success is True
    assert result.user_dn == "cn=Alice,ou=Users,dc=example,dc=com"
    # User bind got the full DN, not the bare username.
    user_call = fake_connect.call_args_list[1]
    assert user_call.args[1] == "cn=Alice,ou=Users,dc=example,dc=com"
    assert user_call.args[2] == "user-password"


def test_search_then_bind_service_account_wrong(clean_env, monkeypatch):
    _set_search_then_bind(monkeypatch)
    svc = MagicMock(); svc.bind.return_value = False
    fake_connect = MagicMock(return_value=svc)
    with patch.object(la, "_build_server"), patch.object(la, "_connect", fake_connect):
        result = la.authenticate("alice", "pw")
    assert result.success is False
    assert result.reason == la.REASON_SERVICE_BIND


def test_search_then_bind_user_not_found(clean_env, monkeypatch):
    _set_search_then_bind(monkeypatch)
    svc = MagicMock()
    svc.bind.return_value = True
    svc.search.return_value = True
    svc.entries = []
    fake_connect = MagicMock(return_value=svc)
    with patch.object(la, "_build_server"), patch.object(la, "_connect", fake_connect):
        result = la.authenticate("ghost", "pw")
    assert result.success is False
    assert result.reason == la.REASON_USER_NOT_FOUND


def test_search_then_bind_ambiguous_match_rejected(clean_env, monkeypatch):
    """If the search filter matches more than one user, refuse rather
    than guess. Otherwise an attacker who controls a directory entry
    could shadow another user by matching them in the same filter."""
    _set_search_then_bind(monkeypatch)
    svc = MagicMock()
    svc.bind.return_value = True
    svc.search.return_value = True
    svc.entries = _make_entries(
        "cn=Alice,ou=A,dc=example,dc=com",
        "cn=Alice,ou=B,dc=example,dc=com",
    )
    fake_connect = MagicMock(return_value=svc)
    with patch.object(la, "_build_server"), patch.object(la, "_connect", fake_connect):
        result = la.authenticate("alice", "pw")
    assert result.success is False
    assert result.reason == la.REASON_USER_NOT_FOUND


def test_search_then_bind_user_bind_fails(clean_env, monkeypatch):
    _set_search_then_bind(monkeypatch)
    svc = MagicMock()
    svc.bind.return_value = True
    svc.search.return_value = True
    svc.entries = _make_entries("cn=Alice,dc=example,dc=com")

    user = MagicMock(); user.bind.return_value = False
    fake_connect = MagicMock(side_effect=[svc, user])
    with patch.object(la, "_build_server"), patch.object(la, "_connect", fake_connect):
        result = la.authenticate("alice", "wrong-password")
    assert result.success is False
    assert result.reason == la.REASON_BIND_FAILED


def test_ldap_injection_in_search_filter_is_escaped(clean_env, monkeypatch):
    """A username containing LDAP filter metacharacters must be escaped
    before interpolation, so an attacker can't widen the search."""
    _set_search_then_bind(monkeypatch)
    svc = MagicMock()
    svc.bind.return_value = True
    svc.search.return_value = True
    svc.entries = []
    fake_connect = MagicMock(return_value=svc)
    with patch.object(la, "_build_server"), patch.object(la, "_connect", fake_connect):
        la.authenticate("alice)(uid=*", "pw")
    # The .search() call should have been made with the escaped input.
    call = svc.search.call_args
    # ldap3 escapes '(', ')', '*', '\\', and NUL — confirm the literal '*'
    # and '(' in the malicious payload are NOT present in the filter that
    # actually went to the directory.
    flt = call.kwargs.get("search_filter") or call.args[1]
    assert "(uid=*)" not in flt
    assert "alice)(uid=*" not in flt   # raw payload is NOT in the filter
