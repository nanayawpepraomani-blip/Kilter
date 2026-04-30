# Active Directory / LDAP Integration

Kilter can authenticate users against the bank's Active Directory or any
LDAPv3 directory. The integration is **per-user, opt-in**, and TOTP
remains the second factor — LDAP only handles the password layer.

This means:

- The bootstrap admin keeps working with TOTP-only login. You can't
  accidentally lock yourself out by mis-configuring LDAP.
- Each user has an `auth_source` field: `'local'` (TOTP-only, default)
  or `'ldap'` (password + TOTP). Admins flip individual users via the
  Users admin page.
- A failed LDAP bind blocks the login *before* TOTP is checked, so a
  leaked TOTP secret alone doesn't grant access for an LDAP-sourced
  user.

## When to enable

You should turn LDAP on once you have:

- An AD or LDAP server reachable from the Kilter container.
- A test user with a known password whose record is in the directory.
- Permission from the bank's security team — most have a documented
  LDAP integration approval process.

Don't migrate the bootstrap admin to LDAP until you have at least one
*other* user successfully logging in via LDAP. That's your safety net.

## Configuration

LDAP is configured entirely via environment variables. There is no
in-app settings page — it's intentionally read-only-from-disk so a
compromised admin account can't quietly redirect authentication.

### Direct-bind mode (simplest)

Use this when the user's DN can be derived from the username with a
template — typical for Active Directory if everyone shares one OU, and
for any directory using userPrincipalName format (`user@domain`).

```env
KILTER_LDAP_URL=ldaps://ad.bank.local:636
KILTER_LDAP_BIND_DN_TEMPLATE={username}@bank.local
```

Active Directory accepts both `user@bank.local` (UPN) and full DNs for
binds, so the UPN form is usually cleanest.

For an OpenLDAP / 389DS layout where users live in one OU:

```env
KILTER_LDAP_BIND_DN_TEMPLATE=uid={username},ou=Users,dc=bank,dc=com
```

### Search-then-bind mode

Use this when:

- Users live in different OUs and you can't template a single DN.
- You want to look up users by `sAMAccountName`, `mail`, or any other
  attribute rather than constructing a DN.

A read-only service account does the lookup, then Kilter rebinds as the
user with their password.

```env
KILTER_LDAP_URL=ldaps://ad.bank.local:636
KILTER_LDAP_USER_SEARCH_BASE=dc=bank,dc=com
KILTER_LDAP_USER_SEARCH_FILTER=(sAMAccountName={username})
KILTER_LDAP_SERVICE_BIND_DN=CN=kilter-svc,OU=Service Accounts,DC=bank,DC=com
KILTER_LDAP_SERVICE_BIND_PASSWORD=<vault-managed>
```

If both `KILTER_LDAP_BIND_DN_TEMPLATE` and the search-mode variables are
set, search-then-bind wins. That's the documented migration path: set
search-mode vars first, verify it works, then remove the template.

### Optional knobs

```env
# Force-enable SSL even when the URL is plain ldap:// (useful only for
# tunneled connections via stunnel / VPN).
KILTER_LDAP_USE_SSL=true

# CA bundle for the directory's TLS certificate. Required when the bank
# uses a self-signed or internal CA. Mount this into the container at
# build/run time.
KILTER_LDAP_CA_CERTS_FILE=/etc/ssl/certs/bank-ca.pem
```

## Login flow with LDAP

```
                    ┌──────────────────────────────┐
   Browser POST     │ Username + password + TOTP   │
                    └───────────────┬──────────────┘
                                    │
                            ┌───────▼────────┐
                            │  /login (FastAPI) │
                            └───────┬────────┘
              auth_source = 'local' │ auth_source = 'ldap'
                                    │
                        ┌──────────┐│┌─────────────────────────┐
                        │ verify   │││ ldap_auth.authenticate( │
                        │ TOTP     │││   user, password)       │
                        └────┬─────┘│└────────────┬────────────┘
                             │      │             │ success
                             │      │             ▼
                             │      │      verify TOTP
                             │      │             │
                             ▼      ▼             ▼
                     ┌─────────────────────────────────┐
                     │ issue_session() → bearer token  │
                     └─────────────────────────────────┘
```

Both factors must succeed. The user only ever sees a generic
"Invalid username or authenticator code." for any failure — the audit
log records the actual reason (`ldap:bind_failed`, `totp`, etc.) for
ops to investigate.

## Promoting a user to LDAP

In the UI:

1. **Sign in as admin** → **Users**.
2. Find the user in the table.
3. In the actions cell, change the **Auth** dropdown from `Local` to
   `LDAP`.
4. The user's existing sessions are revoked automatically — they'll be
   re-prompted to sign in, and this time they'll need their AD password.

Or via the API:

```bash
curl -X PATCH https://kilter.bank/users/alice \
  -H "X-Session-Token: $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"auth_source":"ldap"}'
```

The user's TOTP secret is **not** reset on the auth-source flip. They
keep using the same Microsoft Authenticator entry — only the password
side of the login is moving to AD.

## Demoting a user back to local

Same flow, change the dropdown to `Local`. They'll be back to TOTP-only
on next sign-in. Useful if a specific user has been locked out of AD
and needs emergency access.

## Operational notes

- **Service account permissions.** The service account only needs read
  access to the user-search base. Don't give it write or replication
  rights.
- **Password rotation.** When the service account's password rotates,
  update `KILTER_LDAP_SERVICE_BIND_PASSWORD` in `.env` and restart the
  container. Direct-bind mode has no service account — just the user's
  own password, which AD already manages.
- **Failed-login monitoring.** The audit-log row for a failed LDAP
  login carries `details.reason = "ldap:<reason>"`. Common reasons:
  - `ldap:bind_failed` — wrong password.
  - `ldap:user_not_found` — username not in the directory (or filter
    mis-configured).
  - `ldap:service_bind_failed` — service account password expired /
    locked.
  - `ldap:ldap_error` — server unreachable, TLS handshake failure.
  Wire these into your SIEM as you would for any AD failed-login log.
- **Connection pooling.** Each `/login` opens a fresh LDAP connection,
  which is fine for the rate the bank actually logs in (one-per-user-
  per-day-ish). If concurrent login storms ever become an issue,
  ldap3's `RESTARTABLE` strategy + a connection pool can be added —
  but don't optimise prematurely.

## Troubleshooting

| Symptom | Likely cause |
|---|---|
| All LDAP logins fail with `ldap:ldap_error` | Cert / TLS issue. Check `KILTER_LDAP_CA_CERTS_FILE` and that the cert is mounted into the container. Verify with `openssl s_client -connect ad.bank.local:636 -CAfile /path/to/bank-ca.pem`. |
| `ldap:user_not_found` for a user you can see in AD | Filter mismatch. AD uses `sAMAccountName` for short names; UPNs need `userPrincipalName` instead. Try `(\|(sAMAccountName={username})(userPrincipalName={username}))`. |
| `ldap:service_bind_failed` after a few days | Service-account password expired. Common in environments with a 90-day password policy. Move to a service account exempt from rotation, or schedule a reminder. |
| User can sign in to AD on their laptop but not Kilter | Check the time. AD Kerberos is not in use here, but a wildly wrong container clock can break TLS cert validation. `docker compose exec kilter date`. |
| Generic "Invalid credentials" for a user you know is correct | Look at the audit log: `SELECT details FROM audit_log WHERE action='login_failed' AND actor='alice' ORDER BY timestamp DESC LIMIT 1`. The `reason` field tells you whether the failure was LDAP or TOTP. |

## Security properties

- Bind credentials are sent over TLS (we refuse plaintext binds in
  production — set `KILTER_LDAP_URL=ldaps://...`).
- Cert verification is **always on**. No `tls_validate=CERT_NONE`
  option — use `KILTER_LDAP_CA_CERTS_FILE` for self-signed CAs
  instead of disabling verification.
- LDAP filter input is escaped with `ldap3.utils.conv.escape_filter_chars`,
  which neutralises `(`, `)`, `*`, `\\`, and NUL in the username
  before it reaches the directory. A user named
  `alice)(uid=*` cannot widen their own search.
- Empty passwords are rejected before any network I/O — RFC 4513 binds
  with an empty password as anonymous, which on a permissive directory
  would succeed.
- Search returning more than one match is treated as a failure
  (`user_not_found`), so an attacker who controls a directory entry
  can't shadow another user by being a second match for the same
  filter.
