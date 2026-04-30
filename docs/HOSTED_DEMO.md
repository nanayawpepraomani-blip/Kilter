# Hosted Demo Setup

Stand up `https://demo.kilter.example.com` (or whichever hostname you
prefer) with a single small VM, TLS via Let's Encrypt, basic-auth
gating, and an automatic daily reseed of the mock dataset. Total time
on a fresh box: ~15 minutes. Total cost: USD 5–10/month.

The hosted demo is a sales accelerant — it lets you turn "schedule a
call" into "click here, password is X" in one outbound email.

## What you're building

```
prospect's browser ── HTTPS ──► demo.kilter.example.com (Caddy)
                                     │
                          basic-auth gate
                                     │
                                     ▼
                              Kilter container
                              (mock data, MFA still on)
                                     │
                          cron @ 03:00 UTC daily
                          ↳ reset-demo.sh wipes + reseeds
```

Two layers between a curious internet visitor and Kilter's data:
the basic-auth credential you give the prospect, and Kilter's normal
TOTP login. A leaked basic-auth pair still hits the MFA wall.

## 1. Pre-requisites (one-off, ~10 minutes)

### 1.1 VM

Pick any small VM. Tested layouts:

| Provider | Plan | Cost |
|---|---|---|
| DigitalOcean | s-1vcpu-2gb | USD 12/month |
| Hetzner | CX22 (Falkenstein/Ashburn) | EUR 4.59/month |
| AWS Lightsail | 2 GB | USD 12/month |
| Linode | Nanode 2 GB | USD 12/month |

Ubuntu 24.04 LTS as the OS. SSH key login only — disable password
auth as part of the standard hardening pass.

### 1.2 DNS

Add an A record:

```
demo.kilter.example.com.    300    A    <vm-ip>
```

Wait for propagation. Verify:

```bash
dig +short demo.kilter.example.com
# should print the VM IP
```

DNS must resolve before the first `docker compose up`, otherwise
Caddy can't request a Let's Encrypt cert and falls into rate-limit
backoff.

### 1.3 Firewall

Open inbound 80/tcp and 443/tcp only. Block 8000 from the public
internet — Caddy reaches Kilter on the internal docker network.

```bash
ufw allow 22/tcp
ufw allow 80/tcp
ufw allow 443/tcp
ufw enable
```

### 1.4 Docker

```bash
curl -fsSL https://get.docker.com | sh
systemctl enable --now docker
```

## 2. Deploy

### 2.1 Clone

```bash
git clone <your-kilter-repo> /opt/kilter
cd /opt/kilter
```

### 2.2 Generate secrets

```bash
# Encryption key for at-rest secrets
docker run --rm python:3.12-slim sh -c \
  "pip install -q cryptography==47.0.0 && python -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())'"

# bcrypt hash for the basic-auth password you'll share
docker run --rm caddy:2.8-alpine caddy hash-password --plaintext "the-password-prospects-type"
```

Save both outputs.

### 2.3 Configure `.env`

```bash
cp infra/demo/.env.demo.example .env
$EDITOR .env
chmod 600 .env
```

Fill in:

```env
KILTER_SECRET_KEY=<from above>
DEMO_HOSTNAME=demo.kilter.example.com
ACME_EMAIL=ops@kilter.example.com
DEMO_BASIC_AUTH_USER=demo
DEMO_BASIC_AUTH_HASH=<bcrypt hash from above>
```

### 2.4 Pre-flight check

Before the first `docker compose up`, run the pre-flight script. It
validates DNS, env vars, ports, and docker — these are the four
mistakes responsible for ~all "first deploy didn't come up cleanly"
incidents. Catches them in seconds; saves an hour of LE rate-limit
backoff.

```bash
chmod +x infra/demo/*.sh
./infra/demo/check-deploy.sh
```

Fix any `FAIL:` lines, re-run until you see `ready to deploy.`

### 2.5 First start

```bash
docker compose -f docker-compose.yml -f infra/demo/docker-compose.demo.yml up -d --build
docker compose logs -f
```

Watch for:

- `[scheduler] started`
- `INFO: Uvicorn running on http://0.0.0.0:8000`
- Caddy: `certificate obtained successfully` for `demo.kilter.example.com`

If Caddy 5xx's on the cert, double-check DNS — that's the most common
failure.

### 2.6 First-time seed

```bash
./infra/demo/reset-demo.sh
```

Note the `[reset-demo] enrollment URL:` line in the output — that's
the link you use to claim the demo-admin account. Save it; you'll
need it before each prospect session.

## 3. Daily reset cron

```bash
# /etc/cron.d/kilter-demo
SHELL=/bin/bash
0 3 * * * root /opt/kilter/infra/demo/reset-demo.sh >> /var/log/kilter-demo-reset.log 2>&1
```

This wipes the demo at 03:00 UTC every day and re-runs the mock-data
seed, so prospects always see a fresh, predictable state.

## 4. Per-prospect access

The simplest pattern: one shared basic-auth credential, rotated on
demand.

For tighter control, use a per-prospect credential. Two-step switch:

1. In `infra/demo/Caddyfile`, replace the inline `basic_auth { … }`
   block with:

   ```caddy
   basic_auth {
       import_file /etc/caddy/htpasswd
   }
   ```

2. Manage entries with the helper script (no manual hashing or file
   editing):

   ```bash
   ./infra/demo/htpasswd-add.sh add    acme-bank   'their-password'
   ./infra/demo/htpasswd-add.sh list
   ./infra/demo/htpasswd-add.sh remove acme-bank
   ```

The script bcrypt-hashes the password, atomically rewrites
`infra/demo/htpasswd`, and reloads Caddy in-place — no dropped TLS
connections, no service restart.

## 5. Sending the link

The fastest path: run `issue-prospect.sh`. It rotates the demo-admin
enrollment token (so a stale link from a prior prospect can't claim
the same account) and prints an email body ready to copy-paste:

```bash
./infra/demo/issue-prospect.sh 'the-basic-auth-password-you-set'
```

You'll see something like:

```
==== Prospect access ====================================
URL:           https://demo.kilter.example.com
Basic-auth:    demo  /  the-basic-auth-password-you-set
Enrollment:    https://demo.kilter.example.com/enroll?user=demo-admin&token=…

==== Email body (copy/paste below) ======================
Hi [name],
…
```

The script's hands-off; the manual version below is the same content
written long-form for context.

### 5.1 Manual sample email body

> Hi [name],
>
> Here's the live demo of Kilter as promised:
>
>   URL: https://demo.kilter.example.com
>   Username: demo
>   Password: [the one-time password]
>
> When you reach the sign-in page, the demo-admin enrolment link is:
>
>   https://demo.kilter.example.com/enroll?user=demo-admin&token=[TOKEN]
>
> Scan the QR with Microsoft Authenticator, log in, and you're in.
>
> The demo resets at 03:00 UTC each night, so feel free to break things.
> If you'd like to walk through a specific workflow live, reply with a
> 30-minute window that suits.
>
> Cheers,
> [your name]

The TOTP requirement is intentional even on the demo — it's the
clearest single signal of the security posture, and a CISO who clicks
through will spot its absence in 5 seconds.

## 6. Tearing down a session

```bash
# Reset the demo (mock data + admin token):
./infra/demo/reset-demo.sh

# Or fully wipe and rebuild (rarely needed):
docker compose -f docker-compose.yml -f infra/demo/docker-compose.demo.yml down -v
docker compose -f docker-compose.yml -f infra/demo/docker-compose.demo.yml up -d --build
```

## 7. Hardening checklist

- `chmod 600 .env` — basic-auth hash leakage is mostly cosmetic, but
  `KILTER_SECRET_KEY` is not.
- `ufw status` — confirm only 22/80/443 are open.
- `docker compose logs --tail 200 caddy | grep "obtained"` — confirm
  cert was issued, not staging-only.
- Sign in once a week to verify the cert hasn't quietly expired (Caddy
  auto-renews, but verify visually before the first prospect of the
  week).
- Keep an eye on `kilter-demo-reset.log` — if cron stops firing, the
  demo state drifts and screenshots from prospect sessions become
  unpredictable.

## 8. What this *isn't*

- Not a multi-tenant production cluster. One container, one user
  community (you + prospects), one daily reset.
- Not a substitute for sending real-data screenshots. Some prospects
  will trust their own click-through more than your slides; some will
  trust your slides more. Have both.
- Not internet-facing customer data. The demo runs on mock data only
  (`demo_data/` + `scripts/_generate_mock_data.py`).
