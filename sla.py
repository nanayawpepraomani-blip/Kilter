"""
sla.py
======

SLA alerting for rolling open items. Surfaces anything older than a
threshold to configured notification channels (Teams webhook / email /
log-only). Wiring is intentionally minimal — each channel type has its
own small post function so adding Slack or PagerDuty later is a one-file
change.

Called two ways:
  * POST /sla/check — on-demand, returns structured counts for the UI
  * python sla_check.py --threshold-days 30 — scheduled via cron

Both paths funnel through `run_check(conn, channel_id=None, dry_run=False)`
so the behaviour is identical.
"""

from __future__ import annotations

import json
import smtplib
import ssl
import urllib.request
import urllib.error
from datetime import datetime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def run_check(conn, channel_id: int | None = None,
              dry_run: bool = False) -> dict:
    """Evaluate all active channels (or one specific channel) and dispatch.
    Returns a summary: {channel_id: {sent, items, error}}. Each channel has
    its own threshold_days + access_area filter, so different teams can
    subscribe to different aging levels without code change."""
    if channel_id is not None:
        rows = conn.execute(
            "SELECT * FROM notification_channels WHERE id=? AND active=1",
            (channel_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM notification_channels WHERE active=1"
        ).fetchall()
    results: dict[int, dict] = {}
    for ch in rows:
        chd = dict(ch)
        try:
            items = _aged_items(conn, chd['threshold_days'],
                                chd.get('access_area_filter'))
            if not items:
                results[chd['id']] = {'sent': 0, 'items': 0, 'skipped': True}
                _update_channel_run(conn, chd['id'], 'no items over threshold')
                continue
            if dry_run:
                results[chd['id']] = {'sent': 0, 'items': len(items),
                                      'dry_run': True}
                continue
            sent, err = _dispatch(chd, items)
            results[chd['id']] = {'sent': sent, 'items': len(items), 'error': err}
            _update_channel_run(conn, chd['id'],
                                f'sent {sent} of {len(items)}; err={err or "none"}')
        except Exception as exc:
            results[chd['id']] = {'sent': 0, 'items': 0, 'error': str(exc)}
            _update_channel_run(conn, chd['id'], f'error: {exc}')
    conn.commit()
    return results


def _aged_items(conn, threshold_days: int,
                access_area_filter: str | None) -> list[dict]:
    """Open items older than threshold_days, optionally scoped to access
    areas. Returns one row per item — caller decides whether to aggregate
    for channel-specific payloads."""
    areas = None
    if access_area_filter:
        try:
            parsed = json.loads(access_area_filter)
            if isinstance(parsed, list) and parsed:
                areas = parsed
        except Exception:
            pass
    if areas:
        placeholders = ','.join('?' for _ in areas)
        rows = conn.execute(
            f"SELECT oi.id, oi.amount, oi.sign, oi.ref, oi.narration, "
            f"oi.opened_at, oi.source_side, oi.functional_group, "
            f"a.label AS account_label, a.access_area, a.currency "
            f"FROM open_items oi JOIN accounts a ON a.id = oi.account_id "
            f"WHERE oi.status='open' AND a.access_area IN ({placeholders}) "
            f"AND julianday('now') - julianday(oi.opened_at) >= ? "
            f"ORDER BY oi.opened_at LIMIT 500",
            (*areas, threshold_days),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT oi.id, oi.amount, oi.sign, oi.ref, oi.narration, "
            "oi.opened_at, oi.source_side, oi.functional_group, "
            "a.label AS account_label, a.access_area, a.currency "
            "FROM open_items oi JOIN accounts a ON a.id = oi.account_id "
            "WHERE oi.status='open' "
            "AND julianday('now') - julianday(oi.opened_at) >= ? "
            "ORDER BY oi.opened_at LIMIT 500",
            (threshold_days,),
        ).fetchall()
    return [dict(r) for r in rows]


def _dispatch(channel: dict, items: list[dict]) -> tuple[int, str | None]:
    kind = channel['kind']
    config = json.loads(channel['config_json']) if channel['config_json'] else {}
    if kind == 'teams':
        return _dispatch_teams(config, channel, items)
    if kind == 'log':
        _dispatch_log(channel, items)
        return len(items), None
    if kind == 'email':
        return _dispatch_email(config, channel, items)
    return 0, f'unknown channel kind: {kind}'


def _dispatch_email(config: dict, channel: dict,
                    items: list[dict]) -> tuple[int, str | None]:
    """Send an HTML summary email via SMTP. config schema:
        {
          "smtp_host": "smtp.office365.com",
          "smtp_port": 587,
          "smtp_user": "kilter@yourbank.com",
          "smtp_password": "…",            # encrypted at rest via
                                            # secrets_vault — decrypted just
                                            # before the SMTP handshake.
          "use_starttls": true,
          "from_addr": "Kilter <kilter@yourbank.com>",
          "to_addrs": ["ops@yourbank.com", "treasurycontrol@yourbank.com"]
        }
    """
    from secrets_vault import decrypt
    host  = (config.get('smtp_host')  or '').strip()
    port  = int(config.get('smtp_port') or 587)
    user  = (config.get('smtp_user')  or '').strip()
    pwd   = decrypt(config.get('smtp_password') or '')
    frm   = (config.get('from_addr')  or user).strip()
    tos   = config.get('to_addrs') or []
    if isinstance(tos, str):
        tos = [a.strip() for a in tos.split(',') if a.strip()]
    use_starttls = bool(config.get('use_starttls', True))

    if not host or not tos:
        return 0, 'email config missing smtp_host or to_addrs'

    total = len(items)
    # Render the payload body.
    by_account: dict[str, list] = {}
    for it in items:
        by_account.setdefault(it['account_label'], []).append(it)

    html_rows = []
    for acct, rows in sorted(by_account.items(), key=lambda x: -len(x[1]))[:20]:
        for r in rows[:5]:  # cap per-account to keep the email scannable
            html_rows.append(
                f"<tr><td>{_h(acct)}</td>"
                f"<td>{_h(r.get('functional_group') or 'PSC TROPS')}</td>"
                f"<td style='text-align:right'>{r['sign']} {abs(r['amount'] or 0):,.2f} {_h(r.get('currency') or '')}</td>"
                f"<td>{_h(r.get('ref') or '')}</td>"
                f"<td style='color:#666'>{_h((r.get('narration') or '')[:80])}</td>"
                f"<td style='color:#b91c1c;white-space:nowrap'>{_h(r.get('opened_at',''))[:10]}</td></tr>"
            )
        if len(rows) > 5:
            html_rows.append(
                f"<tr><td colspan='6' style='color:#52606d;font-style:italic'>"
                f"&hellip; and {len(rows)-5} more for {_h(acct)}</td></tr>"
            )

    html = f"""\
<!doctype html><html><body style="font-family:Segoe UI,Arial,sans-serif;color:#0f172a">
<div style="max-width:720px;margin:0 auto;padding:20px">
  <div style="background:linear-gradient(135deg,#dc2626,#b45309);color:white;padding:16px 20px;border-radius:8px;margin-bottom:16px">
    <div style="font-size:13px;letter-spacing:1px;text-transform:uppercase;opacity:0.85">Kilter · SLA breach</div>
    <div style="font-size:22px;font-weight:700;margin-top:4px">
      {total} open item{'s' if total != 1 else ''} aged over {channel['threshold_days']} days
    </div>
  </div>
  <p style="color:#52606d">These breaks have passed the configured SLA threshold and need investigation or write-off.</p>
  <table style="width:100%;border-collapse:collapse;font-size:13px">
    <thead>
      <tr style="background:#f5f7fa;color:#52606d;text-transform:uppercase;font-size:11px">
        <th style="text-align:left;padding:6px">Account</th>
        <th style="text-align:left;padding:6px">Team</th>
        <th style="text-align:right;padding:6px">Amount</th>
        <th style="text-align:left;padding:6px">Ref</th>
        <th style="text-align:left;padding:6px">Narration</th>
        <th style="text-align:left;padding:6px">Opened</th>
      </tr>
    </thead>
    <tbody>
      {''.join(html_rows) or '<tr><td colspan="6" style="padding:12px;color:#52606d">(empty)</td></tr>'}
    </tbody>
  </table>
  <p style="margin-top:20px;font-size:12px;color:#9aa5b1">
    Sent by Kilter · channel <code>{_h(channel['name'])}</code> at {datetime.utcnow().isoformat(timespec='seconds')} UTC.<br>
    This is an automated message. Do not reply.
  </p>
</div>
</body></html>"""

    text = (
        f"Kilter SLA alert\n"
        f"------------------\n\n"
        f"{total} open item(s) aged over {channel['threshold_days']} days.\n\n"
        + '\n'.join(
            f"- {acct}: {len(rows)} item(s)"
            for acct, rows in sorted(by_account.items(), key=lambda x: -len(x[1]))[:20]
        )
        + '\n\nOpen Kilter for details.\n'
    )

    msg = MIMEMultipart('alternative')
    msg['Subject'] = f"[Kilter] {total} aged reconciliation break(s)"
    msg['From']    = frm
    msg['To']      = ', '.join(tos)
    msg.attach(MIMEText(text, 'plain'))
    msg.attach(MIMEText(html, 'html'))

    try:
        with smtplib.SMTP(host, port, timeout=15) as smtp:
            if use_starttls:
                ctx = ssl.create_default_context()
                smtp.starttls(context=ctx)
            if user:
                smtp.login(user, pwd)
            smtp.sendmail(frm, tos, msg.as_string())
    except Exception as exc:
        return 0, f"SMTP error: {type(exc).__name__}: {exc}"
    return total, None


def _h(s) -> str:
    """Tiny HTML escape for email bodies (avoid pulling in markupsafe)."""
    if s is None:
        return ''
    return (str(s)
            .replace('&', '&amp;').replace('<', '&lt;')
            .replace('>', '&gt;').replace('"', '&quot;'))


def _dispatch_teams(config: dict, channel: dict,
                    items: list[dict]) -> tuple[int, str | None]:
    """Post a single summary message to a Microsoft Teams incoming webhook.
    MS Teams' legacy "MessageCard" format is used — the newer Adaptive
    Cards schema is richer but requires Power Automate workflows and adds
    operational cost ops doesn't want yet."""
    webhook = config.get('webhook_url', '').strip()
    if not webhook:
        return 0, 'no webhook_url in config'

    # Group items for a compact payload.
    total = len(items)
    by_group: dict[str, int] = {}
    by_account: dict[str, int] = {}
    for it in items:
        by_group[it.get('functional_group') or 'PSC TROPS'] = \
            by_group.get(it.get('functional_group') or 'PSC TROPS', 0) + 1
        by_account[it['account_label']] = by_account.get(it['account_label'], 0) + 1

    facts = [
        {'name': 'Aged items', 'value': str(total)},
        {'name': 'Threshold', 'value': f"> {channel['threshold_days']} days"},
    ]
    for grp, n in sorted(by_group.items(), key=lambda x: -x[1])[:5]:
        facts.append({'name': grp, 'value': str(n)})
    top_accounts = sorted(by_account.items(), key=lambda x: -x[1])[:5]
    accounts_text = '\n'.join(f"- **{a}** — {n}" for a, n in top_accounts)

    card = {
        '@type': 'MessageCard',
        '@context': 'https://schema.org/extensions',
        'summary': f'Kilter SLA breach: {total} aged open items',
        'themeColor': 'DC2626',
        'title': f'⚠️ {total} open item{"s" if total != 1 else ""} aged over {channel["threshold_days"]} days',
        'sections': [{
            'activityTitle': 'Kilter SLA alert',
            'facts': facts,
            'text': f'**Top accounts:**\n{accounts_text}' if top_accounts else '',
            'markdown': True,
        }],
    }
    req = urllib.request.Request(
        webhook,
        data=json.dumps(card).encode('utf-8'),
        headers={'Content-Type': 'application/json'},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            if 200 <= r.status < 300:
                return total, None
            return 0, f'teams webhook returned {r.status}'
    except urllib.error.HTTPError as e:
        return 0, f'HTTP {e.code}: {e.reason}'
    except urllib.error.URLError as e:
        return 0, f'URL error: {e.reason}'


def _dispatch_log(channel: dict, items: list[dict]) -> None:
    """Write-to-audit-log channel. Useful for testing the flow without a
    live webhook, and doubles as a forensic record."""
    # noqa: this isn't pretty but keeps the module self-contained.
    import sqlite3
    from db import DB_PATH
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO audit_log (session_id, action, actor, timestamp, details) "
        "VALUES (NULL, 'sla_alert_log', 'system', ?, ?)",
        (datetime.utcnow().isoformat(),
         json.dumps({'channel_id': channel['id'], 'items': len(items),
                     'first_five': [
                         {'account': i['account_label'], 'amount': i['amount'],
                          'ref': i['ref'], 'opened_at': i['opened_at']}
                         for i in items[:5]]})),
    )
    conn.commit()
    conn.close()


def _update_channel_run(conn, channel_id: int, result: str) -> None:
    conn.execute(
        "UPDATE notification_channels SET last_run_at=?, last_result=? WHERE id=?",
        (datetime.utcnow().isoformat(), result, channel_id),
    )
