# Kilter — CISO Pack

This folder is the standing security packet to hand to a prospective
pilot bank's information-security team. It exists so we can short-cut
the typical 4–8 week vendor-review cycle: most banks ask the same
30-ish questions, and most of those answers don't change between
prospects. Hand this over with the pilot agreement, save everyone two
months.

## Contents

| File | Purpose | Audience |
|---|---|---|
| [PENTEST_SUMMARY.md](PENTEST_SUMMARY.md) | Test scope, methodology, findings, remediation status, retest date | CISO / IS team |
| [ARCHITECTURE.md](ARCHITECTURE.md) | Network topology, components, data flow, deployment options | Architects |
| [THREAT_MODEL.md](THREAT_MODEL.md) | STRIDE threats, mitigations, residual risk, assumptions | Architects / IS team |
| [SECURITY_FAQ.md](SECURITY_FAQ.md) | Pre-filled answers to the most common ~35 questions on SIG-Lite / CAIQ-Lite / SIG Core | Risk / vendor management |

## How to use it

1. After a prospect signs the [Pilot Agreement](../legal/03_pilot_agreement.md)
   and the [DPA](../legal/04_dpa.md), email them this folder
   (zip it; do not send raw markdown to a CISO — convert via pandoc as
   we do for the legal pack).
2. Their IS team reviews and sends back a delta — typically a handful
   of bank-specific follow-up questions. Answer those in writing,
   merge into a "Customer Q&A" appendix when stable patterns emerge.
3. If they require their own bespoke questionnaire (some banks do),
   use [SECURITY_FAQ.md](SECURITY_FAQ.md) as the source of truth and
   port answers across.

## What's not here (yet)

These are deferred until either a pilot bank specifically asks or we
have budget for them:

- **External pentest report.** Internal-pentest summary only at this
  stage; flagged in [PENTEST_SUMMARY.md](PENTEST_SUMMARY.md) so we don't
  misrepresent. A formal third-party engagement (Bishop Fox /
  Trail of Bits / NCC Group / a regional Big-4 cyber arm) costs
  USD 15k–30k for a focused 1-week engagement and should happen before
  the second paid customer.
- **SOC 2 Type II report.** Earliest realistic point is 12 months
  after the first pilot ships. SOC 2 Type I (point-in-time) can be
  done in ~3 months for ~USD 15k if a bank requires it as a
  condition.
- **ISO 27001 certificate.** Heavy lift; defer until we have ≥3 paying
  customers and a full-time security lead.
- **Penetration-test attestation letter.** Once an external pentest
  closes out, the firm typically issues a one-page attestation suitable
  for sharing with customers without the full report.
