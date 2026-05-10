# Pilot Evaluation Agreement — Kilter

**Status:** DRAFT for lawyer review.
**Form:** Paid 90-day pilot at USD 5,000, fully credited against Year 1
fees on conversion. Designed to be short and signable in one day (a real
bank legal team will redline anyway, so don't gold-plate it).

---

This Pilot Evaluation Agreement (this "**Pilot Agreement**") is entered
into as of [EFFECTIVE DATE] between:

- **Timeless Nypo Tech** ("**Kilter**"); and
- **[BANK LEGAL NAME]** ("**Bank**").

## 1. Purpose

Kilter wishes to enable Bank to evaluate Kilter's reconciliation software
(the "**Software**") in Bank's own environment for a limited period at a
fixed fee, and Bank wishes to evaluate the Software's fit for its nostro,
mobile-money, and (where elected) card-scheme reconciliation operations.

## 2. Pilot Term and Fee

2.1 The pilot evaluation period (the "**Pilot Term**") begins on the
Effective Date and runs for ninety (90) calendar days unless extended by
written agreement.

2.2 In consideration of access to the Software and the implementation
support set out in Section 3.2, Bank shall pay Kilter a one-time pilot
fee of **USD 5,000** (or local-currency equivalent at the rate agreed at
signing). The fee shall be invoiced on the Effective Date and is payable
net 30.

2.3 The pilot fee is **fully creditable against Year 1 production
licence fees** if Bank signs a Master Services Agreement and Order Form
within thirty (30) days after the Pilot Term ends (Section 5.2).

2.4 Either Party may terminate the pilot at any time on five (5)
business days' written notice. If Bank terminates without cause before
day forty-five (45) of the Pilot Term, Kilter shall refund 50% of the
pilot fee. Termination after day forty-five (45) is non-refundable.

## 3. Scope of Pilot

3.1 Bank shall provide:
- Sample SWIFT MT940/MT950 and/or ISO 20022 camt.053/054 statements
  covering up to three (3) nostro / GL account pairs.
- Up to two (2) mobile-money / wallet account feeds, where Bank elects
  to exercise the mobile-money module during the pilot.
- One (1) card-acquirer or card-issuer settlement feed, where Bank
  elects to exercise the cards module during the pilot.
- One technical contact authorised to coordinate with Kilter.
- Access to a non-production environment (Bank-controlled VM, Docker
  host, or air-gapped laptop) for Software installation.

3.2 Kilter shall provide:
- A licensed copy of the Software for the Pilot Term with **all
  modules unlocked** for evaluation, hard-capped at the account counts
  in Section 3.1.
- Deployment and initial onboarding session (typically 90 minutes).
- Up to ten (10) hours of remote implementation support during the
  Pilot Term.
- Documentation, including the operator manual and onboarding wizard.

3.3 The Software shall be deployed **on-premises or in Bank's private
cloud**. Kilter shall not host or transmit Bank data outside Bank's
infrastructure during the pilot unless Bank expressly elects a
Kilter-managed evaluation in writing.

## 4. Success Criteria

The Parties agree that the pilot shall be deemed successful if all of the
following are demonstrated to Bank's reasonable satisfaction by the end of
the Pilot Term:

(a) The Software ingests at least 95% of Bank's submitted SWIFT and GL
    files without manual format intervention.
(b) The Software achieves an automated match rate of at least
    [SEVENTY-FIVE PERCENT (75%)] on tier-1 (exact-match) candidates,
    measured over a contiguous five-business-day sample.
(c) End-of-day reconciliation cycle time is reduced by at least
    [FIFTY PERCENT (50%)] versus Bank's then-current baseline.
(d) No critical defects (data loss, incorrect match commitment, or audit
    log gap) are observed during the Pilot Term.

> Lawyer/sales note: These percentages are negotiable per pilot. The
> point of stating them explicitly is to make the conversion conversation
> a yes/no question, not a debate. Set them where you are confident the
> Software will land.

## 5. Conversion to Paid Engagement

5.1 If the success criteria are met, the Parties shall negotiate in good
faith to enter into a Master Services Agreement and Order Form within
thirty (30) days after the Pilot Term ends.

5.2 **Pilot fee credit.** If Bank executes a paid Order Form within
thirty (30) days after the Pilot Term ends, the full pilot fee paid
under Section 2.2 (USD 5,000) shall be credited against Year 1
production licence fees.

5.3 **Design-partner incentive.** If, in addition, the success criteria
in Section 4 have been demonstrated to Bank's reasonable satisfaction
and Bank executes a paid Order Form within sixty (60) days after the
Pilot Term ends, Bank shall additionally be entitled to:
- A [50%] discount on Year 1 production licence fees (applied after the
  pilot fee credit in Section 5.2).
- A [25%] discount on Year 2 production licence fees.
- A right to be named as a reference customer (subject to Bank's prior
  written approval of any specific reference text or logo use).

> Sales/legal note: the credit (5.2) is automatic — pilot fee always
> returns to Bank on conversion. The discount stack (5.3) is gated by
> Section 4 success criteria. Both can apply simultaneously and the
> order matters: credit first, then discount on the remainder.

5.4 If the success criteria are **not** met, neither Party has any
obligation to proceed. Bank shall return or destroy the Software within
fifteen (15) business days. Kilter shall destroy or return any Bank data
shared during the pilot within the same period and certify destruction in
writing on request. The pilot fee paid under Section 2.2 is non-refundable
in this scenario.

## 6. Confidentiality

6.1 Each Party shall protect the other's Confidential Information with at
least reasonable care and shall not disclose it to any third party except
employees and advisors with a need to know who are bound by
confidentiality obligations.

6.2 Confidential Information includes the Software, pricing, security
architecture, Bank's transaction data, system topology, and any non-public
business or technical information disclosed during the pilot.

6.3 This obligation survives termination for three (3) years.

## 7. Bank Data

7.1 Bank retains all right, title, and interest in any data shared with
or processed by the Software during the pilot ("**Pilot Data**").

7.2 Kilter shall use Pilot Data solely to operate and demonstrate the
Software for Bank during the Pilot Term. Kilter shall not transmit Pilot
Data outside Bank's environment, copy it for Kilter's records, or use it
to train models or improve the Software in any way that retains Bank's
data, identity, or counterparty information.

7.3 Kilter may collect aggregated, anonymised operational metrics solely
about the Software's performance (e.g., parser throughput, match rates),
provided no Pilot Data is identifiable.

7.4 To the extent Pilot Data includes personal data within the meaning of
Ghana's Data Protection Act 2012 (Act 843) or other applicable privacy
law, the Data Processing Addendum at `legal/04_dpa.md` applies.

## 8. Intellectual Property

8.1 The Software remains the exclusive property of Timeless Nypo Tech. No licence is
granted other than a temporary, non-exclusive, non-transferable evaluation
licence for the Pilot Term.

8.2 Bank shall not reverse-engineer, copy, modify, or sublicense the
Software during or after the Pilot Term, except to the extent required by
mandatory law.

8.3 Any feedback, suggestions, or feature requests provided by Bank may be
used by Kilter without obligation, provided no Bank Confidential
Information is disclosed.

## 9. Warranties and Limitation of Liability

9.1 **Pilot Disclaimer.** THE SOFTWARE IS PROVIDED FOR EVALUATION
PURPOSES ONLY, "AS IS" AND WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED. BANK SHOULD NOT RELY ON THE SOFTWARE FOR PRODUCTION FINANCIAL
CONTROLS DURING THE PILOT TERM AND SHALL MAINTAIN ITS EXISTING
RECONCILIATION CONTROLS IN PARALLEL.

9.2 **Liability Cap.** EACH PARTY'S AGGREGATE LIABILITY ARISING FROM THE
PILOT IS LIMITED TO USD [TEN THOUSAND (10,000)]. THIS CAP DOES NOT APPLY
TO BREACH OF CONFIDENTIALITY (SECTION 6) OR FRAUD/WILFUL MISCONDUCT.

9.3 **No Consequential Damages.** Neither Party is liable for indirect,
incidental, special, or consequential damages.

## 10. Governing Law

This Pilot Agreement is governed by the laws of [GHANA / BANK'S
JURISDICTION — typically negotiated]. Disputes shall be resolved by
arbitration in [SEAT] under the rules of [INSTITUTION].

> Lawyer note: For pilots with non-Ghanaian banks, expect them to insist
> on their own jurisdiction. Acceptable trade — the pilot value comes
> from getting in the door, not from the legal terms.

## 11. General

11.1 This Pilot Agreement constitutes the entire agreement between the
Parties regarding the pilot and supersedes prior discussions.

11.2 No Party may assign this Pilot Agreement without the other's written
consent.

11.3 Sections 6, 7, 8, 9, and 11 survive termination.

---

**IN WITNESS WHEREOF**, the Parties have executed this Pilot Agreement
as of the Effective Date.

| KILTER LIMITED | [BANK LEGAL NAME] |
|---|---|
| Signed: __________________ | Signed: __________________ |
| Name: [NAME] | Name: [NAME] |
| Title: [TITLE] | Title: [TITLE] |
