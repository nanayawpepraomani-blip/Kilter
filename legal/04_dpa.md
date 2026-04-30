# Data Processing Addendum — Kilter

**Status:** DRAFT for lawyer review.
**Form:** Stand-alone DPA, incorporated by reference into the MSA or
Pilot Agreement.

> Lawyer note: This DPA is drafted to be portable across jurisdictions.
> The Ghana Data Protection Act 2012 (Act 843) is the primary framework;
> GDPR/UK GDPR concepts are aligned for European pilot banks. Local
> finance regulators (BoG, FCA, ECB) may impose additional outsourcing
> requirements — verify before signing with regulated banks.

---

This Data Processing Addendum (this "**DPA**") forms part of and is
incorporated into the agreement between **Kilter Limited** ("**Kilter**")
and **[CUSTOMER]** ("**Customer**") under which Kilter provides software
and services to Customer (the "**Principal Agreement**"). In the event of
conflict between this DPA and the Principal Agreement on the subject of
data protection, this DPA controls.

## 1. Definitions

- **"Applicable Privacy Law"** — Ghana's Data Protection Act 2012
  (Act 843), and any other privacy or data-protection law of any
  jurisdiction applicable to the processing of Personal Data under the
  Principal Agreement, including (where applicable) the EU GDPR
  (Regulation 2016/679) and the UK GDPR.
- **"Personal Data"** — any information relating to an identified or
  identifiable natural person, as defined under Applicable Privacy Law.
- **"Controller", "Processor", "Sub-processor", "Data Subject",
  "Process/Processing"** — as defined in Applicable Privacy Law.
- **"Customer Data"** — all data uploaded to or processed by the
  Software on Customer's behalf.
- **"Sub-processor"** — any third-party Processor engaged by Kilter to
  Process Personal Data on Customer's behalf.

## 2. Roles of the Parties

2.1 With respect to Personal Data Processed under the Principal
Agreement, **Customer is the Controller and Kilter is the Processor**.
Customer is responsible for the lawfulness of the data it submits and
for obtaining all necessary consents and notices from Data Subjects.

2.2 Kilter shall Process Personal Data only on the documented
instructions of Customer, including with regard to transfers, unless
required to do otherwise by law (in which case Kilter shall inform
Customer of that legal requirement before Processing, unless prohibited).

## 3. Subject Matter, Duration, Nature, and Purpose

| Item | Detail |
|---|---|
| Subject matter | Provision of the Kilter reconciliation Software and related services. |
| Duration | The term of the Principal Agreement, plus any retention period required by law. |
| Nature & purpose | Ingestion, parsing, matching, storage, and reporting of financial transaction data on Customer's behalf for nostro/GL reconciliation. |
| Categories of Data Subjects | Customer's employees and contractors who are Authorised Users; counterparties named in transaction narratives. |
| Categories of Personal Data | Authorised User identifiers (username, email, role, MFA secret); transaction metadata (counterparty name, narrative text, account references). No special-category data is intended to be Processed; Customer shall not submit such data unless agreed in writing. |

## 4. Customer's Obligations

4.1 Customer warrants that it has a lawful basis under Applicable Privacy
Law to disclose Personal Data to Kilter for Processing under the
Principal Agreement.

4.2 Customer shall provide all required notices and obtain all required
consents from Data Subjects.

4.3 Customer shall not submit special-category Personal Data, payment
card numbers, or government identification numbers to the Software
without Kilter's prior written agreement and an appropriate amendment to
this DPA.

## 5. Kilter's Obligations

5.1 **Confidentiality.** Kilter shall ensure that personnel authorised
to Process Personal Data have committed themselves to confidentiality or
are under a statutory duty of confidentiality.

5.2 **Security.** Kilter shall implement appropriate technical and
organisational measures to protect Personal Data, including those set
out in Annex A.

5.3 **Assistance with Data Subject Rights.** Taking into account the
nature of Processing, Kilter shall assist Customer by appropriate
technical and organisational measures, insofar as possible, in
fulfilling Customer's obligation to respond to requests for exercising
Data Subject rights (access, rectification, erasure, restriction,
portability, objection).

5.4 **Assistance with Compliance.** Kilter shall assist Customer in
ensuring compliance with obligations regarding security, breach
notification, data protection impact assessments, and prior consultation
with supervisory authorities, taking into account the nature of
Processing and the information available to Kilter.

5.5 **Return or Deletion.** Upon termination of the Principal Agreement,
Kilter shall, at Customer's choice, return or delete all Personal Data
within thirty (30) days, unless retention is required by law. Kilter
shall certify deletion in writing on request.

5.6 **Records.** Kilter shall maintain records of Processing activities
in accordance with Applicable Privacy Law and shall make them available
to Customer or supervisory authorities on request.

## 6. Sub-processors

6.1 Customer provides general written authorisation for Kilter to engage
the Sub-processors listed in Annex B.

6.2 Kilter shall give Customer at least thirty (30) days' prior written
notice of any addition or replacement of Sub-processors. Customer may
object on reasonable data-protection grounds within fifteen (15)
business days. If the Parties cannot agree, Customer may terminate the
affected portion of the Principal Agreement on written notice without
penalty.

6.3 Kilter shall impose contractual data-protection obligations on each
Sub-processor that are no less protective than those in this DPA, and
remains fully liable to Customer for the Sub-processor's performance.

## 7. International Transfers

7.1 Kilter shall not transfer Personal Data outside Ghana (or, where
applicable to Customer, outside the European Economic Area or United
Kingdom) without ensuring an adequate level of protection through one of
the following: (a) an adequacy decision by the relevant supervisory
authority; (b) Standard Contractual Clauses (SCCs) or equivalent; (c)
binding corporate rules; or (d) any other lawful transfer mechanism
under Applicable Privacy Law.

7.2 Where SCCs apply, the Parties shall execute the relevant SCC module
(typically Module 2: Controller-to-Processor) as a separate document or
attached annex.

## 8. Personal Data Breach

8.1 Kilter shall notify Customer without undue delay, and in any event
within seventy-two (72) hours, after becoming aware of a Personal Data
breach affecting Customer Data.

8.2 The notification shall include, to the extent known: (a) the nature
of the breach; (b) the categories and approximate number of Data
Subjects and records concerned; (c) likely consequences; (d) measures
taken or proposed to address the breach and mitigate effects.

8.3 Kilter shall cooperate with Customer's reasonable requests in
investigating and remediating the breach.

## 9. Audits

9.1 Kilter shall make available to Customer, on reasonable written
request and no more than once per twelve (12) months (except in the
event of a material breach or regulatory request), information necessary
to demonstrate compliance with this DPA. This may be satisfied by
Kilter's then-current SOC 2 Type II report, ISO/IEC 27001 certificate,
or equivalent third-party attestation.

9.2 If the above is insufficient to demonstrate compliance, Customer (or
an independent auditor mandated by Customer and reasonably acceptable to
Kilter) may conduct an on-site audit on thirty (30) days' written
notice, during business hours, subject to confidentiality obligations
and at Customer's expense.

## 10. Liability

The liability of each Party under this DPA is subject to the limitations
in the Principal Agreement, except where Applicable Privacy Law
prohibits such limitation.

## 11. Term and Termination

This DPA remains in effect for so long as Kilter Processes Personal Data
on Customer's behalf, and survives termination of the Principal
Agreement to the extent necessary for Kilter to comply with its
obligations under this DPA.

---

## Annex A — Technical and Organisational Security Measures

| Domain | Measures |
|---|---|
| **Access control** | Role-based access; principle of least privilege; mandatory MFA (TOTP) for all Authorised Users; named individual accounts (no shared logins); session token expiry (8 hours); admin actions logged. |
| **Encryption — at rest** | TOTP secrets and SMTP credentials encrypted with Fernet (AES-128-CBC + HMAC-SHA256). Keys held in OS environment / HSM; never in source. Database files reside on Customer's filesystem; full-disk encryption is Customer's responsibility. |
| **Encryption — in transit** | HTTPS/TLS 1.2+ for all browser and API traffic. HSTS enforced. Internal API calls use mutual TLS where the deployment topology supports it. |
| **Network** | Private deployment; no inbound traffic from public internet to the application instance other than to the HTTPS listener. CORS allowlist. Rate limiting on authentication endpoints. |
| **Application security** | OWASP top-10 hardening; security headers (CSP, X-Frame-Options, X-Content-Type-Options, Referrer-Policy); input validation; parameterised SQL; CSRF protection on state-changing endpoints. |
| **Secure development** | Code review prior to merge; pinned dependencies; vulnerability scanning on dependencies (`pip-audit` or equivalent) at least monthly. |
| **Logging & audit** | Append-only audit log of authentication, MFA enrollment, session issuance/revocation, file ingestion, match commitment, and configuration change. Retained for the term plus 12 months. |
| **Backup & recovery** | Daily encrypted backups under Customer's control. Restore tested at least annually. Backups inherit the same protection as production. |
| **Personnel** | Background checks where local law permits; written confidentiality undertakings; data-protection training on hire and at least annually. |
| **Incident response** | Documented incident-response plan; named on-call contact; 72-hour breach-notification clock from confirmed awareness. |
| **Data minimisation** | The Software stores only the data needed for reconciliation. Statement file payloads are retained for the audit window; Authorised User passwords are not stored (TOTP only); no analytics SDKs are embedded. |
| **Sub-processor management** | Annex B; written contracts with at-least-equivalent obligations; due-diligence review prior to onboarding. |

## Annex B — Authorised Sub-processors (initial list)

| Sub-processor | Purpose | Location | Notes |
|---|---|---|---|
| [None at go-live for on-premises deployments] | — | — | Software runs entirely within Customer's infrastructure for on-prem and private-cloud deployments. |
| [SMTP / email-relay provider, if used by SLA Notifier] | Outbound email notifications | [REGION] | Only if Customer enables SLA email notifications and uses Kilter-recommended provider. Customer may substitute its own SMTP. |
| [Cloud host, if Kilter-managed deployment] | Application hosting | [REGION] | Only applicable to Kilter-managed deployments. Default: AWS Frankfurt (eu-central-1) for European customers; AWS Cape Town (af-south-1) for African customers. |
| [Telemetry / error-reporting service, if any] | Application error reporting | [REGION] | Only if Customer opts in; PII is scrubbed before transmission. |

---

**IN WITNESS WHEREOF**, the Parties have executed this DPA as of the
Effective Date of the Principal Agreement.

| KILTER LIMITED | [CUSTOMER LEGAL NAME] |
|---|---|
| Signed: __________________ | Signed: __________________ |
| Name: [NAME] | Name: [NAME] |
| Title: [TITLE] | Title: [TITLE] |
