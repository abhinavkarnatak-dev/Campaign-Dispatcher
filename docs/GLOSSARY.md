# Glossary

The vocabulary we use every day at Agentic Universe. Both assignments use these terms.

| Term | Meaning |
|---|---|
| **Tenant** | A client company using the platform (for example an insurer). Limits like call concurrency apply per tenant. |
| **Workflow** | A business process run by voice agents, such as `motor_renewal` (remind customers to renew their policy) or `policy_feedback` (collect feedback after purchase). Each workflow has its own lead schema and disposition catalogue. |
| **Campaign** | One run of outbound calling for a tenant: a set of leads, a calling window, and retry rules. |
| **Lead** | A customer we intend to call, uploaded by the client. Identified within a workflow by phone number. |
| **Batch** | One uploaded file of leads. Reports are often grouped by batch ("how is Monday's upload doing?"). |
| **Lead schema** | Per-workflow definition of the columns we accept, their types, which are required, and which are PII. |
| **PII** | Personally identifiable information: names, phone numbers. Must be protected at rest. |
| **Call / Interaction** | One attempt to reach a lead. A call may fail at the telephony layer, ring out unanswered, or connect. |
| **Connectivity rate** | Connected calls ÷ total calls. A core health metric for every campaign. |
| **Disposition** | The classified outcome of a connected call, for example `renewed`, `callback_requested`, `not_interested`. Set by the AI agent after the call. |
| **Terminal disposition** | A disposition that ends our work on a lead. No more calls. |
| **Success disposition** | A disposition that counts as a business win for the client (for example `renewed`). |
| **Override** | A human supervisor correcting the AI's disposition. Recorded as a **new** event; the original is never edited. |
| **Interaction ledger** | The append-only record of everything that happened on every call. Reports are derived from it; it is the single source of truth. |
| **Calling window** | The hours (and days) during which we may start calls. Regulation in India restricts commercial calls to 09:00–21:00. |
| **DND** | "Do Not Disturb": the customer has asked never to be called. Calling them again is a compliance breach. |
| **Callback** | The customer asked to be called back at a specific time. Honouring it promptly matters a lot for conversion. |
| **Telephony provider** | The carrier API that actually places calls and reports results via webhooks. |
| **Idempotency key** | A client-chosen ID (`request_id`) that makes retrying a request safe: the provider performs the action at most once per key. |
