# Client Submission: Reliable Encounter Summary Backend

**Submitted by:** Sandhani  
**Submission date:** 12 November 2026  
**Deliverables:** Design, API contract, database structure, processing pseudocode, failure analysis, verification evidence, and optional runnable code.

## Submission overview

This document describes a backend that accepts encounter snapshots without waiting for AI generation, preserves the newest version under duplicate or out-of-order delivery, and processes durable background summary jobs. Each summary remains tied to its exact input version. It includes the design decisions and their trade-offs, rather than claiming exactly-once execution of a paid external operation.

**Optional implementation:** [GitHub repository](https://github.com/sandhani/encounter-summary-service). Run instructions and known limitations are included at the end of this document. The implementation provides local orchestration and synthetic test tools; clinical summary model design is outside scope.

## Assumptions

1. Events contain complete snapshots, not incremental changes. `payload.transcription` is an existing full transcript; this service does not perform speech-to-text.
2. Versions are positive integers ordered independently for each encounter. Missing intermediate versions do not block newer snapshots.
3. Each encounter belongs to one fixed patient and one fixed encounter type. One patient can have multiple encounters. An incident in the optional bulk dataset groups multiple patient encounters; it is not a separate database entity.
4. Retry deliveries reuse an event ID and identical logical content. A second event ID for an identical encounter/version is treated as a duplicate. Violations of these content/identity assumptions are rejected with 409.
5. API and worker processes share a single SQLite database on durable local storage. Recovery assumes that committed database files remain available after a process restart.
6. The AI dependency can be slow, fail, charge for repeated invocations and produce different wording. No remote idempotency or result-lookup capability is assumed.
7. The ten-second acceptance-to-ready SLA is a normal-operation target with adequate capacity, not a deadline that terminates work or a guarantee demonstrated by synthetic tests.
8. All supplied test patients, incident IDs, transcriptions and summaries are synthetic. Live AI quality and production clinical deployment are outside this exercise.

## 1. Design

### Architecture and assumptions

The partner submits complete encounter snapshots to an HTTP API. The API atomically records the update and durable summary job in SQLite, then acknowledges immediately. Independent workers poll the database, call the external summary service, and save results associated with the exact input version. Clients poll the API for the current version's progress and summary.

[Editable HLD and LLD diagrams](https://lucid.app/lucidchart/dbd10843-88ef-4450-8731-6344f7b33dec/edit) show the component overview, ingestion flow, and worker/recovery flow.

The reference implementation uses Python 3.11+ and SQLite WAL with foreign keys and synchronous FULL. API and worker processes share durable local storage on one host. SQLite is both the state store and durable queue; there is no separate message broker or handoff to lose. Multi-host deployment would require a database such as PostgreSQL and equivalent transactional row locking, rather than a SQLite database on a network filesystem.

Each event contains a full snapshot. Versions are positive integers, may skip numbers, and are ordered independently per encounter. Patient identity and encounter type remain fixed. Valid types are `Appointment`, `MedicationRefill`, and `TelephoneTriage`. The API accepts a nonempty transcription up to its 2,000,000-byte HTTP body limit; the complete transcription is passed unchanged to the provider.

Core guarantees:

- The current encounter version increases monotonically.
- Duplicate receipts and duplicate logical versions cannot schedule duplicate jobs.
- A committed current update always has a committed job.
- A worker reads its own immutable version input.
- An old result never appears as the current version's summary.
- External execution is at least once under recovery, with bounded automatic attempts; exactly-once paid provider invocation is not promised.

### API contract

#### Accept an update

```http
POST /events
Content-Type: application/json
```

```json
{
  "event_id": "evt-102",
  "encounter_id": "enc-42",
  "patient_id": "pat-77",
  "encounter_type": "TelephoneTriage",
  "version": 12,
  "payload": {
    "transcription": "Nurse: How can I help today? Patient: My throat hurts. Nurse: When did it start? Patient: Yesterday."
  }
}
```

The transcript is synthetic and abbreviated for readability. Production snapshots can contain hundreds of lines.

**Accepted and current — HTTP 202**

```json
{
  "outcome": "accepted_current",
  "encounter_id": "enc-42",
  "version": 12,
  "current_version": 12
}
```

202 means the state and background work are committed, while summary generation is pending. The API does not invoke or wait for AI. “Current” describes the transaction's outcome: another request may advance the encounter immediately afterward.

**Duplicate — HTTP 200**

Retry the same event, or send identical logical content for version 12 with a new event ID:

```json
{
  "outcome": "duplicate",
  "encounter_id": "enc-42",
  "version": 12,
  "current_version": 13
}
```

This example assumes version 13 arrived meanwhile. Existing version 12 takes precedence over the stale classification. A new alias event ID gets a receipt, but no additional version or job. Reusing an existing event ID requires matching logical content.

**Stale — HTTP 200**

Submit a previously unseen version 11 while version 13 is current:

```json
{
  "outcome": "stale",
  "encounter_id": "enc-42",
  "version": 11,
  "current_version": 13
}
```

200 confirms successful handling, even though the current state does not advance. The old snapshot and receipt are retained for deduplication and inspection, with no summary job.

**Rejected conflicts — HTTP 409**

```json
{"error": "encounter identity conflict"}
```

Other conflict messages are `event ID content conflict` and `encounter version content conflict`. All conflicts roll back the transaction. This protects against source defects rather than choosing one conflicting payload arbitrarily.

Malformed input gets 400; an empty or oversized declared body gets 413. Storage errors get 503, so the caller should retry the identical event. A lost HTTP acknowledgement is also safe to retry.

#### Retrieve current progress or summary

```http
GET /encounters/enc-42/summary
```

Every successful retrieval returns 200, including a failed workflow: the request to read its state succeeded. Unknown encounters return 404. Timestamps are Unix seconds.

**Queued**

```json
{
  "encounter_id": "enc-42",
  "patient_id": "pat-77",
  "encounter_type": "TelephoneTriage",
  "version": 12,
  "status": "queued",
  "summary": null,
  "attempts": 0,
  "accepted_at": 1800000000.0,
  "started_at": null,
  "finished_at": null,
  "next_attempt_at": 1800000000.0,
  "error": null,
  "sla_breached": false
}
```

**Processing**

```json
{
  "encounter_id": "enc-42",
  "patient_id": "pat-77",
  "encounter_type": "TelephoneTriage",
  "version": 12,
  "status": "processing",
  "summary": null,
  "attempts": 1,
  "accepted_at": 1800000000.0,
  "started_at": 1800000000.2,
  "finished_at": null,
  "next_attempt_at": null,
  "error": null,
  "sla_breached": false
}
```

**Ready**

```json
{
  "encounter_id": "enc-42",
  "patient_id": "pat-77",
  "encounter_type": "TelephoneTriage",
  "version": 12,
  "status": "ready",
  "summary": "Patient reports a sore throat beginning yesterday.",
  "attempts": 1,
  "accepted_at": 1800000000.0,
  "started_at": 1800000000.2,
  "finished_at": 1800000006.0,
  "next_attempt_at": null,
  "error": null,
  "sla_breached": false
}
```

**Failed after retry exhaustion**

```json
{
  "encounter_id": "enc-42",
  "patient_id": "pat-77",
  "encounter_type": "TelephoneTriage",
  "version": 12,
  "status": "failed",
  "summary": null,
  "attempts": 6,
  "accepted_at": 1800000000.0,
  "started_at": 1800000000.2,
  "finished_at": 1800000062.0,
  "next_attempt_at": null,
  "error": "provider_http_503",
  "sla_breached": true
}
```

These are separate illustrative snapshots, not one continuous execution. A permanent provider error can fail on the first attempt. A transient failure returns the job to `queued`, showing a sanitized error and the next retry time.

When version 13 arrives, this endpoint immediately reports version 13 and a null summary until its own result is ready. It never fills that gap with version 12's result.

#### Retrieve exact-version history

```http
GET /encounters/enc-42/versions/12
```

```json
{
  "version": 12,
  "status": "ready",
  "attempts": 1,
  "started": 1800000000.2,
  "finished": 1800000006.0,
  "summary": "Patient reports a sore throat beginning yesterday.",
  "error": null
}
```

Historical queued work skipped before invocation has status `superseded`. A stale-only snapshot has no job, so this job-history endpoint returns 404 even though its input version is stored.

### Data model

The following names match the actual schema; the API maps `patient` to `patient_id` and `type` to `encounter_type`.

| Record | Fields | Keys and relationships |
|---|---|---|
| `encounters` | `id`, `patient`, `type`, `current_version` | PK `id`; one fixed patient/type pair per encounter |
| `versions` | `encounter`, `version`, `transcription`, `digest`, `accepted` | Composite PK `(encounter, version)`; FK `encounter → encounters.id` |
| `events` | `id`, `digest`, `encounter`, `version` | Global PK `id`; composite FK `(encounter, version) → versions` |
| `jobs` | `encounter`, `version`, `status`, `attempts`, `due`, `token`, `lease`, `started`, `finished`, `summary`, `error` | Composite PK and FK `(encounter, version) → versions`; index `(status, due)` |

An encounter owns many versions; a version has one or more event receipts and zero or one job. A result lives on its job row and therefore inherits exact encounter/version identity. Patient and encounter type are reached through the owning encounter; they are never supplied by a worker or inferred from transcription.

Example stored records:

```text
encounters: id=enc-42, patient=pat-77, type=TelephoneTriage, current_version=13
versions:   (enc-42,12), transcription=<full v12 input>, digest=<hash>, accepted=t0
versions:   (enc-42,13), transcription=<full v13 input>, digest=<hash>, accepted=t1
events:     evt-102 → (enc-42,12)
events:     evt-103 → (enc-42,13)
jobs:       (enc-42,12), ready, summary=<v12 result>
jobs:       (enc-42,13), processing, token=<claim token>, attempts=1
```

Database primary keys enforce both required uniqueness rules. Canonical SHA-256 digests cover encounter ID, patient ID, type, version and payload, excluding event ID. Sorting JSON keys makes field-order differences irrelevant. Matching digests are treated as matching logical content, assuming negligible cryptographic collision risk.

Version immutability, fixed encounter identity, valid statuses/types, positive versions, and monotonic current pointers are enforced by application validation and transactional write paths. The schema does not contain triggers or CHECK constraints enforcing all of these rules against arbitrary direct SQL writers. Only the service should write these tables. The current pointer is maintained transactionally rather than through a composite foreign key.

All versions are retained, including stale arrivals. This uses more storage than keeping only current state and processing snapshots, but makes exact inputs and deduplication straightforward. No worker re-reads a mutable “latest transcript.” Retention and deletion are outside this exercise.

#### Physical SQLite schema

The following DDL is taken directly from `Store.__init__()` in `encounter_service/store.py`. `PK` means primary key, `FK` means foreign key, and a composite key identifies a row using multiple columns together.

```sql
CREATE TABLE IF NOT EXISTS encounters (
  id TEXT PRIMARY KEY, patient TEXT NOT NULL, type TEXT NOT NULL,
  current_version INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS versions (
  encounter TEXT NOT NULL REFERENCES encounters(id), version INTEGER NOT NULL,
  transcription TEXT NOT NULL, digest TEXT NOT NULL, accepted REAL NOT NULL,
  PRIMARY KEY(encounter,version));
CREATE TABLE IF NOT EXISTS events (
  id TEXT PRIMARY KEY, digest TEXT NOT NULL,
  encounter TEXT NOT NULL, version INTEGER NOT NULL,
  FOREIGN KEY(encounter,version) REFERENCES versions(encounter,version));
CREATE TABLE IF NOT EXISTS jobs (
  encounter TEXT NOT NULL, version INTEGER NOT NULL,
  status TEXT NOT NULL, attempts INTEGER NOT NULL DEFAULT 0,
  due REAL NOT NULL, token TEXT, lease REAL, started REAL, finished REAL,
  summary TEXT, error TEXT,
  PRIMARY KEY(encounter,version),
  FOREIGN KEY(encounter,version) REFERENCES versions(encounter,version));
CREATE INDEX IF NOT EXISTS jobs_due ON jobs(status,due);
```

#### Column definitions

**`encounters` — current encounter identity and version pointer**

| Column | SQLite type | Required / default | Meaning |
|---|---|---|---|
| `id` | TEXT | PK; nonempty string validated by API | Encounter ID; API field `encounter_id` |
| `patient` | TEXT | NOT NULL | Fixed patient ID; API field `patient_id` |
| `type` | TEXT | NOT NULL | Fixed encounter type; API field `encounter_type` |
| `current_version` | INTEGER | NOT NULL | Highest accepted version for this encounter |

There is no separate patient table: the service stores the partner's patient identifier, not a patient demographic record. Patient IDs are not unique in `encounters`; one patient can own multiple encounters of the same or different types. There is no incident table or `incident_id` column. Incident grouping in the bulk test is dataset metadata, represented in synthetic encounter IDs.

**`versions` — immutable snapshot input**

| Column | SQLite type | Required / default | Meaning |
|---|---|---|---|
| `encounter` | TEXT | NOT NULL; composite PK; FK | Owning `encounters.id` |
| `version` | INTEGER | NOT NULL; composite PK | Version within this encounter; gaps allowed |
| `transcription` | TEXT | NOT NULL | Full input transcript for this exact version |
| `digest` | TEXT | NOT NULL | Canonical SHA-256 digest of logical event content |
| `accepted` | REAL | NOT NULL | First stored timestamp for this version, in Unix seconds |

A stale version is also retained here, even though it has no summary job. Duplicate delivery does not reset `accepted`. Immutability is a write-path rule; there is no database UPDATE-prevention trigger.

**`events` — global delivery identity / deduplication receipts**

| Column | SQLite type | Required / default | Meaning |
|---|---|---|---|
| `id` | TEXT | PK; nonempty string validated by API | Global `event_id` |
| `digest` | TEXT | NOT NULL | Detects inconsistent reuse of the same event ID |
| `encounter` | TEXT | NOT NULL; composite FK | Target version's encounter |
| `version` | INTEGER | NOT NULL; composite FK | Target version number |

The FK uses `(encounter, version)` together. Neither field alone identifies a version. Multiple event IDs can reference the same version when identical logical content arrives under alias IDs. Therefore `(encounter, version)` is intentionally **not unique in events**; it is unique in `versions`.

**`jobs` — durable queue, processing ownership, status and result**

| Column | SQLite type | Required / default | Meaning |
|---|---|---|---|
| `encounter` | TEXT | NOT NULL; composite PK/FK | Exact version's encounter |
| `version` | INTEGER | NOT NULL; composite PK/FK | Exact summary input version |
| `status` | TEXT | NOT NULL | `queued`, `processing`, `ready`, `failed`, or `superseded` |
| `attempts` | INTEGER | NOT NULL; DEFAULT 0 | Claims consumed in the current automatic retry/redrive cycle |
| `due` | REAL | NOT NULL | Earliest claim time when queued; initially acceptance time |
| `token` | TEXT | Nullable | Unique generated claim token used to fence outdated completions |
| `lease` | REAL | Nullable | Lease-expiry timestamp for the claim |
| `started` | REAL | Nullable | First processing-start timestamp, preserved across retries/redrive |
| `finished` | REAL | Nullable | Terminal completion/supersession timestamp; cleared on redrive |
| `summary` | TEXT | Nullable | Generated result for this exact version; normally present only when ready |
| `error` | TEXT | Nullable | Sanitized provider or attempt-budget error category |

`due`, `lease`, `started`, `finished`, and `accepted` use Unix seconds, including fractional seconds. Some inactive fields are retained: for example, a ready job can retain its previous lease/due values. Interpret those fields alongside `status`; they do not make a terminal job eligible for processing. `token` is cleared on accepted completion. It is generated with UUID hex but has no UNIQUE constraint in the schema.

#### Primary keys, indexes and relationship enforcement

| Key / index | Enforcement | Purpose |
|---|---|---|
| `encounters` PK `(id)` | SQLite primary-key unique index | One row per encounter |
| `versions` PK `(encounter, version)` | SQLite composite primary-key unique index | One logical snapshot per encounter/version |
| `events` PK `(id)` | SQLite primary-key unique index | Global event-ID uniqueness |
| `jobs` PK `(encounter, version)` | SQLite composite primary-key unique index | At most one job/result row per version |
| `jobs_due(status, due)` | Explicit nonunique index | Find due queued jobs by status and time |

No explicit patient/type, token, or receipt-version lookup index exists. Composite PK indexes support lookups by encounter and encounter/version. SQLite does not automatically add a child-column index for every foreign key.

Relationships enforced with `PRAGMA foreign_keys=ON` on every service connection:

- `versions.encounter` references `encounters.id`.
- `events.(encounter, version)` references `versions.(encounter, version)`.
- `jobs.(encounter, version)` references `versions.(encounter, version)`.

The application creates at least one receipt for every version and exactly one job for every advancing version. The database relationships alone allow a version with zero receipts or zero jobs; those minimum cardinalities are application guarantees. Stale-only versions deliberately have zero jobs. Summary results are stored inside `jobs`, not in a separate summary table.

No cascading delete or update actions are declared: SQLite's default `NO ACTION` applies, so deleting referenced parents is rejected while child rows remain. A future retention process must delete dependents in the correct order or introduce an explicit retention schema/policy.

**SQLite constraint limits:** these are ordinary SQLite rowid tables, not STRICT or WITHOUT ROWID tables. In this schema, the single-column `TEXT PRIMARY KEY` declarations do not explicitly say `NOT NULL`; ordinary SQLite rowid tables can permit NULL in such primary keys. The application rejects missing/empty IDs. Composite keys explicitly mark both participating columns NOT NULL. SQLite type affinity also does not replace application type validation. Direct SQL writers must not bypass the service's rules.

The pointer `(encounters.id, encounters.current_version)` is a logical reference to a version, not a declared composite FK. `Store.ingest()` maintains it in the same transaction as version/job insertion. Fixed patient/type, positive integer version, allowed status/type values, monotonic version advancement, result/status consistency and input immutability are enforced by the application rather than CHECK constraints or triggers.

#### Useful schema and workflow inspection queries

```sql
-- Confirm which SQLite file is open.
PRAGMA database_list;

-- Inspect columns, PK positions, FKs and indexes.
PRAGMA table_info(encounters);
PRAGMA table_info(versions);
PRAGMA table_info(events);
PRAGMA table_info(jobs);
PRAGMA foreign_key_list(jobs);
PRAGMA index_list(jobs);
PRAGMA index_info(jobs_due);
PRAGMA foreign_key_check;

-- Exact CREATE statements, including explicit indexes.
SELECT type, name, sql
FROM sqlite_master
WHERE type IN ('table', 'index') AND sql IS NOT NULL
ORDER BY type, name;

-- Current workflow per encounter without reading clinical content.
SELECT e.id AS encounter_id, e.current_version,
       j.status, j.attempts, j.due, j.lease, j.started, j.finished, j.error
FROM encounters AS e
JOIN jobs AS j
  ON j.encounter = e.id AND j.version = e.current_version
ORDER BY e.id
LIMIT 50;

-- Current jobs grouped by state; excludes historical jobs.
SELECT j.status, COUNT(*) AS job_count
FROM encounters AS e
JOIN jobs AS j
  ON j.encounter = e.id AND j.version = e.current_version
GROUP BY j.status;
```

Foreign-key enforcement is a connection setting: enable it explicitly if you open an external SQL client and intend to write. These inspection queries are read-only. For the full incident test, connect to `test_data/incident-e2e-full/encounters.db`; the default root `encounters.db` is a different database.

### Processing logic

#### Ingestion pseudocode

```text
validate event fields
logical_digest = canonical_hash(all logical fields except event_id)

BEGIN IMMEDIATE                       # transaction A
  load event receipt by global event_id
  reject 409 if receipt digest conflicts

  load encounter by encounter_id
  reject 409 if fixed patient/type conflict

  load version by (encounter_id, version)
  reject 409 if existing version digest conflicts

  if version exists:
      outcome = duplicate
  else if encounter exists and incoming version < current_version:
      outcome = stale
  else:
      outcome = accepted_current

  insert encounter if absent
  insert immutable version if absent
  insert event receipt if absent

  if accepted_current:
      advance encounter.current_version
      insert queued job referencing this exact version
COMMIT                                # receipt, state and job become durable together

return 202 for accepted_current; otherwise 200
return submitted version and current version observed in this transaction
```

SQLite's `BEGIN IMMEDIATE` serializes writers across threads and processes. A second concurrent request checks the committed state of the first. Database uniqueness constraints provide an additional safeguard. If any write fails, all writes roll back.

#### Background worker pseudocode

```text
loop:
  BEGIN IMMEDIATE                     # transaction B: atomic claim
    recover processing jobs whose lease expired:
        set queued and clear token
    mark queued jobs older than current version superseded
    mark queued jobs with attempts >= 6 failed
    select one due queued job
    if found:
        assign fresh token and 45-second lease
        set processing; increment attempts
        preserve first started timestamp
        load full transcription from that job's immutable version
  COMMIT

  if no job:
      sleep 200 ms; continue

  call generate_summary(transcription) # no database transaction held
  classify success or error

  BEGIN IMMEDIATE                     # transaction C: fenced completion
    locate job with matching version, token and processing status
    if not found:
        discard completion; ownership changed
    else if success:
        store summary, ready status and finish timestamp
    else if permanent error or attempts >= 6:
        store failed status, sanitized error and finish timestamp
    else:
        store queued status and sanitized error
        due = now + uniform(0.5,1) * min(300, 2^attempts) seconds
    clear token on an accepted completion
  COMMIT
```

Expired leases are recovered on a subsequent worker poll. Lease expiry alone does not invalidate a completion: if the original worker commits before recovery changes its token/status, that result wins. After replacement, the original token is fenced and cannot overwrite the replacement's result.

The current-summary query joins only the job whose version equals `encounters.current_version`. Saving an old result cannot advance that pointer. Already running obsolete calls may finish into history. Queued obsolete work is superseded before another call, saving cost without attempting unreliable cancellation of an external request already in progress.

#### Retry and paid-call policy

There are at most six automatic claims per job cycle. Claims consume attempts conservatively even when a worker crashes before dispatch. Retryable failures include 408, 429, 5xx, timeouts, and network exceptions. Other 4xx and provider failures raised as `ValueError` fail immediately. The adapter validates nonempty summary strings; malformed JSON and invalid summary values follow that permanent-error path. A missing `summary` key in the HTTP adapter is now a permanent invalid-response failure.

After attempts 1–5, the delay ranges are 1–2, 2–4, 4–8, 8–16 and 16–32 seconds. There is no delay after terminal attempt 6. The general formula caps delays at 300 seconds, though the six-attempt default never reaches that cap. `Retry-After` and a shared circuit breaker are not implemented.

The external call is non-idempotent and paid. A crash after provider acceptance but before the local completion commit leaves an ambiguous outcome. Recovery may invoke it again, incur another charge, and produce different wording. A local transaction cannot make this remote operation exactly once. Fencing guarantees one accepted completion per ownership cycle; it does not deduplicate external billing. A provider-supported idempotency key or result lookup would be needed to close that gap.

### SLA handling

The normal target is acceptance-to-ready under ten seconds. A 200 ms idle polling interval plus a typical 2–8-second provider call can meet this target when workers and provider capacity are available. Queue saturation, contention, provider slowness and retries can breach it.

The API computes `sla_breached` when elapsed time exceeds ten seconds from the stored version acceptance timestamp. For unfinished jobs it uses the current time; for finished jobs it uses completion time. A late ready result remains marked late. The exact ten-second boundary is currently not marked breached (`> 10`). An SLA breach never changes a job to failed.

The real provider adapter uses a 20-second socket timeout and a 45-second lease. Socket timeout is not a strict total wall-clock deadline for a streaming response. A production adapter should enforce a total deadline shorter than the lease, or renew leases with fencing, to reduce overlapping paid calls. Correct current-version selection remains independent of that latency limitation.

## 2. Failure scenarios

| Scenario | Expected stored state | What the client sees | Continuation / recovery |
|---|---|---|---|
| Two service instances receive the same event concurrently | One event receipt, one immutable version, one job; current advances once | One 202; other request returns 200 duplicate and current version | Writer serialization and PKs prevent a second job. Different event IDs for identical logical content add receipts only. |
| Version 12 arrives over stored 10; 11 arrives late or never | Current points to 12 immediately; 12 has a job. Late 11 gets input/receipt records only | 12 is accepted/current; late 11 is stale, current_version=12 | No dependency on consecutive versions. Unstarted 10 work is superseded; already running 10 may finish into history. |
| Crash after saving encounter but before worker handoff | Either encounter/version/receipt/job are all committed, or transaction rolls back | Lost response can be retried; committed retry returns duplicate | There is no separate handoff. Restarted worker polls the durable job. No committed accepted job is lost. |
| Worker crashes during processing | Processing row retains token, lease and consumed attempt until recovery | Processing, possibly with SLA breach; then queued/processing or failed | After lease expiry, a poll recovers it. New token fences old completion. Abandoned claims can exhaust the six-attempt budget. |
| Worker saves result, then crashes before acknowledgement | Ready status and result are already committed together | Ready result remains available | No broker acknowledgement exists. Ready jobs are not eligible to claim, so restart does not invoke AI again for that job. |
| Version 13 arrives while 12 is being summarized | Current=13 with its own job; 12 remains independently processing | Current endpoint reports 13 with no summary until 13 completes | 12 may finish into history. If its lease expires before recovery, obsolete work is superseded instead of retried. |
| Version 13 completes before 12 | Both may have ready results; current remains 13 | Current endpoint returns only 13's result; exact-version history can return 12 | Neither completion modifies the current pointer. Completion order cannot regress state. |
| AI unavailable for 20 minutes | Attempts and sanitized errors persist; queued retries eventually become failed | Queued between attempts, processing during calls, failed after exhaustion; SLA flag can become true independently | Fast failures normally exhaust after 31–62 seconds of backoff plus call/queue time. The job remains failed for the rest of the outage and after recovery until explicitly redriven. |
| Existing encounter arrives with different patient/type | Transaction rolls back; previous identity, pointer, jobs and receipts remain intact | 409 identity conflict | Repair the source event; do not silently reassign an encounter. Apply the same reject-and-rollback policy to event-ID or version-content conflicts. |

### Inspecting and safely redriving failed work

Read `GET /encounters/{id}/summary` or `GET /encounters/{id}/versions/{version}` to inspect status, attempt count, timestamps and sanitized errors. Operators can inspect due times and leases directly in the database without selecting transcription or summary columns.

After resolving the provider problem, an authorized local operator can run:

```python
from encounter_service.store import Store
store = Store('encounters.db')
store.history('enc-42', 12)
was_requeued = store.redrive('enc-42', 12)
```

Redrive is a conditional transaction: only a failed job that is still current can be reset. It clears the error/finish time, resets attempts, and sets queued/due-now. Concurrent redrives cannot both succeed because the first changes the status. Queued, processing, ready, obsolete and missing jobs are rejected with `False`. There is no unauthenticated HTTP redrive route.

An identical event replay does not redrive a failed job: it is still a duplicate. Redrive is a deliberate operator action because it opens a fresh paid-attempt budget. This implementation resets attempts in place and does not retain an append-only attempt/redrive audit trail; production auditing should add that record. The original acceptance and first-start timestamps remain, so redrive does not erase an SLA breach.

## 3. Verification and investigation

### Six meaningful verification cases

| Test case | Stimulus | Expected outcome |
|---|---|---|
| Concurrent duplicate ingestion and competing workers | Submit one event from 16 concurrent requests; concurrently claim the resulting job | Exactly one accepted/current, other deliveries duplicate; one version/job; exactly one claim succeeds before expiry. |
| Gaps, stale delivery and encounter independence | Ingest 10, 12, then 11; submit version 1 for a different encounter | First encounter remains at 12; no job for stale 11; worker receives exact v12 input. Second encounter independently accepts version 1. |
| Transaction crash and restart | Abort insertion of the job using a failing SQL trigger; separately restart the store after successful ingestion | Injected failure leaves no partial encounter/version/receipt. Successful commit remains claimable after restart. |
| Worker crash, fencing and committed-result replay | Claim, expire lease, reclaim, attempt old completion, save new completion, restart | Old token cannot overwrite replacement. Only the replacement result persists; committed ready work is never claimed again. Repeated abandoned claims eventually fail. |
| Newer version and out-of-order completion | Claim 12, ingest/claim 13, complete 13 then 12; separately leave an obsolete job queued | Current returns 13's result; 12 is historical. Queued obsolete work is superseded with no provider invocation. |
| Outage, conflicts and operational boundaries | Inject retryable/permanent provider errors, exhaust attempts and redrive; submit conflicting identities; cross SLA boundary | Retry timing follows due times; permanent errors fail immediately; six claims bound recovery cost; only failed-current redrive succeeds; conflicts leave state unchanged; SLA breach alone preserves workflow status. |

The project contains 41 automated tests, including HTTP request/response lifecycle coverage, long Unicode transcript preservation and error sanitization. Run:

```sh
python3 -m unittest discover -s tests -v
```

Tests use temporary SQLite databases, injected clocks, mocked providers and in-memory HTTP connections. They verify orchestration rather than model quality or live provider availability. A process kill/power-loss deployment test, load test and real-provider integration test would extend this coverage; existing crash tests simulate rollback, reopen and abandoned leases rather than actual host failure.

### Privacy-preserving operational evidence

**Already persisted or exposed:** acceptance, first-start and completion timestamps; attempt count; status; due/retry time; lease deadline; sanitized error code; and the derived current-job SLA flag. `/health` verifies database access, not AI readiness.

**Recommended metrics:** acceptance-to-ready latency histogram; queue wait and oldest current-job age; queued/processing/failed/superseded counts; expired-lease recovery count; provider latency, timeout, 429 and 5xx counts; attempts per job; redrive counts; and provider invocation/billing counts where available. Monitor obsolete work separately so it does not distort current-summary availability.

**Recommended alerts:** unfinished current job older than ten seconds; sustained queue-age growth; expired leases with no worker recovery; spikes in provider errors/rate limits; terminal failures; and database write failures. A ten-second alert reports an SLA breach rather than forcing terminal failure.

**Recommended structured logs:** opaque correlation/job identifiers, version, worker identity, attempt number, state transition, duration and sanitized error category. Avoid patient IDs where possible, keep identifiers out of metric labels, and do not log transcriptions, summaries, payload digests, raw provider bodies, credentials or exception text. The current HTTP handler suppresses default access logs; worker failures persist safe categories rather than raw exception messages. Structured worker transition logging is implemented: see the README section “Follow worker processing in the logs.” Metrics exporters and alerts remain integration recommendations, not installed components.

To investigate a stuck job: determine whether it is current, compare acceptance age to the SLA, inspect status/due/lease/attempts, check worker liveness and queue depth, then correlate provider error/latency and database health. If terminally failed, fix the underlying dependency before an explicit redrive. No patient content is needed for this diagnosis.


## 4. Optional code and reproduction instructions

The [repository](https://github.com/sandhani/encounter-summary-service) contains the HTTP API, SQLite store, worker/provider adapter, workflow logging, tests and generated synthetic datasets. It uses the Python standard library; no third-party packages are required. Use Python 3.11 or later as the documented target runtime.

### Start the service locally

```sh
git clone https://github.com/sandhani/encounter-summary-service.git
cd encounter-summary-service

# Terminal 1: HTTP API on loopback port 8000.
python3 -m encounter_service.api --db encounters.db --port 8000

# Terminal 2: worker using the SAME database file and free demo provider.
SUMMARY_MOCK=1 python3 -m encounter_service.worker --db encounters.db \
  --log-file /tmp/encounter-worker.jsonl
```

Use the POST and GET examples in section 1 to submit a snapshot and retrieve its status. `SUMMARY_MOCK=1` deliberately returns a synthetic demonstration summary. It does not validate clinical model quality.

For a real configured provider, replace mock mode with `SUMMARY_URL`:

```sh
SUMMARY_URL=https://your-provider.example/summary \
  python3 -m encounter_service.worker --db encounters.db
```

This placeholder endpoint must be replaced. The adapter POSTs `{"transcription":"<full input>"}` and expects `{"summary":"<nonempty result>"}`. Production provider authentication and a strict total deadline still need integration. Real provider invocations may incur charges.

### Run the focused verification suite

```sh
python3 -m unittest discover -s tests -v
```

The current suite has **41 passing tests**, including concurrency, rollback, restart/lease recovery, current-version protection, retries, conflicts, API contracts, Unicode input and privacy-safe workflow logs.

### Run the 10,000 isolated scenario campaign

```sh
python3 tools/scenario_cases.py run
```

The recorded campaign passed **10,000 of 10,000 cases**. Each case uses an isolated temporary database and injected failure/clock behavior. The report is `test_data/results.json`. This campaign makes no paid AI calls and is a correctness exercise rather than a throughput benchmark.

### Run the live incident/API/worker campaign

```sh
python3 tools/incident_e2e.py run --output test_data/incident-e2e-client-run
```

Choose a new output directory each time. The runner starts a real local HTTP API, eight worker processes and a synthetic HTTP summary provider. It submits updates over HTTP, checks the expected outcomes, restarts one worker and verifies every current result against its exact transcription version.

Recorded full-run evidence:

| Measure | Observed result |
|---|---:|
| Incident groups | 1,000 |
| Distinct synthetic patients | 3,000 |
| Encounters, all three types per patient | 9,000 |
| HTTP update requests | 90,000 |
| Accepted/current responses | 18,000 |
| Duplicate responses | 27,000 |
| Stale responses | 9,000 |
| Expected conflict responses | 36,000 |
| Current ready summaries | 6,750 |
| Expected permanent-failure results | 2,250 |
| Current results with an SLA breach | 1 |
| Synthetic provider invocations | 14,437 |
| Total harness duration | 108.887 seconds |

The campaign passed because all outcomes matched expectations. The 2,250 failed jobs deliberately exercise permanent provider errors; they are not unexpected test failures. The one SLA breach did not prevent successful recovery. Invocation counts and duration can vary with scheduling, retries and obsolete work. These measurements do not demonstrate production model latency or an SLA guarantee.

The retained summary is `test_data/incident-e2e-results.json`. A new run stores `report.json`, `encounters.db` and `process.log` in its output directory. Runtime databases and process logs are ignored by Git; the synthetic datasets and summary reports are included. The bulk-run database is separate from the default root `encounters.db`.

The live campaign does not force every possible interleaving. Short provider delays may allow version 12 to finish before version 13 arrives, or allow version 12 work to be superseded before starting. Deterministic tests separately force out-of-order completion, token fencing, exhaustion, redrive and a simulated twenty-minute outage.

### Trace a worker workflow

For one encounter/version/claim ID, follow these JSON events:

```text
job_claimed
summary_generation_started
summary_generation_finished   (or summary_generation_failed)
job_state_committed           (or completion_rejected if ownership changed)
```

`job_state_committed` is emitted after the result/status database transaction commits and reports the state, attempt count, retry time, current version and whether the result is current. Logs exclude transcript/summary text and patient IDs. Encounter IDs and worker PIDs provide operational correlation. Logging is best effort; a crash after commit but before log emission can leave a durable update without its log entry. The database remains authoritative.

## 5. Known limitations and production considerations

1. **Single-host storage:** SQLite serializes writes and is not a multi-host queue. Multi-host scale requires a shared transactional database such as PostgreSQL with appropriate claim locking. No high-availability deployment is supplied.
2. **Paid-call ambiguity:** external generation is not exactly once. A crash or lease overlap can repeat an invocation and its charge. Bounded claims limit automatic exposure, but an operator redrive opens another budget.
3. **Deadline behavior:** the current 20-second socket timeout is not a strict end-to-end deadline; there is no lease heartbeat. A slow streaming response can outlive the 45-second lease. Fencing protects saved results while duplicate external cost remains possible.
4. **Retry policy scope:** Retry-After, shared rate limiting and circuit breaking are not implemented. Invalid provider responses, including missing summary text, fail immediately. Worker process supervision/restart is an operator responsibility outside the local test runner.
5. **Schema hardening:** key/FK uniqueness is enforced in SQLite, while several invariants rely on application write paths. The tables are not STRICT, have no immutability triggers, and lack some CHECK/explicit NOT NULL constraints detailed in the data-model section. Arbitrary direct SQL writes can violate application rules.
6. **Security and privacy deployment:** the local HTTP server is a reference server bound to loopback. Authentication, tenant/patient authorization, TLS, encryption at rest, secrets, audited access and managed backups must be supplied before use with clinical data. No regulatory certification or clinical suitability claim is made.
7. **Retention and audit history:** snapshots/receipts/results are retained indefinitely. No cleanup workflow or append-only attempt/redrive audit trail is implemented. Redrive resets the attempt count in place.
8. **Operational integration:** timestamps, errors, workflow logs and an SLA field are implemented. Metrics exporters, alert routing, capacity management and an operational dashboard are not installed. Logs are not a transactional audit ledger.
9. **Verification scope:** synthetic correctness tests do not prove production throughput, clinical summary accuracy, provider billing semantics or host power-loss recovery. The live bulk run includes one worker kill/restart; it does not force every crash window or replace targeted recovery tests.
10. **Incident grouping:** the incident-to-patient-to-encounter hierarchy is a test-data convention. The production input contract and database model encounters, not a separate incident aggregate. Supporting incident APIs would be a distinct extension.

## Submission delivery

This document and the optional repository link are intended to be returned through the same channel in which the task was received. No external email or client-channel delivery is performed by the implementation. The design, schema, transaction boundaries, failure trade-offs and verification limits above provide the basis for explaining each part of the submission.


## LLM adapter implementation update — 12 November 2026

A direct OpenAI Responses adapter is available in `encounter_service/summary_adapters.py`. Its callable contract remains `transcription -> summary`; database ownership, version fencing and retry transactions are unchanged. Select it using `USE_LLM=1` or `--use-llm` and supply `OPENAI_API_KEY` securely. The initial default is `gpt-6.1-terra`, configurable using `LLM_MODEL`; reasoning defaults to `low`. `--no-use-llm` overrides the environment flag, retaining the previous HTTP/mock provider selection. The API key is required before work is claimed.

The adapter sends the complete transcript with separate factual-summary instructions, `store=false`, no tools, and a 2,048-output-token cap. It validates completed assistant output and rejects refusal, incomplete/truncated or empty output. One adapter call makes one remote request; the durable job policy remains responsible for retries. No live paid model invocation has been performed as part of this change; tests mock the HTTP boundary. The bulk runner disables LLM mode by default; explicit `run --use-llm` enables paid generation and `--limit` bounds the selected encounters.

Model selection is a candidate recommendation, not clinical validation. [GPT-6.1 Terra](https://developers.openai.com/api/docs/models/gpt-6.1-terra) is the initial balance of writing and efficiency; [GPT-6 Luna](https://developers.openai.com/api/docs/models/gpt-6-luna) is the efficient high-volume candidate; [GPT-6 Astra](https://developers.openai.com/api/docs/models/gpt-6-astra) is the demanding-work candidate. Evaluate factual omissions, invented facts, negation/dose preservation, latency and cost on representative transcripts. Model access, provider terms, clinical review, strict deadlines and production security remain deployment considerations. Model/prompt metadata is logged at startup but not saved per job result. See the README's LLM section for complete configuration and run instructions.
