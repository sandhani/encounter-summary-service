---
name: Encounter Summary Builder
description: "Build and verify a reliable encounter-update API with durable background AI summary generation, idempotency, ordering, retries, and SLA visibility."
argument-hint: "Describe a change to the encounter summary service or ask to build it from the supplied requirements."
---

# Encounter Summary Builder

Implement a reliable encounter ingestion and summary orchestration service. Read the user requirements and existing project files before changing code. Deliver working code, tests, and concise operator/API documentation; do not stop after proposing a design.

## Architecture

- Keep the API, durable store, background worker, and summary-provider adapter as explicit boundaries.
- Treat the database as the source of truth. A successful current update and its summary job must commit atomically; never rely on an in-memory queue for accepted work.
- Enforce global `event_id` uniqueness and `(encounter_id, version)` uniqueness in storage. Retries with identical content are duplicates; reuse with different content and violations of fixed encounter identity are conflicts.
- Store `patient_id` and `encounter_type` on the encounter keyed by `encounter_id`. They are immutable identity attributes.
- Store the latest encounter state and immutable summary input snapshots per version. A job must use only its own snapshot, never re-read mutable encounter input to generate a summary.
- Associate every result with its exact encounter version. The current-summary API must select only the job matching the encounter's current version.
- Use atomic job claiming, expiring leases, bounded retries with exponential backoff and jitter, and a terminal failed state. External generation is paid and not guaranteed idempotent: do not claim exactly-once calls; explain the crash window and minimize duplicate invocations.
- Make SLA status observable using acceptance/start/completion timestamps and a derived or reported breach indicator. A breach alone does not fail a job.
- Do not log or return transcription content except where a client explicitly requests a result or the provider call requires it. Never include patient content in operational logs.

## HLD

Describe and implement these flows:

1. **Ingest:** validate request; in one transaction enforce event and version uniqueness, check immutable identity, record the event, advance the current encounter only for a greater version, and enqueue its immutable summary snapshot. Acknowledge without waiting for generation.
2. **Generate:** background workers atomically claim due jobs, call the provider outside the database transaction, and commit result/status together. Retry transient errors; fail permanently or after a bounded attempt policy; recover abandoned leases after worker interruption.
3. **Read:** expose current encounter summary progress/result and exact-version history without allowing an older result to masquerade as current.
4. **Operate:** expose health and safe failed-job inspection/redrive; report age/SLA signals and document alerts and privacy-safe correlation fields.

Document consistency, transaction boundaries, concurrent writers/workers, failure recovery, the provider's at-least-once cost trade-off, and why version gaps are valid.

## LLD

- Define concrete request/response shapes and HTTP status codes for current acceptance, duplicate, stale, conflict, processing, ready, and failed outcomes.
- Define database tables, primary/unique/foreign keys, status transitions, lease fields, retry schedule, timestamps, and immutable snapshot/result association.
- Include ingestion and worker pseudocode or precise equivalent implementation.
- Specify transient/permanent provider errors, timeout behavior, maximum attempts, backoff, lease recovery, safe redrive, and behavior when a newer version arrives during generation.
- Specify how a caller identifies the current version and how a version-specific result is retrieved.
- Use clear types, input validation, explicit errors, and parameterized SQL. Keep clinical content out of logs.

## Verification and delivery

- Add focused tests covering at least six meaningful cases, including concurrent duplicates, out-of-order versions/version gaps, crash recovery or retry, obsolete work completing after a newer version, identity conflicts, and exhausted retries/redrive.
- Include API examples and a failure-scenario walkthrough in the project documentation.
- Provide simple local run/test instructions and configuration for the provider and database.
- Run the narrowest relevant tests and any available static checks; fix regressions introduced by the work.
- Preserve unrelated user changes. Do not commit unless explicitly requested.
