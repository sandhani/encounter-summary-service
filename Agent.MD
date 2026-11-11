# Build instructions

Build a runnable encounter ingestion and asynchronous summary backend in Python 3.11+, using the standard library and SQLite. Run `python -m unittest discover -s tests -v` before delivering changes. Never log patient content or credentials.

## Architecture / HLD

HTTP API → transactional SQLite store ← independent polling workers → summary provider.
The database is the durable queue: ingestion commits version, event receipt, current pointer and job together. No broker handoff can be lost. WAL and short `BEGIN IMMEDIATE` transactions serialize writers across processes. Deploy on a single host with durable local storage; scale beyond this using PostgreSQL row locks / SKIP LOCKED, not a shared network SQLite file.

## LLD and invariants

* Encounters fix patient and type and point to the highest observed version.
* Immutable versions keyed by (encounter_id, version) retain exact input snapshots. Event IDs are unique receipts with canonical content digests; a second event ID for the same logical version is a duplicate if content matches.
* All valid arrivals have receipts, including stale events. Only advancing versions get jobs. Reject conflicting identities, payloads or reused IDs with HTTP 409 without mutations.
* Jobs keyed by encounter/version hold status, attempts, lease token/deadline, retry time, timestamps and summary. Provider calls run outside transactions. Commit results only with the current lease token. A current summary is selected exclusively through the encounter's current version.
* Skip obsolete jobs before invocation. Already running obsolete calls may complete and be retained as history; they cannot become current. Cancellation cannot reliably refund a paid request.
* A lease permits recovery, not exactly-once paid calls. A crash after a provider accepts a call but before durable completion can incur another call. Fence late workers; bound attempts and use exponential jittered backoff. Provider timeout must be shorter than lease.
* SLA breach is derived at ten seconds since acceptance and does not change workflow status. Track current and historical jobs separately.

## Implementation workflow

Preserve transactional boundaries and immutable inputs. Use injected clocks/providers in tests. Verify concurrent duplicate delivery, skipped versions, transactional restart recovery, fenced lease recovery, out-of-order results, conflicts and exhaustion/redrive. Keep examples synthetic. README must explain API outcomes, deployment, failure scenarios and operational limits.
