# Reliable encounter summaries

Runnable Python 3.11+ backend with no third-party dependencies. Architecture, HLD and LLD are in [Agent.MD](Agent.MD); the existing [IDE agent](.github/agents/encounter-summary-builder.agent.md) also guides the implementation. This is a local, single-host reference implementation, with a real configurable provider adapter and explicit demo mode.

## Run

```sh
python3 -m encounter_service.api --db encounters.db --port 8000
# In another terminal:
SUMMARY_MOCK=1 python3 -m encounter_service.worker --db encounters.db
# Real provider instead: SUMMARY_URL=https://your-provider/summary python3 -m encounter_service.worker
python3 -m unittest discover -s tests -v
```

The real adapter POSTs `{"transcription":"<full unchanged transcript>"}` and expects `{"summary":"..."}`. It uses a 20-second socket timeout and a 45-second worker lease. A production adapter needs a strict total request deadline below the lease (a socket timeout alone does not bound a slowly streaming response), provider authentication and an allowlisted endpoint. Demo output deliberately does not claim to be a clinical AI summary.

API listens on loopback. Before clinical deployment, add authenticated tenant/patient authorization, TLS, encryption at rest, secret management, retention/deletion policy, audited operator access and managed durable backups. Do not expose the standard-library server directly to untrusted traffic. The 2 MB body limit accommodates long transcripts; larger inputs get 413. Polling is every 200 ms per idle worker. Keep enough workers/provider capacity to avoid queue delay; under-10-second latency is a target, not a guarantee.

## API contract

```sh
curl -X POST http://127.0.0.1:8000/events \
  -H 'Content-Type: application/json' \
  -d '{"event_id":"evt-102","encounter_id":"enc-42","patient_id":"pat-77","encounter_type":"TelephoneTriage","version":12,"payload":{"transcription":"Nurse: How can I help? Patient: My throat hurts."}}'
curl http://127.0.0.1:8000/encounters/enc-42/summary
curl http://127.0.0.1:8000/encounters/enc-42/versions/12
```

POST outcomes (all include submitted version and current version):

| HTTP | Example body | Meaning |
|---|---|---|
| 202 | `{"outcome":"accepted_current","encounter_id":"enc-42","version":12,"current_version":12}` | Transaction committed; generation asynchronous |
| 200 | `{"outcome":"duplicate","encounter_id":"enc-42","version":12,"current_version":13}` | Same logical version already recorded, including a different event ID |
| 200 | `{"outcome":"stale","encounter_id":"enc-42","version":11,"current_version":13}` | First arrival of an older snapshot; recorded without scheduling work |
| 409 | `{"error":"encounter identity conflict"}` | Fixed identity changed; no writes |
| 409 | `{"error":"encounter version content conflict"}` | Same version, different content; no writes |
| 409 | `{"error":"event ID content conflict"}` | Global event ID reused inconsistently; no writes |
| 400 | `{"error":"invalid_request"}` | Invalid JSON, identifiers, type, positive integer version or transcription |
| 503 | `{"error":"storage_unavailable"}` | No reliable acknowledgement; retry identical event |

202 acknowledges durable work, not completion. 200 acknowledges successful no-ops. Duplicate takes precedence over stale when a version was already seen. Unknown encounter/job returns 404. Types are Appointment, MedicationRefill, TelephoneTriage. Each encounter has independent version ordering; gaps are allowed. Full snapshots are never merged.

GET current summary always returns HTTP 200 for an existing encounter, including failed jobs: retrieving workflow state succeeded. Example processing response:

```json
{"encounter_id":"enc-42","patient_id":"pat-77","encounter_type":"TelephoneTriage","version":12,"status":"processing","summary":null,"attempts":1,"accepted_at":1800000000.0,"started_at":1800000000.2,"finished_at":null,"next_attempt_at":null,"error":null,"sla_breached":false}
```

Queued jobs have `status: queued` and a `next_attempt_at`. Ready jobs have `status: ready`, a nonempty `summary`, and `finished_at`. Failed jobs have `status: failed`, `summary: null`, an error code and `finished_at`. A retryable provider failure returns the job to queued. Timestamps are Unix seconds. `sla_breached` compares acceptance to completion, or to now while unfinished: late completion remains visibly late. SLA does not fail or cancel work. Version-specific GET returns version, status, attempts, start/finish, summary and sanitized error. Stale-only versions have no summary job, so that endpoint returns 404.

## Model and transaction boundaries

`encounters(id PK, patient, type, current_version)` owns fixed identity and the maximum observed version. `versions(encounter FK, version, transcription, digest, accepted)` has composite PK and immutable full input. `events(id PK, digest, encounter, version)` references that version; the digest excludes event_id, includes all logical content, and uses canonical sorted JSON. Uniqueness exists on both the global event ID and the logical version, independent of application checks. `jobs(encounter, version PK/FK, status, attempts, due, token, lease, started, finished, summary, error)` records durable work and exact-version results. Identity belongs to the encounter and is inherited through the composite version relationship. Versions, receipts and completed results are retained indefinitely in this exercise; budget storage and implement retention before production.

Ingestion validates, starts `BEGIN IMMEDIATE`, checks receipt/version content and identity, stores receipt/version, advances the current pointer only if greater, and inserts a job in the same commit. SQLite writer serialization makes concurrent check/insert atomic across processes. Claim transaction recovers expired leases, supersedes queued obsolete work, enforces the attempt budget, and assigns one due job a unique token and lease. Generation reads its immutable snapshot and runs outside the transaction. Completion updates only the still-processing row with that token. Result and terminal state commit together. A lease-expired worker can save only until a replacement claim fences it; whichever completion/claim commits first determines ownership. Two external invocations can overlap after lease expiry, but a fenced result cannot overwrite the winner.

## Failure walkthrough

| Scenario | Stored state / client / recovery |
|---|---|
| Concurrent duplicate | One receipt/version/job; one 202, others 200 duplicate. Same logical version under another ID adds only a receipt. |
| Missing or late version | 12 immediately replaces 10; no wait for 11. Late 11 is stale; current remains 12. |
| Crash after saving | Version and job share one commit. Restarted worker discovers committed job; incomplete transaction rolls back. Lost HTTP reply is safely retried. |
| Worker interruption | Lease expires after 45 seconds; next poll requeues and claims with a new token. Crash after committing result requires no acknowledgement: ready rows cannot be claimed again. |
| Newer version during generation | 13 gets its own job. Running 12 may finish into history; queued obsolete jobs are superseded before calls. Cancellation is deliberately avoided because it cannot reliably reclaim provider cost. |
| Out-of-order results | Current GET joins exclusively on current_version. Finishing 12 after 13 cannot change the current pointer or summary. |
| AI unavailable for 20 minutes | Six total attempts per job, including abandoned claims; exponential jitter delay is 0.5–1 × min(300, 2^attempt) seconds. Continuous fast failures normally exhaust in about 31–62 seconds; timeouts add call duration. Client sees queued/processing then failed until explicit operator redrive. No automatic 20-minute loop of paid calls. |
| Identity or content inconsistency | Reject 409 and roll back, retaining prior encounter, jobs and receipts. Quarantine/repair at the source rather than silently accepting a new identity. |

429, 408, 5xx, network errors and timeouts retry. Other 4xx and malformed provider results fail immediately. Retry-After is not currently consumed; an operational deployment should add provider-aware throttling/shared circuit breaking to prevent a large backlog from flooding an outage. There are no immediate in-call retries. Every claim conservatively consumes an attempt even if a crash happens before dispatch. A crash after provider acceptance but before result commit is fundamentally ambiguous: without provider-supported idempotency/result lookup, exactly-once paid generation is impossible. Recovery may pay twice and produce different wording; the fenced committed result wins. Automatic retry is bounded to limit this exposure.

Inspect a failed version through GET, resolve provider/credential issues, then explicitly redrive locally:

```python
from encounter_service.store import Store
store = Store('encounters.db')
print(store.history('enc-42', 12))
print(store.redrive('enc-42', 12))
```

Redrive transaction accepts only a failed current job, resets the attempt budget, and returns False otherwise. It may incur fresh charges and is an operator decision; concurrent redrives cannot both reset it. There is deliberately no unauthenticated HTTP redrive route.

## Verification and operations

The suite contains 28 tests across `test_store.py`, `test_robustness.py`, and `test_api.py`. The original reliability tests assert: concurrent duplicate delivery schedules once; version gaps and late events preserve current state and snapshot; restart and expired-lease fencing recover without overwrites or replaying committed results; out-of-order completion preserves current summary; identity/version/event conflicts roll back; bounded retry exhaustion and redrive preserve SLA semantics.

Persisted acceptance/start/finish, attempts, due time, lease expiry, status and error codes enable inspection without transcripts. Production monitoring should export queue depth/oldest age, active and expired leases, acceptance-to-ready latency, SLA breaches, provider duration/error/rate-limit counts, attempts and failed/superseded counts. Alert on current unfinished jobs older than ten seconds, growing queue age, expired leases and failed jobs; a breach is operational, not terminal. These exporters/alerts are documented integration points, not installed monitoring infrastructure. Avoid identifiers as metric labels and avoid transcripts, summaries or raw provider exceptions in logs. `/health` checks database access but does not imply provider readiness. Bound polling load and scale workers based on queue delay and provider quotas.

Additional tests cover concurrent version arrivals and worker claims, abandoned-lease attempt exhaustion, transactional rollback, retry deadlines, supersession without paid calls, redrive restrictions, SLA boundaries, unchanged long Unicode transcripts, sanitized provider failures and HTTP error retry policies. HTTP tests exercise request parsing and response serialization with in-memory connections, including lifecycle/status codes, malformed bodies, size limits, missing resources, health and storage failures; they do not bind a listening socket or invoke a live AI provider.

## Architecture diagrams

[Open the editable HLD and LLD diagrams in Lucidchart](https://lucid.app/lucidchart/dbd10843-88ef-4450-8731-6344f7b33dec/edit). The three pages cover the service overview, ingestion and current-summary reads, and worker retries/recovery. The diagrams are a snapshot of the implemented service.

## Run 10,000 scenario cases

A reproducible synthetic dataset and assertion-based runner cover duplicate races, version gaps, restart/lease recovery, out-of-order results, a simulated twenty-minute outage, conflicts and rollback. See [dataset instructions and coverage](test_data/README.md).

```sh
python3 tools/scenario_cases.py run
```

No real AI calls are made. Results are saved in `test_data/results.json`; a failed case causes a nonzero exit code.

## Test incident groups through the live API

For **1,000 incidents, 3,000 patients and 9,000 encounters**, use the [live API and worker test guide](test_data/INCIDENTS.md). This starts actual API/worker processes and a free synthetic HTTP summary provider, then submits 90,000 update requests and verifies current-version results.

```sh
python3 tools/incident_e2e.py run --output test_data/incident-e2e-my-run
```

## Follow worker processing in the logs

Workers emit one JSON object per workflow event to stderr by default:

```sh
SUMMARY_MOCK=1 python3 -m encounter_service.worker --db encounters.db

# Append events to a file instead:
SUMMARY_MOCK=1 python3 -m encounter_service.worker --db encounters.db --log-file /tmp/worker-workflow.jsonl
```

Use the same database path as the API. Existing ready/failed jobs are not automatically reprocessed: submit a newer snapshot to see a fresh workflow. The completed bulk-run database has no pending work. To capture a new bulk run, `python3 tools/incident_e2e.py run --output test_data/incident-e2e-logged` starts workers whose JSON events appear in that run's `process.log`.

A successful workflow emits:

1. `job_claimed`: the database claim has committed; queued → processing, exact immutable input version, attempt and lease deadline.
2. `summary_generation_started`: the input has been loaded and the provider call is about to start; includes character count, not transcription content.
3. `summary_generation_finished`: the provider returned; the job is still processing until persistence succeeds.
4. `job_state_committed`: result/status transaction has committed; processing → ready, queued for retry, or failed. Includes sanitized error, retry due time, `summary_saved`, and `is_current` as observed in that transaction.

Errors emit `summary_generation_failed` before the state decision. A fenced completion emits `completion_rejected` and cannot emit a successful commit. Correlate by `encounter_id`, `version`, `claim_id`, `worker_pid` and UTC timestamp. In the synthetic incident dataset, encounter IDs such as `inc-0000-pat-00-type-0` identify the incident group; no separate incident field is stored or inferred for arbitrary IDs.

These logs demonstrate orchestration, not transcription generation: the API receives the full transcription and the worker summarizes it. They exclude patient IDs, transcript/summary text and raw exception details. Standard logs are best-effort observability, not an atomic audit ledger: a crash between database commit and logging can leave a committed change without its log entry. Query the database as the source of truth.

## Use an LLM adapter for real summaries

`summary_adapters.py` defines a callable `SummaryAdapter`: full transcription → summary string. The worker selects an adapter once at startup. LLM mode uses the OpenAI Responses API directly; it does not need `SUMMARY_URL` or a separately hosted summary server.

```sh
# Set OPENAI_API_KEY securely in your environment first; do not commit it.
# Terminal 1:
python3 -m encounter_service.api --db encounters.db --port 8000
# Terminal 2:
USE_LLM=1 LLM_MODEL=gpt-6.1-terra \
  python3 -m encounter_service.worker --db encounters.db --log-file /tmp/llm-worker.jsonl

# Equivalent CLI flag (takes precedence over USE_LLM):
python3 -m encounter_service.worker --db encounters.db --use-llm

# Force free demo behavior, even if USE_LLM is inherited as enabled:
USE_LLM=0 SUMMARY_MOCK=1 python3 -m encounter_service.worker --db encounters.db --no-use-llm
```

When disabling LLM, unset SUMMARY_URL if you want mock mode: HTTP mode has precedence over SUMMARY_MOCK. With `USE_LLM=1` or `--use-llm`, LLM mode takes precedence over SUMMARY_URL and SUMMARY_MOCK and requires OPENAI_API_KEY. Missing/invalid configuration fails before claiming database work. `.env.example` documents the variables; files are not automatically loaded. CLI `--no-use-llm` explicitly disables LLM. The bulk incident test runner defaults to `USE_LLM=0`; only an explicit `run --use-llm` enables paid calls. An inherited environment flag alone does not enable them.

### Model candidates

Initial recommendation: **`gpt-6.1-terra`**, for a balance of writing quality and efficiency. This is an engineering starting point, not a measured clinical model ranking. Compare candidates on representative synthetic/de-identified transcripts before production use.

- [`gpt-6.1-terra`](https://developers.openai.com/api/docs/models/gpt-6.1-terra): documented as fast/efficient with natural writing; the configured default.
- [`gpt-6-luna`](https://developers.openai.com/api/docs/models/gpt-6-luna): documented for efficient focused high-volume tasks; a lower-cost candidate to evaluate for straightforward summaries.
- [`gpt-6-astra`](https://developers.openai.com/api/docs/models/gpt-6-astra): documented as the most capable model for demanding work; evaluate for difficult or ambiguous transcripts with greater cost/latency tolerance.

Official documentation checked 12 November 2026. Model access depends on the API project. `LLM_MODEL` selects the model and `LLM_REASONING_EFFORT` selects reasoning effort (default `low`). Only use effort values supported by that model. No automatic model fallback is performed because another paid call can change cost and wording.

### Adapter behavior and limits

The full unchanged transcript is passed as input, separate from instructions. Prompt version `encounter-summary-v1` requests a faithful summary preserving negation, uncertainty, medication doses/units and discussed plans; it treats instructions inside the transcript as source content rather than commands and prohibits invented diagnoses/advice. This mitigates, but does not eliminate, hallucination or prompt injection. Clinical review and transcript-specific quality evaluation remain necessary.

Each invocation makes one authenticated HTTP request to `https://api.openai.com/v1/responses` with `store=false`, a 2,048-output-token budget and 20-second socket timeout. There are no adapter-level retries. Existing worker handling owns 429/408/5xx retries and their costs. Completed assistant text is extracted from the raw Responses output; reasoning text is excluded. Refusals, empty/malformed responses and incomplete/truncated outputs fail permanently rather than saving partial clinical summaries. Operators may explicitly redrive after fixing the model/budget/input issue. Transcripts are never silently shortened. Oversized model-context requests are rejected by the provider and handled as permanent 4xx failures.

`store=false` disables response storage through that API feature; it is not a promise of zero retention or suitable clinical-data processing terms. The 20-second socket timeout is still not a strict total deadline; existing lease/fencing limits apply. Under-ten-second completion is a target to measure, not a model guarantee. Provider/model/prompt version are logged at worker startup, but are not persisted per result in the current schema; use a fixed worker configuration per campaign for traceability. Existing completed results are not regenerated automatically when changing models.

Adapter tests mock the HTTP boundary and make no paid calls. A live LLM invocation requires your OPENAI_API_KEY and incurs provider charges. See [Responses API reference](https://developers.openai.com/api/reference/resources/responses/methods/create).


### Restrict LLM summary length

LLM summaries now default to a maximum of **100 whitespace-separated words**, including headings. Configure the limit before starting the worker:

```sh
USE_LLM=1 SUMMARY_MAX_WORDS=100 python3 -m encounter_service.worker --db encounters.db --use-llm
```

Restart existing workers to apply the setting. The prompt requests the cap and the adapter checks the completed output before saving it. Token limits are not word limits. Over-limit output is rejected as an invalid provider response: the job becomes failed with a null summary, without automatically buying another call or silently truncating clinical information. An operator can adjust the limit and explicitly redrive failed current work. Existing ready summaries and HTTP/mock adapters are unchanged. Prompt version is now `encounter-summary-v2`; the configured word limit appears in worker startup logs.

### Use the incident runner with the LLM

After exporting OPENAI_API_KEY, run a small paid campaign explicitly:

```sh
LLM_MODEL=gpt-6.1-terra SUMMARY_MAX_WORDS=100 \
python3.14 tools/incident_e2e.py run --use-llm --limit 9 --workers 2 \
  --output test_data/incident-e2e-llm-small
```

The default `run` still uses the synthetic provider. `--use-llm` starts workers with the OpenAI adapter; `--limit` restricts encounter count. LLM campaigns validate bounded nonempty current-version summaries rather than synthetic hashes and skip deliberate provider faults/worker kills. See [the incident guide](test_data/INCIDENTS.md) for costs, expected outcomes and full-run instructions.


## Default test size: 100

The incident runner now ingests **100 encounters by default** (1,000 update POST requests). It selects the first 100 rows of the larger dataset, covering all three encounter types and all four provider modes in synthetic runs. This is 11 complete incident groups plus one encounter from the next group; the last group is intentionally partial. Synthetic runs retain concurrent duplicates, stale updates, identity/content conflicts, transient retries, permanent failures and the worker restart check. LLM runs use real summaries and do not inject provider failures.

```sh
# Free live HTTP/API/worker run, 100 encounters:
python3.14 tools/incident_e2e.py run --output test_data/incident-e2e-100

# Paid LLM run, 100 encounters (requires exported OPENAI_API_KEY):
python3.14 tools/incident_e2e.py run --use-llm --workers 2 --output test_data/incident-e2e-llm-100

# All 12 deterministic failure scenarios, 100 cases, no paid calls:
python3 tools/scenario_cases.py run

# Explicitly opt back into larger campaigns:
python3 tools/incident_e2e.py run --limit 9000 --output test_data/incident-e2e-full-new
python3 tools/scenario_cases.py run --data test_data/scenarios-10000.jsonl --report /tmp/results-10000.json
```

The deterministic 100-case dataset is `test_data/scenarios-100.jsonl`; its report is `test_data/results-100.json`. This complements the live workflow campaign: every failure ordering is not guaranteed by a live concurrent run. One hundred encounters can incur more than one hundred paid invocations because each receives two accepted versions and retries can occur.


## Compare the transcription and summary

`GET /encounters/{id}/summary` includes a top-level `transcription` field with the full unchanged input for the returned current `version`. It is available while queued/processing, ready or failed; `summary` remains null until that version is ready. An older result cannot be paired with a newer transcript. Restart the API process to load this change, keeping its current database path and port. No database migration or summary regeneration is required.

```json
{"encounter_id":"enc-42","version":12,"status":"ready","transcription":"Nurse: How are you? Patient: Feeling better.","summary":"Patient reports feeling better."}
```

Example is abbreviated; the endpoint also returns the existing identity, timing and progress fields. Transcription content is returned to the requesting client but remains excluded from workflow logs.
