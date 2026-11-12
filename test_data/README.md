# 10,000 synthetic scenario cases

`scenarios-10000.jsonl` contains exactly 10,000 independently runnable scenario records, generated with seed 42. Each line contains a unique case ID, scenario name, synthetic base event, and expected final state. The runner expands the base event into deliveries, conflicts, retries and worker actions. These are **10,000 scenario executions**, rather than 10,000 HTTP requests; a scenario can perform many operations.

Run from the repository root:

```sh
# Run all cases (no server, credentials, or provider needed):
python3 tools/scenario_cases.py run

# Run a quick sample covering every scenario twice:
python3 tools/scenario_cases.py run --limit 24 --report /tmp/encounter-smoke.json

# Reproduce the committed data:
python3 tools/scenario_cases.py generate --count 10000 --seed 42

# Generate a different/larger dataset without replacing the original:
python3 tools/scenario_cases.py generate --count 20000 --seed 99 --data /tmp/cases.jsonl
python3 tools/scenario_cases.py run --data /tmp/cases.jsonl --report /tmp/results.json

# Also run HTTP contract and focused unit tests:
python3 -m unittest discover -s tests -v
```

`results.json` records counts, elapsed execution time, scenario/type distributions and failures identified by case ID. The command exits nonzero if any case fails or no cases execute. Each scenario gets an isolated temporary on-disk SQLite database; data is removed after each execution. No external summary service is called. A deterministic mock summary is used, while the outage scenario drives the real worker `tick()` with a failing provider. Simulated clocks avoid sleeping for leases or a twenty-minute outage. SQLite uses the actual production transaction implementation.

## Coverage

| Scenario | Cases | Checks |
|---|---:|---|
| Concurrent duplicate | 834 | Four deliveries released at a barrier; one acceptance; alias ID deduplication; four competing claims produce one owner |
| Version gap | 834 | Accept N-2 then N; late N-1 is stale; exact snapshot input; obsolete queued job superseded |
| Restart after commit | 834 | Reopen store after ingestion and after completion; durable work recovered and ready work never replayed |
| Worker interruption | 834 | Recover at 45-second lease boundary; fresh token; old completion fenced; completion replay rejected |
| Newer during processing | 833 | N+1 gets independent input/job; old completion cannot satisfy the new current job |
| Out-of-order results | 833 | Finish N+1 before N; old result never replaces current summary |
| Outage/exhaustion/redrive | 833 | Six transient failures; bounded jitter ranges; no early retry; failed state persists through twenty simulated minutes; explicit redrive only once; SLA breach remains visible |
| Identity conflict | 833 | Alternate patient and encounter-type conflicts; rollback leaves exactly one original record per table |
| Payload conflict | 833 | Same logical version under another event ID with different transcript rejected atomically |
| Event ID conflict | 833 | Same global event ID reused for another encounter rejected atomically |
| Transaction rollback | 833 | SQL trigger aborts job insertion; no partial state; retry succeeds after removing fault |
| Validation | 833 | Reject boolean/zero versions, empty patient, invalid type and whitespace transcription without writes |

All three encounter types occur in the dataset. Every hundredth case contains 300 dialogue lines; the rest have 2–8 lines. Inputs vary IDs, versions, type, transcript length and Unicode content. Everything is synthetic and contains no real patient data.

## Interpretation and limits

The scenario templates and generated input are reproducible; thread scheduling, lease tokens, backoff jitter and execution timings vary. Assertions check invariant outcomes and delay ranges rather than a particular schedule. A generated JSONL row is a scenario description, **not** a raw POST body: its `event` member is the base API input.

This is a correctness campaign, not a throughput benchmark or proof of all possible interleavings. Failures/restarts are injected or simulated, not real process/host kills. The suite does not test live HTTP networking, real AI latency, rate-limit headers, provider billing, production authentication, retention, or a shared database containing 10,000 simultaneous active encounters. HTTP behavior and additional validation/error classes are covered by the existing 28-test suite. The duration in the report measures the harness and temporary-database overhead and must not be presented as service throughput or SLA performance.
