> **Current default: 100 encounters.** Commands without `--limit` now ingest 100 rows, not the full dataset. Use `--limit 9000` for the original full campaign. Use `python3 tools/scenario_cases.py run` for 100 deterministic cases covering all 12 scenario groups. Historical full-run results below remain unchanged.

# Live API and worker incident testing

This dataset models **1,000 incident groups × 3 distinct patients per incident × 3 encounter types per patient = 9,000 encounters**. Each patient has an Appointment, MedicationRefill and TelephoneTriage encounter. `incident_id` is grouping metadata in the dataset; the existing API stores each encounter with one fixed patient and type. No service schema change is required.

`incidents-1000.jsonl` contains a base API event per encounter plus incident/scenario metadata. All transcripts and identifiers are synthetic. The loader expands these into **90,000 HTTP POST requests**:

- Version 12 accepted, followed by three concurrent duplicates (including a different event ID).
- Late version 11 acknowledged as stale, then version 13 accepted.
- Conflicting patient, type, same-version content, and reused event ID rejected with 409.

The loader sends requests to the actual HTTP API. Separately running worker processes claim database jobs and call an actual local HTTP provider stub. The stub produces a transcript hash in its synthetic summary so polling can verify that the exact version-13 input was summarized. Four modes are distributed equally: normal, transient 503 once per snapshot, permanent 400, and a short variable delay. Expected final counts are **6,750 ready and 2,250 failed**; those failed cases are intentional and must contain `provider_http_400`.

## One-command local run

From the repository root:

```sh
# Dataset is already generated. Run API, provider and eight real worker processes:
python3 tools/incident_e2e.py run --output test_data/incident-e2e-my-run
```

Use a new output directory for every run. The command refuses an existing directory to avoid mixing prior data. It starts servers on ephemeral loopback ports, waits for API health, ingests data, restarts one worker, and polls every encounter until it reaches the expected terminal state. All started processes are stopped afterward. The output retains:

- `report.json`: incident/patient/encounter counts, HTTP outcomes, terminal results, SLA breach count, provider call count, duration and pass/fail.
- `encounters.db`: populated SQLite database for inspection and reuse.
- `process.log`: API/worker process errors (default request logging is disabled).

The output folders are ignored by Git. Every final result must have current version 13, its original patient/type, and either the correct input-hash summary or the expected permanent error. Wrong statuses, version mismatches, incorrect summaries, timeouts and unexpected HTTP responses fail the command.

```sh
# Re-create the dataset:
python3 tools/incident_e2e.py generate

# Small live smoke test:
python3 tools/incident_e2e.py generate --incidents 2 --data /tmp/incident-smoke.jsonl
python3 tools/incident_e2e.py run --data /tmp/incident-smoke.jsonl --output /tmp/incident-smoke-new

# Configure load/concurrency:
python3 tools/incident_e2e.py run --clients 8 --workers 8 --timeout 3600 --output /tmp/incidents-new
```

`--timeout` bounds the final polling phase; ingestion requests have individual 60-second timeouts. Eight client threads can each launch three duplicate-request threads. SQLite serializes writes, so this is a correctness/workflow campaign, not a claim of production throughput.

## Call your own running API and workers

```sh
python3 -m encounter_service.api --db /tmp/incidents.db --port 8000
# In another terminal, use explicit mock mode for free synthetic summaries:
SUMMARY_MOCK=1 python3 -m encounter_service.worker --db /tmp/incidents.db
# Then send the dataset through HTTP:
python3 tools/incident_e2e.py load --api http://127.0.0.1:8000
```

`load` only sends and checks ingestion responses; it does not start processes or validate summary/provider failure modes. The ordinary SUMMARY_MOCK worker always succeeds and does not interpret the scenario markers. Use `run` for the controllable provider and full terminal-result verification. Do not point this synthetic campaign at a paid provider unless you intentionally accept the invocation cost.

To inspect a retained run, start the ordinary API with its retained `encounters.db`, then call:

```sh
curl http://127.0.0.1:8000/encounters/inc-0000-pat-00-type-0/summary
curl http://127.0.0.1:8000/encounters/inc-0000-pat-00-type-0/versions/12
```

## Coverage boundaries

The live campaign verifies a shared populated database, actual HTTP transport, multiple processes, duplicates, skipped versions, stale updates, conflicts, retries and exact-version results. It restarts a worker while the run may have a backlog; whether that worker was inside an AI call depends on scheduling. It does **not** claim every failure ordering is forced in this bulk run. In particular, short delays do not guarantee an out-of-order finish, and obsolete work may be superseded before a worker starts it.

Run the focused tests and earlier scenario runner for deterministic lease fencing, transaction rollback, result ordering, SLA boundaries, six-attempt exhaustion, redrive and a simulated twenty-minute outage:

```sh
python3 -m unittest discover -s tests -v
python3 tools/scenario_cases.py run
```

The live bulk provider uses one transient error followed by success or an immediate permanent error. It does not simulate a twenty-minute real outage, power loss, or production AI quality. No real AI calls are made by `run`.

## Verified full run

The full run completed successfully: 1,000 incidents, 3,000 patients, 9,000 encounters and 90,000 HTTP update requests. Final current states were 6,750 ready and 2,250 expected permanent failures. One SLA breach was observed and recovered successfully after the worker restart. There were 14,437 synthetic provider invocations (including historical versions and retries). Total harness duration was 108.887 seconds on this machine; this is not a production SLA or throughput guarantee. See `incident-e2e-results.json` for the retained summary and `incident-e2e-full/` for the local database and process log.

## Run this campaign using a real LLM

The runner now supports explicit paid LLM mode. Export `OPENAI_API_KEY` securely in the same terminal, then start small:

```sh
LLM_MODEL=gpt-6.1-terra SUMMARY_MAX_WORDS=100 \
python3.14 tools/incident_e2e.py run --use-llm --limit 9 --workers 2 \
  --output test_data/incident-e2e-llm-small
```

`--limit 9` selects the first nine encounters (one incident, three patients and all three types). It still sends ten update requests per encounter, including version 12 and 13, duplicates, stale requests and conflicts. Both accepted versions can incur a generation charge, and retry/lease recovery may incur further charges. The runner does not ask for a key interactively or load `.env`; it validates the exported key/configuration before creating the output directory or starting workers.

To run all 9,000 encounters with the LLM, explicitly set `--limit 9000`:

```sh
LLM_MODEL=gpt-6.1-terra SUMMARY_MAX_WORDS=100 \
python3.14 tools/incident_e2e.py run --use-llm --limit 9000 --workers 4 \
  --output test_data/incident-e2e-llm-full
```

In LLM mode the runner sets `USE_LLM=1`, removes inherited SUMMARY_URL/SUMMARY_MOCK from child process environments, and does not start the synthetic provider. The `normal/transient/permanent/slow` labels are dataset annotations, not live OpenAI fault instructions. All current jobs are expected to become ready; real model/API failures fail the campaign and are retained in its database/logs. Deliberate worker kill/restart is disabled to avoid intentionally repeating paid calls.

Verification checks current version 13, fixed patient/type, nonempty summary text, rejection of the synthetic placeholder/hash outputs, and SUMMARY_MAX_WORDS (default 100). It cannot validate exact text hashes, clinical factuality or model attribution from the text alone. The runner controls adapter selection, and worker startup logs show adapter/model. Reports record provider_mode, model, word limit and verification scope; provider_invocations is null for OpenAI because no billing count is measured. A failed campaign can be inspected through its retained database and process.log.

Without `--use-llm`, `run` continues to force synthetic mode even if USE_LLM=1 is exported. Use the flag to make the paid choice explicit. `--limit` also works with `load`; `--use-llm` does not, because `load` uses workers you started separately. Use Python 3.11+ (`python3.14` above matches the user's local interpreter).
