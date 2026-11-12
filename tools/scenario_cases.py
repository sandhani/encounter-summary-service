"""Generate and verify synthetic encounter orchestration scenarios (no AI calls)."""
import argparse
import concurrent.futures
import copy
import json
from pathlib import Path
import random
import sqlite3
import sys
import tempfile
import threading
import time
from collections import Counter

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from encounter_service.store import Conflict, Store
from encounter_service.worker import tick

SCENARIOS = ('concurrent_duplicate', 'version_gap', 'restart_after_commit',
             'worker_interruption', 'newer_during_processing', 'out_of_order_results',
             'outage_exhaustion_redrive', 'identity_conflict', 'payload_conflict',
             'event_id_conflict', 'transaction_rollback', 'validation')


def generate(path, count, seed):
    rng = random.Random(seed)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w') as out:
        for i in range(count):
            version = rng.randint(3, 100000)
            # Every hundredth case exercises a multi-hundred-line full transcript.
            lines = 300 if i % 100 == 0 else rng.randint(2, 8)
            transcript = '\n'.join(f'{"Nurse" if n % 2 == 0 else "Patient"}: Synthetic dialogue {i}, line {n}; feeling better. ठीक है.' for n in range(lines))
            event = dict(event_id=f'synthetic-event-{i}', encounter_id=f'synthetic-enc-{i}',
                         patient_id=f'synthetic-pat-{i}', encounter_type=rng.choice(
                             ['Appointment', 'MedicationRefill', 'TelephoneTriage']),
                         version=version, payload={'transcription': transcript})
            case = dict(case_id=i, scenario=SCENARIOS[i % len(SCENARIOS)], seed=seed,
                        event=event, expected={'current_version': version, 'status': 'ready'})
            if case['scenario'] in ('newer_during_processing', 'out_of_order_results'):
                case['expected']['current_version'] += 1
            if case['scenario'] == 'validation':
                case['expected'] = {'rejected': True, 'rows': 0}
            out.write(json.dumps(case, ensure_ascii=False) + '\n')
    return count


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def exercise(case, path):
    now = [1000.0]
    store = Store(str(path), lambda: now[0])
    e = case['event']
    enc, v, scenario = e['encounter_id'], e['version'], case['scenario']

    def changed(version, suffix):
        x = copy.deepcopy(e)
        x.update(version=version, event_id=e['event_id'] + suffix)
        x['payload']['transcription'] += f'\nPatient: Synthetic snapshot version {version}.'
        return x

    def ready(job=None):
        job = store.claim() if job is None else job
        check(job is not None, 'expected a claimable job')
        check(store.complete(job, 'Synthetic summary'), 'completion rejected')

    if scenario == 'validation':
        variants = [('version', True), ('version', 0), ('patient_id', ''),
                    ('encounter_type', 'Invalid'), ('payload', {'transcription': ' '})]
        for key, value in variants:
            invalid = copy.deepcopy(e)
            invalid[key] = value
            try:
                store.ingest(invalid)
            except ValueError:
                pass
            else:
                raise AssertionError('invalid event was accepted')
        with store.tx() as db:
            check(all(db.execute(f'SELECT count(*) FROM {table}').fetchone()[0] == 0
                      for table in ('events', 'versions', 'jobs', 'encounters')), 'validation wrote rows')
        return

    if scenario == 'concurrent_duplicate':
        barrier = threading.Barrier(4)
        def deliver(_):
            barrier.wait(timeout=30)
            return store.ingest(e)['outcome']
        with concurrent.futures.ThreadPoolExecutor(4) as pool:
            outcomes = list(pool.map(deliver, range(4)))
        check(Counter(outcomes) == {'accepted_current': 1, 'duplicate': 3}, 'duplicate race')
        alias = copy.deepcopy(e)
        alias['event_id'] += '-alias'
        check(store.ingest(alias)['outcome'] == 'duplicate', 'logical duplicate not detected')
        with concurrent.futures.ThreadPoolExecutor(4) as pool:
            claims = [j for j in pool.map(lambda _: store.claim(), range(4)) if j]
        check(len(claims) == 1, 'multiple worker claims')
        ready(claims[0])
    elif scenario == 'version_gap':
        store.ingest(changed(v-2, '-old'))
        store.ingest(e)
        check(store.ingest(changed(v-1, '-late'))['outcome'] == 'stale', 'late version advanced')
        job = store.claim()
        check(job['version'] == v and job['transcription'] == e['payload']['transcription'], 'wrong snapshot')
        ready(job)
        check(store.history(enc, v-2)['status'] == 'superseded', 'obsolete queue not skipped')
    elif scenario == 'transaction_rollback':
        with store.tx() as db:
            db.execute("CREATE TRIGGER fail_job BEFORE INSERT ON jobs BEGIN SELECT RAISE(ABORT,'injected'); END")
        try:
            store.ingest(e)
        except sqlite3.IntegrityError:
            pass
        else:
            raise AssertionError('injected transaction failure did not fire')
        with store.tx() as db:
            check(all(db.execute(f'SELECT count(*) FROM {t}').fetchone()[0] == 0
                      for t in ('events', 'versions', 'jobs', 'encounters')), 'partial transaction persisted')
            db.execute('DROP TRIGGER fail_job')
        store.ingest(e)
        ready()
    else:
        store.ingest(e)
        if scenario == 'restart_after_commit':
            store = Store(str(path), lambda: now[0])
            ready()
            store = Store(str(path), lambda: now[0])
            check(store.claim() is None, 'ready result replayed after restart')
        elif scenario == 'worker_interruption':
            old = store.claim()
            now[0] += 45
            replacement = store.claim()
            check(replacement and replacement['token'] != old['token'], 'lease not recovered')
            check(not store.complete(old, 'obsolete'), 'unfenced old worker')
            ready(replacement)
            check(not store.complete(replacement, 'duplicate completion'), 'completion replay accepted')
        elif scenario in ('newer_during_processing', 'out_of_order_results'):
            old = store.claim()
            newer = changed(v+1, '-new')
            store.ingest(newer)
            check(store.progress(enc)['summary'] is None, 'old summary leaked')
            new = store.claim()
            check(new['transcription'] == newer['payload']['transcription'], 'wrong new input')
            if scenario == 'newer_during_processing':
                store.complete(old, 'Old summary')
                check(store.progress(enc)['status'] == 'processing', 'obsolete result became current')
                ready(new)
            else:
                ready(new)
                store.complete(old, 'Old summary')
            check(store.progress(enc)['summary'] == 'Synthetic summary', 'result order regressed current')
        elif scenario == 'outage_exhaustion_redrive':
            def unavailable(_):
                raise TimeoutError('synthetic timeout')
            for attempt in range(1, 7):
                check(tick(store, unavailable), 'missing retry job')
                result = store.progress(enc)
                check(result['attempts'] == attempt, 'incorrect attempt budget')
                if attempt < 6:
                    delay = result['next_attempt_at'] - now[0]
                    check(2**(attempt-1) <= delay <= 2**attempt, 'backoff outside bounds')
                    check(store.claim() is None, 'retry before due')
                    now[0] = result['next_attempt_at']
            check(store.progress(enc)['status'] == 'failed', 'outage did not exhaust')
            now[0] = 2200  # Twenty minutes since acceptance, without wall-clock sleep.
            check(store.claim() is None, 'failed job automatically retried')
            check(store.progress(enc)['sla_breached'], 'SLA breach hidden')
            check(store.redrive(enc, v), 'failed current redrive rejected')
            check(not store.redrive(enc, v), 'duplicate redrive accepted')
            ready()
        elif scenario in ('identity_conflict', 'payload_conflict', 'event_id_conflict'):
            bad = copy.deepcopy(e)
            if scenario == 'identity_conflict':
                bad['event_id'] += '-bad'
                if (case['case_id'] // len(SCENARIOS)) % 2:
                    bad['patient_id'] += '-other'
                else:
                    bad['encounter_type'] = 'Appointment' if e['encounter_type'] != 'Appointment' else 'MedicationRefill'
            elif scenario == 'payload_conflict':
                bad['event_id'] += '-bad'
                bad['payload']['transcription'] += ' conflicting content'
            else:
                bad['encounter_id'] += '-other'
            try:
                store.ingest(bad)
            except Conflict:
                pass
            else:
                raise AssertionError('conflict was accepted')
            with store.tx() as db:
                check(all(db.execute(f'SELECT count(*) FROM {t}').fetchone()[0] == 1
                          for t in ('events', 'versions', 'jobs', 'encounters')), 'conflict wrote partial state')
            ready()
        else:
            raise ValueError('unknown scenario')

    result = store.progress(enc)
    for key, value in case['expected'].items():
        check(result[key.replace('current_version', 'version')] == value, f'expected {key}={value}')
    check(result['patient_id'] == e['patient_id'] and result['encounter_type'] == e['encounter_type'], 'identity changed')
    check(store.claim() is None, 'unexpected remaining work')


def run(dataset, report_path, limit=None):
    started = time.monotonic()
    totals, failures, types = Counter(), [], Counter()
    with tempfile.TemporaryDirectory(prefix='encounter-cases-') as root, dataset.open() as source:
        for index, line in enumerate(source):
            if limit is not None and index >= limit:
                break
            case = json.loads(line)
            try:
                # One isolated database per case, deleted immediately after execution.
                with tempfile.TemporaryDirectory(dir=root) as folder:
                    exercise(case, Path(folder) / 'case.db')
            except Exception as exc:
                failures.append({'case_id': case['case_id'], 'scenario': case['scenario'],
                                 'error_type': type(exc).__name__, 'message': str(exc)})
            totals[case['scenario']] += 1
            types[case['event']['encounter_type']] += 1
            if (index+1) % 1000 == 0:
                print(f'Executed {index+1} cases; failures={len(failures)}', flush=True)
    count = sum(totals.values())
    report = dict(total=count, passed=count-len(failures), failed=len(failures),
                  duration_seconds=round(time.monotonic()-started, 3),
                  scenarios=dict(totals), encounter_types=dict(types), failures=failures,
                  scope='Synthetic store/worker orchestration; no live HTTP, AI calls or host kills')
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps({k: v for k, v in report.items() if k != 'failures'}, indent=2))
    return 1 if failures or count == 0 else 0


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('command', choices=('generate', 'run'))
    parser.add_argument('--data', type=Path, default=Path('test_data/scenarios-10000.jsonl'))
    parser.add_argument('--count', type=int, default=10000)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--limit', type=int)
    parser.add_argument('--report', type=Path, default=Path('test_data/results.json'))
    args = parser.parse_args()
    if args.count < 1 or (args.limit is not None and args.limit < 1):
        parser.error('count and limit must be positive')
    if args.command == 'generate':
        print(f'Generated {generate(args.data, args.count, args.seed)} cases at {args.data}')
        return 0
    return run(args.data, args.report, args.limit)


if __name__ == '__main__':
    raise SystemExit(main())
