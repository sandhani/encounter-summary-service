"""Failure-path tests using isolated databases and deterministic time."""
import concurrent.futures
import tempfile
import sqlite3
import unittest
from unittest.mock import Mock, patch
from urllib.error import HTTPError

from encounter_service.store import Store
from encounter_service.worker import tick
from test_store import event


class RobustnessTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.now = 100.0
        self.store = Store(self.tmp.name + '/db', lambda: self.now)

    def test_invalid_inputs_leave_no_records(self):
        cases = [('version', v) for v in (0, -1, True, 1.5, '1', None)]
        cases += [(key, value) for key in ('event_id', 'patient_id', 'encounter_id')
                  for value in ('', '  ', None, 123)]
        cases += [('encounter_type', 'Unknown'), ('payload', None),
                  ('payload', {}), ('payload', {'transcription': '  '}),
                  ('payload', {'transcription': 123})]
        for key, value in cases:
            with self.subTest(key=key, value=value):
                data = event()
                data[key] = value
                with self.assertRaises(ValueError):
                    self.store.ingest(data)
        with self.store.tx() as db:
            for table in ('encounters', 'versions', 'events', 'jobs'):
                self.assertEqual(db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0], 0)

    def test_concurrent_different_versions_converge_to_maximum(self):
        with concurrent.futures.ThreadPoolExecutor(8) as pool:
            list(pool.map(lambda v: self.store.ingest(event(v)), (7, 2, 12, 1, 9, 4, 11, 3)))
        self.assertEqual(self.store.progress('enc')['version'], 12)
        job = self.store.claim()
        self.assertEqual(job['version'], 12)
        self.assertIsNone(self.store.claim())

    def test_concurrent_workers_only_one_claims_job(self):
        self.store.ingest(event())
        with concurrent.futures.ThreadPoolExecutor(8) as pool:
            jobs = list(pool.map(lambda _: self.store.claim(), range(16)))
        self.assertEqual(sum(job is not None for job in jobs), 1)
        self.assertEqual(self.store.progress('enc')['attempts'], 1)

    def test_stale_receipt_is_deduplicated_without_job(self):
        self.store.ingest(event(12))
        self.assertEqual(self.store.ingest(event(11))['outcome'], 'stale')
        self.assertEqual(self.store.ingest(event(11, eid='alias'))['outcome'], 'duplicate')
        with self.assertRaises(KeyError):
            self.store.history('enc', 11)
        self.assertEqual(self.store.progress('enc')['version'], 12)

    def test_independent_encounters_do_not_share_version_sequence(self):
        self.store.ingest(event(100))
        other = event(1, eid='other-event')
        other['encounter_id'] = 'other'
        other['patient_id'] = 'other-patient'
        self.store.ingest(other)
        self.assertEqual(self.store.progress('other')['version'], 1)
        self.assertEqual(self.store.progress('enc')['version'], 100)

    def test_long_unicode_snapshot_is_passed_unchanged(self):
        transcript = 'Nurse: कैसे हैं?\nPatient: Feeling better. 🩺\n' * 3000
        data = event()
        data['payload']['transcription'] = transcript
        self.store.ingest(data)
        provider = Mock(return_value='summary')
        self.assertTrue(tick(self.store, provider))
        provider.assert_called_once_with(transcript)
        self.assertFalse(tick(self.store, provider))
        self.assertEqual(self.store.progress('enc')['summary'], 'summary')

    def test_superseded_queue_never_calls_provider(self):
        self.store.ingest(event(1))
        self.store.ingest(event(2))
        provider = Mock(return_value='new summary')
        tick(self.store, provider)
        provider.assert_called_once_with('Patient: version 2')
        self.assertEqual(self.store.history('enc', 1)['status'], 'superseded')

    def test_expired_obsolete_work_is_not_retried(self):
        self.store.ingest(event())
        old = self.store.claim()
        self.store.ingest(event(2))
        self.now += 45
        self.assertEqual(self.store.claim()['version'], 2)
        self.assertFalse(self.store.complete(old, 'obsolete'))
        self.assertEqual(self.store.history('enc', 1)['status'], 'superseded')

    def test_abandoned_claims_exhaust_attempt_budget(self):
        self.store.ingest(event())
        for _ in range(6):
            self.assertIsNotNone(self.store.claim())
            self.now += 45
        self.assertIsNone(self.store.claim())
        result = self.store.progress('enc')
        self.assertEqual(result['status'], 'failed')
        self.assertEqual(result['error'], 'attempt_budget_exhausted')
        self.assertEqual(result['attempts'], 6)

    def test_retry_waits_until_due_and_keeps_original_start(self):
        self.store.ingest(event())
        job = self.store.claim()
        with patch('encounter_service.store.random.uniform', return_value=1):
            self.store.complete(job, error='temporary')
        self.assertEqual(self.store.progress('enc')['next_attempt_at'], 102)
        self.now = 101.999
        self.assertIsNone(self.store.claim())
        self.now = 102
        self.assertIsNotNone(self.store.claim())
        self.assertEqual(self.store.progress('enc')['started_at'], 100)

    def test_http_provider_errors_have_correct_retry_policy(self):
        for code, expected in ((400, 'failed'), (401, 'failed'), (408, 'queued'),
                               (429, 'queued'), (500, 'queued'), (503, 'queued')):
            with self.subTest(code=code):
                data = event(eid=f'event-{code}')
                data['encounter_id'] = f'enc-{code}'
                self.store.ingest(data)
                provider = Mock(side_effect=HTTPError('https://example.invalid', code, 'sensitive text', {}, None))
                tick(self.store, provider)
                result = self.store.progress(data['encounter_id'])
                self.assertEqual(result['status'], expected)
                self.assertEqual(result['error'], f'provider_http_{code}')
                self.assertIsNone(result['summary'])

    def test_provider_timeout_does_not_persist_sensitive_error(self):
        self.store.ingest(event())
        tick(self.store, Mock(side_effect=TimeoutError('private transcription')))
        result = self.store.progress('enc')
        self.assertEqual(result['status'], 'queued')
        self.assertEqual(result['error'], 'provider_call_failed')
        self.assertNotIn('private transcription', str(result))

    def test_invalid_provider_response_is_terminal(self):
        self.store.ingest(event())
        tick(self.store, Mock(side_effect=ValueError('bad provider JSON')))
        self.assertEqual(self.store.progress('enc')['status'], 'failed')
        self.assertIsNone(self.store.claim())

    def test_redrive_rejects_active_ready_missing_and_obsolete_jobs(self):
        self.assertFalse(self.store.redrive('missing', 1))
        self.store.ingest(event())
        self.assertFalse(self.store.redrive('enc', 1))
        job = self.store.claim()
        self.assertFalse(self.store.redrive('enc', 1))
        self.store.complete(job, error='permanent', permanent=True)
        self.store.ingest(event(2))
        self.assertFalse(self.store.redrive('enc', 1))
        job = self.store.claim()
        self.store.complete(job, 'ready')
        self.assertFalse(self.store.redrive('enc', 2))

    def test_sla_boundary_and_completed_latency_do_not_drift(self):
        self.store.ingest(event())
        self.now = 110
        self.assertFalse(self.store.progress('enc')['sla_breached'])
        self.now = 110.001
        self.assertTrue(self.store.progress('enc')['sla_breached'])
        job = self.store.claim()
        self.store.complete(job, 'late')
        self.now = 1000
        self.assertTrue(self.store.progress('enc')['sla_breached'])
        self.store.ingest(event(2))
        job = self.store.claim()
        self.store.complete(job, 'fast')
        self.now += 1000
        self.assertFalse(self.store.progress('enc')['sla_breached'])

    def test_transaction_rollback_does_not_leave_partial_ingestion(self):
        with self.store.tx() as db:
            db.execute("""CREATE TRIGGER fail_job BEFORE INSERT ON jobs
                          BEGIN SELECT RAISE(ABORT, 'injected crash'); END""")
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.ingest(event())
        with self.store.tx() as db:
            for table in ('encounters', 'versions', 'events', 'jobs'):
                self.assertEqual(db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0], 0)
            db.execute('DROP TRIGGER fail_job')
        self.assertEqual(self.store.ingest(event())['outcome'], 'accepted_current')
