import json
import tempfile
import unittest
from unittest.mock import Mock

from encounter_service.store import Store
from encounter_service.worker import tick
from test_store import event


class WorkflowLoggingTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.now = 100
        self.store = Store(self.tmp.name+'/db', lambda: self.now)
        self.store.ingest(event())

    def records(self, captured):
        return [json.loads(r.getMessage()) for r in captured.records]

    def test_success_logs_order_and_commit_without_content(self):
        with self.assertLogs('encounter_service.workflow', level='INFO') as logs:
            tick(self.store, Mock(return_value='private summary'))
        rows = self.records(logs)
        self.assertEqual([r['event'] for r in rows], [
            'job_claimed', 'summary_generation_started',
            'summary_generation_finished', 'job_state_committed'])
        self.assertEqual(len({r['claim_id'] for r in rows}), 1)
        self.assertEqual(rows[-1]['state'], 'ready')
        self.assertTrue(rows[-1]['summary_saved'])
        self.assertNotIn('private summary', str(rows))
        self.assertNotIn('Patient: version', str(rows))
        self.assertEqual(self.store.progress('enc')['status'], 'ready')

    def test_failure_logs_retry_and_sanitizes_exception(self):
        with self.assertLogs('encounter_service.workflow', level='INFO') as logs:
            tick(self.store, Mock(side_effect=TimeoutError('private provider details')))
        rows = self.records(logs)
        self.assertEqual(rows[-1]['state'], 'queued')
        self.assertGreater(rows[-1]['next_attempt_at'], self.now)
        self.assertEqual(rows[-1]['error'], 'provider_call_failed')
        self.assertNotIn('private provider details', str(rows))

    def test_fenced_completion_does_not_log_commit(self):
        old = self.store.claim()
        self.now += 45
        self.store.claim()
        with self.assertLogs('encounter_service.workflow', level='INFO') as logs:
            self.assertFalse(self.store.complete(old, 'old'))
        self.assertEqual([r['event'] for r in self.records(logs)], ['completion_rejected'])

    def test_historical_result_is_explicitly_marked(self):
        old = self.store.claim()
        self.store.ingest(event(2))
        with self.assertLogs('encounter_service.workflow', level='INFO') as logs:
            self.store.complete(old, 'old')
        row = self.records(logs)[0]
        self.assertFalse(row['is_current'])
        self.assertEqual(row['current_version'], 2)
        self.assertIsNone(self.store.progress('enc')['summary'])
